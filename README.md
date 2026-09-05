# WalTier

WalTier is an embeddable tiered write-ahead log for Rust: memory, local disk, and S3, coordinated by nothing but S3 conditional writes. You get a durable, totally ordered log per resource with user-defined compaction and read-only replicas. No consensus, no leases, no leader election — the etag chain is the arbiter.

It's the generalized core of the pattern in [Cursor's "git at any scale"](https://cursor.com/blog/git-at-any-scale), with nothing git-specific in it.

<!-- TOC -->

- [Quick start](#quick-start)
- [How does WalTier work?](#how-does-waltier-work)
  - [Writes and conflicts](#writes-and-conflicts)
  - [Batch reconciliation](#batch-reconciliation)
  - [Compaction](#compaction)
  - [Replicas](#replicas)
  - [The tiers](#the-tiers)
- [Failure semantics](#failure-semantics)
- [Application contract](#application-contract)
- [API migration](#api-migration)
- [Resource limits](#resource-limits)
- [Object stores](#object-stores)
- [Testing](#testing)
- [Benchmarks](#benchmarks)
- [What WalTier doesn't do](#what-waltier-doesnt-do)

<!-- /TOC -->

## Quick start

Implement `WalApp` — how entries build your state, how state folds into snapshots, and when to fold:

```rust
use waltier::{Entry, Lsn, Options, Reconcile, WalApp, WalError, WalStats, WalTier};

struct Kv;

impl WalApp for Kv {
    type State = BTreeMap<String, String>;

    fn init(&self) -> Self::State { BTreeMap::new() }
    fn apply(&self, state: &mut Self::State, lsn: Lsn, entry: &[u8]) { /* fold entry into state */ }
    fn restore(&self, snapshot: &[u8]) -> Result<Self::State, WalError> { /* rebuild from snapshot */ }
    fn compact(&self, base: Option<&[u8]>, entries: &[Entry]) -> Result<Vec<u8>, WalError> { /* fold into new snapshot */ }
    fn should_compact(&self, stats: &WalStats) -> bool { stats.live_entries >= 1024 }
    fn reconcile(&self, state: &Self::State, pending: &[u8]) -> Reconcile { Reconcile::Retry }
}

let mut wal = WalTier::open(store, Kv, Options::new("/var/lib/mylog"))?;
let lsn = wal.write(b"set a 1".to_vec())?; // LSNs are assigned for you: 0, 1, 2, ...
```

`cargo run --example kv` is a complete last-write-wins KV store against the filesystem-backed store.

## How does WalTier work?

The whole log is **one small S3 object** (the WAL image), rewritten wholesale on every write with a compare-and-swap on its etag:

```
{prefix}wal            snapshot pointer + live entries (CAS'd on every write)
{prefix}snap/<lsn>-<id> immutable snapshots published with create-only PUT
```

The image is its own manifest, so bootstrap is one GET. Entries are small metadata — every write re-uploads the whole image, so you write large payloads as separate immutable objects first (through the same `ObjectStore`), then append an entry that references them.

### Writes and conflicts

`write(entry)` assigns the next LSN and CASes the new image. The accepted conditional PUT is the commit point. If another instance wrote first, WalTier refreshes the committed state and calls `reconcile_batch` with the entire pending batch:

- `ReconcileBatch::Retry` — retry the unchanged batch against the new tip.
- `ReconcileBatch::Replace(entries)` — use this complete replacement batch.
- `ReconcileBatch::Abort` (default) — return `WriteError` with `source: WalError::ReconcileAborted` and the pending entries.

`write_batch` commits all entries in one CAS and returns the contiguous LSN range of the **final accepted batch**. A replacement can change the count; an empty batch or empty replacement is a no-op returning an empty range. `write` uses the same callback but rejects zero or multiple replacement entries with `InvalidReplacement`, before another CAS, so its single returned LSN is always meaningful. The legacy `reconcile` callback remains the default adapter for independent entries; every entry sees the same committed state, without tentative effects of preceding batch entries.

Every append failure returns `WriteError { entries, source, outcome }`. `entries` preserves the final attempted batch, including replacements rejected by a resource limit. `source` distinguishes an application abort, exhausted contention budget, invalid input, and storage failure. `outcome` is `NotApplied` unless a WAL PUT may have landed. A failed refresh GET after a rejected CAS is still `NotApplied`, regardless of the backend error's default classification. No callback runs after the final allowed CAS attempt, and uncertain PUTs are never automatically retried.

Commits are serialized by the etag chain at roughly one per store round trip; batching amortizes that request over many entries. `cargo run --release --example group_commit` demonstrates batching.

### Batch reconciliation

Dependent operations must be rebuilt together. For example, allocating two IDs against `next_id = 7` should produce `[7, 8]`; if another writer allocates `7`, rebuild the batch as `[8, 9]`. Independent per-entry callbacks would both see `next_id = 8` and could both allocate `8`.

```rust
fn reconcile_batch(&self, state: &Self::State, pending: &[Vec<u8>]) -> ReconcileBatch {
    let first = state.next_id;
    ReconcileBatch::Replace(
        (0..pending.len()).map(|i| (first + i as u64).to_le_bytes().to_vec()).collect()
    )
}
```

This example treats each pending entry as a request for one allocation. Real commands should preserve their request IDs and other intent when rewriting. `cargo run --example batch` runs the complete two-writer allocation example. The callback returns a complete candidate without mutating committed state or requiring `State: Clone`.

### Compaction

When `should_compact` fires, a background thread runs `compact` and uploads an immutable snapshot. The fold installs on the writer's **next PUT** or an explicit `flush()`, sharing the append's CAS. Snapshot construction and upload run in the background; starting compaction still copies live entries, and installing a fold currently performs cache and cleanup work before acknowledgement. A fold superseded by a remote compaction is discarded. Replicas never compact.

Maintenance results are explicit:

- `compaction_status()` polls without waiting: `Idle`, `Running`, `Ready`, `Installed`, or `Superseded`, or `Err(Compaction(...))`.
- `wait_for_compaction()` waits for running work and returns its state or failure; `Ready` still needs installation.
- `flush()` waits for running work and installs any ready fold. An exhausted CAS budget returns `Contention { operation: "flush", .. }` and keeps the fold available for retry.
- A compactor failure or panic stays visible to wait, flush, and close. Automatic triggers do not clear it. Call `compact_now()` to explicitly retry, or `take_compaction_error()` to acknowledge and abandon failed maintenance.
- `close()` waits and flushes, returning the final status or error. It consumes the handle even on failure. Call `flush(&mut self)` first when you want to retain the handle for retry. Failed maintenance never rolls back acknowledged appends; abandoned snapshots may remain as offline-collectable orphans.

```rust
wal.compact_now();
let status = wal.wait_for_compaction()?; // Ready, or an explicit failure
println!("compaction: {status:?}");
println!("flush: {:?}", wal.flush()?);  // Installed or Superseded when work finished
wal.close()?;
```

Plain drop does not wait: a detached compactor may finish uploading a snapshot, but cannot install a WAL reference. Drain compaction before an offline sweep; dropping a handle alone does not prove its upload has stopped. Thread creation failure is recorded as a maintenance error, preserving the success of any append that triggered it.

### Replicas

`Replica::open` bootstraps from the same objects and `refresh()` polls with a conditional GET — a cheap 304 when nothing changed. A replica that fell behind a fold rebuilds from the snapshot via your `restore`. `state()` is the last observed committed prefix, not a fresh or linearizable read; call `refresh()` to observe a newer prefix. Writers have the same freshness rule. Replicas never write.

### The tiers

- **Memory** — your `State`, built by applying entries in LSN order
- **Local disk** — a warm-start cache of the image and snapshot. Records bind the backend namespace and complete object key; the image is also etag-validated on open. Checksums make damaged files read as cache misses. Reusing a cache directory across resources is safe, though competing cache users can evict each other’s files. Old cache formats are automatically ignored.
- **S3** — the durable copy and the arbiter of truth

## Failure semantics

- CAS prevents a stale writer from overwriting newer history. It does not grant exclusive ownership: multiple writers can successfully alternate commits after reconciling.
- `WriteError.outcome == MutationOutcome::Unknown` can hide a PUT that landed, such as a timeout after S3 applied it. The error retains the candidate entries; WalTier returns without locally applying or retrying that candidate. Refresh, then inspect application request IDs before deciding whether to resubmit. Refresh can discover the uncertain entries. Caller resubmission can append duplicates, so use stable request IDs or idempotent commands; this is not exactly-once delivery.
- Snapshots use random 128-bit IDs and create-only PUTs. A key collision is retried without overwriting the existing object. Failed uploads can leave possible orphan objects; the compaction error names the candidate key. An ambiguous WAL installation never authorizes deleting its candidate snapshot.
- Orphan sweeping is supported **only offline**: stop new writes, drain or terminate every writer and compaction/publication request, and discard all old handles so no pending fold can later install. After that, reread the authoritative WAL, keep its referenced snapshot, delete other objects under that WAL’s `snap/` prefix, and reopen writers. A pending upload or fold is not garbage merely because the current WAL does not reference it. Rereading the WAL during an online sweep, or adding a minimum object age, does not make that sweep safe. Do not use age-based expiration: it can delete the live snapshot of an idle log.
- Normal compaction may delete a snapshot proven superseded by an accepted WAL transition. A reader racing that deletion rereads the WAL and reconstructs from its newer snapshot. Repeated changing references exhaust a `Contention` budget; two authoritative reads of the same version referencing a missing snapshot return `Corrupt`. Local cache cleanup only removes disposable files.

## Application contract

`init`, `apply`, `restore`, `compact`, and reconciliation must be deterministic and have no external side effects. Restoring a compacted prefix must produce the same state and future replay behavior as applying that prefix from `init`. `apply` runs in LSN order within each reconstruction; reopening and snapshot reconstruction can replay LSNs. It is not an exactly-once hook for email, billing, or other external effects.

Callbacks must return without panicking. A foreground callback panic can happen after the durable CAS, so discard that handle and reopen to reconstruct state. Compactor errors and panics are maintenance failures and leave acknowledged history intact. The library does not validate application commands before their first CAS; validate them before calling `write` or `write_batch`.

## API migration

This is a breaking API update from 0.2, intended for 0.3. The `WTL1` object format is unchanged. Update append error handling from `WalError::Conflict { entries }` to `WriteError { entries, source, outcome }`; application rejection is `WalError::ReconcileAborted`, while retry exhaustion is `WalError::Contention`. Inspect `outcome` before resubmitting, and keep `entries` when propagating failures. Whole-batch overrides use `ReconcileBatch`; existing independent-entry `reconcile` implementations continue to work through its default adapter.

Replace boolean `wait_for_compaction()` checks with its `Result<CompactionStatus, WalError>`. `flush` and `close` also return a status, wait for running work, and surface failures. Handle a failed compactor explicitly instead of relying on automatic retry. See the object-store section for `StoreError` and filesystem-root migration.

## Resource limits

`Options` defaults to a **64 MiB encoded WAL image**, **1,000,000 live entries**, and **256 MiB snapshots**. Set `max_image_bytes`, `max_live_entries`, and `max_snapshot_bytes` for your workload. Image limits include framing and the snapshot reference, so leave room for a fold's reference as well as the live entries. Byte budgets are capped to `ObjectStore::max_object_bytes()` when the store advertises a smaller limit. Use compatible limits on every writer and replica; lowering a reader's limits below existing objects returns `LimitExceeded`.

Every candidate image is checked before CAS. Exceeding an image/count budget rejects the **whole batch**, does not apply any of that batch locally or change acknowledged history, and returns `WriteError` with a `LimitExceeded` source (a conflict refresh may already have advanced local state); it does not wait for a slow or failing compactor. Start or finish compaction, install its fold, and retry when space becomes available. Snapshots exceeding their budget are rejected before upload. These are acceptance/decoding budgets, not peak process-memory caps: S3 can buffer up to its own transport limit before the WAL applies a smaller limit, and FsStore/custom GETs return a fully allocated body. Application state, callback allocations, and caller-owned pending batches remain the application's responsibility. Cache reads enforce the configured byte budgets before loading the file body.

`WTL1` remains compatible. Lengths/counts and LSN arithmetic are checked; malformed stored images return `Corrupt` in debug and release. LSNs run from 0 through `u64::MAX - 1`, reserving `u64::MAX` as the terminal next-LSN value; further appends return `LsnExhausted`. Zero retry budgets, zero entry/snapshot budgets, and image budgets smaller than the empty 9-byte image are invalid options.

## Object stores

`ObjectStore` is the seam: conditional get, CAS put (`If-Match` / `If-None-Match: *`), plain put, delete.

- `S3Store` (default feature `s3`) — sync HTTP via `rusty-s3` + `ureq`. Needs S3 conditional writes, which general-purpose buckets support in all regions. `S3Store::new` keeps the existing `S3Config` API; `new_with_options(config, S3Options)` configures transport budgets.
- `MemoryStore` — an isolated in-memory resource for tests.
- `FsStore` — a development backend with one atomic file per object (validator plus data), nonaliasing object paths, and an OS lock held for the store’s lifetime. Share an `Arc<FsStore>`; a second independent open of the root is rejected, including from another process. It requires Rust 1.89 or later for standard-library file locking. It does not fsync files or directories and does not promise power-loss durability. Existing directories using the old data-plus-`.etag` layout are rejected: use a fresh root or export/reimport data with the old version before upgrading. The authoritative `WTL1` object format is unchanged.

Custom stores must implement atomic conditional replacement, coherent data/validator reads, and strong read-after-write behavior. Validators identify an object version, not a whole backend; identical content may repeat an ETag. Reserve WalTier’s WAL and snapshot keys from application mutations. Implement `cache_namespace()` with a stable identity for the backing resource to enable persistent caching, and forward it through wrappers. Its default `None` safely bypasses cached data; cache directory setup and stale-file cleanup may still occur. Built-in stores provide namespaces; S3 scopes them to endpoint, bucket, access-key identity, region, and addressing mode.

`S3Options` defaults to a 10-second connection timeout, a 60-second request deadline, and a 1 GiB maximum body for **both GET and PUT** (including application payload objects). `connect_timeout` is capped to `request_timeout`; request deadlines must be positive and below the 300-second signing TTL. The request deadline covers response headers, response bodies, and upload progress. The upload checks elapsed time between 8 KiB chunks. DNS resolution and an already executing transport/TLS call cannot be forcibly cancelled and may overrun the nominal deadline; this blocking API does not promise hard cancellation. Custom stores must implement their own time and allocation bounds, and user callbacks must return; WalTier cannot interrupt them. Transport tests use a local HTTP server, with no real S3 credentials or service access.

Storage failures expose `StoreError { message, operation, key, status, mutation_outcome }`; S3 preserves the operation, object key, and any received HTTP status. `MutationOutcome::Unknown` means a failed PUT or DELETE may have landed, including a successful PUT response missing its ETag. A conditional 409/412 returns `PreconditionFailed` so the WAL can refresh before retrying. Custom-store migration: replace `StoreError(message)` with `StoreError::new(message)`, whose default is conservatively **Unknown**; use `.not_applied()` only when the backend can prove it did not apply the mutation, and `.with_context(...)` to add operation/key/status. Forward `max_object_bytes()` through wrappers along with `cache_namespace()`.

## Testing

`cargo test` runs unit tests, integration tests (fencing, all reconcile modes, competing compactions, ambiguous failures), and **deterministic simulation tests**: seeded runs interleave writes, refreshes, compactions, and crash/reopen cycles across writers and replicas, with and without injected store faults. An oracle checks after every step that the committed history only grows, every instance's state is an exact prefix of it, and no snapshot object leaks or vanishes. A run is a pure function of its seed — reproduce with `DST_SEED=<seed>`, watch with `DST_TRACE=1`, scale with `DST_SEEDS` / `DST_STEPS`.

The `waltier::sim` module (feature `sim`, on by default) provides the seeded RNG and `SimStore` — fault injection and a latency model — and works just as well for testing apps built on WalTier.

## Benchmarks

`cargo bench` runs against a simulated S3 (sleeps RTT + bytes/bandwidth per op):

```
cargo bench --bench wal -- --rtt-ms 15 --mbps 100 --writes 200
```

It reports write latency percentiles and throughput, per-write cost growth without compaction, cold vs warm open, and replica poll cost. Commit latency is RTT-bound; with no store latency the library sustains ~70k writes/s on one thread.

## What WalTier doesn't do

- Route requests to the current writer — run one writer per log and let stale ones lose CASes.
- Retry a failed compactor automatically — inspect the failure and explicitly restart or acknowledge it.
- Async — the API is single-threaded and blocking; the one internal thread is the compactor.
