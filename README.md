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
  - [Cache policies](#cache-policies)
- [Failure semantics](#failure-semantics)
- [Application contract](#application-contract)
- [API migration](#api-migration)
- [Resource limits](#resource-limits)
- [Object stores](#object-stores)
- [Testing](#testing)
  - [Real S3 conformance](#real-s3-conformance)
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

Commits are serialized by the etag chain at roughly one per store round trip; batching amortizes that request over many entries. `cargo run --release --example group_commit` demonstrates batching with a bounded queue, maximum entry size, bounded producer receipt windows, and one result per submission after its batch succeeds. Its defaults bound queued payload lengths to 512 KiB plus a 256 KiB writer batch; channel/request overhead, Vec spare capacity, and producer-owned entries/receipts are additional. Dropping all submitters drains accepted work. An append failure is delivered to that batch's producers, stops new admissions, and returns queued entries as `NotApplied`; uncertain batches are not automatically retried. This example adapter uses independent entries with `Retry` reconciliation, so receipt-to-LSN mapping stays one-to-one.

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

When `should_compact` fires, a background thread runs `compact` and uploads an immutable snapshot. The fold installs on the writer's **next PUT** or an explicit `flush()`, sharing the append's CAS. Snapshot construction, upload, and snapshot-cache publication run on the compactor. A prepared fold's installing append makes one WAL CAS and queues obsolete snapshots without issuing remote DELETEs. Starting compaction still copies live entries; the small-metadata workload and measured costs are documented in [PERFORMANCE.md](PERFORMANCE.md). A fold superseded by a remote compaction is discarded. Replicas never compact.

Maintenance results are explicit:

- `compaction_status()` polls without waiting: `Idle`, `Running`, `Ready`, `Installed`, or `Superseded`, or `Err(Compaction(...))`.
- `wait_for_compaction()` waits for running work and returns its state or failure; `Ready` still needs installation.
- `flush()` waits for running work, installs any ready fold, checkpoints an `OnFlush` cache, and drains tracked cleanup. It returns `MaintenanceStatus { compaction, garbage }`. An exhausted CAS budget returns `Contention { operation: "flush", .. }` and keeps the fold available for retry.
- A compactor failure or panic stays visible to wait, flush, and close. Automatic triggers do not clear it. Call `compact_now()` to explicitly retry, or `take_compaction_error()` to acknowledge and abandon failed maintenance.
- `close()` waits and flushes, returning the same maintenance report or error. It consumes the handle even on failure. Call `flush(&mut self)` first when you want to retain the handle for retry. Failed maintenance never rolls back acknowledged appends; abandoned snapshots may remain as offline-collectable orphans.

```rust
wal.compact_now();
let status = wal.wait_for_compaction()?; // Ready, or an explicit failure
println!("compaction: {status:?}");
let report = wal.flush()?;
println!("fold: {:?}; cleanup: {:?}", report.compaction, report.garbage);
let closed = wal.close()?;
if closed.garbage.overflowed > 0 {
    // Record this debt for an offline sweep after every writer/upload stops.
}
```

`collect_garbage()` explicitly deletes queued snapshots proven obsolete by WAL transitions. `garbage_status()` reports the queued count, the last DELETE failure, and cumulative `overflowed` candidates. `max_pending_deletes` defaults to 128 identities; a full queue leaves safe orphan objects and records debt for an offline sweep. Duplicate overflow candidates may be counted more than once. This report covers one handle's observations, not a global orphan inventory, and successful close still reports any overflow debt. A failed DELETE remains queued for retry, including an uncertain DELETE result; flushing or closing surfaces that error even if its fold was already installed. Neither append nor refresh waits for remote cleanup, and cleanup failure cannot change an acknowledged append into a failed append.

Plain drop does not wait: a detached compactor may finish uploading a snapshot, but cannot install a WAL reference. Drain compaction before an offline sweep; dropping a handle alone does not prove its upload has stopped. Thread creation failure is recorded as a maintenance error, preserving the success of any append that triggered it.

### Replicas

`Replica::open` bootstraps from the same objects and `refresh()` polls with a conditional GET — a cheap 304 when nothing changed. A replica that fell behind a fold rebuilds from the snapshot via your `restore`. `state()` is the last observed committed prefix, not a fresh or linearizable read; call `refresh()` to observe a newer prefix. Writers have the same freshness rule. Replicas never write.

### The tiers

- **Memory** — your `State`, built by applying entries in LSN order
- **Local disk** — an optional warm-start cache of the image and snapshot. Records bind the backend namespace and complete object key; the image is also etag-validated on open. Checksums make damaged files read as cache misses. Reusing a cache directory across resources is safe, though competing cache users can evict each other’s files. Old cache formats are automatically ignored.
- **S3** — the durable copy and the arbiter of truth

### Cache policies

`Options::default()` disables filesystem caching. `Options::new(path)` enables `CachePolicy::EveryCommit`, which saves each observed WAL version. Set `options.cache_policy = CachePolicy::OnFlush` to checkpoint the WAL only on explicit `flush`, `close`, or `checkpoint_cache()`. Replicas can call `checkpoint_cache()` directly. Snapshot fetches and confirmed uploads populate the snapshot cache under either enabled policy; snapshot publication finishes on the compactor before it reports `Ready`.

Cache setup and writes are best effort: an unavailable directory does not prevent opening or writing the log. Disabled caches and stores without a known `cache_namespace()` perform no cache filesystem operations. A stale checkpoint is always ETag-validated against the authoritative store, and cache loss never changes acknowledged history. Cache files remain compatible with `WTC2`; streaming writes avoid concatenating whole image/snapshot bodies for framing and checksumming. Policy comparisons and their complete checkpoint costs are in [PERFORMANCE.md](PERFORMANCE.md).

## Failure semantics

- CAS prevents a stale writer from overwriting newer history. It does not grant exclusive ownership: multiple writers can successfully alternate commits after reconciling.
- `WriteError.outcome == MutationOutcome::Unknown` can hide a PUT that landed, such as a timeout after S3 applied it. The error retains the candidate entries; WalTier returns without locally applying or retrying that candidate. Refresh, then inspect application request IDs before deciding whether to resubmit. Refresh can discover the uncertain entries. Caller resubmission can append duplicates, so use stable request IDs or idempotent commands; this is not exactly-once delivery.
- Snapshots use random 128-bit IDs and create-only PUTs. A key collision is retried without overwriting the existing object. Failed uploads can leave possible orphan objects; the compaction error names the candidate key. An ambiguous WAL installation never authorizes deleting its candidate snapshot.
- Orphan sweeping is supported **only offline**: stop new writes, discard all old handles, and establish that every old backend mutation has finished or can no longer apply, including compaction uploads and uncertain WAL CAS requests. Client cancellation, thread join, drop, close, an `Unknown` timeout, or one fresh WAL GET does not by itself establish backend completion: a delayed CAS could still install its snapshot afterward. Do not sweep while that uncertainty remains. Once backend quiescence is established, reread the authoritative WAL, keep its referenced snapshot, delete other objects under that WAL’s `snap/` prefix, and reopen writers. A pending upload or fold is not garbage merely because the current WAL does not reference it. Rereading the WAL during an online sweep, or adding a minimum object age, does not make that sweep safe. Do not use age-based expiration: it can delete the live snapshot of an idle log.
- Explicit cleanup may delete a snapshot proven superseded by an accepted WAL transition. A reader racing that deletion rereads the WAL and reconstructs from its newer snapshot. Repeated changing references exhaust a `Contention` budget; two authoritative reads of the same version referencing a missing snapshot return `Corrupt`. Local cache cleanup only removes disposable files.

## Application contract

`init`, `apply`, `restore`, `compact`, and reconciliation must be deterministic and have no external side effects. Restoring a compacted prefix must produce the same state and future replay behavior as applying that prefix from `init`. `apply` runs in LSN order within each reconstruction; reopening and snapshot reconstruction can replay LSNs. It is not an exactly-once hook for email, billing, or other external effects.

Callbacks must return without panicking. A foreground callback panic can happen after the durable CAS, so discard that handle and reopen to reconstruct state. Compactor errors and panics are maintenance failures and leave acknowledged history intact. The library does not validate application commands before their first CAS; validate them before calling `write` or `write_batch`.

## API migration

Version 0.3 is a breaking API update from 0.2 and requires Rust 1.89 or later. The `WTL1` object format is unchanged. Update append error handling from `WalError::Conflict { entries }` to `WriteError { entries, source, outcome }`; application rejection is `WalError::ReconcileAborted`, while retry exhaustion is `WalError::Contention`. Inspect `outcome` before resubmitting, and keep `entries` when propagating failures. Whole-batch overrides use `ReconcileBatch`; existing independent-entry `reconcile` implementations continue to work through its default adapter.

Replace boolean `wait_for_compaction()` checks with its `Result<CompactionStatus, WalError>`. `flush` and `close` return `MaintenanceStatus { compaction, garbage }`, wait for running work, and surface compaction or cleanup failures. Inspect the garbage report for offline-sweep debt. Handle a failed compactor explicitly instead of relying on automatic retry. See the object-store section for `StoreError` and filesystem-root migration.

## Resource limits

`Options` defaults to a **64 MiB encoded WAL image**, **1,000,000 live entries**, and **256 MiB snapshots**. Set `max_image_bytes`, `max_live_entries`, and `max_snapshot_bytes` for your workload. Image limits include framing and the snapshot reference, so leave room for a fold's reference as well as the live entries. Byte budgets are capped to `ObjectStore::max_object_bytes()` when the store advertises a smaller limit. Use compatible limits on every writer and replica; lowering a reader's limits below existing objects returns `LimitExceeded`.

Every candidate image is checked before CAS. Exceeding an image/count budget rejects the **whole batch**, does not apply any of that batch locally or change acknowledged history, and returns `WriteError` with a `LimitExceeded` source (a conflict refresh may already have advanced local state); it does not wait for a slow or failing compactor. Start or finish compaction, install its fold, and retry when space becomes available. Snapshots exceeding their budget are rejected before upload. These are acceptance/decoding budgets, not peak process-memory caps: S3 can buffer up to its own transport limit before the WAL applies a smaller limit, and FsStore/custom GETs return a fully allocated body. Application state, callback allocations, and caller-owned pending batches remain the application's responsibility. Cache reads enforce the configured byte budgets before loading the file body.

`WTL1` remains compatible. Lengths/counts and LSN arithmetic are checked; malformed stored images return `Corrupt` in debug and release. LSNs run from 0 through `u64::MAX - 1`, reserving `u64::MAX` as the terminal next-LSN value; further appends return `LsnExhausted`. Zero retry budgets, zero entry/snapshot budgets, and image budgets smaller than the empty 9-byte image are invalid options.

## Object stores

`ObjectStore` is the seam: conditional get, CAS put (`If-Match` / `If-None-Match: *`), plain put, delete.

- `S3Store` (default feature `s3`) — sync HTTP via `rusty-s3` + `ureq`. Needs S3 conditional writes, which general-purpose buckets support in all regions. `S3Store::new` keeps the existing `S3Config` API; `new_with_options(config, S3Options)` configures transport budgets.
- `MemoryStore` — an isolated in-memory resource for tests.
- `FsStore` — a development backend with one atomic file per object (validator plus data), nonaliasing object paths, and an OS lock held for the store’s lifetime. Share an `Arc<FsStore>`; a second independent open of the root is rejected, including from another process. It requires Rust 1.89 or later for standard-library file locking. It does not fsync files or directories and does not promise power-loss durability. Existing directories using the old data-plus-`.etag` layout are rejected: use a fresh root or export/reimport data with the old version before upgrading. The authoritative `WTL1` object format is unchanged.

Custom stores must implement atomic conditional replacement, coherent data/validator reads, and strong read-after-write behavior. Validators identify an object version, not a whole backend; identical content may repeat an ETag. Reserve WalTier’s WAL and snapshot keys from application mutations. Implement `cache_namespace()` with a stable identity for the backing resource to enable persistent caching, and forward it through wrappers. Its default `None` bypasses all cache filesystem operations. Built-in stores provide namespaces; S3 scopes them to endpoint, bucket, access-key identity, region, and addressing mode.

`S3Options` defaults to a 10-second connection timeout, a 60-second request deadline, and a 1 GiB maximum body for **both GET and PUT** (including application payload objects). `connect_timeout` is capped to `request_timeout`; request deadlines must be positive and below the 300-second signing TTL. The request deadline covers response headers, response bodies, and upload progress. The upload checks elapsed time between 8 KiB chunks. DNS resolution and an already executing transport/TLS call cannot be forcibly cancelled and may overrun the nominal deadline; this blocking API does not promise hard cancellation. Custom stores must implement their own time and allocation bounds, and user callbacks must return; WalTier cannot interrupt them. Transport tests use a local HTTP server, with no real S3 credentials or service access.

Storage failures expose `StoreError { message, operation, key, status, mutation_outcome }`; S3 preserves the operation, object key, and any received HTTP status. `MutationOutcome::Unknown` means a failed PUT or DELETE may have landed, including a successful PUT response missing its ETag. A conditional 409/412 returns `PreconditionFailed` so the WAL can refresh before retrying. Custom-store migration: replace `StoreError(message)` with `StoreError::new(message)`, whose default is conservatively **Unknown**; use `.not_applied()` only when the backend can prove it did not apply the mutation, and `.with_context(...)` to add operation/key/status. Forward `max_object_bytes()` through wrappers along with `cache_namespace()`.

## Testing

`cargo test` runs codec/cache/store tests, targeted correctness regressions, local HTTP transport tests, and two complementary simulation suites:

- `tests/dst.rs` keeps the existing seeded Abort/Retry/Replace interleavings across writers and replicas. It settles each compactor and services cleanup between steps; its Replica-based history checks are supplementary.
- `tests/concurrency.rs` places a recording store **below fault injection**, so it observes successful CAS operations even when their responses are lost. A separate WTL1/snapshot parser reconstructs authoritative history without calling Replica or the application's snapshot decoder. Every appended tail must equal a registered submitted batch; returned acknowledgement ranges must match those original commands, and uncertain attempts must be absent or complete. Live, cold, and warm states are compared with this independent history.

The independent suite audits actual object bytes and snapshot identities under clean and ambiguous faults, tracks candidate ownership and uncertain installations, rejects deletion of live or still-installable pending snapshots, and verifies cleanup after faults stop. Offline test sweeping occurs only after the scheduled writers/workers are drained. Negative oracle tests deliberately supply wrong ranges, truncated/changed/reordered history, partial/reordered/duplicated uncertain attempts, unsafe pending deletion, and a missing live snapshot; each must be rejected.

Both seeded suites settle compactors to make their schedules reproducible. Separate channel-controlled tests overlap actual threads during snapshot upload, writer drop with active compaction, a delayed lost mutation response, and a reader's WAL/snapshot fetch racing replacement and deletion. They use explicit release barriers, with timeouts only to prevent hung tests. A same-seed test compares normalized logical traces, replacing random snapshot IDs with candidate ordinals.

Reproduce the legacy suite with `DST_SEED=<seed>` and `DST_TRACE=1`; scale it with `DST_SEEDS` and `DST_STEPS`. The independent corpus uses `ORACLE_SEED`, `ORACLE_TRACE`, `ORACLE_SEEDS`, and `ORACLE_STEPS` (default 24 seeds, each clean and faulted, at 200 steps). The `waltier::sim` module supplies seeded fault/latency injection for application tests as well.

CI checks all features, no default features, S3 alone, and simulation alone; formatting and Clippy; release codec/limits/outcomes; the group-commit example's backpressure/receipt/shutdown tests; expanded deterministic corpora; and Rust 1.89 across all targets. To run the example tests locally, use `cargo test --all-features --example group_commit`.

### Real S3 conformance

**Actual S3 service conformance has not been verified for this change.** Local HTTP transport tests pass independently of credentials. The ignored service test in `tests/s3_conformance.rs` checks create-only publication, conditional reads, two competing CAS requests, immutable objects, and cold WAL recovery against an explicitly configured endpoint.

Set `WALTIER_S3_TEST_ENDPOINT`, `WALTIER_S3_TEST_REGION`, `WALTIER_S3_TEST_BUCKET`, `WALTIER_S3_TEST_ACCESS_KEY`, `WALTIER_S3_TEST_SECRET_KEY`, `WALTIER_S3_TEST_PATH_STYLE` (`true` or `false`), and `WALTIER_S3_TEST_PREFIX` (a nonempty reserved test prefix ending in `/`). Supply credentials through your environment or secret manager, then run:

```sh
cargo test --locked --no-default-features --features s3 --test s3_conformance -- --ignored --nocapture
```

The test acquires a create-only reservation under a random subprefix, restricts its operations to that subprefix, and removes only confirmed-created objects after writers and requests finish. On failure it retains the prefix for inspection; there is no automatic cleanup that could race an uncertain or unfinished mutation. The output names the test prefix and does not print credentials. Ordinary tests and CI never run this service test. Record the endpoint/backend and passing run before claiming service conformance.

## Benchmarks

`cargo bench` runs against a simulated S3 (sleeps RTT + bytes/bandwidth per op):

```
cargo bench --bench wal -- --rtt-ms 15 --mbps 100 --writes 200
```

It reports write latency percentiles and throughput, per-write cost growth without compaction, cold versus warm open, and replica polling cost. The review benchmarks cover sustained folds, batch sizes, cache policies, larger images, and snapshot/compaction memory. See [PERFORMANCE.md](PERFORMANCE.md) for reproducible inputs, repeated measurements, and the supported workload envelope. Results from simulated latency are not measurements of a real S3 service.

In the final six-round comparison against the previous `main`, replacement-fold acknowledgement p99 fell from 31.19 to 15.42 ms at 15 ms simulated RTT; total maintenance cost stayed essentially unchanged. A prepared 16 MiB snapshot retained about 16 MiB less heap. Batching cache metadata writes, avoiding eager encoder errors, and reusing exclusively created temporary filenames recovered extended zero-RTT throughput from 1.60M to 1.75M entries/s, within 1.1% of main. The 1 ms batch64 workload measured 37,045 entries/s versus main's 36,926. Short zero-RTT throughput remained 6.4% lower and unpinned cold open 19.1% slower; the [investigation](PERFORMANCE_INVESTIGATION.md) includes all results, raw runs, and allocation/page-state controls for the cold-open gap. These measurements do not establish a cold-open improvement.

## What WalTier doesn't do

- Route requests to the current writer — run one writer per log and let stale ones lose CASes.
- Retry a failed compactor automatically — inspect the failure and explicitly restart or acknowledge it.
- Async — the API is single-threaded and blocking; the one internal thread is the compactor.
