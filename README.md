# WalTier

WalTier is an embeddable tiered write-ahead log for Rust: memory, local disk, and S3, coordinated by nothing but S3 conditional writes. You get a durable, totally ordered log per resource with user-defined compaction and read-only replicas. No consensus, no leases, no leader election — the etag chain is the arbiter.

It's the generalized core of the pattern in [Cursor's "git at any scale"](https://cursor.com/blog/git-at-any-scale), with nothing git-specific in it.

<!-- TOC -->

- [Quick start](#quick-start)
- [How does WalTier work?](#how-does-waltier-work)
  - [Writes and conflicts](#writes-and-conflicts)
  - [Compaction](#compaction)
  - [Replicas](#replicas)
  - [The tiers](#the-tiers)
- [Failure semantics](#failure-semantics)
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
{prefix}snap/<lsn>-<n> immutable snapshots written by compaction
```

The image is its own manifest, so bootstrap is one GET. Entries are small metadata — every write re-uploads the whole image, so you write large payloads as separate immutable objects first (through the same `ObjectStore`), then append an entry that references them.

### Writes and conflicts

`write(entry)` assigns the next LSN and CASes the new image. The S3 PUT is the commit point: an acked write is durable. If another instance wrote first, the PUT fails, WalTier pulls the missed entries into your state, and asks your `reconcile`:

- `Retry` — append unchanged at the new tip (right when your entries commute)
- `Replace(bytes)` — append a rewritten entry instead
- `Abort` (default) — you get `Conflict { entries }` back with your state already refreshed

`write_batch` commits many entries in one CAS PUT — atomically, each with its own LSN. Commits are serialized by the etag chain at roughly one per round trip (~40–70/s against S3), so batching is the throughput lever: buffer entries behind a channel and drain it into `write_batch`. `cargo run --release --example group_commit` demonstrates the pattern.

### Compaction

Compaction is insert-triggered and never blocks writes. When `should_compact` fires, a background thread runs your `compact` and uploads the result as a new snapshot object. The fold installs on the writer's **next PUT** (or an explicit `flush()`): same CAS as the append, so compaction never contends with its own writer. A fold that loses to a remote compaction is discarded and its orphan deleted — there's no retry loop, the next trigger just tries again. Because the trigger lives inside `write`, replicas structurally cannot compact.

### Replicas

`Replica::open` bootstraps from the same objects and `refresh()` polls with a conditional GET — a cheap 304 when nothing changed. A replica that fell behind a fold rebuilds from the snapshot via your `restore`. Replicas never write.

### The tiers

- **Memory** — your `State`, built by applying entries in LSN order
- **Local disk** — a warm-start cache of the image and snapshot, validated by etag on open
- **S3** — the durable copy and the arbiter of truth

## Failure semantics

- A stale writer corrupts nothing — it loses CASes until it catches up.
- An error can hide a PUT that landed (a timeout after S3 applied it). WalTier never applies unacked entries locally; the next refresh picks them up. A caller that resubmits appends a duplicate, so writes are **at-least-once** — make entries idempotent or catch duplicates in `reconcile`.
- A crash between uploading a snapshot and installing it orphans one object; pair the `snap/` prefix with a lifecycle rule if that matters to you.

## Object stores

`ObjectStore` is the seam: conditional get, CAS put (`If-Match` / `If-None-Match: *`), plain put, delete.

- `S3Store` (default feature `s3`) — sync HTTP via `rusty-s3` + `ureq`. Needs S3 conditional writes, which general-purpose buckets support in all regions.
- `MemoryStore` and `FsStore` — for tests and single-process development.

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
- Retry a lost compaction eagerly — the next valid trigger retries.
- Async — the API is single-threaded and blocking; the one internal thread is the compactor.
