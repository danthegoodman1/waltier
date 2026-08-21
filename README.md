# WalTier

A tiered write-ahead log — memory, local disk, and object storage — with
etag-CAS fencing and user-defined compaction. WalTier is the generalized core
of the pattern in [Cursor's "git at any scale"](https://cursor.com/blog/git-at-any-scale):
a durable, totally ordered log per resource, arbitrated by S3 conditional
writes, with nothing git-specific in it. MIT licensed.

## The model

The whole log lives in **one small object** (the WAL image) that is rewritten
wholesale on every write with a compare-and-swap on its etag:

```
{prefix}wal            the WAL image: snapshot pointer + live entries (CAS'd)
{prefix}snap/<lsn>-<n> immutable snapshots written by compaction
```

The image is its own manifest. A write appends an entry to the in-memory image
and PUTs it with `If-Match: <etag>`. If another instance wrote first, the PUT
fails: the library re-pulls the image, applies the missed entries to your
state, and asks your app how to reconcile. That etag chain is the only
coordination in the system — competing writers and competing compactions fence
each other naturally, with no leases, locks, or consensus.

**The contract: entries are small metadata.** Every write re-uploads the whole
image, so per-write cost is proportional to image size. Store large payloads as
separate immutable objects first (through the same `ObjectStore`), then append
an entry that references them.

The tiers:

- **Memory** — your `State`, built by applying entries in LSN order.
- **Local disk** — a warm-start cache of the image and snapshot, validated by
  etag on open. Losing it costs one download.
- **Object storage** — the durable copy and the arbiter of truth.

## Quick start

```rust
use std::collections::BTreeMap;
use std::sync::Arc;
use waltier::{Entry, Lsn, MemoryStore, Options, Reconcile, WalApp, WalError, WalStats, WalTier};

struct Kv;

impl WalApp for Kv {
    type State = BTreeMap<String, String>;

    fn init(&self) -> Self::State { BTreeMap::new() }

    fn apply(&self, state: &mut Self::State, _lsn: Lsn, entry: &[u8]) {
        // decode the entry and fold it into state
    }

    fn restore(&self, snapshot: &[u8]) -> Result<Self::State, WalError> {
        // rebuild state from a snapshot your compact() wrote
    }

    fn compact(&self, base: Option<&[u8]>, entries: &[Entry]) -> Result<Vec<u8>, WalError> {
        // fold base + entries into a new snapshot
    }

    fn should_compact(&self, stats: &WalStats) -> bool {
        stats.live_entries >= 1024 || stats.live_entry_bytes >= 256 << 10
    }

    fn reconcile(&self, _state: &Self::State, _pending: &[u8]) -> Reconcile {
        Reconcile::Retry // safe when your entries commute; the default is Abort
    }
}

let store = Arc::new(MemoryStore::new());
let mut wal = WalTier::open(store, Kv, Options::new("/var/lib/mylog"))?;
let lsn = wal.write(b"set a 1".to_vec())?; // LSNs are assigned for you: 0, 1, 2, ...
```

`cargo run --example kv` runs a complete last-write-wins KV store against the
filesystem-backed store.

## Writes and conflicts

`write(entry)` assigns the next LSN and CASes the new image. On success it
applies the entry to your state and returns the LSN — the S3 PUT is the commit
point, so an acked write is durable.

On an etag conflict the library refreshes (one GET, then `apply` for each
missed entry) and calls your `reconcile(state, pending)`:

- `Retry` — append unchanged at the new tip. Right when entries commute.
- `Replace(bytes)` — append a rewritten entry instead.
- `Abort` (the default) — `write` returns `WalError::Conflict { entry }` with
  your state already refreshed, so you can re-validate and resubmit.

## Compaction

Compaction is insert-triggered: after each successful write the library checks
`should_compact(stats)` and, when true, spawns one background thread that runs
your `compact(base_snapshot, entries)` and uploads the result as a new
snapshot object. Nothing blocks — the writer keeps appending while the fold
runs.

The fold installs on the writer's **next PUT**: the next `write` (or an
explicit `flush()`) swaps the snapshot pointer, drops the folded entries, and
deletes the old snapshot object, all under the same CAS as the append. This is
why compaction never contends with its own writer, and why a fold that loses
to a remote compaction simply gets discarded — the etag mismatch on the next
PUT reveals the newer snapshot, and the orphaned object is deleted. There is
no separate retry loop; the next trigger tries again.

Because the trigger lives inside `write`, read-only replicas structurally
cannot compact.

Failures (including a lost fold) are recorded and readable via
`last_compaction_error()`. One orphaned snapshot object can persist if the
process dies between uploading a snapshot and installing it; the next
installed fold does not remove it, so pair the `snap/` prefix with a lifecycle
rule or an occasional sweep if that matters to you.

## Replicas

`Replica::open` bootstraps from the same objects and `refresh()` polls with a
conditional GET — a cheap 304 when nothing changed. A replica that fell behind
a fold rebuilds from the snapshot via your `restore`, then applies the live
entries. Replicas never write and never compact.

## Stores

`ObjectStore` is the seam: `get`, conditional `get_if_changed`, CAS
`put_if_match` (If-Match, or If-None-Match: * for create), plain `put`, and
`delete`.

- `S3Store` (default feature `s3`) — blocking HTTP via `rusty-s3` + `ureq`.
  Needs S3 conditional writes, which general-purpose buckets support in all
  regions. Not yet exercised against real S3; the core logic is store-agnostic
  and tested through the local stores.
- `MemoryStore` — for tests, with operation counters.
- `FsStore` — directory-backed, for single-process development and examples.

Your app can reuse the same store (via `wal.store()`) for its large payload
objects.

## Non-goals

- Routing requests to the current writer. Run one writer per log; a stale
  writer corrupts nothing — it just loses CASes until it catches up.
- Retrying a lost compaction eagerly. The next valid trigger retries.
- Async. The API is single-threaded and blocking; the one internal thread is
  the compactor.

## Testing

`cargo test` runs three layers:

- Unit tests for the image format, the conditional-write semantics of every
  local store, and the simulation plumbing.
- Integration tests over `MemoryStore` covering fencing, all three reconcile
  modes, compaction and fold installation, competing compactions, replicas,
  warm starts, and the ambiguous-failure cases (a PUT that lands but reports
  an error).
- **Deterministic simulation tests** (`tests/dst.rs`): seeded runs interleave
  writes, refreshes, flushes, compactions, and crash/reopen cycles across two
  writers and a replica, with and without injected store faults. An oracle
  replica checks after every step that the committed history only grows, every
  instance's state is an exact prefix of it, `apply` runs exactly once per LSN
  in order, and no snapshot object leaks or vanishes. A run is a pure function
  of its seed. Reproduce a failure with `DST_SEED=<seed>`, watch it with
  `DST_TRACE=1`, and scale coverage with `DST_SEEDS` / `DST_STEPS`.

The `waltier::sim` module (feature `sim`, on by default) provides the seeded
RNG and the `SimStore` wrapper — fault injection (clean and ambiguous
failures) plus a latency model — and works just as well for testing apps
built on WalTier.

## Benchmarks

`cargo bench` runs a custom harness against a simulated S3 (`SimStore` over
`MemoryStore`, sleeping RTT + bytes/bandwidth per op):

```
cargo bench --bench wal -- --rtt-ms 15 --mbps 100 --writes 200
cargo bench --bench wal -- --quick
```

It reports write latency percentiles and throughput (with folds riding on
writes), the per-write cost growth when compaction is off, cold vs warm open
with a large snapshot, and replica poll cost. Commit latency is RTT-bound:
with no store latency the library itself sustains ~70k writes/s on one
thread.

## Development

```
cargo test                        # includes the DST suite
cargo run --example kv            # demo app
cargo bench                       # simulated-S3 benchmarks
cargo build --no-default-features # core without the S3 and sim modules
```
