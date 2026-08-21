# Plan: an object-native LSM KV on WalTier

An LSM key-value store where S3 holds everything: WalTier provides the log,
the manifest lifecycle, and fencing; the app provides SSTables, merges, and
reads. One writer per partition gets high write throughput through batching;
replicas and caching give reads at arbitrary scale.

## Architecture

The database state is **memtable + manifest**. The WAL is the log of edits to
that state, so one log carries both data writes and structural changes:

```
entry = Set { key, value }            // small values inline
      | SetRef { key, blob_key }      // large values written outside first
      | Del { key }
      | ManifestEdit { add: [SstRef], remove: [SstRef] }
```

Object layout:

```
{prefix}wal           WAL image: snapshot pointer + pending entries (CAS'd)
{prefix}snap/<lsn>-*  the manifest, replaced each fold (WalTier-managed)
{prefix}sst/<ulid>    SSTables: immutable bulk data (app-managed)
{prefix}blob/<ulid>   large values committed by reference (app-managed)
```

The manifest is the complete list of live SSTables, one record per file:
`{ object_key, level, min_key, max_key, size, entry_count }`, plus level
configuration and the LSN it covers. Liveness is membership: a file the
manifest names is live, anything else under `sst/` is garbage. Bloom filters
stay in each SSTable's footer, cached locally on first read.

`ManifestEdit` entries are deltas in flight; the snapshot is the checkpoint
they merge into. Each fold rewrites the whole manifest, so history is
discarded and nothing re-asserts liveness.

## Write path

Producers feed a channel; one writer thread drains it into `write_batch`
(the group-commit pattern in `examples/group_commit.rs`). Commits serialize
on the etag chain at ~1/RTT (~60–70/s on S3), so batching is the throughput
lever: entries/s scales with batch size into the tens of thousands.

Large values upload to `blob/` first, then commit by reference with a
`SetRef` entry. This keeps the WAL image small: budget roughly
`image_size x commits/s` of upstream bandwidth.

## Flush (WalTier's fold)

`should_compact` fires on entry count/bytes. `compact(base, entries)`:

1. Decode the base manifest from the snapshot bytes.
2. Fold `Set`/`SetRef`/`Del` entries into a new L0 SSTable, upload it.
3. Apply any `ManifestEdit` entries.
4. Add the new L0 ref and re-encode the complete manifest.

WalTier uploads the result as the new `snap/` object and installs it on the
writer's next PUT. The fold bounds memory: in-memory state is always
`restore(snapshot)` plus the handful of unfolded entries.

## Merge (app-level LSM compaction)

A background job reads SSTables, writes merged replacements to `sst/`
(unreferenced, safe from any machine), then submits one
`ManifestEdit { add, remove }` through the writer's channel. The writer
validates the edit against the current manifest before appending — if the
inputs are gone, a competing merge won; drop the edit and let its outputs
get swept. `reconcile`: `Retry` for data entries (LWW commutes), `Abort`
when two merges touch the same files.

## Read path

Memtable, then SSTables by level through a read-through disk cache.
Immutable objects cache perfectly (etags never change). The manifest's key
ranges route a lookup to at most one file per level; bloom filters cut cold
probes. Replicas (`Replica::open` + `refresh`) serve reads at any fan-out
and structurally cannot compact.

## Garbage collection

Mark-and-sweep: list `sst/` and `blob/`, diff against the current manifest,
delete unreferenced objects older than a grace period. Alternative if LIST
is awkward: carry `pending_delete: [(object_key, removed_at_lsn)]` in the
manifest and drain it after the grace period.

## Failure semantics

- Writes are at-least-once (WalTier's contract). LWW sets are idempotent, so
  duplicates are harmless; read-modify-write ops must go through `Replace`
  or CAS-style entries.
- A crash between uploading an SSTable and committing its ref orphans the
  object; the sweep reclaims it.
- Stale writers and stale merge jobs lose CASes or fail validation; they
  corrupt nothing.

## Scale

- Reads: arbitrary. State grows on S3; the image and manifest stay small.
- Writes per partition: ~1/RTT commits, tens of thousands of entries/s with
  batching. Beyond that, shard the keyspace across WAL prefixes — each
  partition is an independent etag chain.
- Manifest: ~16k files ≈ 2–4MB at 64MB SSTables per TB — fine as a snapshot
  object. Escape hatch at extreme counts: per-level manifest objects with
  the snapshot holding refs.
- Latency floor is one RTT per commit. S3 Express One Zone supports
  conditional writes at single-digit-ms PUTs if that matters.

## Milestones

1. **Core KV, no levels.** New workspace crate `waltier-lsm`. Entry
   encoding, memtable, flush-to-L0, flat manifest, point reads with full-file
   scan. Tests against `MemoryStore`.
2. **Real SSTables.** Sorted blocks, footer with fence pointers + bloom
   filter, binary search reads, disk cache integration.
3. **Merge.** Size-tiered or leveled policy, the `ManifestEdit` path,
   writer-side validation, orphan sweep. DST coverage: merges racing
   writers, crash between SSTable upload and edit commit.
4. **Throughput.** Group-commit writer thread, `SetRef` blobs, benchmarks on
   `SimStore` (entries/s vs batch size, read latency cold/warm, merge
   amplification).
5. **Scale-out.** Range or hash partitioning across WAL prefixes; a thin
   router. Replica read fan-out example.

## Open questions

- Iterator/scan API surface: merge iterators across memtable + levels; how
  much to expose in v1.
- Snapshot isolation for readers mid-merge: pin a manifest version per
  iterator, which delays GC of its files.
- Whether `waltier-lsm` ships in this repo as a workspace member or stands
  alone once stable.
