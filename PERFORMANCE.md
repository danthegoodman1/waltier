# Performance comparison

Fold-install acknowledgement p99 fell from **31.01 to 15.42 ms** at 15 ms simulated RTT. A prepared 16 MiB snapshot now retains about **16 MiB less heap**. Explicit-flush caching also reduces local append cost. Total deletion work and peak compaction heap are essentially unchanged; the slower cold-open and startup measurements are included below.

The baseline is `d5dda89fb176d590d03c7812d047ced2712bba94`, the reviewed `main`. Candidate measurements include this PR’s correctness and performance changes. All figures are medians of three release runs with Rust 1.97.0 on the same machine. [performance-results.json](performance-results.json) contains every run, compiler/platform metadata, and the candidate HEAD/working-change marker. These are MemoryStore/SimStore measurements, not actual S3 results.

## Workloads and timing

`benches/review.rs` runs identical sources against baseline and candidate. SimStore adds fixed RTT and transfer time at 100 MB/s, with no jitter. Zero-latency cases remove both limits. Every shared case uses `Options::new(path)`, retaining caching at every commit on both versions. The counter app has an eight-byte snapshot.

- Replacement fold: twenty timed appends, each installing a replacement snapshot; 15 ms RTT per operation. The timer for total elapsed work also includes the twenty preceding appends, compactions and final close.
- Sustained: twelve installed folds, sixteen commits per fold, 64-byte entries, batch sizes 1/8/64/256. A larger-entry case uses batch64 × 4 KiB and reaches about 4 MiB of live image.
- Lag: barriers hold each compactor at its application callback while another sixteen batch64 appends commit, then release it for installation. Twelve folds measure a deterministic cohort of lag, with live images reaching about 140 KiB; no thread-scheduling sleep decides which appends overlap.

Acknowledgement percentiles time only `write`/`write_batch`. Total elapsed time and throughput include explicit compaction, flush, cleanup and close. Each p99 is a per-run percentile, not a pooled production tail estimate. Short zero-latency timings are particularly sensitive to allocator, filesystem and scheduling variation.

## Shared before/after results

Each cell is **baseline → candidate**.

| Workload | p50 ack (ms) | p99 ack (ms) | Total (s) | Entries/s including maintenance | Uploaded bytes/entry |
| --- | ---: | ---: | ---: | ---: | ---: |
| Replacement fold, 15 ms RTT | 30.511 → 15.253 | 31.013 → 15.421 | 1.2256 → 1.2250 | 33 → 33 | 169.5 → 183.3 |
| Batch 1, 1 ms RTT | 1.086 → 1.092 | 1.225 → 1.263 | 0.2504 → 0.2552 | 767 → 752 | 643.9 → 656.6 |
| Batch 8, 1 ms RTT | 1.131 → 1.136 | 1.211 → 1.193 | 0.2573 → 0.2597 | 5,970 → 5,915 | 586.2 → 587.8 |
| Batch 64, 1 ms RTT | 1.487 → 1.494 | 1.881 → 1.905 | 0.3308 → 0.3345 | 37,144 → 36,736 | 579.0 → 579.2 |
| Batch 256, 1 ms RTT | 2.783 → 2.736 | 4.288 → 4.398 | 0.5789 → 0.5820 | 84,912 → 84,446 | 578.3 → 578.3 |
| Batch 64, zero latency | 0.028 → 0.029 | 0.089 → 0.095 | 0.0102 → 0.0107 | 1,209,944 → 1,143,836 | 579.0 → 579.2 |
| Batch 64 × 4 KiB, 1 ms RTT | 26.882 → 25.809 | 51.322 → 49.898 | 5.2870 → 5.2293 | 2,324 → 2,350 | 34,851.0 → 34,851.2 |
| Batch 64 with held compactor, 1 ms RTT | 2.234 → 2.238 | 2.653 → 2.672 | 0.4782 → 0.4801 | 25,694 → 25,594 | 1,650.0 → 1,650.2 |

Both versions perform **60 PUTs and 20 DELETEs** in the replacement-fold case. The candidate defers DELETE until explicit maintenance, so its acknowledgement loses one RTT while total work remains about 1.225 s. A deterministic regression also blocks DELETE and proves the installing append returns after exactly one WAL CAS.

Every sustained/lag case performs **216 PUTs and 11 DELETEs**, excluding creation; cached snapshot reads require no GETs during these cases. That is 227 store requests for 192 batch commits, or about **0.0185 requests/entry** at batch64. Longer random snapshot identities slightly increase image bytes, especially for batch1. Streaming cache writes remove concatenation buffers but do not change the authoritative whole-image upload cost.

## Snapshot retention and startup

`benches/resources.rs` runs in a separate process with a counting allocator. It counts requested Rust heap bytes, including MemoryStore, and excludes allocator metadata, resident-page accounting, and thread stacks. Deltas start immediately before compaction. Barriers hold the large-entry compactor at its callback for startup measurements.

| Measurement | Baseline | Candidate |
| --- | ---: | ---: |
| 16 MiB snapshot, ready but not installed: retained heap delta (bytes) | 33,554,631 | 16,777,383 |
| Same compaction: peak heap delta (bytes) | 33,653,794 | 33,654,104 |
| Cold replica opening that snapshot, no store latency (ms) | 14.8493 | 19.1664 |
| Starting compaction over 4,096 × 4 KiB live entries (ms) | 3.7310 | 4.3200 |
| Same compactor held at callback: retained heap delta (bytes) | 16,909,111 | 16,909,181 |
| Same startup: peak heap delta (bytes) | 16,909,135 | 16,909,190 |

The ready fold no longer retains its own snapshot body: the remaining 16 MiB belongs to the in-memory backend. Peak heap still includes the application-produced snapshot and backend copy during upload. Snapshot caching finishes in the compactor before it reports Ready.

Cold-open latency was slower in this run set (baseline 14.11–17.32 ms; candidate 15.06–20.07 ms). Compaction-start latency was also slower; entry copying is unchanged. **No cold-open, startup, or peak-memory improvement is claimed.** These instrumented cases establish retention and expose remaining costs; they are not process memory caps or replica latency guarantees.

## Optional cache cadence

`benches/cache.rs` compares candidate policies at zero store latency. Each case uses batch64, twelve folds and sixteen commits per fold. Total time includes flush/close, so checkpoint work is counted. `OnFlush` saves the WAL on explicit checkpoints while still caching snapshots; `Disabled` can require extra snapshot GETs when a store has latency.

| Entry size | Cache policy | p50 ack (ms) | p99 ack (ms) | Total (s) | Entries/s including maintenance |
| --- | --- | ---: | ---: | ---: | ---: |
| 64 B | Disabled | 0.0079 | 0.0580 | 0.0068 | 1,806,536 |
| 64 B | EveryCommit | 0.0257 | 0.0589 | 0.0087 | 1,417,372 |
| 64 B | OnFlush | 0.0041 | 0.0103 | 0.0039 | 3,130,447 |
| 4,096 B | Disabled | 0.2985 | 1.9516 | 0.0960 | 127,969 |
| 4,096 B | EveryCommit | 1.0432 | 2.3820 | 0.2139 | 57,448 |
| 4,096 B | OnFlush | 0.3045 | 1.1009 | 0.0911 | 134,923 |

For 4 KiB entries, explicit-flush caching reduced median append time from 1.0432 to 0.3045 ms in this local workload. Cache policy does not move durability to local disk: all acknowledgements still require the object-store CAS. Cached WAL checkpoints are validated by resource identity and ETag before reuse.

## Architecture decision

**Keep the single CAS image for small metadata logs.** The principal case is 64-byte entries, batch64 and compaction every 1,024 entries, reaching about 70 KiB of live image. It meets the chosen simulation gate of at least 30,000 entries/s and p99 acknowledgement below 5 ms: the candidate measured **36,736 entries/s and 1.905 ms p99**. The batch256 exploration reaches about 280 KiB and measured 84,446 entries/s with 4.398 ms p99. These are measured workloads, not enforced performance limits or production SLOs.

Compaction lag matters: holding a fold for another cohort increases uploaded bytes/entry from 579.2 to 1,650.2 and reduces throughput to about 25,594 entries/s. The 4 MiB image case reaches about 49.9 ms p99 and 34,851 uploaded bytes per 4,096-byte entry. Configure image limits and compaction thresholds for the application’s latency budget; use separate immutable application objects for larger payloads.

A segmented alternative would upload an immutable batch object, then CAS a small manifest. That avoids repeated live-payload uploads but adds a publication request before each commit, extra catch-up GETs, orphan tracking for failed/uncertain manifest CAS, and authoritative format migration. Its commit point remains the manifest CAS. Those costs warrant a separate design if larger retained logs become the product target; the measured small-metadata target does not require that rewrite.

Compaction still copies live entry payloads at startup. Changing every entry to shared ownership would alter the public entry representation and add ownership work to all appends. The measured small-image target supports retaining the simple Vec representation here, while removing redundant snapshot retention and cache concatenations.

## Reproduction

With dependencies already cached, run from this checkout:

```sh
python3 scripts/compare-performance.py --runs 3 --output /tmp/waltier-performance.json
cargo bench --bench review --bench resources --bench cache
```

The script extracts the baseline into a temporary directory, seeds each fixture with that revision’s Cargo.lock, and builds offline in release mode. It runs identical review/resources sources for both versions and the cache-policy benchmark for the candidate. Use `--candidate <commit>` to measure an immutable candidate that supports the new cache API instead of the working checkout. The group-commit example’s output is a smoke test, not evidence for these comparisons.
