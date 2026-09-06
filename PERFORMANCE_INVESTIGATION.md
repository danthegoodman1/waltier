# Performance regression investigation

Three focused changes recover most of the sustained append throughput loss while preserving the correctness checks. In the final interleaved comparison, the extended zero-RTT workload improves from **1.602M to 1.750M entries/s**, versus **1.770M on main**. The 1 ms batch64 workload recovers to main's level. **The short zero-RTT and unpinned cold-open cases remain slower**; their results and diagnostic limits are included below.

The [original report](PERFORMANCE.md) and its three-run results remain available. [performance-investigation.json](performance-investigation.json) records the new raw runs, intermediate cohorts, medians, ranges, source/binary hashes and diagnostics. These are local MemoryStore/SimStore workloads, not real S3 measurements or performance guarantees.

## Changes and causal evidence

| Change | Reason and evidence | Preserved behavior |
| --- | --- | --- |
| Batch cache metadata into one prefix, then write the borrowed body | The original PR split a WAL cache publication into seven writes and a snapshot into five. A syscall trace over 2,112 commits and 132 folds falls from **16,382 to 4,756 cache writes**, with exactly **78,512,566 bytes** in both PR variants and no short writes. | WTC2 bytes, checksum, identity and bounds remain unchanged; no full-body concatenation buffer. |
| Construct encoder errors only on failure | With Rust 1.97, eager `ok_or(WalError::LsnExhausted)` compiled into error drop glue on each successful entry-size check. Explicit failure branches remove five drop-call sites; the encoder symbol shrinks from 2,000 to 1,760 bytes. | The same count, key, entry, total-size and LSN checks run in the same order. This removes CPU work, not a per-entry heap allocation. |
| Reuse the common temporary filename after atomic rename | Try a process-specific name with exclusive creation; fall back to counter-suffixed names on collision. This reduces repeated filename creation overhead in the measured workload. | Every attempt uses `create_new(true)`. Overlapping writers get separate files; rename publishes a complete frame, and cleanup only removes the current writer's file. |

Separate twelve-pair ablations, after two warm-ups per binary, isolate the latter two changes. Adding the encoder change to the prefix fix increases median extended zero-RTT throughput **3.8% unpinned / 8.1% on CPU 0** (9/12 and 12/12 paired wins). Adding temporary-name reuse increases it **4.7% / 5.1%** (9/12 and 10/12 wins). These are ratios of cohort medians; individual pairs vary. They are separate experiments and their percentages should not be added. One-millisecond results are mixed and much less sensitive to these local costs.

## Final comparison

The machine is an Intel Core Ultra 9 285: eight performance cores (logical CPUs 0–7) and sixteen efficiency cores (8–23), Linux 7.0.0-30, Rust 1.97.0. `main` is `d5dda89`; “original PR” is `7a4b4c6`; “final” contains all three changes above. The raw artifact records final production source and benchmark hashes because measurements preceded the commit.

All versions were built before timing, with each revision's Cargo.lock seeding offline dependency resolution. Each binary received one warm-up, followed by six rounds covering all six version orders. Headline results use **no CPU affinity**. Snapshot and compaction-start cases run in separate processes to avoid cross-case allocation history. These conditions differ from the original three-run experiment, so compare versions within a cohort. Pinned controls constrain the compactor as well as the caller and are not interchangeable with normal scheduling.

The original sustained cases still use twelve folds and 192 commits. An added extended zero-RTT case uses 120 folds and **1,920 commits**, with the same batch64 workload. Total time and throughput include compaction, flush, cleanup and close; acknowledgement percentiles time appends only. The eight-byte counter snapshot and simulated transfer settings are described in the original report. Values below are medians, not pooled tail estimates.

| Workload / metric | Main | Original PR | Final | Final vs main |
| --- | ---: | ---: | ---: | ---: |
| Replacement fold, 15 ms RTT: p99 (ms) | 31.186 | 15.465 | 15.425 | -50.5% |
| Batch1, 1 ms RTT: entries/s | 762 | 754 | 756 | -0.8% |
| Batch8, 1 ms RTT: entries/s | 5,856 | 5,903 | 5,945 | +1.5% |
| Batch64, 1 ms RTT: entries/s | 36,926 | 36,280 | 37,045 | +0.3% |
| Batch256, 1 ms RTT: entries/s | 84,524 | 83,097 | 84,673 | +0.2% |
| Short batch64, zero RTT: entries/s | 1,273,430 | 1,210,724 | 1,191,902 | -6.4% |
| Batch64 × 4 KiB, 1 ms RTT: entries/s | 2,366 | 2,386 | 2,359 | -0.3% |
| Held compactor, batch64, 1 ms RTT: entries/s | 25,276 | 25,596 | 25,745 | +1.9% |
| Extended batch64, zero RTT: entries/s | 1,770,245 | 1,602,107 | 1,750,177 | -1.1% |

The extended case recovers **9.2% against the original PR**. Its main/original/final p50 is **0.0225/0.0245/0.0225 ms**, p99 **0.0651/0.0711/0.0682 ms**, and total **0.06945/0.07670/0.07020 s**. The short case remains 6.4% below main and 1.6% below the original PR; it lasts approximately ten milliseconds per run, with final p99 0.0887 ms versus main 0.0822 ms. The longer case helps reduce that sensitivity, but does not erase the short-case result or establish a universal throughput win. Across earlier unpinned cohorts, even the two-fix candidate ranged from a focused improvement to an 8.5% aggregate deficit against main; those cohorts are retained in the raw artifact.

At 1 ms RTT, final batch64 p50/p99 are **1.5026/1.9092 ms**, versus main **1.4915/1.8989 ms**. This still meets the existing simulation target of 30,000 entries/s and p99 below 5 ms. No performance threshold or architecture decision changed to accommodate the results.

Replacement-fold total work remains **1.2232/1.2245/1.2253 s** with **60 PUTs and 20 DELETEs** in each version. Every twelve-fold sustained case still uses 216 PUTs and 11 DELETEs; the extended case uses 2,160 and 119. Uploaded bytes per entry for extended batch64 are **579.1/579.3/579.3**. Cache prefix batching and filename reuse change local work only.

## Memory, startup and cold open

| Measurement | Main | Original PR | Final |
| --- | ---: | ---: | ---: |
| Ready 16 MiB snapshot: retained heap delta (bytes) | 33,554,631 | 16,777,383 | 16,777,383 |
| Same compaction: peak heap delta (bytes) | 33,653,794 | 33,654,104 | 33,654,171 |
| Cold replica opening that snapshot (ms) | 12.4834 | 14.7465 | 14.8735 |
| Start compaction over 4,096 × 4 KiB entries (ms) | 6.0422 | 4.2250 | 4.2570 |
| Startup held at callback: retained heap delta (bytes) | 16,909,111 | 16,909,181 | 16,909,181 |
| Startup peak heap delta (bytes) | 16,909,135 | 16,909,190 | 16,909,190 |

The original 16% startup slowdown did not reproduce: an earlier fifteen-round CPU-0 control using the original combined resource workload measured main/original PR at **3.7816/3.4583 ms**; the isolated final experiment above also favors the PR. Entry copying at startup is unchanged. There is no basis for claiming a causal startup optimization from these three fixes. The retained-snapshot saving remains about 16 MiB, with essentially unchanged peak heap; the final prefix adds only metadata-sized temporary storage.

**The final unpinned cold-open median is still 19.1% slower than main.** A separate instrumented probe brackets the backend snapshot GET and the interval from GET return to application restore (cache framing/checksum/publication). Each nine-round probe builds all versions first, warms once and interleaves version order. These timings include observation overhead and do not replace the headline workload:

| Diagnostic condition | Stage | Main (ms) | Original PR (ms) | Final (ms) |
| --- | --- | ---: | ---: | ---: |
| Unpinned, cold | Total open | 12.498 | 14.460 | 14.041 |
| Unpinned, cold | Snapshot GET | 5.116 | 7.253 | 7.059 |
| Unpinned, cold | Cache interval | 7.337 | 7.255 | 7.193 |
| CPU 8, cold | Total open | 12.028 | 11.969 | 11.974 |
| CPU 8, cold | Snapshot GET | 5.067 | 4.954 | 5.019 |
| Unpinned, prior diagnostic GET | Total open | 9.006 | 8.772 | 8.885 |
| Unpinned, prior diagnostic GET | Snapshot GET | 1.305 | 1.286 | 1.304 |

The slowdown is concentrated in `MemoryStore::get` cloning the 16 MiB snapshot, whose copy implementation is unchanged. Every cold GET records **two allocations / 16,777,219 bytes / 4,096 minor faults**, with no major faults; the cache interval has no minor faults. Pinning to one efficiency core removes the gap. In the last control, a separate snapshot clone/drop before timing removes the GET's minor faults and the timing gap while the replica's local cache remains empty. Earlier CPU-0/CPU-8 and primed controls agree and are retained in the artifact.

These observations implicate allocation/page state and execution conditions, rather than added checksum or cache I/O time. They do **not** uniquely identify why each page fault/copy is slower under normal scheduling, or prove cold-open latency is restored. Retaining another 16 MiB would undo the retention gain; a separate warm-up copy would add work outside the timer. Production code and headline timing perform no such warm-up. Real S3 fetch latency remains unmeasured.

## Cache-policy results and validation

Final zero-RTT batch64 cache-policy medians include all explicit checkpoint work:

| Entry bytes | Policy | p50 ack (ms) | p99 ack (ms) | Total (s) | Entries/s |
| --- | --- | ---: | ---: | ---: | ---: |
| 64 | Disabled | 0.0062 | 0.0463 | 0.00530 | 2,338,105 |
| 64 | EveryCommit | 0.0235 | 0.0580 | 0.00805 | 1,529,964 |
| 64 | OnFlush | 0.0029 | 0.0219 | 0.00335 | 3,716,714 |
| 4,096 | Disabled | 0.3017 | 2.0146 | 0.09880 | 124,362 |
| 4,096 | EveryCommit | 1.0320 | 2.4277 | 0.21505 | 57,145 |
| 4,096 | OnFlush | 0.2547 | 1.2136 | 0.09005 | 136,732 |

Policy changes remain optional; every acknowledgement still requires the authoritative WAL CAS. Small comparisons vary even with caching disabled, so these rows establish policy tradeoffs rather than isolate each source change.

The final production changes passed **113 all-feature tests**, **85 no-default tests**, **30 release library tests**, **27 release limits/outcomes tests**, and **3 group-commit example tests**, plus Clippy across all targets, formatting and Rust 1.89 all-targets checks. Cache regressions verify exact WTC2 bytes at empty/large metadata boundaries, exclusive-name collision ownership, and actual overlapping publication with bounded channel waits. Existing cache recovery/corruption, limit/LSN and uncertain-outcome tests remain in place. The opt-in real-S3 test was ignored; bounded tests are not exhaustive proof. Remote CI status is recorded in the PR and development-plan gate.

## Reproduction

With the revisions and locked dependencies available locally, Python 3.11+ and the Rust toolchain:

```sh
python3 scripts/compare-performance.py --before 7a4b4c6 --runs 6 --warmups 1 --isolate-resources --output /tmp/final.json
python3 scripts/compare-performance.py --before 7a4b4c6 --benches review --review-small --runs 12 --warmups 2 --cpu 0 --output /tmp/small.json
python3 scripts/profile-cold-open.py --runs 9 --output /tmp/cold.json
python3 scripts/profile-cold-open.py --runs 9 --cpu 8 --output /tmp/cold-cpu8.json
python3 scripts/profile-cold-open.py --runs 9 --prime-get --output /tmp/cold-primed.json
```

Use `--candidate <revision>` to archive an immutable candidate. Both tools build all versions before measurement and preserve raw observations. CPU affinity and the cold profiler require Linux; the profiler adds only a fixture dependency on the already-locked libc version, forwards cache identity/budget methods when available, and leaves the library dependencies unchanged. The prior-GET control is diagnostic only. `--keep-binaries <directory>` preserves comparison executables for `strace -f -qq -yy -e trace=write,writev` or `objdump -Cd`; filter cache-file descriptors separately from stdout when counting writes.
