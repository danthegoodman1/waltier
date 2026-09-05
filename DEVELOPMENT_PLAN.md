# WalTier development plan

Reviewed `main` at `d5dda89fb176d590d03c7812d047ced2712bba94` on 2026-09-04. Implementation is in progress. Phase ledgers distinguish completed, verified changes from remaining work; review findings describe the original baseline.

## Overarching goal

Make WalTier a small, predictable object-store WAL whose acknowledged history survives contention, compaction, cache reuse, and recovery. Preserve atomic batches, contiguous LSNs, durable acknowledgements, optimistic writer fencing, and replicas that reconstruct a committed prefix. Preserve the documented possibility of duplicate submissions after an ambiguous write; exactly-once application effects remain an application responsibility.

The single CAS object and shared writer/replica replay core are good foundations for small metadata logs. The immediate leverage is in object identity, snapshot lifetime, explicit operation outcomes, bounded resource use, and tests that exercise actual overlap. Keep the current storage layout initially; decide whether to replace it using the Phase 4 workload measurements. Public APIs and internal boundaries may change where that removes ambiguity or makes atomic operations composable.

## Review findings

Priorities describe implementation order: **P1** can lose history, prevent recovery, or indefinitely stop an operation; **P2** affects API correctness, predictable performance, or confidence. “Reproduced” means a scratch test demonstrated the current behavior, not that a regression fix exists. Line numbers below refer to the reviewed commit.

| ID | Priority | Finding and consequence | Evidence | Phase |
| --- | --- | --- | --- | --- |
| F1 | P1 | The documented snapshot sweep is unsafe while writers can publish folds. An uploaded, pending snapshot is absent from the current WAL reference; deleting it allows a later successful CAS to replace acknowledged entries with a missing snapshot. | **Reproduced:** `README.md:88`; `src/wal.rs:463`, `src/wal.rs:661`. After upload → sweep → flush, cold open fails although flush succeeded. | 1 |
| F2 | P1 | Cache validity is bound only to an ETag, not the backend and WAL key. Reusing a cache directory across resources with matching validators can load another history and subsequently overwrite the intended log through a valid CAS. Checksums do not detect this. | **Reproduced:** `src/cache.rs:83`, `src/cache.rs:92`; `src/wal.rs:128`. Two independent MemoryStores both issue ETag `"1"`; reopening B with A's cache changes B's acknowledged history from `B` to `AC`. This does not establish a same-content S3 ETag collision. | 1 |
| F3 | P2 | FsStore does not preserve object-key identity: `a/b` and `a__b` alias, and object `k.etag` overwrites `k`'s validator. Data and ETag are also published separately; locks protect one FsStore instance, not every handle to a directory. | **Aliases reproduced; publication/locking issues inspected:** `src/cache.rs:15`, `src/cache.rs:21`; `src/store.rs:175`, `src/store.rs:200`, `src/store.rs:227`. Scope is the development backend, not S3. | 1 |
| F4 | P1 | S3 requests have no configured read, write, or overall deadline. A stalled response can block append, refresh, a compactor, or close indefinitely. An attempt count does not bound elapsed time. | **Inspected:** `src/s3.rs:53`; resolved ureq 2.12.1 defaults described below. | 2 |
| F5 | P1/P2 | Write acceptance and read limits disagree: S3 GET rejects bodies above 1 GiB, but PUT and compaction publication do not enforce that limit. The codec narrows lengths/counts to u32 and uses unchecked LSN arithmetic. A tiny image with snapshot LSN `u64::MAX` passes decode and then panics in debug or produces inconsistent state in release. | **LSN behavior reproduced; size problems inspected:** `src/image.rs:38`, `src/image.rs:97`, `src/image.rs:101`, `src/image.rs:125`; `src/s3.rs:16`, `src/s3.rs:64`, `src/s3.rs:150`. No multi-gigabyte allocation was attempted. | 2 |
| F6 | P2 | `flush()` returns `Ok(())` after exhausting CAS attempts with a fold still pending. `close()` can then return success without the promised installation, and also hides compactor failure behind a boolean/side channel. | **Reproduced:** `src/wal.rs:367`, `src/wal.rs:409`, `src/wal.rs:422`. Existing retry-bound coverage tests write, not flush/close. This loses maintenance progress, not the durability of an already acknowledged append. | 3 |
| F7 | P2 | Per-entry reconciliation against one unchanged committed state cannot naturally rebuild a dependent atomic batch. For example, assigning two new IDs after a conflict yields the same ID twice if each replacement uses `state.len()`. This is the documented API behavior, not a violation of batch atomicity. | **Limitation demonstrated:** `src/wal.rs:315`, `src/wal.rs:347`; `src/lib.rs:111`. The callback sees neither the whole batch nor earlier proposed replacements. | 3 |
| F8 | P2 | Compaction maintenance is partly synchronous with acknowledgement: deleting the previous snapshot and writing the new snapshot cache happen after a winning CAS, before write returns. Triggering compaction also clones every live payload on the writer thread. Each commit separately copies/checksums/writes its full image cache. | **DELETE delay reproduced; remaining costs inspected:** `src/wal.rs:487`, `src/wal.rs:503`, `src/wal.rs:609`; `src/cache.rs:99`. Injecting only a 100 ms DELETE delay added about 100.3 ms to the installing append. “Never blocks writes” overstates the implementation. | 4 |
| F9 | P2 | DST settles each compactor immediately, so it omits overlap during snapshot fetch/upload and crash during an active compactor. Its oracle is another Replica using the same replay implementation; write acknowledgements are not independently recorded, and object accounting is disabled under faults. No S3 transport tests or CI configuration are checked in. | **Inspected:** `tests/dst.rs:250`, `tests/dst.rs:393`, `tests/dst.rs:449`; `src/s3.rs`; repository file inventory. Existing DST is useful but does not prove these cases. | 5, plus regressions in every phase |

Two related contract gaps belong with the fixes, rather than separate redesign projects:

- Snapshot keys use wall-clock nanoseconds plus a process-local counter, then unconditional PUT (`src/wal.rs:40`, `src/wal.rs:661`). That is not a cross-process uniqueness proof. Publish with create-only semantics and handle collisions; never let an ID collision overwrite an immutable snapshot. A collision was not reproduced.
- `ObjectStore` requires an ETag change on every successful PUT (`src/store.rs:57`), whereas S3 ETags can be content hashes and repeat for identical bytes. Specify the actual dependency: atomic conditional replacement, coherent object/validator reads, strong read-after-write behavior, and reserved WAL/snapshot keys that applications do not overwrite. Validators are scoped to an object, not a global resource identity. AWS documents both [conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html) and [content-based ETags](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html).

## Review evidence

Baseline validation on Rust/Cargo 1.97.0:

| Command | Result |
| --- | --- |
| `cargo test --all-features` | Passed: 14 unit tests, 25 integration tests, 2 DST tests. DST defaults cover 40 seeds × 250 steps in each of the fault-free and faulted suites. |
| `cargo test --no-default-features` | Passed: 10 unit tests, 20 integration tests. |
| `cargo clippy --all-features --all-targets -- -D warnings` | Passed. |
| `cargo bench --bench wal -- --quick` | Completed. Single local run: approximately 95k writes/s without injected store latency; at simulated 5 ms RTT, batches of 1/8/64 achieved approximately 178/1,413/9,635 entries/s. These are local simulation observations, not an S3 capacity claim. |

Seven isolated diagnostic tests passed in both debug and release by asserting the **undesirable current behavior**: F1, F2, F3's key aliases, F5's invalid LSN, F6, F7, and F8's DELETE delay. The scratch harness is at `/tmp/waltier-review/tests/findings.rs`, invoked with `cargo test --offline --manifest-path /tmp/waltier-review/Cargo.toml -- --nocapture` and the corresponding `--release` command. It is not a committed regression suite; the scenarios below must become repository tests asserting the corrected behavior.

Reproduction recipes for the most consequential failures:

1. **F1:** Write `A`, start and wait for compaction without flushing, delete unreferenced `snap/` objects as the README advises, then flush. The WAL contains zero live entries and references the deleted snapshot. Cold open returns `Corrupt("open kept racing with concurrent compactions")`.
2. **F2:** Write `A` in MemoryStore A under prefix `a/` and cache D. Separately write `B` in MemoryStore B under prefix `b/`. Both validators are `"1"`. Reopen B using D, observe state `A`, then append `C`. A cold reader of B sees `AC`; acknowledged `B` has disappeared.
3. **F5:** Encode `WTL1`, snapshot flag 1, snapshot LSN `u64::MAX`, a one-byte existing snapshot key, and zero live entries. Open panics at `src/image.rs:39` in debug; release accepts nonempty restored state while reporting no tip.
4. **F6:** Prepare a pending fold, force every subsequent conditional PUT to return `PreconditionFailed`, call flush and close. Both return success; a cold reader sees no installed snapshot.

The S3 deadline finding was checked against the resolved dependency source and [ureq 2.12.1's AgentBuilder implementation](https://github.com/algesten/ureq/blob/2.12.1/src/agent.rs): connection timeout defaults to 30 seconds; read, write, and overall timeouts are unset. Real S3 behavior, credential flows, power-loss recovery, and large-scale memory bounds were not exercised in this review.

## Implementation principles

- Keep the WAL CAS as the sole commit point. Maintenance failures must not turn a known successful append into a reported failed append.
- Make resource identity explicit. Cache corruption, cache reuse, and snapshot-key collisions must not alter authoritative history.
- Treat snapshots as unpublished, published, or provably obsolete. An unreferenced object is not necessarily obsolete.
- Prefer a small synchronous core with explicit maintenance state. Avoid introducing leases, a general task framework, or a new runtime merely to fix local lifecycle issues.
- Make operation results express what happened: committed, definitely rejected, outcome unknown, or maintenance incomplete. Never infer that a timeout means no write occurred.
- Define `WalApp` callbacks as deterministic state transitions and snapshot reconstruction. They must preserve replay equivalence, avoid external side effects, and not panic; the “once per LSN” wording must be scoped to an uninterrupted state reconstruction, not process lifetime or external delivery.
- Coordinate breaking API/format changes into a documented release. Invalidate disposable old cache formats; retain authoritative WAL compatibility or provide an explicit migration. FsStore development-data compatibility can be narrower if clearly stated.

## Testing strategy

- Land a focused reproducer with each fix. Assert acknowledged-history preservation, batch indivisibility, replica prefixes, cold recovery, and exact publication outcomes rather than internal helper structure.
- Use barriers or a controllable store to force storage-operation boundaries. Use fake time for logical schedules; keep only actual HTTP deadline tests dependent on wall time.
- Run debug and release codec tests, a malformed-input corpus, and size boundaries using small configurable limits. Validate arithmetic without allocating multi-gigabyte buffers.
- Run all-feature and no-default-feature suites plus Clippy for implementation phases. Exercise S3 behavior with a local scripted HTTP server; keep an actual S3 conformance job opt-in until its test environment exists.
- Measure operation counts, uploaded bytes per logical entry, peak memory, and p50/p99 latency. Include sustained workloads spanning many folds; the current quick benchmark's 60-write cases never reach their 64-entry compaction threshold.

## Phase 1: Protect object identity and snapshot lifetime

Goal:
Remove the demonstrated routes to acknowledged-history loss and make immutable-object ownership explicit. Addresses F1–F3 and snapshot publication collisions.

Scope:

- **1A — Safe collection contract:** Correct the README sweep procedure. Initially support orphan sweeping only after all writers and compaction/publication requests have been drained or stopped, no old handle can later install a pending fold, and the authoritative WAL has been reread. Then reopen writers. Keep deletion of snapshots proven superseded by a successful WAL transition. Retaining uncertain garbage is preferable to deleting possible live data.
- **1B — Immutable publication:** Use create-only snapshot PUTs with collision-resistant IDs and explicit collision handling. Retain enough candidate identity to handle ambiguous uploads/installations safely. Account for objects orphaned by failed uploads and dropped tasks. An existence check before install is not a substitute for a safe collection protocol.
- **1C — Cache identity:** Bind both WAL and snapshot cache records to stable backend namespace and full object key; validate that identity before trusting checksums/ETags. Built-in stores supply suitable namespaces and wrappers preserve them. Custom stores lacking stable identity must bypass persistent reuse safely. Prevent shared temporary-file races with unique temporary files or explicit ownership.
- **1D — FsStore coherence:** Use a key mapping that cannot alias another key or internal metadata, publish data plus validator in one atomic framed object, and require one shared store instance per root or reject conflicting opens. Keep this a development backend; state its crash/power-loss limitations explicitly.

Out of scope:
An online global garbage collector. If one is later required, design a CAS-backed generation/publication barrier first; rereading the WAL or adding an age threshold alone does not protect indefinitely pending folds.

Completion gate:
No supported collection operation or cache reuse can remove or substitute acknowledged history. Snapshot-key collisions cannot overwrite data. FsStore keys and validators remain independent under its documented ownership model.

Testing plan:

- Turn F1/F2/F3 recipes into regressions; test two resources with deliberately equal validators and same snapshot key but different bytes.
- Force snapshot-ID collisions, ambiguous upload/install, obsolete-fold discard, and a reader paused between WAL GET and snapshot GET. Cold reopen must always recover acknowledged entries after a supported operation.
- Test cache reuse across prefixes/backends, damaged caches, concurrent cache access, FsStore reserved-looking keys, conflicting root opens, and failure between object preparation and publication.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Doc | 1A: Safe collection procedure and snapshot lifetime rules | README offline sweep procedure; `tests/identity.rs::offline_sweep_after_draining_and_dropping_all_writers_preserves_recovery`. |
| Complete | Work | 1B: Create-only snapshot publication and ambiguous-outcome ownership | `src/wal.rs::publish_snapshot`; forced-collision unit test and `ambiguous_snapshot_upload_is_retained_and_never_installed_as_success`. |
| Complete | Work | 1C: Resource-bound cache and safe file publication | `src/cache.rs` identity framing, concurrent publication tests; `reused_cache_with_equal_etags_cannot_replace_another_stores_history`. |
| Complete | Work | 1D: Coherent FsStore key mapping, publication, and root ownership | `src/store.rs` framed objects, alias/publication/legacy tests and subprocess root-lock test; README compatibility notes. |
| Complete | Test | Identity, lifetime, and cold-recovery matrix | `tests/identity.rs` (5 tests with all features, 4 without); cache/store/publication unit regressions. |
| Complete | Gate | Supported operations preserve every acknowledged prefix | Independent Phase 1 review approved; all-feature (55 total), no-default (43 total), Clippy all-targets, and diff checks passed. |

## Phase 2: Bound storage operations and accepted data

Goal:
Prevent unbounded storage waits and stop accepting images or snapshots the library cannot read. Addresses F4/F5 and the ObjectStore contract.

Scope:

- **2A — Store contract and transport:** Specify atomic CAS and coherent reads, object-scoped validators, reserved keys, and durability on successful PUT. Add configurable S3 connection/read/write/request deadlines. Preserve HTTP status and operation context in structured errors and classify ambiguous mutations conservatively. Document DNS, user-callback, and custom-store limits instead of promising cancellation the synchronous API cannot enforce.
- **2B — Checked format and limits:** Validate snapshot LSN, next-LSN arithmetic, lengths, entry count, and total encoded size before mutating storage. Align PUT/GET limits for WAL images and snapshots. Bound decoded entry count/allocations as well as byte length. Use small configurable thresholds for tests; distinguish invalid stored data from a valid append rejected for size.
- **2C — Compaction lag behavior:** Add a hard live-image budget independent of `should_compact`. Return explicit backpressure/limit errors before CAS when a fold is slow or repeatedly fails; preserve the accepted prefix and permit recovery after maintenance succeeds. Validate options, including zero retry budgets.

Completion gate:
A successful append or fold is readable under the configured limits; malformed images return errors in debug and release. Configured S3 deadlines bound network waits within the documented transport limitations. Reaching a size budget never partially commits a batch.

Testing plan:

- Run codec corpus/property tests for truncation, overflow, extreme counts, and valid boundary round trips. Assert limit rejection makes no publication and leaves state/tip unchanged.
- Script slow headers/body, stalled writes, missing ETags, 304/404, 409/412, and ambiguous success responses through an HTTP test server. Verify conditional headers and error classification against [AWS's conditional-write behavior](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html).
- Hold/fail compaction while appending to the image limit; then release it and prove continued progress and cold recovery.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 2A: Store contract, S3 deadlines, structured storage errors | Missing: trait/configuration documentation, transport changes, and observable deadline behavior. |
| Incomplete | Work | 2B: Checked encoding/decoding and compatible read/write limits | Missing: fallible validation before publication and allocation bounds. |
| Incomplete | Work | 2C: Hard image budget and explicit overload behavior | Missing: limits independent of application compaction triggers and option validation. |
| Incomplete | Test | Codec boundaries, HTTP conformance, and compaction-lag recovery | Missing: repository tests covering each Phase 2 boundary in debug/release as applicable. |
| Incomplete | Gate | Accepted data is readable; network waits and growth are bounded | Missing: passing limit/deadline tests and baseline suites. |

## Phase 3: Make batch and maintenance outcomes explicit

Goal:
Let callers reason about an atomic batch and distinguish completed work from pending, rejected, or ambiguous work. Addresses F6/F7.

Scope:

- **3A — Whole-batch reconciliation:** Add a callback that receives the refreshed state and entire pending batch and can retry, replace the batch, or abort it as one operation. Preserve `write` as a convenience over a batch. Deprecate the per-entry path or keep it as an explicitly limited adapter for independent entries. Return the range for the final accepted batch. Do not require cloning arbitrary application state to simulate tentative application.
- **3B — Write/error contract:** Distinguish application abort, contention-budget exhaustion, input limits, storage failure, and unknown commit outcome. Preserve the attempted batch, including replacements, where a caller needs it to recover. Retry only outcomes known safe to retry; document application idempotency and refresh behavior after uncertainty. Report repeated snapshot-fetch races as contention unless the authoritative reference is demonstrably broken.
- **3C — Maintenance lifecycle:** Replace boolean/side-channel completion with an explicit result for idle, running/pending, installed, superseded, and failed work as appropriate. Flush must report budget exhaustion if work remains; close must surface compaction failure and define ownership/retry behavior on failure. Consolidate mutually exclusive running/ready states. Document drop behavior and never delete an ambiguously installed snapshot during cleanup.
- **3D — Application contract and migration:** Document replay/compaction equivalence, callback purity/panic assumptions, replica freshness, and the distinction between stale-writer CAS protection and exclusive writer ownership. Provide a migration example using a dependent batch and update examples to inspect maintenance outcomes.

Completion gate:
No successful completion result leaves promised work pending. A state-dependent batch can be rebuilt after a conflict without duplicate allocations or partial application. Ambiguous storage errors retain the documented durability uncertainty.

Testing plan:

- Race a batch allocating two IDs or consuming a shared quota against another writer; prove the whole replacement batch obeys the invariant and commits atomically.
- Exercise repeated conflicts, changed replacement-batch size, abort, final-attempt behavior, and ambiguous PUT after replacement.
- Test flush exhaustion, compactor errors/panics, idle flush, close while running, superseded folds, and drop after ambiguous installation. Verify old ambiguous-fold regressions still protect the live snapshot.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 3A: Whole-batch reconciliation and single-write adapter | Missing: API implementation and dependent-batch examples. |
| Incomplete | Work | 3B: Explicit write outcomes and recoverable attempted batches | Missing: error variants, uncertainty semantics, and retry tests. |
| Incomplete | Work | 3C: Explicit maintenance state and truthful flush/close | Missing: replacement lifecycle contract and failed-completion regressions. |
| Incomplete | Doc | 3D: Application guarantees and API migration | Missing: updated trait docs, examples, and release notes. |
| Incomplete | Test | Batch conflict and maintenance outcome matrix | Missing: tests for every Phase 3 success/failure outcome. |
| Incomplete | Gate | Completion results match durable and maintenance state | Missing: passing Phase 3 regressions and baseline suites. |

## Phase 4: Shorten the commit path and choose the scaling boundary

Goal:
Keep maintenance latency out of acknowledgement and establish the workload envelope of the current architecture. Addresses F8 and the whole-image rewrite cost.

Scope:

- **4A — Maintenance separation:** After a known successful CAS, update local committed state and return without waiting for remote garbage collection. Move proven-safe deletion and retry bookkeeping into bounded maintenance work with observable backlog/failures. Reuse a small worker or explicit maintenance mechanism; do not add an unbounded queue. Preserve snapshot-lifetime rules from Phase 1.
- **4B — Cache and memory cost:** Make caching optional, including opening when cache storage is unavailable. Measure checkpoint cadence versus caching every commit; keep cache lag harmless through identity/ETag validation. Reduce redundant full-image copies and live-entry byte scans where measurements support it. Consider shared immutable entry buffers to avoid copying every payload when compaction starts, without exposing implementation detail unnecessarily in the API.
- **4C — Useful group commit:** Update the example to bound both queued entries and bytes, and acknowledge producers only after batch success. Define shutdown, partial queue draining, and failure delivery. Keep this adapter optional; the core remains synchronous.
- **4D — Architecture decision:** Benchmark sustained batches, retained-image sizes, compaction lag, snapshot sizes, and replica catch-up. Record the supported envelope in bytes, memory, requests/entry, and p99 latency. Keep one CAS image if it meets the selected workload. If image reuploads dominate within that workload, write a separate design for immutable log segments plus a small CAS manifest, explicitly costing the extra publication request, reads, garbage collection, and migration. Do not silently change the durability point to local disk to improve throughput.

Completion gate:
An uncontended append with a prepared fold performs one foreground WAL CAS and no foreground snapshot DELETE. Cache policy is optional and correctness-neutral. Sustained measurements and a recorded workload target justify retaining or replacing the current layout; no unmeasured architecture rewrite is part of this phase.

Testing plan:

- Block DELETE after the WAL CAS and prove append acknowledgement is independent of release; then release/fail cleanup and verify eventual safe progress or an explicit backlog condition.
- Compare cache-disabled/every-commit/checkpoint policies and compaction startup memory cost. Verify cold recovery after cache loss and pending cache writes.
- Run enough iterations for at least ten installed folds per steady-state case, including slow compaction. Compare batches 1/8/64/256, small and large live images, and report bytes/entry, request counts, peak memory, and latency distributions. Record repeatable inputs and several runs, not a single peak number.
- Exercise a slow/failing store behind the bounded producer queue and verify acknowledgement, backpressure, and shutdown behavior.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 4A: Bounded maintenance off the acknowledgement path | Missing: foreground operation-count proof, deletion scheduling, and backlog visibility. |
| Incomplete | Work | 4B: Optional cache and measured copy/allocation reductions | Missing: cache policy implementation, recovery tests, and memory measurements. |
| Incomplete | Work | 4C: Bounded group-commit example with producer results | Missing: queue bounds and failure/shutdown validation. |
| Incomplete | Decision | 4D: Workload envelope and single-image/segmented-layout decision | Missing: explicit workload target, sustained measurements, and recorded tradeoffs. |
| Incomplete | Test | Maintenance latency, cache policies, sustained folds, producer pressure | Missing: deterministic blocked-delete test and repeatable benchmark artifacts. |
| Incomplete | Gate | One foreground CAS, bounded maintenance, evidenced architecture choice | Missing: operation counts, passing suites, and completed 4D decision. |

## Phase 5: Prove concurrent recovery independently

Goal:
Expand confidence beyond serial API interleavings and shared implementation assumptions. Addresses F9. Start each earlier phase's targeted tests with its fix; this phase supplies the broader reusable model and release automation.

Scope:

- **5A — Controlled overlap:** Add a small deterministic scheduling seam at store operations and compaction completion. Exercise writers/replicas overlapping snapshot fetch, upload, CAS, and deletion. Include crashes/drops while compaction is running, delayed responses after successful mutations, and same-seed reproducible traces. Avoid depending on thread timing or sleeps for these schedules.
- **5B — Independent commit oracle:** Record successful CAS histories and client acknowledgements independently of Replica replay. Check that acknowledged batches appear in full at their returned ranges, uncertain batches appear at most as complete attempts, and every live/restored state matches a committed prefix. Use a small independent test-format model instead of deriving truth only through production replay.
- **5C — Faulted object accounting:** Track live references, upload candidates, pending/uncertain installs, and known orphans by identity under faults. Prove missing live snapshots never get counted as acceptable garbage. Require cleanup to make progress once faults stop under the chosen maintenance policy.
- **5D — Release gates:** Add CI for feature configurations, Clippy, debug/release codec tests, deterministic regression schedules, and a bounded seed corpus. Add an opt-in real S3 conformance suite for competing conditional PUTs, conditional reads, immutable snapshots, and cold recovery; clearly record when credentials/environment prevent it from running. Carry forward format/API migration coverage.

Completion gate:
Every reviewed failure is represented by a corrected regression. Controlled overlapping schedules preserve the independent history and object-lifetime invariants, and ordinary CI runs them reproducibly. Before claiming actual S3 conformance for a release, record a passing run against the configured service.

Testing plan:

- Re-run all fixed reproductions as deterministic schedules, including pending-fold collection, cache aliasing, ambiguous replacement batches, and blocked cleanup.
- Prove same-seed repeatability; run an expanded seed corpus with fault recovery and both cold/warm reopen.
- Deliberately perturb acknowledgement ranges, replay, or snapshot accounting in temporary test mutations to confirm the independent oracle catches them. Record the CI and real-S3 results separately.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 5A: Deterministic operation-boundary overlap and active-task crashes | Missing: scheduler seam, representative schedules, and repeatability proof. |
| Incomplete | Work | 5B: Independent CAS/acknowledgement oracle | Missing: test model and positive/negative validation of atomic ranges and history. |
| Incomplete | Work | 5C: Object identity accounting under faults | Missing: candidate/uncertain/orphan model and post-fault cleanup evidence. |
| Incomplete | Work | 5D: CI, service conformance, and migration gates | Missing: checked-in automation and opt-in S3 integration suite. |
| Incomplete | Test | Expanded schedules, seed corpus, and oracle sensitivity checks | Missing: repeatable traces, mutation checks, and recorded results. |
| Incomplete | Gate | Independently verified concurrent recovery and release evidence | Missing: all prior gates, reproducible CI results, and actual S3 conformance evidence before that claim is made. |
