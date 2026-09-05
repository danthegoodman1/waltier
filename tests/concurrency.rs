#![cfg(feature = "sim")]
//! An independent CAS/acknowledgement oracle, including faulted object identity
//! accounting. Seeded calls settle compactors; channel schedules below overlap
//! real threads at publication and fetch boundaries without timing-based sleeps.
mod support;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::time::Duration;
use support::{ClientStore, History, HistoryApp, RecordingStore, check_ack, check_attempt, grows};
use tempfile::TempDir;
use waltier::sim::{Faults, Rng, SimStore};
use waltier::{
    CachePolicy, CompactionStatus, CondGet, CondPut, Entry, Lsn, MutationOutcome, ObjectStore,
    Options, Reconcile, Replica, StoreError, Stored, WalApp, WalError, WalTier,
};
const APP: HistoryApp = HistoryApp { compact_at: 5 };
const MANUAL: HistoryApp = HistoryApp {
    compact_at: u64::MAX,
};
const FAULTS: Faults = Faults {
    fail_clean: 0.08,
    fail_ambiguous: 0.06,
};
fn client(
    recording: &Arc<RecordingStore>,
    inner: Arc<dyn ObjectStore>,
    owner: u8,
) -> Arc<dyn ObjectStore> {
    recording.owner(owner, true, false);
    Arc::new(ClientStore {
        inner,
        recording: recording.clone(),
        owner,
    })
}
fn record_write<A: WalApp>(recording: &RecordingStore, wal: &mut WalTier<A>, batch: History) {
    recording.submitting(&batch);
    let result = wal.write_batch(batch.clone());
    // This helper is for settled calls. Drain background publication before
    // logging the returned acknowledgement so the logical trace has one order.
    if wal.compaction_running() {
        let _ = wal.wait_for_compaction();
    }
    match result {
        Ok(range) => recording.acknowledge(batch, range),
        Err(error) => recording.failed(batch, &error),
    }
}
fn settle<A: WalApp>(recording: &RecordingStore, owner: u8, wal: &mut WalTier<A>) {
    if wal.compaction_running() {
        let _ = wal.wait_for_compaction();
    }
    recording.owner(owner, true, wal.has_pending_fold());
}
fn allowed<T>(result: Result<T, WalError>, faults: bool) {
    if let Err(error) = result {
        assert!(
            (faults && matches!(error, WalError::Store(_) | WalError::Compaction(_)))
                || matches!(error, WalError::Compaction(ref message) if message.contains("is gone")),
            "{error}"
        );
    }
}
struct Run {
    trace: Vec<String>,
    acks: usize,
    unknown: usize,
    folds: usize,
    swept: usize,
    unknown_uploads: usize,
    unknown_installs: usize,
}
fn seeded(seed: u64, steps: usize, faults: bool) -> Run {
    let recording = Arc::new(RecordingStore::default());
    let faulted =
        Arc::new(SimStore::new(recording.clone()).with_faults(seed ^ 0xabcdef, Faults::default()));
    let stores: Vec<_> = (0..2)
        .map(|owner| client(&recording, faulted.clone(), owner))
        .collect();
    let mut directories: Vec<_> = (0..2).map(|_| TempDir::new().unwrap()).collect();
    let options = |path: &std::path::Path, owner: usize| {
        let mut opts = Options::new(path);
        opts.cache_policy = if owner == 0 {
            CachePolicy::EveryCommit
        } else {
            CachePolicy::OnFlush
        };
        opts.max_pending_deletes = 2;
        opts
    };
    let mut writers: Vec<_> = (0..2)
        .map(|owner| {
            Some(
                WalTier::open(
                    stores[owner].clone(),
                    APP,
                    options(directories[owner].path(), owner),
                )
                .unwrap(),
            )
        })
        .collect();
    let mut reader = Replica::open(faulted.clone(), APP, Options::default()).unwrap();
    if faults {
        faulted.set_faults(FAULTS);
    }
    let mut rng = Rng::new(seed);
    let mut command = 0;
    for step in 0..steps {
        let owner = rng.below(2) as usize;
        let operation = rng.below(100);
        if writers[owner].is_none() && operation < 85 {
            match WalTier::open(
                stores[owner].clone(),
                APP,
                options(directories[owner].path(), owner),
            ) {
                Ok(wal) => {
                    writers[owner] = Some(wal);
                    recording.owner(owner as u8, true, false);
                }
                Err(error) => allowed::<()>(Err(error), faults),
            }
        }
        if let Some(wal) = &mut writers[owner] {
            match operation {
                0..=44 => {
                    let batch = (0..1 + rng.below(3))
                        .map(|_| {
                            command += 1;
                            format!("seed{seed}-command{command}").into_bytes()
                        })
                        .collect();
                    record_write(&recording, wal, batch);
                    settle(&recording, owner as u8, wal);
                }
                45..=54 => allowed(wal.refresh(), faults),
                55..=66 => {
                    wal.compact_now();
                    settle(&recording, owner as u8, wal);
                }
                67..=75 => allowed(wal.flush(), faults),
                76..=84 => allowed(reader.refresh(), faults),
                85..=92 => {
                    assert!(!wal.compaction_running());
                    writers[owner] = None;
                    recording.owner(owner as u8, false, false);
                    if rng.chance(0.5) {
                        directories[owner] = TempDir::new().unwrap();
                    }
                }
                _ => {
                    allowed(wal.collect_garbage().map_err(WalError::Store), faults);
                }
            }
        }
        for (owner, wal) in writers.iter_mut().enumerate() {
            if let Some(wal) = wal {
                settle(&recording, owner as u8, wal);
                recording.assert_prefix(wal.state());
            }
        }
        recording.assert_prefix(reader.state());
        recording.audit().unwrap();
        if step % 13 == 0 {
            let cold = Replica::open(recording.clone(), APP, Options::default()).unwrap();
            assert_eq!(cold.state(), &recording.history());
        }
        if step % 17 == 0 {
            let warm = Replica::open(
                recording.clone(),
                APP,
                Options::new(directories[owner].path()),
            )
            .unwrap();
            assert_eq!(warm.state(), &recording.history());
        }
    }
    faulted.set_faults(Faults::default());
    for owner in 0..2 {
        let mut wal = writers[owner].take().unwrap_or_else(|| {
            WalTier::open(
                stores[owner].clone(),
                APP,
                options(directories[owner].path(), owner),
            )
            .unwrap()
        });
        recording.owner(owner as u8, true, wal.has_pending_fold());
        wal.refresh().unwrap();
        wal.take_compaction_error();
        wal.flush().unwrap();
        assert_eq!(
            wal.garbage_status().pending,
            0,
            "tracked cleanup progresses after faults stop"
        );
        record_write(
            &recording,
            &mut wal,
            vec![format!("seed{seed}-final{owner}").into_bytes()],
        );
        settle(&recording, owner as u8, &mut wal);
        wal.take_compaction_error();
        wal.close().unwrap();
        recording.owner(owner as u8, false, false);
    }
    drop(reader);
    recording.audit().unwrap();
    let inventory = recording.inventory();
    let unknown_uploads = inventory.uncertain_uploads.len();
    let unknown_installs = inventory.uncertain_installs.len();
    let swept = recording.offline_sweep();
    let cold = Replica::open(recording.clone(), APP, Options::default()).unwrap();
    let warm = Replica::open(recording.clone(), APP, Options::new(directories[0].path())).unwrap();
    assert_eq!(cold.state(), &recording.history());
    assert_eq!(warm.state(), cold.state());
    let (acks, unknown, folds, _) = recording.counts();
    Run {
        trace: recording.trace(),
        acks,
        unknown,
        folds,
        swept,
        unknown_uploads,
        unknown_installs,
    }
}
#[test]
fn independent_seed_corpus_checks_acknowledgements_and_object_identities_under_faults() {
    let selected = std::env::var("ORACLE_SEED")
        .ok()
        .map(|value| value.parse::<u64>().unwrap());
    let seeds = if selected.is_some() {
        1
    } else {
        std::env::var("ORACLE_SEEDS")
            .ok()
            .map(|value| value.parse::<u64>().unwrap())
            .unwrap_or(24)
    };
    let steps = std::env::var("ORACLE_STEPS")
        .ok()
        .map(|value| value.parse::<usize>().unwrap())
        .unwrap_or(200);
    assert!((1..=256).contains(&seeds) && (50..=2000).contains(&steps));
    let mut totals = [0usize; 6];
    let first = selected.unwrap_or(500);
    for seed in first..first + seeds {
        for faults in [false, true] {
            let run = seeded(seed, steps, faults);
            if std::env::var("ORACLE_TRACE").is_ok() {
                for event in &run.trace {
                    eprintln!("seed {seed} faults={faults}: {event}");
                }
            }
            for (total, value) in totals.iter_mut().zip([
                run.acks,
                run.unknown,
                run.folds,
                run.swept,
                run.unknown_uploads,
                run.unknown_installs,
            ]) {
                *total += value;
            }
        }
    }
    eprintln!(
        "independent corpus: {seeds} seeds × clean/faulted × {steps} steps; ack/unknown/fold/sweep/uncertain-upload/uncertain-install = {totals:?}"
    );
    assert!(
        selected.is_some() || totals.iter().all(|count| *count > 0),
        "corpus coverage ack/unknown/fold/sweep/upload/install: {totals:?}"
    );
}
#[test]
fn same_seed_has_the_same_logical_trace_after_snapshot_id_normalization() {
    let first = seeded(719, 180, true);
    let second = seeded(719, 180, true);
    if first.trace != second.trace {
        let index = first
            .trace
            .iter()
            .zip(&second.trace)
            .position(|(a, b)| a != b)
            .unwrap_or(first.trace.len().min(second.trace.len()));
        panic!(
            "logical trace differs at event {index}: {:?} vs {:?}; lengths {} vs {}",
            first.trace.get(index),
            second.trace.get(index),
            first.trace.len(),
            second.trace.len()
        );
    }
    assert_eq!(
        (first.acks, first.unknown, first.swept),
        (second.acks, second.unknown, second.swept)
    );
}
#[test]
fn oracle_rejects_bad_ranges_changed_history_partial_uncertainty_and_missing_live_snapshot() {
    let history = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
    assert!(check_ack(&history, 1..3, &history[1..]).is_ok());
    assert!(check_ack(&history, 0..2, &history[1..]).is_err());
    assert!(check_ack(&history, 1..2, &history[1..]).is_err());
    assert!(grows(&history, &vec![b"a".to_vec(), b"x".to_vec(), b"c".to_vec()]).is_err());
    assert!(grows(&history, &history[..2].to_vec()).is_err());
    assert!(grows(&history, &vec![b"b".to_vec(), b"a".to_vec(), b"c".to_vec()]).is_err());
    assert!(
        check_attempt(
            &history,
            &[b"c".to_vec(), b"b".to_vec()],
            MutationOutcome::Unknown
        )
        .is_err()
    );
    assert!(
        check_attempt(
            &vec![b"a".to_vec(), b"a".to_vec()],
            &[b"a".to_vec()],
            MutationOutcome::Unknown
        )
        .is_err()
    );
    assert!(
        check_attempt(
            &history,
            &[b"b".to_vec(), b"missing".to_vec()],
            MutationOutcome::Unknown
        )
        .is_err()
    );
    assert!(check_attempt(&history, &[b"b".to_vec()], MutationOutcome::NotApplied).is_err());
    let recording = Arc::new(RecordingStore::default());
    let mut wal = WalTier::open(
        client(&recording, recording.clone(), 0),
        MANUAL,
        Options::default(),
    )
    .unwrap();
    record_write(&recording, &mut wal, vec![b"original".to_vec()]);
    wal.compact_now();
    wal.wait_for_compaction().unwrap();
    let pending = recording.inventory().pending.into_iter().next().unwrap();
    assert!(
        recording
            .deletion_allowed(&pending)
            .unwrap_err()
            .contains("still-installable")
    );
    wal.close().unwrap();
    recording.owner(0, false, false);
    recording.audit().unwrap();
    recording.remove_live_unchecked();
    assert!(
        recording
            .audit()
            .unwrap_err()
            .contains("missing live snapshot")
    );
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Point {
    BeforeUpload,
    AfterWalPut,
    BeforeSnapshotGet,
}
struct Gate {
    point: Point,
    fired: AtomicBool,
    reached: mpsc::Sender<String>,
    release: Mutex<mpsc::Receiver<()>>,
}
impl Gate {
    fn stop(&self, key: &str, point: Point) {
        if self.point == point && !self.fired.swap(true, Ordering::SeqCst) {
            self.reached.send(key.into()).unwrap();
            self.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
        }
    }
}
struct GatedStore {
    inner: Arc<dyn ObjectStore>,
    gate: Gate,
}
impl ObjectStore for GatedStore {
    fn cache_namespace(&self) -> Option<String> {
        self.inner.cache_namespace()
    }
    fn get(&self, key: &str) -> Result<Option<Stored>, StoreError> {
        if key.starts_with("snap/") {
            self.gate.stop(key, Point::BeforeSnapshotGet);
        }
        self.inner.get(key)
    }
    fn get_if_changed(&self, key: &str, etag: Option<&str>) -> Result<CondGet, StoreError> {
        self.inner.get_if_changed(key, etag)
    }
    fn put_if_match(
        &self,
        key: &str,
        etag: Option<&str>,
        data: &[u8],
    ) -> Result<CondPut, StoreError> {
        if key.starts_with("snap/") {
            self.gate.stop(key, Point::BeforeUpload);
        }
        let result = self.inner.put_if_match(key, etag, data);
        if key == "wal" && etag.is_some() {
            self.gate.stop(key, Point::AfterWalPut);
        }
        result
    }
    fn put(&self, key: &str, data: &[u8]) -> Result<String, StoreError> {
        self.inner.put(key, data)
    }
    fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key)
    }
}
fn gated(
    inner: Arc<dyn ObjectStore>,
    point: Point,
) -> (
    Arc<dyn ObjectStore>,
    mpsc::Receiver<String>,
    mpsc::Sender<()>,
) {
    let (reached, notification) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    (
        Arc::new(GatedStore {
            inner,
            gate: Gate {
                point,
                fired: AtomicBool::new(false),
                reached,
                release: Mutex::new(wait),
            },
        }),
        notification,
        release,
    )
}

#[test]
fn another_writer_commits_while_snapshot_upload_is_held() {
    let recording = Arc::new(RecordingStore::default());
    let (held, reached, release) = gated(recording.clone(), Point::BeforeUpload);
    let mut first = WalTier::open(client(&recording, held, 0), MANUAL, Options::default()).unwrap();
    record_write(&recording, &mut first, vec![b"a".to_vec()]);
    assert!(first.compact_now());
    reached.recv_timeout(Duration::from_secs(5)).unwrap();
    let mut second = WalTier::open(
        client(&recording, recording.clone(), 1),
        MANUAL,
        Options::default(),
    )
    .unwrap();
    record_write(&recording, &mut second, vec![b"b".to_vec(), b"c".to_vec()]);
    assert_eq!(
        recording.history(),
        [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
    );
    recording.audit().unwrap();
    release.send(()).unwrap();
    assert_eq!(
        first.wait_for_compaction().unwrap(),
        CompactionStatus::Ready
    );
    recording.owner(0, true, true);
    assert_eq!(recording.inventory().pending.len(), 1);
    record_write(&recording, &mut first, vec![b"d".to_vec()]);
    recording.owner(0, true, false);
    recording.audit().unwrap();
    first.close().unwrap();
    second.close().unwrap();
    recording.owner(0, false, false);
    recording.owner(1, false, false);
    recording.offline_sweep();
    let cold = Replica::open(recording.clone(), MANUAL, Options::default()).unwrap();
    assert_eq!(cold.state(), &recording.history());
}

/// Final app destruction signals that a detached worker, including its upload,
/// is finished. No production hook or timing-based polling is needed.
struct DrainedApp {
    done: mpsc::Sender<()>,
}
impl Drop for DrainedApp {
    fn drop(&mut self) {
        let _ = self.done.send(());
    }
}
impl WalApp for DrainedApp {
    type State = History;
    fn init(&self) -> History {
        MANUAL.init()
    }
    fn apply(&self, state: &mut History, lsn: Lsn, entry: &[u8]) {
        MANUAL.apply(state, lsn, entry);
    }
    fn restore(&self, bytes: &[u8]) -> Result<History, WalError> {
        MANUAL.restore(bytes)
    }
    fn compact(&self, base: Option<&[u8]>, entries: &[Entry]) -> Result<Vec<u8>, WalError> {
        MANUAL.compact(base, entries)
    }
    fn reconcile(&self, _: &History, _: &[u8]) -> Reconcile {
        Reconcile::Retry
    }
}
#[test]
fn dropped_active_writer_is_drained_before_offline_orphan_sweep() {
    let recording = Arc::new(RecordingStore::default());
    let (held, reached, release) = gated(recording.clone(), Point::BeforeUpload);
    let (done, drained) = mpsc::channel();
    let mut writer = WalTier::open(
        client(&recording, held, 0),
        DrainedApp { done },
        Options::default(),
    )
    .unwrap();
    record_write(&recording, &mut writer, vec![b"durable".to_vec()]);
    writer.compact_now();
    reached.recv_timeout(Duration::from_secs(5)).unwrap();
    drop(writer);
    recording.owner(0, false, true);
    assert!(matches!(drained.try_recv(), Err(mpsc::TryRecvError::Empty)));
    release.send(()).unwrap();
    drained.recv_timeout(Duration::from_secs(5)).unwrap();
    recording.owner(0, false, false);
    recording.audit().unwrap();
    assert_eq!(recording.inventory().orphans.len(), 1);
    assert_eq!(recording.offline_sweep(), 1);
    assert_eq!(
        Replica::open(recording.clone(), MANUAL, Options::default())
            .unwrap()
            .state(),
        &recording.history()
    );
}
#[test]
fn delayed_lost_response_records_commit_before_other_writer_acknowledges() {
    let recording = Arc::new(RecordingStore::default());
    let faulted = Arc::new(SimStore::new(recording.clone()));
    let (held, reached, release) = gated(faulted.clone(), Point::AfterWalPut);
    let first = WalTier::open(client(&recording, held, 0), MANUAL, Options::default()).unwrap();
    faulted.fail_next_mutation_ambiguously("wal");
    recording.submitting(&vec![b"uncertain-a".to_vec(), b"uncertain-b".to_vec()]);
    let handle = std::thread::spawn(move || {
        let mut first = first;
        let result = first.write_batch(vec![b"uncertain-a".to_vec(), b"uncertain-b".to_vec()]);
        (first, result)
    });
    reached.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(
        recording.history().len(),
        2,
        "actual CAS precedes its lost response"
    );
    let mut second = WalTier::open(
        client(&recording, recording.clone(), 1),
        MANUAL,
        Options::default(),
    )
    .unwrap();
    record_write(&recording, &mut second, vec![b"acknowledged-c".to_vec()]);
    release.send(()).unwrap();
    let (first, result) = handle.join().unwrap();
    let error = result.unwrap_err();
    assert_eq!(error.outcome, MutationOutcome::Unknown);
    recording.failed(
        vec![b"uncertain-a".to_vec(), b"uncertain-b".to_vec()],
        &error,
    );
    recording.assert_prefix(first.state());
    recording.audit().unwrap();
    first.close().unwrap();
    second.close().unwrap();
    recording.owner(0, false, false);
    recording.owner(1, false, false);
    recording.offline_sweep();
}
#[test]
fn reader_retries_after_old_snapshot_is_deleted_between_wal_and_snapshot_gets() {
    let recording = Arc::new(RecordingStore::default());
    let mut writer = WalTier::open(
        client(&recording, recording.clone(), 0),
        MANUAL,
        Options::default(),
    )
    .unwrap();
    record_write(&recording, &mut writer, vec![b"a".to_vec()]);
    writer.compact_now();
    writer.flush().unwrap();
    recording.owner(0, true, false);
    let old = recording.inventory().live.unwrap();
    let (held, reached, release) = gated(recording.clone(), Point::BeforeSnapshotGet);
    let reader = std::thread::spawn(move || Replica::open(held, MANUAL, Options::default()));
    assert_eq!(reached.recv_timeout(Duration::from_secs(5)).unwrap(), old);
    record_write(&recording, &mut writer, vec![b"b".to_vec()]);
    writer.compact_now();
    writer.flush().unwrap();
    recording.owner(0, true, false);
    assert!(recording.get(&old).unwrap().is_none());
    release.send(()).unwrap();
    let reader = reader.join().unwrap().unwrap();
    assert_eq!(reader.state(), &recording.history());
    recording.audit().unwrap();
    writer.close().unwrap();
    recording.owner(0, false, false);
    drop(reader);
    recording.offline_sweep();
}
