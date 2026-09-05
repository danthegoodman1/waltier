//! Batch ownership and maintenance outcomes under deterministic conflicts/faults.
use std::collections::VecDeque;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc,
};
use std::time::Duration;
use tempfile::TempDir;
use waltier::{
    CompactionStatus, CondPut, Entry, Lsn, MemoryStore, MutationOutcome, ObjectStore, Options,
    ReconcileBatch, Replica, StoreError, Stored, WalApp, WalError, WalTier,
};

#[derive(Clone, Copy)]
enum Action {
    Conflict,
    Unknown,
    Reject,
}
#[derive(Default)]
struct ScriptStore {
    inner: MemoryStore,
    actions: Mutex<VecDeque<Action>>,
    puts: AtomicUsize,
    fail_get: AtomicBool,
}
impl ScriptStore {
    fn script(&self, actions: impl IntoIterator<Item = Action>) {
        *self.actions.lock().unwrap() = actions.into_iter().collect();
        self.puts.store(0, Ordering::SeqCst);
    }
}
impl ObjectStore for ScriptStore {
    fn get(&self, key: &str) -> Result<Option<Stored>, StoreError> {
        if self.fail_get.load(Ordering::SeqCst) {
            return Err(StoreError::new("GET failed"));
        }
        self.inner.get(key)
    }
    fn put_if_match(
        &self,
        key: &str,
        etag: Option<&str>,
        data: &[u8],
    ) -> Result<CondPut, StoreError> {
        if key == "wal" && etag.is_some() {
            self.puts.fetch_add(1, Ordering::SeqCst);
            match self.actions.lock().unwrap().pop_front() {
                Some(Action::Conflict) => return Ok(CondPut::PreconditionFailed),
                Some(Action::Reject) => return Err(StoreError::new("not sent").not_applied()),
                Some(Action::Unknown) => {
                    assert!(matches!(
                        self.inner.put_if_match(key, etag, data)?,
                        CondPut::Ok { .. }
                    ));
                    return Err(StoreError::new("reply lost"));
                }
                None => {}
            }
        }
        self.inner.put_if_match(key, etag, data)
    }
    fn put(&self, key: &str, data: &[u8]) -> Result<String, StoreError> {
        self.inner.put(key, data)
    }
    fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key)
    }
}

#[derive(Clone, Copy)]
enum Mode {
    Allocate,
    Count(usize),
    Retry,
    Abort,
}
struct Gate {
    started: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}
struct App {
    mode: Mode,
    reconciles: Arc<AtomicUsize>,
    compact_failure: Arc<AtomicUsize>,
    gate: Option<Gate>,
    automatic: bool,
}
impl App {
    fn new(mode: Mode) -> Self {
        Self {
            mode,
            reconciles: Arc::new(AtomicUsize::new(0)),
            compact_failure: Arc::new(AtomicUsize::new(0)),
            gate: None,
            automatic: false,
        }
    }
}
fn batch(first: u64, count: usize) -> Vec<Vec<u8>> {
    (first..first + count as u64)
        .map(|id| id.to_le_bytes().to_vec())
        .collect()
}
impl WalApp for App {
    type State = Vec<u64>;
    fn init(&self) -> Self::State {
        vec![]
    }
    fn apply(&self, state: &mut Self::State, _: Lsn, entry: &[u8]) {
        let id = u64::from_le_bytes(entry.try_into().unwrap());
        assert_eq!(
            id,
            state.len() as u64,
            "allocations must be unique and contiguous"
        );
        state.push(id);
    }
    fn reconcile_batch(&self, state: &Self::State, pending: &[Vec<u8>]) -> ReconcileBatch {
        self.reconciles.fetch_add(1, Ordering::SeqCst);
        match self.mode {
            Mode::Allocate => ReconcileBatch::Replace(batch(state.len() as u64, pending.len())),
            Mode::Count(count) => ReconcileBatch::Replace(batch(state.len() as u64, count)),
            Mode::Retry => ReconcileBatch::Retry,
            Mode::Abort => ReconcileBatch::Abort,
        }
    }
    fn should_compact(&self, _: &waltier::WalStats) -> bool {
        self.automatic
    }
    fn restore(&self, bytes: &[u8]) -> Result<Self::State, WalError> {
        Ok(bytes
            .chunks_exact(8)
            .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
            .collect())
    }
    fn compact(&self, base: Option<&[u8]>, entries: &[Entry]) -> Result<Vec<u8>, WalError> {
        if let Some(gate) = &self.gate {
            gate.started.send(()).unwrap();
            gate.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
        }
        match self.compact_failure.load(Ordering::SeqCst) {
            1 => return Err(WalError::App("deliberate compactor failure".into())),
            2 => panic!("deliberate compactor panic"),
            _ => {}
        }
        let mut bytes = base.unwrap_or_default().to_vec();
        for entry in entries {
            bytes.extend_from_slice(&entry.data);
        }
        Ok(bytes)
    }
}
fn writer(store: &Arc<ScriptStore>, app: App) -> (WalTier<App>, TempDir) {
    let dir = TempDir::new().unwrap();
    (
        WalTier::open(store.clone(), app, Options::new(dir.path())).unwrap(),
        dir,
    )
}
fn assert_cold(store: &Arc<ScriptStore>, expected: &[u64]) {
    let dir = TempDir::new().unwrap();
    let reader = Replica::open(
        store.clone(),
        App::new(Mode::Abort),
        Options::new(dir.path()),
    )
    .unwrap();
    assert_eq!(reader.state(), expected);
}

#[test]
fn dependent_batch_rebuilds_both_ids_against_one_refreshed_state() {
    let store = Arc::new(ScriptStore::default());
    let (mut a, _da) = writer(&store, App::new(Mode::Abort));
    let (mut b, _db) = writer(&store, App::new(Mode::Allocate));
    a.write(batch(0, 1).remove(0)).unwrap();
    assert_eq!(b.write_batch(batch(0, 2)).unwrap(), 1..3);
    assert_eq!(b.state(), &[0, 1, 2]);
    assert_cold(&store, &[0, 1, 2]);
}

#[test]
fn replacement_count_controls_returned_range_and_zero_is_a_noop() {
    for count in [0, 3] {
        let store = Arc::new(ScriptStore::default());
        let (mut a, _da) = writer(&store, App::new(Mode::Abort));
        let (mut b, _db) = writer(&store, App::new(Mode::Count(count)));
        a.write(batch(0, 1).remove(0)).unwrap();
        store.puts.store(0, Ordering::SeqCst);
        assert_eq!(b.write_batch(batch(0, 2)).unwrap(), 1..1 + count as u64);
        assert_eq!(
            store.puts.load(Ordering::SeqCst),
            if count == 0 { 1 } else { 2 }
        );
        assert_cold(&store, &(0..1 + count as u64).collect::<Vec<_>>());
    }
}

#[test]
fn single_write_rejects_non_single_replacement_before_publication() {
    for count in [0, 2] {
        let store = Arc::new(ScriptStore::default());
        let (mut a, _da) = writer(&store, App::new(Mode::Abort));
        let (mut b, _db) = writer(&store, App::new(Mode::Count(count)));
        a.write(batch(0, 1).remove(0)).unwrap();
        store.puts.store(0, Ordering::SeqCst);
        let error = b.write(batch(0, 1).remove(0)).unwrap_err();
        assert!(matches!(error.source, WalError::InvalidReplacement { actual } if actual == count));
        assert_eq!(error.entries, batch(1, count));
        assert_eq!(error.outcome, MutationOutcome::NotApplied);
        assert_eq!(store.puts.load(Ordering::SeqCst), 1);
        assert_cold(&store, &[0]);
    }
}

#[test]
fn abort_preserves_the_entire_pending_batch() {
    let store = Arc::new(ScriptStore::default());
    let (mut wal, _dir) = writer(&store, App::new(Mode::Abort));
    store.script([Action::Conflict]);
    let error = wal.write_batch(batch(0, 2)).unwrap_err();
    assert!(matches!(error.source, WalError::ReconcileAborted));
    assert_eq!(error.entries, batch(0, 2));
    assert_eq!(error.outcome, MutationOutcome::NotApplied);
    assert_cold(&store, &[]);
}

#[test]
fn final_attempt_can_succeed_and_exhaustion_does_not_reconcile_an_unattempted_batch() {
    for conflicts in [2, 3] {
        let store = Arc::new(ScriptStore::default());
        let app = App::new(Mode::Retry);
        let reconciles = app.reconciles.clone();
        let dir = TempDir::new().unwrap();
        let mut opts = Options::new(dir.path());
        opts.max_write_attempts = 3;
        let mut wal = WalTier::open(store.clone(), app, opts).unwrap();
        store.script(std::iter::repeat_n(Action::Conflict, conflicts));
        let result = wal.write_batch(batch(0, 2));
        if conflicts == 2 {
            assert_eq!(result.unwrap(), 0..2);
            assert_cold(&store, &[0, 1]);
        } else {
            let error = result.unwrap_err();
            assert!(matches!(
                error.source,
                WalError::Contention {
                    operation: "write",
                    attempts: 3
                }
            ));
            assert_eq!(error.entries, batch(0, 2));
            assert_eq!(error.outcome, MutationOutcome::NotApplied);
            assert_cold(&store, &[]);
        }
        assert_eq!(store.puts.load(Ordering::SeqCst), 3);
        assert_eq!(reconciles.load(Ordering::SeqCst), 2);
    }
}

#[test]
fn uncertain_put_retains_replacement_and_never_retries_it() {
    let store = Arc::new(ScriptStore::default());
    let (mut a, _da) = writer(&store, App::new(Mode::Abort));
    let (mut b, _db) = writer(&store, App::new(Mode::Allocate));
    a.write(batch(0, 1).remove(0)).unwrap();
    store.script([Action::Conflict, Action::Unknown]);
    let error = b.write_batch(batch(0, 2)).unwrap_err();
    assert_eq!(error.outcome, MutationOutcome::Unknown);
    assert_eq!(error.entries, batch(1, 2));
    assert!(matches!(error.source, WalError::Store(_)));
    assert_eq!(store.puts.load(Ordering::SeqCst), 2);
    assert_eq!(b.state(), &[0], "uncertain entries are not applied locally");
    assert_cold(&store, &[0, 1, 2]);
    b.refresh().unwrap();
    assert_eq!(b.state(), &[0, 1, 2]);
}

#[test]
fn rejected_put_and_failed_refresh_are_known_not_applied() {
    for get_failure in [false, true] {
        let store = Arc::new(ScriptStore::default());
        let (mut wal, _dir) = writer(&store, App::new(Mode::Retry));
        store.script([if get_failure {
            Action::Conflict
        } else {
            Action::Reject
        }]);
        store.fail_get.store(get_failure, Ordering::SeqCst);
        let error = wal.write_batch(batch(0, 2)).unwrap_err();
        assert_eq!(error.outcome, MutationOutcome::NotApplied);
        assert_eq!(error.entries, batch(0, 2));
        let WalError::Store(source) = error.source else {
            panic!("expected storage failure")
        };
        assert_eq!(
            source.mutation_outcome,
            if get_failure {
                MutationOutcome::Unknown
            } else {
                MutationOutcome::NotApplied
            }
        );
        store.fail_get.store(false, Ordering::SeqCst);
        assert_cold(&store, &[]);
    }
}

#[test]
fn limit_failure_retains_the_replacement_batch() {
    let store = Arc::new(ScriptStore::default());
    let (mut a, _da) = writer(&store, App::new(Mode::Abort));
    let dir = TempDir::new().unwrap();
    let mut opts = Options::new(dir.path());
    opts.max_live_entries = 2;
    let mut b = WalTier::open(store.clone(), App::new(Mode::Count(3)), opts).unwrap();
    a.write(batch(0, 1).remove(0)).unwrap();
    let error = b.write_batch(batch(0, 1)).unwrap_err();
    assert!(matches!(error.source, WalError::LimitExceeded { .. }));
    assert_eq!(error.entries, batch(1, 3));
    assert_eq!(error.outcome, MutationOutcome::NotApplied);
    assert_cold(&store, &[0]);
}

#[test]
fn idle_ready_installed_and_superseded_are_explicit() {
    let store = Arc::new(ScriptStore::default());
    let (mut a, _da) = writer(&store, App::new(Mode::Retry));
    assert_eq!(a.wait_for_compaction().unwrap(), CompactionStatus::Idle);
    assert_eq!(a.flush().unwrap().compaction, CompactionStatus::Idle);
    a.write_batch(batch(0, 2)).unwrap();
    let (mut b, _db) = writer(&store, App::new(Mode::Retry));
    assert!(a.compact_now());
    assert!(b.compact_now());
    assert_eq!(a.wait_for_compaction().unwrap(), CompactionStatus::Ready);
    assert_eq!(b.wait_for_compaction().unwrap(), CompactionStatus::Ready);
    assert_eq!(a.flush().unwrap().compaction, CompactionStatus::Installed);
    assert_eq!(b.flush().unwrap().compaction, CompactionStatus::Superseded);
    assert!(!b.has_pending_fold());
    assert_cold(&store, &[0, 1]);
}

#[test]
fn flush_exhaustion_preserves_the_ready_fold_for_retry() {
    let store = Arc::new(ScriptStore::default());
    let dir = TempDir::new().unwrap();
    let mut opts = Options::new(dir.path());
    opts.max_write_attempts = 3;
    let mut wal = WalTier::open(store.clone(), App::new(Mode::Retry), opts).unwrap();
    wal.write_batch(batch(0, 2)).unwrap();
    assert!(wal.compact_now());
    assert_eq!(wal.wait_for_compaction().unwrap(), CompactionStatus::Ready);
    store.script([Action::Conflict; 3]);
    assert!(matches!(
        wal.flush(),
        Err(WalError::Contention {
            operation: "flush",
            attempts: 3
        })
    ));
    assert_eq!(store.puts.load(Ordering::SeqCst), 3);
    assert_eq!(wal.compaction_status().unwrap(), CompactionStatus::Ready);
    assert!(wal.stats().snapshot_lsn.is_none());
    assert_cold(&store, &[0, 1]);
    assert_eq!(wal.flush().unwrap().compaction, CompactionStatus::Installed);
    assert_eq!(wal.stats().snapshot_lsn, Some(1));
}

#[test]
fn compactor_errors_and_panics_survive_wait_flush_and_close() {
    for failure in [1, 2] {
        let store = Arc::new(ScriptStore::default());
        let app = App::new(Mode::Retry);
        app.compact_failure.store(failure, Ordering::SeqCst);
        let (mut wal, _dir) = writer(&store, app);
        wal.write_batch(batch(0, 2)).unwrap();
        assert!(wal.compact_now());
        assert!(matches!(
            wal.wait_for_compaction(),
            Err(WalError::Compaction(_))
        ));
        assert!(!wal.compaction_running());
        assert!(!wal.has_pending_fold());
        assert!(matches!(wal.flush(), Err(WalError::Compaction(_))));
        assert!(matches!(wal.close(), Err(WalError::Compaction(_))));
        assert_cold(&store, &[0, 1]);
    }
}

#[test]
fn compaction_failure_can_be_acknowledged_or_retried() {
    let store = Arc::new(ScriptStore::default());
    let app = App::new(Mode::Retry);
    let failure = app.compact_failure.clone();
    failure.store(1, Ordering::SeqCst);
    let (mut wal, _dir) = writer(&store, app);
    wal.write_batch(batch(0, 2)).unwrap();
    assert!(wal.compact_now());
    wal.wait_for_compaction().unwrap_err();
    assert!(wal.take_compaction_error().unwrap().contains("deliberate"));
    assert_eq!(wal.flush().unwrap().compaction, CompactionStatus::Idle);
    assert!(wal.compact_now());
    wal.wait_for_compaction().unwrap_err();
    failure.store(0, Ordering::SeqCst);
    assert!(wal.compact_now(), "starting new work also clears the error");
    assert_eq!(wal.close().unwrap().compaction, CompactionStatus::Installed);
    assert_cold(&store, &[0, 1]);
}

#[test]
fn close_waits_for_running_compaction_then_installs_it() {
    let store = Arc::new(ScriptStore::default());
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut app = App::new(Mode::Retry);
    app.gate = Some(Gate {
        started: started_tx,
        release: Mutex::new(release_rx),
    });
    let (mut wal, _dir) = writer(&store, app);
    wal.write_batch(batch(0, 2)).unwrap();
    assert!(wal.compact_now());
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(wal.compaction_status().unwrap(), CompactionStatus::Running);
    let closing = std::thread::spawn(move || wal.close());
    assert_cold(&store, &[0, 1]);
    release_tx.send(()).unwrap();
    assert_eq!(
        closing.join().unwrap().unwrap().compaction,
        CompactionStatus::Installed
    );
    let dir = TempDir::new().unwrap();
    let reader = Replica::open(store, App::new(Mode::Abort), Options::new(dir.path())).unwrap();
    assert_eq!(reader.stats().snapshot_lsn, Some(1));
    assert_eq!(reader.state(), &[0, 1]);
}

/// Returns images whose snapshots disappeared. A changing reference models
/// bounded concurrent compaction races; a stable reference is broken history.
struct MissingSnapshots {
    calls: AtomicUsize,
    changing: bool,
    enabled: AtomicBool,
}
impl ObjectStore for MissingSnapshots {
    fn get(&self, key: &str) -> Result<Option<Stored>, StoreError> {
        if key != "wal" {
            return Ok(None);
        }
        if !self.enabled.load(Ordering::SeqCst) {
            return Ok(Some(Stored {
                data: b"WTL1\0\0\0\0\0".to_vec(),
                etag: "empty".into(),
            }));
        }
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let version = if self.changing { call } else { 0 };
        let key = format!("snap/{version}");
        let mut data = b"WTL1\x01".to_vec();
        data.extend_from_slice(&(version as u64).to_le_bytes());
        data.extend_from_slice(&(key.len() as u32).to_le_bytes());
        data.extend_from_slice(key.as_bytes());
        data.extend_from_slice(&0u32.to_le_bytes());
        Ok(Some(Stored {
            data,
            etag: version.to_string(),
        }))
    }
    fn put_if_match(&self, _: &str, _: Option<&str>, _: &[u8]) -> Result<CondPut, StoreError> {
        unreachable!()
    }
    fn put(&self, _: &str, _: &[u8]) -> Result<String, StoreError> {
        unreachable!()
    }
    fn delete(&self, _: &str) -> Result<(), StoreError> {
        unreachable!()
    }
}
#[test]
fn changing_missing_snapshot_is_contention_but_stable_missing_reference_is_corrupt() {
    for changing in [false, true] {
        for refresh in [false, true] {
            let store = Arc::new(MissingSnapshots {
                calls: AtomicUsize::new(0),
                changing,
                enabled: AtomicBool::new(!refresh),
            });
            let dir = TempDir::new().unwrap();
            let result = if refresh {
                let mut reader = Replica::open(
                    store.clone(),
                    App::new(Mode::Abort),
                    Options::new(dir.path()),
                )
                .unwrap();
                store.enabled.store(true, Ordering::SeqCst);
                reader.refresh().map(|_| ())
            } else {
                Replica::open(
                    store.clone(),
                    App::new(Mode::Abort),
                    Options::new(dir.path()),
                )
                .map(|_| ())
            };
            if changing {
                assert!(matches!(
                    result,
                    Err(WalError::Contention { attempts: 8, .. })
                ));
                assert_eq!(store.calls.load(Ordering::SeqCst), 8);
            } else {
                assert!(
                    matches!(result, Err(WalError::Corrupt(message)) if message.contains("missing snapshot snap/0"))
                );
                assert_eq!(store.calls.load(Ordering::SeqCst), 2);
            }
        }
    }
}

#[test]
fn automatic_trigger_preserves_failure_until_explicit_retry() {
    let store = Arc::new(ScriptStore::default());
    let mut app = App::new(Mode::Retry);
    app.automatic = true;
    let failure = app.compact_failure.clone();
    failure.store(1, Ordering::SeqCst);
    let (mut wal, _dir) = writer(&store, app);
    wal.write_batch(batch(0, 1)).unwrap();
    wal.wait_for_compaction().unwrap_err();
    failure.store(0, Ordering::SeqCst);
    wal.write_batch(batch(1, 1)).unwrap();
    assert!(matches!(
        wal.compaction_status(),
        Err(WalError::Compaction(_))
    ));
    assert!(matches!(wal.flush(), Err(WalError::Compaction(_))));
    assert_cold(&store, &[0, 1]);
    assert!(wal.compact_now());
    assert_eq!(wal.close().unwrap().compaction, CompactionStatus::Installed);
}

#[test]
fn consuming_close_reports_exhaustion_and_preserves_candidate_object() {
    let store = Arc::new(ScriptStore::default());
    let (mut wal, _dir) = writer(&store, App::new(Mode::Retry));
    wal.write_batch(batch(0, 1)).unwrap();
    assert!(wal.compact_now());
    assert_eq!(wal.wait_for_compaction().unwrap(), CompactionStatus::Ready);
    let candidates = store.inner.keys();
    assert_eq!(candidates.len(), 2);
    store.script([Action::Conflict; 8]);
    assert!(matches!(
        wal.close(),
        Err(WalError::Contention {
            operation: "flush",
            attempts: 8
        })
    ));
    assert_eq!(store.inner.keys(), candidates);
    assert_cold(&store, &[0]);
}

#[test]
fn drop_after_uncertain_fold_install_preserves_the_live_snapshot() {
    for append in [false, true] {
        let store = Arc::new(ScriptStore::default());
        let (mut wal, _dir) = writer(&store, App::new(Mode::Retry));
        wal.write_batch(batch(0, 1)).unwrap();
        assert!(wal.compact_now());
        assert_eq!(wal.wait_for_compaction().unwrap(), CompactionStatus::Ready);
        store.script([Action::Unknown]);
        if append {
            let error = wal.write_batch(batch(1, 1)).unwrap_err();
            assert_eq!(error.outcome, MutationOutcome::Unknown);
            assert_eq!(error.entries, batch(1, 1));
        } else {
            let error = wal.flush().unwrap_err();
            assert!(
                matches!(error, WalError::Store(source) if source.mutation_outcome == MutationOutcome::Unknown)
            );
        }
        assert!(wal.has_pending_fold());
        drop(wal); // No refresh/adoption before dropping the uncertain owner.
        assert_cold(&store, if append { &[0, 1] } else { &[0] });
        let (reader, _dir) = writer(&store, App::new(Mode::Abort));
        assert_eq!(reader.stats().snapshot_lsn, Some(0));
        assert_eq!(
            store.inner.keys().len(),
            2,
            "live snapshot must survive drop"
        );
    }
}
