//! Foreground latency boundaries and correctness-neutral optional caching.
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc,
};
use std::time::Duration;
use tempfile::TempDir;
use waltier::{
    CachePolicy, CompactionStatus, CondGet, CondPut, Entry, Lsn, MemoryStore, ObjectStore, Options,
    Reconcile, Replica, StoreError, Stored, WalApp, WalError, WalTier,
};

struct History;
impl WalApp for History {
    type State = Vec<u8>;
    fn init(&self) -> Self::State {
        vec![]
    }
    fn apply(&self, state: &mut Self::State, _: Lsn, entry: &[u8]) {
        state.extend_from_slice(entry);
    }
    fn restore(&self, snapshot: &[u8]) -> Result<Self::State, WalError> {
        Ok(snapshot.to_vec())
    }
    fn compact(&self, base: Option<&[u8]>, entries: &[Entry]) -> Result<Vec<u8>, WalError> {
        let mut snapshot = base.unwrap_or_default().to_vec();
        for entry in entries {
            snapshot.extend_from_slice(&entry.data);
        }
        Ok(snapshot)
    }
    fn reconcile(&self, _: &Self::State, _: &[u8]) -> Reconcile {
        Reconcile::Retry
    }
}
struct DeleteGate {
    started: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}
struct Store {
    inner: MemoryStore,
    wal_puts: AtomicUsize,
    deletes: AtomicUsize,
    cache_hits: AtomicUsize,
    known_namespace: bool,
    fail_delete: AtomicBool,
    gate: Option<DeleteGate>,
}
impl Store {
    fn new() -> Self {
        Self {
            inner: MemoryStore::new(),
            wal_puts: AtomicUsize::new(0),
            deletes: AtomicUsize::new(0),
            cache_hits: AtomicUsize::new(0),
            known_namespace: true,
            fail_delete: AtomicBool::new(false),
            gate: None,
        }
    }
}
impl ObjectStore for Store {
    fn cache_namespace(&self) -> Option<String> {
        self.known_namespace
            .then(|| self.inner.cache_namespace().unwrap())
    }
    fn get(&self, key: &str) -> Result<Option<Stored>, StoreError> {
        self.inner.get(key)
    }
    fn get_if_changed(&self, key: &str, etag: Option<&str>) -> Result<CondGet, StoreError> {
        let result = self.inner.get_if_changed(key, etag)?;
        if matches!(result, CondGet::NotModified) {
            self.cache_hits.fetch_add(1, Ordering::SeqCst);
        }
        Ok(result)
    }
    fn put_if_match(
        &self,
        key: &str,
        etag: Option<&str>,
        data: &[u8],
    ) -> Result<CondPut, StoreError> {
        if key == "wal" {
            self.wal_puts.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.put_if_match(key, etag, data)
    }
    fn put(&self, key: &str, data: &[u8]) -> Result<String, StoreError> {
        self.inner.put(key, data)
    }
    fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.deletes.fetch_add(1, Ordering::SeqCst);
        if let Some(gate) = &self.gate {
            gate.started.send(()).unwrap();
            gate.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
        }
        if self.fail_delete.load(Ordering::SeqCst) {
            return Err(StoreError::new("held DELETE failure"));
        }
        self.inner.delete(key)
    }
}
fn ready(wal: &mut WalTier<History>) {
    assert!(wal.compact_now());
    assert_eq!(wal.wait_for_compaction().unwrap(), CompactionStatus::Ready);
}
fn cold(store: &Arc<Store>, expected: &[u8]) {
    assert_eq!(
        Replica::open(store.clone(), History, Options::default())
            .unwrap()
            .state(),
        expected
    );
}
fn installing_writer(store: &Arc<Store>, opts: Options) -> WalTier<History> {
    let mut wal = WalTier::open(store.clone(), History, opts).unwrap();
    wal.write(b"a").unwrap();
    ready(&mut wal);
    wal.flush().unwrap();
    wal.write(b"b").unwrap();
    ready(&mut wal);
    wal
}

#[test]
fn prepared_fold_acknowledges_one_cas_before_any_blocked_delete() {
    let (started, notification) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    let store = Arc::new(Store {
        gate: Some(DeleteGate {
            started,
            release: Mutex::new(wait),
        }),
        ..Store::new()
    });
    let mut wal = installing_writer(&store, Options::default());
    store.wal_puts.store(0, Ordering::SeqCst);
    let (acked, ack) = mpsc::channel();
    let appender = std::thread::spawn(move || {
        let result = wal.write(b"c");
        acked.send((wal, result)).unwrap();
    });
    let received = ack.recv_timeout(Duration::from_secs(5));
    if received.is_err() {
        let _ = release.send(());
    } // Unblock a regressed implementation before failing.
    let (mut wal, result) = received.expect("append acknowledgement must not wait for DELETE");
    assert_eq!(result.unwrap(), 2);
    appender.join().unwrap();
    assert_eq!(store.wal_puts.load(Ordering::SeqCst), 1);
    assert_eq!(store.deletes.load(Ordering::SeqCst), 0);
    assert_eq!(wal.garbage_status().pending, 1);
    cold(&store, b"abc");
    let collector = std::thread::spawn(move || wal.collect_garbage());
    notification.recv_timeout(Duration::from_secs(5)).unwrap();
    release.send(()).unwrap();
    assert_eq!(collector.join().unwrap().unwrap().pending, 0);
    cold(&store, b"abc");
}

#[test]
fn cleanup_failure_stays_visible_and_retries_without_poisoning_append_success() {
    let store = Arc::new(Store::new());
    let mut wal = installing_writer(&store, Options::default());
    wal.write(b"c").unwrap();
    store.fail_delete.store(true, Ordering::SeqCst);
    assert!(wal.collect_garbage().is_err());
    assert_eq!(wal.garbage_status().pending, 1);
    assert!(
        wal.garbage_status()
            .last_error
            .unwrap()
            .contains("held DELETE")
    );
    let deletes = store.deletes.load(Ordering::SeqCst);
    assert_eq!(wal.write(b"d").unwrap(), 3);
    assert_eq!(store.deletes.load(Ordering::SeqCst), deletes);
    assert!(matches!(wal.flush(), Err(WalError::Store(_))));
    store.fail_delete.store(false, Ordering::SeqCst);
    let report = wal.flush().unwrap();
    assert_eq!(report.garbage.pending, 0);
    assert_eq!(report.garbage.last_error, None);
    cold(&store, b"abcd");
}

#[test]
fn cleanup_queue_cap_reports_offline_sweep_debt_even_on_consuming_close() {
    let store = Arc::new(Store::new());
    let opts = Options {
        max_pending_deletes: 1,
        ..Options::default()
    };
    let mut wal = WalTier::open(store.clone(), History, opts).unwrap();
    wal.write(b"a").unwrap();
    for entry in [b"b", b"c", b"d", b"e"] {
        ready(&mut wal);
        wal.write(entry).unwrap();
        assert!(wal.garbage_status().pending <= 1);
    }
    assert_eq!(store.deletes.load(Ordering::SeqCst), 0);
    assert_eq!(wal.garbage_status().overflowed, 2);
    let report = wal.close().unwrap();
    assert_eq!(report.garbage.pending, 0);
    assert_eq!(report.garbage.overflowed, 2);
    assert_eq!(
        store.inner.keys().len(),
        4,
        "WAL, live snapshot, two safe orphans"
    );
    cold(&store, b"abcde");
}

#[test]
fn disabled_and_unknown_namespace_cache_never_touch_the_requested_directory() {
    for unknown in [false, true] {
        let store = Arc::new(Store {
            known_namespace: !unknown,
            ..Store::new()
        });
        let root = TempDir::new().unwrap();
        let path = root.path().join("must-not-exist");
        let mut opts = Options::new(&path);
        if !unknown {
            opts.cache_policy = CachePolicy::Disabled;
        }
        let mut wal = WalTier::open(store.clone(), History, opts).unwrap();
        wal.write(b"a").unwrap();
        ready(&mut wal);
        wal.close().unwrap();
        assert!(!path.exists());
        cold(&store, b"a");
    }
    // Unknown namespaces do not clean up files belonging to an earlier user.
    let store = Arc::new(Store {
        known_namespace: false,
        ..Store::new()
    });
    let dir = TempDir::new().unwrap();
    let sentinel = dir.path().join("snap-previous");
    std::fs::write(&sentinel, b"leave untouched").unwrap();
    WalTier::open(store, History, Options::new(dir.path()))
        .unwrap()
        .close()
        .unwrap();
    assert_eq!(std::fs::read(sentinel).unwrap(), b"leave untouched");
}

#[test]
fn unavailable_cache_directory_does_not_prevent_writes_or_cold_recovery() {
    let store = Arc::new(Store::new());
    let root = TempDir::new().unwrap();
    let file = root.path().join("file");
    std::fs::write(&file, b"not a directory").unwrap();
    let mut wal = WalTier::open(store.clone(), History, Options::new(file.join("cache"))).unwrap();
    wal.write(b"a").unwrap();
    ready(&mut wal);
    wal.close().unwrap();
    assert_eq!(std::fs::read(file).unwrap(), b"not a directory");
    cold(&store, b"a");
}

#[test]
fn every_commit_and_on_flush_validate_fresh_and_stale_checkpoints() {
    for policy in [CachePolicy::EveryCommit, CachePolicy::OnFlush] {
        let store = Arc::new(Store::new());
        let dir = TempDir::new().unwrap();
        let mut opts = Options::new(dir.path());
        opts.cache_policy = policy;
        let mut wal = WalTier::open(store.clone(), History, opts).unwrap();
        wal.write(b"a").unwrap();
        let path = dir.path().join("wal.cache");
        assert_eq!(path.exists(), policy == CachePolicy::EveryCommit);
        wal.flush().unwrap();
        let checkpoint = std::fs::read(&path).unwrap();
        let mut reader_opts = Options::new(dir.path());
        reader_opts.cache_policy = CachePolicy::OnFlush;
        let mut warm = Replica::open(store.clone(), History, reader_opts).unwrap();
        assert_eq!(warm.state(), b"a");
        assert_eq!(store.cache_hits.load(Ordering::SeqCst), 1);
        wal.write(b"b").unwrap();
        assert_eq!(
            std::fs::read(&path).unwrap() == checkpoint,
            policy == CachePolicy::OnFlush
        );
        let mut reader_opts = Options::new(dir.path());
        reader_opts.cache_policy = CachePolicy::OnFlush;
        let stale = Replica::open(store.clone(), History, reader_opts).unwrap();
        assert_eq!(stale.state(), b"ab");
        assert_eq!(
            store.cache_hits.load(Ordering::SeqCst),
            if policy == CachePolicy::OnFlush { 1 } else { 2 }
        );
        warm.refresh().unwrap();
        warm.checkpoint_cache();
        assert_ne!(std::fs::read(&path).unwrap(), checkpoint);
        // Cache loss cannot erase durable state, even before OnFlush checkpoints.
        wal.write(b"c").unwrap();
        std::fs::remove_file(path).unwrap();
        drop(wal);
        cold(&store, b"abc");
    }
}

#[test]
fn on_flush_close_checkpoints_latest_image_and_ready_snapshot_is_already_cached() {
    let store = Arc::new(Store::new());
    let dir = TempDir::new().unwrap();
    let mut opts = Options::new(dir.path());
    opts.cache_policy = CachePolicy::OnFlush;
    let mut wal = WalTier::open(store.clone(), History, opts).unwrap();
    wal.write(b"a").unwrap();
    ready(&mut wal);
    assert!(!dir.path().join("wal.cache").exists());
    assert_eq!(
        std::fs::read_dir(dir.path()).unwrap().count(),
        1,
        "snapshot cached before Ready"
    );
    wal.write(b"b").unwrap();
    assert!(!dir.path().join("wal.cache").exists());
    wal.close().unwrap();
    let reader = Replica::open(store.clone(), History, Options::new(dir.path())).unwrap();
    assert_eq!(reader.state(), b"ab");
    assert_eq!(store.cache_hits.load(Ordering::SeqCst), 1);
}

#[test]
fn repeatedly_superseded_folds_do_not_accumulate_local_snapshot_cache_files() {
    let store = Arc::new(Store::new());
    let a_dir = TempDir::new().unwrap();
    let b_dir = TempDir::new().unwrap();
    let mut a = WalTier::open(store.clone(), History, Options::new(a_dir.path())).unwrap();
    let mut b = WalTier::open(store.clone(), History, Options::new(b_dir.path())).unwrap();
    for _ in 0..8 {
        a.write(b"a").unwrap();
        b.refresh().unwrap();
        ready(&mut a);
        ready(&mut b);
        a.flush().unwrap();
        assert_eq!(b.flush().unwrap().compaction, CompactionStatus::Superseded);
        assert!(
            std::fs::read_dir(b_dir.path()).unwrap().count() <= 2,
            "only WAL and at most current snapshot remain cached"
        );
    }
    cold(&store, b"aaaaaaaa");
}
