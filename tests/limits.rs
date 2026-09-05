use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc,
};
use std::time::Duration;

use tempfile::TempDir;
use waltier::{
    CondGet, CondPut, Entry, Lsn, MemoryStore, ObjectStore, Options, Replica, StoreError, WalApp,
    WalError, WalStats, WalTier,
};

#[derive(Default)]
struct CountingStore {
    inner: MemoryStore,
    puts: AtomicUsize,
    limit: Option<usize>,
}
impl ObjectStore for CountingStore {
    fn max_object_bytes(&self) -> Option<usize> {
        self.limit
    }
    fn get(&self, key: &str) -> Result<Option<waltier::Stored>, StoreError> {
        self.inner.get(key)
    }
    fn put_if_match(
        &self,
        key: &str,
        etag: Option<&str>,
        data: &[u8],
    ) -> Result<CondPut, StoreError> {
        self.puts.fetch_add(1, Ordering::SeqCst);
        self.inner.put_if_match(key, etag, data)
    }
    fn put(&self, key: &str, data: &[u8]) -> Result<String, StoreError> {
        self.inner.put(key, data)
    }
    fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key)
    }
}

struct History {
    gate: Option<(mpsc::Sender<()>, Mutex<mpsc::Receiver<()>>)>,
    fail: Arc<AtomicBool>,
    automatic: bool,
}
impl History {
    fn plain() -> Self {
        Self {
            gate: None,
            fail: Arc::new(AtomicBool::new(false)),
            automatic: false,
        }
    }
}
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
        if let Some((started, release)) = &self.gate {
            let _ = started.send(());
            release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
        }
        if self.fail.load(Ordering::SeqCst) {
            return Err(WalError::App("held failure".into()));
        }
        let mut result = base.unwrap_or_default().to_vec();
        for entry in entries {
            result.extend_from_slice(&entry.data);
        }
        Ok(result)
    }
    fn should_compact(&self, _: &WalStats) -> bool {
        self.automatic
    }
}

fn options(dir: &TempDir, bytes: usize, entries: usize) -> Options {
    let mut opts = Options::new(dir.path());
    opts.max_image_bytes = bytes;
    opts.max_live_entries = entries;
    opts
}

#[test]
fn image_byte_boundary_rejects_whole_batch_before_publication() {
    let store = Arc::new(CountingStore::default());
    let dir = TempDir::new().unwrap();
    let mut wal = WalTier::open(store.clone(), History::plain(), options(&dir, 19, 100)).unwrap();
    assert_eq!(
        wal.write_batch(vec![b"a".to_vec(), b"b".to_vec()]).unwrap(),
        0..2
    );
    assert_eq!(wal.stats().image_bytes, 19);
    let puts = store.puts.load(Ordering::SeqCst);
    assert!(matches!(
        wal.write_batch(vec![b"c".to_vec(), b"d".to_vec()]),
        Err(waltier::WriteError {
            source: WalError::LimitExceeded {
                resource: "WAL image bytes",
                ..
            },
            ..
        })
    ));
    assert_eq!(store.puts.load(Ordering::SeqCst), puts);
    assert_eq!(wal.tip(), Some(1));
    assert_eq!(wal.state(), b"ab");
    let cold = TempDir::new().unwrap();
    let reader = Replica::open(store, History::plain(), options(&cold, 19, 100)).unwrap();
    assert_eq!(reader.state(), b"ab");
}

#[test]
fn held_compaction_applies_backpressure_then_recovers() {
    let store = Arc::new(CountingStore::default());
    let dir = TempDir::new().unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let app = History {
        gate: Some((started_tx, Mutex::new(release_rx))),
        automatic: false,
        ..History::plain()
    };
    let mut wal = WalTier::open(store.clone(), app, options(&dir, 1024, 2)).unwrap();
    wal.write(b"a").unwrap();
    assert!(wal.compact_now());
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    wal.write(b"b").unwrap();
    let puts = store.puts.load(Ordering::SeqCst);
    assert!(matches!(
        wal.write(b"c"),
        Err(waltier::WriteError {
            source: WalError::LimitExceeded {
                resource: "live entries",
                ..
            },
            ..
        })
    ));
    assert_eq!(store.puts.load(Ordering::SeqCst), puts);
    assert_eq!(wal.state(), b"ab");
    release_tx.send(()).unwrap();
    assert!(wal.wait_for_compaction().unwrap() == waltier::CompactionStatus::Ready);
    assert_eq!(wal.write(b"c").unwrap(), 2);
    assert_eq!(wal.stats().live_entries, 2);
    let cold = TempDir::new().unwrap();
    let reader = Replica::open(store, History::plain(), options(&cold, 1024, 2)).unwrap();
    assert_eq!(reader.state(), b"abc");
}

#[test]
fn repeated_compaction_failure_cannot_grow_past_the_budget() {
    let store = Arc::new(CountingStore::default());
    let dir = TempDir::new().unwrap();
    let fail = Arc::new(AtomicBool::new(true));
    let app = History {
        fail: fail.clone(),
        ..History::plain()
    };
    let mut wal = WalTier::open(store.clone(), app, options(&dir, 1024, 1)).unwrap();
    wal.write(b"a").unwrap();
    for _ in 0..3 {
        assert!(wal.compact_now());
        assert!(wal.wait_for_compaction().is_err());
        assert!(matches!(
            wal.write(b"b"),
            Err(waltier::WriteError {
                source: WalError::LimitExceeded { .. },
                ..
            })
        ));
    }
    assert_eq!(
        store.puts.load(Ordering::SeqCst),
        2,
        "only creation and acknowledged append"
    );
    fail.store(false, Ordering::SeqCst);
    assert!(wal.compact_now());
    assert!(wal.wait_for_compaction().unwrap() == waltier::CompactionStatus::Ready);
    wal.write(b"b").unwrap();
    let cold = TempDir::new().unwrap();
    assert_eq!(
        Replica::open(store, History::plain(), options(&cold, 1024, 1))
            .unwrap()
            .state(),
        b"ab"
    );
}

#[test]
fn oversized_snapshot_is_never_uploaded_or_installed() {
    let store = Arc::new(CountingStore::default());
    let dir = TempDir::new().unwrap();
    let mut opts = options(&dir, 1024, 10);
    opts.max_snapshot_bytes = 1;
    let mut wal = WalTier::open(store.clone(), History::plain(), opts).unwrap();
    wal.write(b"ab").unwrap();
    let puts = store.puts.load(Ordering::SeqCst);
    wal.compact_now();
    assert!(wal.wait_for_compaction().is_err());
    assert!(
        wal.last_compaction_error()
            .unwrap()
            .contains("snapshot bytes limit exceeded")
    );
    assert_eq!(store.puts.load(Ordering::SeqCst), puts);
    assert_eq!(wal.state(), b"ab");
    let cold = TempDir::new().unwrap();
    assert_eq!(
        Replica::open(store, History::plain(), Options::new(cold.path()))
            .unwrap()
            .state(),
        b"ab"
    );
}

#[test]
fn store_limit_caps_wal_acceptance_and_read_limits() {
    let store = Arc::new(CountingStore {
        limit: Some(14),
        ..CountingStore::default()
    });
    let dir = TempDir::new().unwrap();
    let mut wal = WalTier::open(store.clone(), History::plain(), Options::new(dir.path())).unwrap();
    wal.write(b"a").unwrap();
    assert!(matches!(
        wal.write(b"b"),
        Err(waltier::WriteError {
            source: WalError::LimitExceeded { limit: 14, .. },
            ..
        })
    ));
    let cold = TempDir::new().unwrap();
    assert_eq!(
        Replica::open(store.clone(), History::plain(), Options::new(cold.path()))
            .unwrap()
            .state(),
        b"a"
    );
    let smaller = TempDir::new().unwrap();
    assert!(matches!(
        Replica::open(store, History::plain(), options(&smaller, 13, 10)),
        Err(WalError::LimitExceeded { .. })
    ));
}

#[test]
fn invalid_options_make_no_storage_calls() {
    for invalid in 0..5 {
        let store = Arc::new(CountingStore::default());
        let dir = TempDir::new().unwrap();
        let mut opts = Options::new(dir.path());
        match invalid {
            0 => opts.max_write_attempts = 0,
            1 => opts.max_image_bytes = 8,
            2 => opts.max_live_entries = 0,
            3 => opts.max_snapshot_bytes = 0,
            _ => opts.max_image_bytes = usize::MAX,
        }
        assert!(matches!(
            WalTier::open(store.clone(), History::plain(), opts),
            Err(WalError::InvalidOptions(_))
        ));
        assert_eq!(store.puts.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn max_snapshot_lsn_is_corrupt_in_both_build_profiles() {
    let store = Arc::new(MemoryStore::new());
    let mut data = b"WTL1\x01".to_vec();
    data.extend_from_slice(&u64::MAX.to_le_bytes());
    data.extend_from_slice(&1u32.to_le_bytes());
    data.extend_from_slice(b"s");
    data.extend_from_slice(&0u32.to_le_bytes());
    store.put("wal", &data).unwrap();
    store.put("s", b"previously-panicked").unwrap();
    let dir = TempDir::new().unwrap();
    assert!(matches!(
        WalTier::open(store, History::plain(), Options::new(dir.path())),
        Err(WalError::Corrupt(_))
    ));
}

struct Invalid304;
impl ObjectStore for Invalid304 {
    fn get(&self, _: &str) -> Result<Option<waltier::Stored>, StoreError> {
        unreachable!()
    }
    fn get_if_changed(&self, _: &str, _: Option<&str>) -> Result<CondGet, StoreError> {
        Ok(CondGet::NotModified)
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
fn unconditional_not_modified_from_custom_store_returns_error() {
    let dir = TempDir::new().unwrap();
    assert!(matches!(
        Replica::open(
            Arc::new(Invalid304),
            History::plain(),
            Options::new(dir.path())
        ),
        Err(WalError::Store(_))
    ));
}

#[test]
fn final_lsn_rejects_append_before_cas_and_still_compacts() {
    let store = Arc::new(CountingStore::default());
    let mut data = b"WTL1\x01".to_vec();
    data.extend_from_slice(&(u64::MAX - 2).to_le_bytes());
    data.extend_from_slice(&1u32.to_le_bytes());
    data.extend_from_slice(b"s");
    data.extend_from_slice(&1u32.to_le_bytes());
    data.extend_from_slice(&1u32.to_le_bytes());
    data.push(b'b');
    store.inner.put("wal", &data).unwrap();
    store.inner.put("s", b"a").unwrap();
    let dir = TempDir::new().unwrap();
    let mut wal = WalTier::open(store.clone(), History::plain(), Options::new(dir.path())).unwrap();
    assert_eq!(wal.tip(), Some(u64::MAX - 1));
    assert_eq!(wal.state(), b"ab");
    assert!(matches!(
        wal.write(b"c"),
        Err(waltier::WriteError {
            source: WalError::LsnExhausted,
            ..
        })
    ));
    assert_eq!(store.puts.load(Ordering::SeqCst), 0);
    wal.compact_now();
    assert!(wal.wait_for_compaction().unwrap() == waltier::CompactionStatus::Ready);
    wal.flush().unwrap();
    assert!(matches!(
        wal.write(b"c"),
        Err(waltier::WriteError {
            source: WalError::LsnExhausted,
            ..
        })
    ));
    let cold = TempDir::new().unwrap();
    let reader = Replica::open(store, History::plain(), Options::new(cold.path())).unwrap();
    assert_eq!(reader.tip(), Some(u64::MAX - 1));
    assert_eq!(reader.state(), b"ab");
}

#[test]
fn snapshot_at_exact_budget_recovers_and_smaller_reader_rejects() {
    let store = Arc::new(MemoryStore::new());
    let dir = TempDir::new().unwrap();
    let mut opts = Options::new(dir.path());
    opts.max_snapshot_bytes = 2;
    let mut wal = WalTier::open(store.clone(), History::plain(), opts).unwrap();
    wal.write(b"ab").unwrap();
    wal.compact_now();
    assert!(wal.wait_for_compaction().unwrap() == waltier::CompactionStatus::Ready);
    wal.flush().unwrap();
    for cache in [dir, TempDir::new().unwrap()] {
        let mut opts = Options::new(cache.path());
        opts.max_snapshot_bytes = 1;
        assert!(matches!(
            Replica::open(store.clone(), History::plain(), opts),
            Err(WalError::LimitExceeded {
                resource: "snapshot bytes",
                ..
            })
        ));
        let mut opts = Options::new(cache.path());
        opts.max_snapshot_bytes = 2;
        assert_eq!(
            Replica::open(store.clone(), History::plain(), opts)
                .unwrap()
                .state(),
            b"ab"
        );
    }
}
