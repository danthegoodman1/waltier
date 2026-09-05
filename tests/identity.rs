//! Regressions for resource identity and immutable snapshot ownership.
use std::sync::{Arc, Mutex, mpsc};
use tempfile::TempDir;
use waltier::{
    CondGet, CondPut, Entry, Lsn, MemoryStore, ObjectStore, Options, Replica, StoreError, Stored,
    WalApp, WalError, WalTier,
};

#[derive(Clone, Copy)]
struct History;
impl WalApp for History {
    type State = Vec<u8>;
    fn init(&self) -> Self::State {
        Vec::new()
    }
    fn apply(&self, state: &mut Self::State, _: Lsn, entry: &[u8]) {
        state.extend_from_slice(entry);
    }
    fn restore(&self, snapshot: &[u8]) -> Result<Self::State, WalError> {
        Ok(snapshot.to_vec())
    }
    fn compact(&self, base: Option<&[u8]>, entries: &[Entry]) -> Result<Vec<u8>, WalError> {
        let mut state = base.unwrap_or_default().to_vec();
        for entry in entries {
            state.extend_from_slice(&entry.data);
        }
        Ok(state)
    }
}

fn options(dir: &TempDir, prefix: &str) -> Options {
    let mut options = Options::new(dir.path());
    options.prefix = prefix.into();
    options
}

/// Independent WTL1 fixture, with an identical snapshot key in both stores.
fn snapshot_image(key: &str) -> Vec<u8> {
    let mut bytes = b"WTL1\x01".to_vec();
    bytes.extend_from_slice(&0_u64.to_le_bytes());
    bytes.extend_from_slice(&(key.len() as u32).to_le_bytes());
    bytes.extend_from_slice(key.as_bytes());
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    bytes
}

fn snapshot_key(store: &dyn ObjectStore) -> String {
    let bytes = store.get("wal").unwrap().unwrap().data;
    assert_eq!(&bytes[..5], b"WTL1\x01");
    let n = u32::from_le_bytes(bytes[13..17].try_into().unwrap()) as usize;
    String::from_utf8(bytes[17..17 + n].to_vec()).unwrap()
}

#[test]
fn reused_cache_with_equal_etags_cannot_replace_another_stores_history() {
    for with_snapshot in [false, true] {
        for prefixes in [("", ""), ("a/", "b/")] {
            let a = Arc::new(MemoryStore::new());
            let b = Arc::new(MemoryStore::new());
            let shared = TempDir::new().unwrap();
            let b_cache = TempDir::new().unwrap();
            if with_snapshot {
                // Reserved-looking identical keys deliberately maximize cache overlap.
                for (store, prefix, data) in [(&a, prefixes.0, b"A"), (&b, prefixes.1, b"B")] {
                    store.put("snap/same", data).unwrap();
                    store
                        .put(&format!("{prefix}wal"), &snapshot_image("snap/same"))
                        .unwrap();
                }
            } else {
                for (store, dir, prefix, data) in [
                    (&a, &shared, prefixes.0, b"A"),
                    (&b, &b_cache, prefixes.1, b"B"),
                ] {
                    let mut w =
                        WalTier::open(store.clone(), History, options(dir, prefix)).unwrap();
                    assert_eq!(w.write(data.to_vec()).unwrap(), 0);
                }
            }
            assert_eq!(
                a.get(&format!("{}wal", prefixes.0)).unwrap().unwrap().etag,
                b.get(&format!("{}wal", prefixes.1)).unwrap().unwrap().etag
            );
            let warm_a = Replica::open(a, History, options(&shared, prefixes.0)).unwrap();
            assert_eq!(warm_a.state(), b"A");
            drop(warm_a);
            let mut warm_b =
                WalTier::open(b.clone(), History, options(&shared, prefixes.1)).unwrap();
            assert_eq!(warm_b.state(), b"B");
            assert_eq!(warm_b.write(b"C".to_vec()).unwrap(), 1);
            let cold = TempDir::new().unwrap();
            let reader = Replica::open(b, History, options(&cold, prefixes.1)).unwrap();
            assert_eq!(reader.state(), b"BC");
        }
    }
}

/// A custom wrapper that intentionally supplies no persistent cache identity.
struct UnknownStore(Arc<MemoryStore>);
impl ObjectStore for UnknownStore {
    fn get(&self, key: &str) -> Result<Option<Stored>, StoreError> {
        self.0.get(key)
    }
    fn put_if_match(
        &self,
        key: &str,
        etag: Option<&str>,
        data: &[u8],
    ) -> Result<CondPut, StoreError> {
        self.0.put_if_match(key, etag, data)
    }
    fn put(&self, key: &str, data: &[u8]) -> Result<String, StoreError> {
        self.0.put(key, data)
    }
    fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.0.delete(key)
    }
}

#[test]
fn unknown_store_namespace_bypasses_persistent_cache_safely() {
    let shared = TempDir::new().unwrap();
    for data in [b"A", b"B"] {
        let store: Arc<dyn ObjectStore> = Arc::new(UnknownStore(Arc::new(MemoryStore::new())));
        let mut writer = WalTier::open(store.clone(), History, options(&shared, "")).unwrap();
        writer.write(data.to_vec()).unwrap();
        assert!(writer.compact_now());
        assert!(writer.wait_for_compaction().unwrap() == waltier::CompactionStatus::Ready);
        writer.flush().unwrap();
        let reopened = Replica::open(store, History, options(&shared, "")).unwrap();
        assert_eq!(reopened.state(), data);
    }
    assert_eq!(std::fs::read_dir(shared.path()).unwrap().count(), 0);
}

#[test]
fn offline_sweep_after_draining_and_dropping_all_writers_preserves_recovery() {
    let store = Arc::new(MemoryStore::new());
    let first_cache = TempDir::new().unwrap();
    let mut first = WalTier::open(store.clone(), History, options(&first_cache, "")).unwrap();
    first.write(b"A".to_vec()).unwrap();
    assert!(first.compact_now());
    assert!(first.wait_for_compaction().unwrap() == waltier::CompactionStatus::Ready);
    // This handle has drained its worker but deliberately abandons an uninstalled fold.
    let abandoned = store
        .keys()
        .into_iter()
        .find(|key| key.starts_with("snap/"))
        .unwrap();
    drop(first);

    let second_cache = TempDir::new().unwrap();
    let mut second = WalTier::open(store.clone(), History, options(&second_cache, "")).unwrap();
    second.write(b"B".to_vec()).unwrap();
    assert!(second.compact_now());
    // close drains upload AND installation; no handle remains able to publish.
    second.close().unwrap();
    let live = snapshot_key(store.as_ref());
    assert_ne!(abandoned, live);
    // Fresh authoritative WAL read above occurs only after all writers are gone.
    for key in store
        .keys()
        .into_iter()
        .filter(|key| key.starts_with("snap/") && key != &live)
    {
        store.delete(&key).unwrap();
    }
    assert!(store.get(&abandoned).unwrap().is_none());
    assert!(store.get(&live).unwrap().is_some());
    let fresh = TempDir::new().unwrap();
    let mut reopened = WalTier::open(store.clone(), History, options(&fresh, "")).unwrap();
    assert_eq!(reopened.state(), b"AB");
    reopened.write(b"C".to_vec()).unwrap();
    let cold = TempDir::new().unwrap();
    assert_eq!(
        Replica::open(store, History, options(&cold, ""))
            .unwrap()
            .state(),
        b"ABC"
    );
}

#[test]
#[cfg(feature = "sim")]
fn ambiguous_snapshot_upload_is_retained_and_never_installed_as_success() {
    let inner = Arc::new(MemoryStore::new());
    let sim = Arc::new(waltier::sim::SimStore::new(inner.clone()));
    let dir = TempDir::new().unwrap();
    let mut writer = WalTier::open(sim.clone(), History, options(&dir, "")).unwrap();
    writer.write(b"A".to_vec()).unwrap();
    sim.fail_next_mutation_ambiguously("snap/");
    writer.compact_now();
    assert!(writer.wait_for_compaction().is_err());
    assert!(!writer.has_pending_fold());
    let orphan = inner
        .keys()
        .into_iter()
        .find(|k| k.starts_with("snap/"))
        .unwrap();
    assert!(writer.last_compaction_error().unwrap().contains(&orphan));
    writer.write(b"B".to_vec()).unwrap();
    writer.compact_now();
    assert!(writer.wait_for_compaction().unwrap() == waltier::CompactionStatus::Ready);
    writer.close().unwrap();
    assert_ne!(snapshot_key(inner.as_ref()), orphan);
    let cold = TempDir::new().unwrap();
    assert_eq!(
        Replica::open(inner, History, options(&cold, ""))
            .unwrap()
            .state(),
        b"AB"
    );
}

struct PauseWalRead {
    inner: Arc<MemoryStore>,
    pause: Mutex<Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>>,
}
impl ObjectStore for PauseWalRead {
    fn cache_namespace(&self) -> Option<String> {
        self.inner.cache_namespace()
    }
    fn get(&self, key: &str) -> Result<Option<Stored>, StoreError> {
        self.inner.get(key)
    }
    fn get_if_changed(&self, key: &str, etag: Option<&str>) -> Result<CondGet, StoreError> {
        let result = self.inner.get_if_changed(key, etag)?;
        if key == "wal"
            && let Some((arrived, resume)) = self.pause.lock().unwrap().take()
        {
            arrived.send(()).unwrap();
            resume.recv().unwrap();
        }
        Ok(result)
    }
    fn put_if_match(
        &self,
        key: &str,
        etag: Option<&str>,
        data: &[u8],
    ) -> Result<CondPut, StoreError> {
        self.inner.put_if_match(key, etag, data)
    }
    fn put(&self, key: &str, data: &[u8]) -> Result<String, StoreError> {
        self.inner.put(key, data)
    }
    fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key)
    }
}

#[test]
fn reader_retries_when_superseded_snapshot_disappears_after_wal_get() {
    let store = Arc::new(MemoryStore::new());
    let dir = TempDir::new().unwrap();
    let mut writer = WalTier::open(store.clone(), History, options(&dir, "")).unwrap();
    writer.write(b"A".to_vec()).unwrap();
    writer.compact_now();
    assert!(writer.wait_for_compaction().unwrap() == waltier::CompactionStatus::Ready);
    writer.flush().unwrap();
    let old_key = snapshot_key(store.as_ref());
    let (arrived_tx, arrived_rx) = mpsc::sync_channel(0);
    let (resume_tx, resume_rx) = mpsc::sync_channel(0);
    let paused = Arc::new(PauseWalRead {
        inner: store.clone(),
        pause: Mutex::new(Some((arrived_tx, resume_rx))),
    });
    let reader = std::thread::spawn(move || {
        let cold = TempDir::new().unwrap();
        Replica::open(paused, History, options(&cold, ""))
            .unwrap()
            .state()
            .clone()
    });
    arrived_rx.recv().unwrap();
    writer.write(b"B".to_vec()).unwrap();
    writer.compact_now();
    assert!(writer.wait_for_compaction().unwrap() == waltier::CompactionStatus::Ready);
    writer.flush().unwrap();
    assert!(
        store.get(&old_key).unwrap().is_none(),
        "reader must observe an actual missing snapshot"
    );
    resume_tx.send(()).unwrap();
    assert_eq!(reader.join().unwrap(), b"AB");
}
