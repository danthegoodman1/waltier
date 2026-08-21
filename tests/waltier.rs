//! End-to-end tests over MemoryStore: fencing, reconciliation, compaction,
//! replicas, and the warm-start cache.

use std::collections::BTreeMap;
use std::sync::Arc;

use tempfile::TempDir;
use waltier::{
    Entry, Lsn, MemoryStore, ObjectStore, Options, Reconcile, Replica, WalApp, WalError, WalStats,
    WalTier,
};

type Map = BTreeMap<String, String>;

#[derive(Clone, Copy)]
enum OnConflict {
    Abort,
    Retry,
    Replace,
}

#[derive(Clone, Copy)]
struct Kv {
    compact_at: u64,
    on_conflict: OnConflict,
}

impl Kv {
    fn new() -> Self {
        Self {
            compact_at: u64::MAX,
            on_conflict: OnConflict::Abort,
        }
    }

    fn compacting_at(n: u64) -> Self {
        Self {
            compact_at: n,
            ..Self::new()
        }
    }

    fn on_conflict(mut self, mode: OnConflict) -> Self {
        self.on_conflict = mode;
        self
    }
}

fn encode_map(map: &Map) -> Vec<u8> {
    map.iter()
        .map(|(k, v)| format!("{k}\t{v}\n"))
        .collect::<String>()
        .into_bytes()
}

fn decode_map(bytes: &[u8]) -> Result<Map, WalError> {
    std::str::from_utf8(bytes)
        .map_err(|_| WalError::App("snapshot is not utf8".into()))?
        .lines()
        .map(|line| {
            line.split_once('\t')
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .ok_or_else(|| WalError::App(format!("bad snapshot line: {line}")))
        })
        .collect()
}

impl WalApp for Kv {
    type State = Map;

    fn init(&self) -> Map {
        Map::new()
    }

    fn apply(&self, state: &mut Map, _lsn: Lsn, entry: &[u8]) {
        let text = String::from_utf8_lossy(entry);
        if let Some(rest) = text.strip_prefix("set ") {
            if let Some((k, v)) = rest.split_once(' ') {
                state.insert(k.to_string(), v.to_string());
            }
        } else if let Some(k) = text.strip_prefix("del ") {
            state.remove(k);
        }
    }

    fn restore(&self, snapshot: &[u8]) -> Result<Map, WalError> {
        decode_map(snapshot)
    }

    fn compact(&self, base: Option<&[u8]>, entries: &[Entry]) -> Result<Vec<u8>, WalError> {
        let mut map = base.map(decode_map).transpose()?.unwrap_or_default();
        for e in entries {
            self.apply(&mut map, e.lsn, &e.data);
        }
        Ok(encode_map(&map))
    }

    fn should_compact(&self, stats: &WalStats) -> bool {
        stats.live_entries >= self.compact_at
    }

    fn reconcile(&self, _state: &Map, _pending: &[u8]) -> Reconcile {
        match self.on_conflict {
            OnConflict::Abort => Reconcile::Abort,
            OnConflict::Retry => Reconcile::Retry,
            OnConflict::Replace => Reconcile::Replace(b"set replaced yes".to_vec()),
        }
    }
}

fn writer(store: &Arc<MemoryStore>, app: Kv) -> (WalTier<Kv>, TempDir) {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> = store.clone();
    (
        WalTier::open(store, app, Options::new(dir.path())).unwrap(),
        dir,
    )
}

fn replica(store: &Arc<MemoryStore>, app: Kv) -> (Replica<Kv>, TempDir) {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> = store.clone();
    (
        Replica::open(store, app, Options::new(dir.path())).unwrap(),
        dir,
    )
}

fn snap_keys(store: &MemoryStore) -> Vec<String> {
    store
        .keys()
        .into_iter()
        .filter(|k| k.starts_with("snap/"))
        .collect()
}

#[test]
fn lsn_sequence_and_reopen() {
    let store = Arc::new(MemoryStore::new());
    let (mut w, _d) = writer(&store, Kv::new());
    assert_eq!(w.tip(), None);
    assert_eq!(w.write(b"set a 1".to_vec()).unwrap(), 0);
    assert_eq!(w.write(b"set b 2".to_vec()).unwrap(), 1);
    assert_eq!(w.write(b"del a".to_vec()).unwrap(), 2);
    assert_eq!(w.tip(), Some(2));
    assert_eq!(w.state().get("b").unwrap(), "2");
    assert!(!w.state().contains_key("a"));

    let (mut w2, _d2) = writer(&store, Kv::new());
    assert_eq!(w2.state(), w.state());
    assert_eq!(w2.write(b"set c 3".to_vec()).unwrap(), 3);
}

#[test]
fn conflict_abort_hands_back_entry() {
    let store = Arc::new(MemoryStore::new());
    let (mut a, _da) = writer(&store, Kv::new());
    let (mut b, _db) = writer(&store, Kv::new());

    assert_eq!(a.write(b"set a 1".to_vec()).unwrap(), 0);

    let err = b.write(b"set b 2".to_vec()).unwrap_err();
    let WalError::Conflict { entries } = err else {
        panic!("expected Conflict, got {err}")
    };
    assert_eq!(entries, vec![b"set b 2".to_vec()]);
    // The refresh already folded in the winning write.
    assert_eq!(b.state().get("a").unwrap(), "1");

    assert_eq!(b.write_batch(entries).unwrap(), 1..2);
    a.refresh().unwrap();
    assert_eq!(a.state(), b.state());
}

#[test]
fn conflict_retry_is_transparent() {
    let store = Arc::new(MemoryStore::new());
    let (mut a, _da) = writer(&store, Kv::new().on_conflict(OnConflict::Retry));
    let (mut b, _db) = writer(&store, Kv::new().on_conflict(OnConflict::Retry));

    assert_eq!(a.write(b"set a 1".to_vec()).unwrap(), 0);
    assert_eq!(b.write(b"set b 2".to_vec()).unwrap(), 1);
    assert_eq!(b.state().len(), 2);

    a.refresh().unwrap();
    assert_eq!(a.state(), b.state());
}

#[test]
fn conflict_replace_rewrites_entry() {
    let store = Arc::new(MemoryStore::new());
    let (mut a, _da) = writer(&store, Kv::new());
    let (mut b, _db) = writer(&store, Kv::new().on_conflict(OnConflict::Replace));

    a.write(b"set a 1".to_vec()).unwrap();
    assert_eq!(b.write(b"set b 2".to_vec()).unwrap(), 1);
    assert_eq!(b.state().get("replaced").unwrap(), "yes");
    assert!(!b.state().contains_key("b"));
}

#[test]
fn insert_triggered_compaction_installs_on_next_write() {
    let store = Arc::new(MemoryStore::new());
    let (mut w, _d) = writer(&store, Kv::compacting_at(3));
    w.write(b"set a 1".to_vec()).unwrap();
    w.write(b"set b 2".to_vec()).unwrap();
    w.write(b"set a 3".to_vec()).unwrap();

    assert!(w.compaction_running() || w.has_pending_fold());
    assert!(w.wait_for_compaction(), "{:?}", w.last_compaction_error());

    // The fold rides on the next PUT.
    assert_eq!(w.write(b"set c 4".to_vec()).unwrap(), 3);
    let stats = w.stats();
    assert_eq!(stats.snapshot_lsn, Some(2));
    assert_eq!(stats.live_entries, 1);
    assert_eq!(stats.tip, Some(3));
    assert_eq!(snap_keys(&store).len(), 1);

    // A fresh instance bootstraps from snapshot + live entries.
    let (w2, _d2) = writer(&store, Kv::new());
    assert_eq!(w2.state(), w.state());
    assert_eq!(w2.tip(), Some(3));
}

#[test]
fn flush_installs_fold_on_idle_log() {
    let store = Arc::new(MemoryStore::new());
    let (mut w, _d) = writer(&store, Kv::new());
    w.write(b"set a 1".to_vec()).unwrap();
    w.write(b"set b 2".to_vec()).unwrap();

    assert!(w.compact_now());
    assert!(w.wait_for_compaction(), "{:?}", w.last_compaction_error());
    w.flush().unwrap();

    let stats = w.stats();
    assert_eq!(stats.snapshot_lsn, Some(1));
    assert_eq!(stats.live_entries, 0);
    assert_eq!(stats.tip, Some(1));
    assert!(!w.has_pending_fold());

    // LSNs continue past the fold.
    assert_eq!(w.write(b"set c 3".to_vec()).unwrap(), 2);
}

#[test]
fn repeated_folds_leave_one_snapshot() {
    let store = Arc::new(MemoryStore::new());
    let (mut w, _d) = writer(&store, Kv::new());
    for round in 0..3 {
        w.write(format!("set k{round} v").into_bytes()).unwrap();
        assert!(w.compact_now());
        assert!(w.wait_for_compaction(), "{:?}", w.last_compaction_error());
        w.flush().unwrap();
        assert_eq!(snap_keys(&store).len(), 1, "old snapshots must be deleted");
    }
    assert_eq!(w.stats().snapshot_lsn, Some(2));
}

#[test]
fn replica_follows_and_restores_across_folds() {
    let store = Arc::new(MemoryStore::new());
    let (mut w, _dw) = writer(&store, Kv::new());
    w.write(b"set a 1".to_vec()).unwrap();

    let (mut r, _dr) = replica(&store, Kv::new());
    assert_eq!(r.state().get("a").unwrap(), "1");

    // The writer moves on and folds entries the replica never saw.
    w.write(b"set b 2".to_vec()).unwrap();
    w.write(b"set a 3".to_vec()).unwrap();
    assert!(w.compact_now());
    assert!(w.wait_for_compaction(), "{:?}", w.last_compaction_error());
    w.flush().unwrap();
    w.write(b"set c 4".to_vec()).unwrap();

    assert!(r.refresh().unwrap());
    assert_eq!(r.state(), w.state());
    assert_eq!(r.tip(), Some(3));
    assert!(
        !r.refresh().unwrap(),
        "no change means a cheap NotModified poll"
    );
}

#[test]
fn replica_can_open_before_the_writer() {
    let store = Arc::new(MemoryStore::new());
    let (mut r, _dr) = replica(&store, Kv::new());
    assert!(r.state().is_empty());
    assert!(!r.refresh().unwrap());

    let (mut w, _dw) = writer(&store, Kv::new());
    w.write(b"set a 1".to_vec()).unwrap();
    assert!(r.refresh().unwrap());
    assert_eq!(r.state().get("a").unwrap(), "1");
}

#[test]
fn competing_folds_one_wins_loser_cleans_up() {
    let store = Arc::new(MemoryStore::new());
    let (mut a, _da) = writer(&store, Kv::new());
    a.write(b"set a 1".to_vec()).unwrap();
    a.write(b"set b 2".to_vec()).unwrap();
    let (mut b, _db) = writer(&store, Kv::new());
    assert_eq!(b.tip(), Some(1));

    assert!(a.compact_now());
    assert!(a.wait_for_compaction());
    assert!(b.compact_now());
    assert!(b.wait_for_compaction());
    assert_eq!(snap_keys(&store).len(), 2, "both snapshot objects uploaded");

    a.flush().unwrap();
    assert_eq!(a.stats().snapshot_lsn, Some(1));

    // B's install loses the CAS, sees A's fold covers its own, and deletes
    // its orphaned snapshot object.
    b.flush().unwrap();
    assert!(!b.has_pending_fold());
    assert_eq!(snap_keys(&store).len(), 1);
    assert_eq!(b.state(), a.state());

    // B keeps working: writes land, and its next fold uses A's snapshot as
    // base even though B never held its bytes.
    assert_eq!(b.write(b"set c 3".to_vec()).unwrap(), 2);
    assert!(b.compact_now());
    assert!(b.wait_for_compaction(), "{:?}", b.last_compaction_error());
    b.flush().unwrap();
    assert_eq!(b.stats().snapshot_lsn, Some(2));
    assert_eq!(snap_keys(&store).len(), 1);

    a.refresh().unwrap();
    assert_eq!(a.state(), b.state());
}

#[test]
#[cfg(feature = "sim")]
fn warm_start_uses_local_cache() {
    let store = Arc::new(waltier::sim::SimStore::new(Arc::new(MemoryStore::new())));
    let dir = TempDir::new().unwrap();
    let s: Arc<dyn ObjectStore> = store.clone();
    let mut w = WalTier::open(s, Kv::new(), Options::new(dir.path())).unwrap();
    w.write(b"set a 1".to_vec()).unwrap();
    w.write(b"set b 2".to_vec()).unwrap();
    assert!(w.compact_now());
    assert!(w.wait_for_compaction());
    w.flush().unwrap();
    let expected = w.state().clone();
    drop(w);

    let before = store.stats();
    let s: Arc<dyn ObjectStore> = store.clone();
    let w = WalTier::open(s, Kv::new(), Options::new(dir.path())).unwrap();
    assert_eq!(w.state(), &expected);
    let after = store.stats();
    assert_eq!(
        after.not_modified,
        before.not_modified + 1,
        "image validated by etag"
    );
    assert_eq!(
        after.gets, before.gets,
        "image and snapshot served from disk cache"
    );
}

#[test]
#[cfg(feature = "sim")]
fn flush_without_fold_is_a_noop() {
    let store = Arc::new(waltier::sim::SimStore::new(Arc::new(MemoryStore::new())));
    let dir = TempDir::new().unwrap();
    let s: Arc<dyn ObjectStore> = store.clone();
    let mut w = WalTier::open(s, Kv::new(), Options::new(dir.path())).unwrap();
    w.write(b"set a 1".to_vec()).unwrap();
    let puts = store.stats().puts;
    w.flush().unwrap();
    assert_eq!(store.stats().puts, puts);
}

#[test]
fn prefix_scopes_all_objects() {
    let store = Arc::new(MemoryStore::new());
    let dir = TempDir::new().unwrap();
    let s: Arc<dyn ObjectStore> = store.clone();
    let mut opts = Options::new(dir.path());
    opts.prefix = "logs/orders/".to_string();
    let mut w = WalTier::open(s, Kv::new(), opts).unwrap();
    w.write(b"set a 1".to_vec()).unwrap();
    assert!(w.compact_now());
    assert!(w.wait_for_compaction());
    w.flush().unwrap();
    assert!(store.keys().iter().all(|k| k.starts_with("logs/orders/")));
}

/// A write acked by an error can still have landed (a timeout after S3
/// applied the PUT). The library never applies unacked entries locally; the
/// next refresh picks them up, and a caller that resubmits appends a
/// duplicate — at-least-once, by design.
#[test]
#[cfg(feature = "sim")]
fn ambiguous_write_put_is_at_least_once() {
    let inner = Arc::new(MemoryStore::new());
    let store = Arc::new(waltier::sim::SimStore::new(inner));
    let dir = TempDir::new().unwrap();
    let s: Arc<dyn ObjectStore> = store.clone();
    let mut w = WalTier::open(s, Kv::new(), Options::new(dir.path())).unwrap();

    store.fail_next_mutation_ambiguously("wal");
    w.write(b"set a 1".to_vec()).unwrap_err();
    assert_eq!(w.tip(), None, "an unacked write is not applied locally");

    assert!(w.refresh().unwrap());
    assert_eq!(
        w.tip(),
        Some(0),
        "the refresh reveals that the write landed"
    );
    assert_eq!(w.state().get("a").unwrap(), "1");

    assert_eq!(
        w.write(b"set a 1".to_vec()).unwrap(),
        1,
        "a resubmit duplicates the entry"
    );
}

/// Regression: when the PUT that installs a fold lands but reports an error,
/// the writer must recognize its own snapshot in the refreshed image and
/// adopt it — deleting it as "superseded" would destroy the live snapshot.
#[test]
#[cfg(feature = "sim")]
fn ambiguous_fold_install_adopts_own_snapshot() {
    let inner = Arc::new(MemoryStore::new());
    let store = Arc::new(waltier::sim::SimStore::new(inner.clone()));
    let dir = TempDir::new().unwrap();
    let s: Arc<dyn ObjectStore> = store.clone();
    let mut w = WalTier::open(s, Kv::new(), Options::new(dir.path())).unwrap();
    w.write(b"set a 1".to_vec()).unwrap();
    w.write(b"set b 2".to_vec()).unwrap();
    assert!(w.compact_now());
    assert!(w.wait_for_compaction());

    store.fail_next_mutation_ambiguously("wal");
    w.flush().unwrap_err();
    assert!(
        w.has_pending_fold(),
        "the fold stays pending after the unacked install"
    );

    assert!(w.refresh().unwrap());
    assert!(!w.has_pending_fold());
    assert_eq!(w.stats().snapshot_lsn, Some(1), "the install landed");
    assert_eq!(
        snap_keys(&inner).len(),
        1,
        "the referenced snapshot must survive"
    );

    // The adopted snapshot serves as base for the next fold, and a cold
    // bootstrap can restore from it.
    w.write(b"set c 3".to_vec()).unwrap();
    assert!(w.compact_now());
    assert!(w.wait_for_compaction(), "{:?}", w.last_compaction_error());
    w.flush().unwrap();
    assert_eq!(w.stats().snapshot_lsn, Some(2));
    let (w2, _d2) = writer(&inner, Kv::new());
    assert_eq!(w2.state(), w.state());
}

/// A store whose CAS can be forced to fail, for pinning the retry bound.
struct ConflictStore {
    inner: MemoryStore,
    conflict: std::sync::atomic::AtomicBool,
    attempts: std::sync::atomic::AtomicU64,
}

impl ObjectStore for ConflictStore {
    fn get(&self, key: &str) -> Result<Option<waltier::Stored>, waltier::StoreError> {
        self.inner.get(key)
    }
    fn get_if_changed(
        &self,
        key: &str,
        etag: Option<&str>,
    ) -> Result<waltier::CondGet, waltier::StoreError> {
        self.inner.get_if_changed(key, etag)
    }
    fn put_if_match(
        &self,
        key: &str,
        etag: Option<&str>,
        data: &[u8],
    ) -> Result<waltier::CondPut, waltier::StoreError> {
        use std::sync::atomic::Ordering;
        if self.conflict.load(Ordering::SeqCst) {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            return Ok(waltier::CondPut::PreconditionFailed);
        }
        self.inner.put_if_match(key, etag, data)
    }
    fn put(&self, key: &str, data: &[u8]) -> Result<String, waltier::StoreError> {
        self.inner.put(key, data)
    }
    fn delete(&self, key: &str) -> Result<(), waltier::StoreError> {
        self.inner.delete(key)
    }
}

#[test]
fn write_retries_are_bounded_by_max_write_attempts() {
    use std::sync::atomic::Ordering;
    let store = Arc::new(ConflictStore {
        inner: MemoryStore::new(),
        conflict: false.into(),
        attempts: 0.into(),
    });
    let dir = TempDir::new().unwrap();
    let s: Arc<dyn ObjectStore> = store.clone();
    let mut opts = Options::new(dir.path());
    opts.max_write_attempts = 3;
    let mut w = WalTier::open(s, Kv::new().on_conflict(OnConflict::Retry), opts).unwrap();
    w.write(b"set a 1".to_vec()).unwrap();

    store.conflict.store(true, Ordering::SeqCst);
    let err = w.write(b"set b 2".to_vec()).unwrap_err();
    let WalError::Conflict { entries } = err else {
        panic!("expected Conflict, got {err}")
    };
    assert_eq!(entries, vec![b"set b 2".to_vec()]);
    assert_eq!(store.attempts.load(Ordering::SeqCst), 3);

    store.conflict.store(false, Ordering::SeqCst);
    assert_eq!(w.write_batch(entries).unwrap(), 1..2);
}

#[test]
fn close_installs_a_pending_fold() {
    let store = Arc::new(MemoryStore::new());
    let (mut w, _d) = writer(&store, Kv::new());
    w.write(b"set a 1".to_vec()).unwrap();
    w.write(b"set b 2".to_vec()).unwrap();
    assert!(w.compact_now());
    w.close().unwrap();

    let (w2, _d2) = writer(&store, Kv::new());
    assert_eq!(w2.stats().snapshot_lsn, Some(1));
    assert_eq!(w2.stats().live_entries, 0);
    assert_eq!(w2.state().get("b").unwrap(), "2");
}

/// A damaged local cache must never poison an open; the store copy wins.
#[test]
fn corrupt_cache_is_ignored_on_open() {
    let store = Arc::new(MemoryStore::new());
    let dir = TempDir::new().unwrap();
    let s: Arc<dyn ObjectStore> = store.clone();
    let mut w = WalTier::open(s, Kv::new(), Options::new(dir.path())).unwrap();
    w.write(b"set a 1".to_vec()).unwrap();
    drop(w);

    std::fs::write(dir.path().join("wal.cache"), b"\x02zzgarbage").unwrap();
    let s: Arc<dyn ObjectStore> = store.clone();
    let w = WalTier::open(s, Kv::new(), Options::new(dir.path())).unwrap();
    assert_eq!(w.state().get("a").unwrap(), "1");
}

#[test]
fn write_batch_is_atomic_and_contiguous() {
    let store = Arc::new(MemoryStore::new());
    let (mut w, _d) = writer(&store, Kv::new());
    assert_eq!(
        w.write_batch(vec![]).unwrap(),
        0..0,
        "empty batch is a no-op"
    );
    let range = w
        .write_batch(vec![
            b"set a 1".to_vec(),
            b"set b 2".to_vec(),
            b"set a 3".to_vec(),
        ])
        .unwrap();
    assert_eq!(range, 0..3);
    assert_eq!(w.state().get("a").unwrap(), "3");
    assert_eq!(w.tip(), Some(2));

    // One PUT carried the whole batch, and a fresh open replays it.
    let (w2, _d2) = writer(&store, Kv::new());
    assert_eq!(w2.state(), w.state());
    assert_eq!(store.keys(), vec!["wal".to_string()]);
}

#[test]
fn write_batch_conflict_modes() {
    let store = Arc::new(MemoryStore::new());
    let (mut a, _da) = writer(&store, Kv::new());
    let (mut b, _db) = writer(&store, Kv::new().on_conflict(OnConflict::Retry));

    a.write(b"set a 1".to_vec()).unwrap();
    // Retry mode: the stale batch lands transparently after the refresh.
    let batch = vec![b"set b 2".to_vec(), b"set c 3".to_vec()];
    assert_eq!(b.write_batch(batch).unwrap(), 1..3);

    // Abort mode: the whole batch comes back, none of it applied.
    let (mut c, _dc) = writer(&store, Kv::new());
    a.refresh().unwrap();
    a.write(b"set d 4".to_vec()).unwrap();
    let batch = vec![b"set e 5".to_vec(), b"set f 6".to_vec()];
    let err = c.write_batch(batch.clone()).unwrap_err();
    let WalError::Conflict { entries } = err else {
        panic!("expected Conflict, got {err}")
    };
    assert_eq!(entries, batch);
    assert!(!c.state().contains_key("e"));
    let range = c.write_batch(entries).unwrap();
    assert_eq!(range.end - range.start, 2);
}
