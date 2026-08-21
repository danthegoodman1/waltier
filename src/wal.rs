//! The writer ([`WalTier`]) and read-only follower ([`Replica`]).

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::JoinHandle;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cache::Cache;
use crate::image::{SnapshotRef, WalImage};
use crate::store::{CondGet, CondPut, ObjectStore, Stored};
use crate::{Entry, Lsn, Reconcile, WalApp, WalError, WalStats};

/// Bound on re-reading the WAL when a concurrent compaction keeps replacing
/// the snapshot mid-operation.
const MAX_RACES: u32 = 8;

pub struct Options {
    /// Directory for the local warm-start cache (WAL image + snapshot).
    pub cache_dir: PathBuf,
    /// Object-key prefix, e.g. `"logs/mylog/"`. The WAL lives at
    /// `{prefix}wal`, snapshots under `{prefix}snap/`.
    pub prefix: String,
    /// Bound on CAS attempts per `write` or `flush` call.
    pub max_write_attempts: u32,
}

impl Options {
    pub fn new(cache_dir: impl Into<PathBuf>) -> Self {
        Self {
            cache_dir: cache_dir.into(),
            prefix: String::new(),
            max_write_attempts: 8,
        }
    }

    fn wal_key(&self) -> String {
        format!("{}wal", self.prefix)
    }

    fn snap_key(&self, lsn: Lsn) -> String {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let count = COUNTER.fetch_add(1, Ordering::SeqCst);
        format!("{}snap/{:020}-{:x}-{:x}", self.prefix, lsn, nanos, count)
    }
}

/// The current snapshot's bytes, tracked alongside `image.snapshot`.
/// `NotLoaded` means the image references a snapshot whose bytes we have not
/// pulled; a later compaction fetches them for its base.
enum SnapBytes {
    None,
    Loaded(Arc<Vec<u8>>),
    NotLoaded,
}

/// A completed compaction waiting to be installed by the writer's next PUT.
#[derive(Clone)]
struct Fold {
    key: String,
    lsn: Lsn,
    bytes: Arc<Vec<u8>>,
}

enum CompactOutcome {
    Done(Fold),
    Failed(String),
}

struct CompactionTask {
    rx: mpsc::Receiver<CompactOutcome>,
    handle: JoinHandle<()>,
}

/// State shared by writer and replica: the synced image, the app state built
/// from it, and the local cache.
struct Core<A: WalApp> {
    app: Arc<A>,
    store: Arc<dyn ObjectStore>,
    opts: Options,
    cache: Cache,
    state: A::State,
    image: WalImage,
    /// `None` only on a replica opened before the WAL exists.
    etag: Option<String>,
    image_len: u64,
    snapshot_bytes: SnapBytes,
}

impl<A: WalApp> Core<A> {
    fn open(
        store: Arc<dyn ObjectStore>,
        app: Arc<A>,
        opts: Options,
        create_if_missing: bool,
    ) -> Result<Self, WalError> {
        let cache = Cache::new(&opts.cache_dir)?;
        let wal_key = opts.wal_key();
        let cached = cache.load_wal();
        for _ in 0..MAX_RACES {
            let stored = match store
                .get_if_changed(&wal_key, cached.as_ref().map(|(e, _)| e.as_str()))?
            {
                CondGet::NotModified => {
                    let (etag, data) = cached.clone().expect("NotModified implies a cached copy");
                    Stored { data, etag }
                }
                CondGet::Changed(s) => s,
                CondGet::Missing if create_if_missing => {
                    let data = WalImage::empty().encode();
                    match store.put_if_match(&wal_key, None, &data)? {
                        CondPut::Ok { etag } => Stored { data, etag },
                        // Lost the creation race; re-read whoever won.
                        CondPut::PreconditionFailed => continue,
                    }
                }
                CondGet::Missing => {
                    let state = app.init();
                    return Ok(Self {
                        app,
                        store,
                        opts,
                        cache,
                        state,
                        image: WalImage::empty(),
                        etag: None,
                        image_len: 0,
                        snapshot_bytes: SnapBytes::None,
                    });
                }
            };
            let image = WalImage::decode(&stored.data)?;
            let (mut state, snapshot_bytes) = match &image.snapshot {
                None => (app.init(), SnapBytes::None),
                Some(sr) => {
                    let bytes = match cache.load_snapshot(&sr.key) {
                        Some(b) => b,
                        None => match store.get(&sr.key)? {
                            Some(s) => {
                                cache.save_snapshot(&sr.key, &s.data);
                                s.data
                            }
                            // Snapshot replaced under us; re-read the WAL.
                            None => continue,
                        },
                    };
                    (app.restore(&bytes)?, SnapBytes::Loaded(Arc::new(bytes)))
                }
            };
            for (lsn, entry) in image.entries_from(image.first_lsn()) {
                app.apply(&mut state, lsn, entry);
            }
            cache.save_wal(&stored.etag, &stored.data);
            let image_len = stored.data.len() as u64;
            return Ok(Self {
                app,
                store,
                opts,
                cache,
                state,
                image,
                etag: Some(stored.etag),
                image_len,
                snapshot_bytes,
            });
        }
        Err(WalError::Corrupt(
            "open kept racing with concurrent compactions".into(),
        ))
    }

    /// Pull the latest WAL image and advance the state. Returns whether
    /// anything changed. When entries we never applied have been folded away,
    /// rebuilds the state from the snapshot.
    fn refresh(&mut self) -> Result<bool, WalError> {
        let wal_key = self.opts.wal_key();
        for _ in 0..MAX_RACES {
            let stored = match self.store.get_if_changed(&wal_key, self.etag.as_deref())? {
                CondGet::NotModified => return Ok(false),
                CondGet::Missing => {
                    if self.etag.is_some() {
                        return Err(WalError::Corrupt("wal object disappeared".into()));
                    }
                    return Ok(false);
                }
                CondGet::Changed(s) => s,
            };
            let remote = WalImage::decode(&stored.data)?;
            let my_next = self.image.next_lsn();
            let mut restored = false;
            if remote.first_lsn() <= my_next {
                for (lsn, entry) in remote.entries_from(my_next) {
                    self.app.apply(&mut self.state, lsn, entry);
                }
            } else {
                let sr = remote.snapshot.clone().ok_or_else(|| {
                    WalError::Corrupt("entries start past lsn 0 with no snapshot".into())
                })?;
                let Some(bytes) = self.fetch_snapshot(&sr.key)? else {
                    // Snapshot replaced under us; re-read the WAL.
                    continue;
                };
                let mut state = self.app.restore(&bytes)?;
                for (lsn, entry) in remote.entries_from(sr.lsn + 1) {
                    self.app.apply(&mut state, lsn, entry);
                }
                self.state = state;
                self.snapshot_bytes = SnapBytes::Loaded(Arc::new(bytes));
                restored = true;
            }
            if !restored && remote.snapshot != self.image.snapshot {
                self.snapshot_bytes = if remote.snapshot.is_some() {
                    SnapBytes::NotLoaded
                } else {
                    SnapBytes::None
                };
            }
            self.cache.save_wal(&stored.etag, &stored.data);
            self.image = remote;
            self.etag = Some(stored.etag);
            self.image_len = stored.data.len() as u64;
            return Ok(true);
        }
        Err(WalError::Corrupt(
            "refresh kept racing with concurrent compactions".into(),
        ))
    }

    fn fetch_snapshot(&self, key: &str) -> Result<Option<Vec<u8>>, WalError> {
        if let Some(bytes) = self.cache.load_snapshot(key) {
            return Ok(Some(bytes));
        }
        match self.store.get(key)? {
            Some(s) => {
                self.cache.save_snapshot(key, &s.data);
                Ok(Some(s.data))
            }
            None => Ok(None),
        }
    }

    fn stats(&self) -> WalStats {
        WalStats {
            tip: self.image.tip(),
            snapshot_lsn: self.image.snapshot.as_ref().map(|s| s.lsn),
            live_entries: self.image.entries.len() as u64,
            live_entry_bytes: self.image.entry_bytes(),
            image_bytes: self.image_len,
        }
    }
}

/// The single writer for one log. Owns the app state, appends entries with a
/// CAS on the WAL object's etag, and orchestrates compaction.
pub struct WalTier<A: WalApp> {
    core: Core<A>,
    compaction: Option<CompactionTask>,
    pending_fold: Option<Fold>,
    last_compaction_error: Option<String>,
}

impl<A: WalApp> WalTier<A> {
    /// Open the log, creating the WAL object if it does not exist.
    pub fn open(store: Arc<dyn ObjectStore>, app: A, opts: Options) -> Result<Self, WalError> {
        let core = Core::open(store, Arc::new(app), opts, true)?;
        Ok(Self {
            core,
            compaction: None,
            pending_fold: None,
            last_compaction_error: None,
        })
    }

    /// Append one entry. Returns its LSN.
    ///
    /// On an etag conflict the library refreshes the state from the store and
    /// consults [`WalApp::reconcile`]; `Abort` surfaces as
    /// [`WalError::Conflict`] with the entry handed back.
    pub fn write(&mut self, entry: impl Into<Vec<u8>>) -> Result<Lsn, WalError> {
        let mut entry = entry.into();
        self.integrate_compaction();
        self.discard_superseded_fold();
        let wal_key = self.core.opts.wal_key();
        let mut attempts = 0u32;
        loop {
            let lsn = self.core.image.next_lsn();
            let (candidate, folded) = self.candidate_image(Some(&entry));
            let data = candidate.encode();
            let etag = self
                .core
                .etag
                .clone()
                .expect("writer always holds the wal etag");
            match self.core.store.put_if_match(&wal_key, Some(&etag), &data)? {
                CondPut::Ok { etag } => {
                    self.core.cache.save_wal(&etag, &data);
                    self.core.etag = Some(etag);
                    self.core.image_len = data.len() as u64;
                    self.commit(candidate, folded);
                    self.core.app.apply(&mut self.core.state, lsn, &entry);
                    self.maybe_trigger_compaction();
                    return Ok(lsn);
                }
                CondPut::PreconditionFailed => {
                    self.core.refresh()?;
                    self.discard_superseded_fold();
                    attempts += 1;
                    if attempts >= self.core.opts.max_write_attempts {
                        return Err(WalError::Conflict { entry });
                    }
                    match self.core.app.reconcile(&self.core.state, &entry) {
                        Reconcile::Retry => {}
                        Reconcile::Replace(e) => entry = e,
                        Reconcile::Abort => return Err(WalError::Conflict { entry }),
                    }
                }
            }
        }
    }

    /// Install a pending fold without appending an entry. A no-op when
    /// nothing is pending. Useful on idle logs and before shutdown; a busy
    /// writer installs folds as a side effect of its next `write`.
    pub fn flush(&mut self) -> Result<(), WalError> {
        self.integrate_compaction();
        self.discard_superseded_fold();
        let wal_key = self.core.opts.wal_key();
        let mut attempts = 0u32;
        while self.pending_fold.is_some() {
            let (candidate, folded) = self.candidate_image(None);
            if folded.is_none() {
                break;
            }
            let data = candidate.encode();
            let etag = self
                .core
                .etag
                .clone()
                .expect("writer always holds the wal etag");
            match self.core.store.put_if_match(&wal_key, Some(&etag), &data)? {
                CondPut::Ok { etag } => {
                    self.core.cache.save_wal(&etag, &data);
                    self.core.etag = Some(etag);
                    self.core.image_len = data.len() as u64;
                    self.commit(candidate, folded);
                }
                CondPut::PreconditionFailed => {
                    self.core.refresh()?;
                    self.discard_superseded_fold();
                    attempts += 1;
                    if attempts >= self.core.opts.max_write_attempts {
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    /// Pull changes other instances have written. Returns whether anything
    /// changed.
    pub fn refresh(&mut self) -> Result<bool, WalError> {
        self.integrate_compaction();
        let changed = self.core.refresh()?;
        self.discard_superseded_fold();
        Ok(changed)
    }

    /// Start a compaction regardless of [`WalApp::should_compact`]. Returns
    /// whether one was started (`false` when one is already running, a fold
    /// is pending, or there are no entries to fold).
    pub fn compact_now(&mut self) -> bool {
        self.integrate_compaction();
        self.discard_superseded_fold();
        if self.compaction.is_some()
            || self.pending_fold.is_some()
            || self.core.image.entries.is_empty()
        {
            return false;
        }
        self.spawn_compaction();
        true
    }

    /// Block until a running compaction finishes. Returns whether a fold is
    /// now pending installation (install it with `flush` or the next `write`).
    pub fn wait_for_compaction(&mut self) -> bool {
        let Some(task) = self.compaction.take() else {
            return self.pending_fold.is_some();
        };
        let outcome = task.rx.recv();
        let _ = task.handle.join();
        match outcome {
            Ok(CompactOutcome::Done(fold)) => {
                self.pending_fold = Some(fold);
                self.discard_superseded_fold();
            }
            Ok(CompactOutcome::Failed(msg)) => self.last_compaction_error = Some(msg),
            Err(_) => self.last_compaction_error = Some("compaction thread died".into()),
        }
        self.pending_fold.is_some()
    }

    /// Finish any running compaction and install it, then drop the handle.
    pub fn close(mut self) -> Result<(), WalError> {
        self.wait_for_compaction();
        self.flush()
    }

    pub fn state(&self) -> &A::State {
        &self.core.state
    }

    pub fn stats(&self) -> WalStats {
        self.core.stats()
    }

    pub fn tip(&self) -> Option<Lsn> {
        self.core.image.tip()
    }

    /// The underlying store, for app payload objects that live outside the WAL.
    pub fn store(&self) -> Arc<dyn ObjectStore> {
        self.core.store.clone()
    }

    pub fn compaction_running(&self) -> bool {
        self.compaction.is_some()
    }

    pub fn has_pending_fold(&self) -> bool {
        self.pending_fold.is_some()
    }

    /// The most recent compaction failure, if any. Failures are recorded and
    /// compaction is simply re-attempted on a later trigger.
    pub fn last_compaction_error(&self) -> Option<&str> {
        self.last_compaction_error.as_deref()
    }

    /// The next PUT's image: the current one, with a valid pending fold
    /// applied and `extra` appended.
    fn candidate_image(&self, extra: Option<&[u8]>) -> (WalImage, Option<Fold>) {
        let mut img = self.core.image.clone();
        let mut folded = None;
        if let Some(f) = &self.pending_fold {
            let first = img.first_lsn();
            if f.lsn >= first && f.lsn < img.next_lsn() {
                for _ in 0..=(f.lsn - first) {
                    img.entries.pop_front();
                }
                img.snapshot = Some(SnapshotRef {
                    key: f.key.clone(),
                    lsn: f.lsn,
                });
                folded = Some(f.clone());
            }
        }
        if let Some(e) = extra {
            img.entries.push_back(e.to_vec());
        }
        (img, folded)
    }

    /// Adopt a successfully PUT candidate. When it installed a fold, garbage-
    /// collect the previous snapshot and adopt the new bytes as the base.
    fn commit(&mut self, candidate: WalImage, folded: Option<Fold>) {
        let old_snapshot = std::mem::replace(&mut self.core.image, candidate).snapshot;
        if let Some(f) = folded {
            if let Some(old) = old_snapshot
                && old.key != f.key
            {
                let _ = self.core.store.delete(&old.key);
                self.core.cache.remove_snapshot(&old.key);
            }
            self.core.cache.save_snapshot(&f.key, &f.bytes);
            self.core.snapshot_bytes = SnapBytes::Loaded(f.bytes);
            self.pending_fold = None;
        }
    }

    /// Drop a pending fold the image has moved past. Normally a remote
    /// compaction won and our snapshot object is an orphan to delete. The
    /// exception: when the image references the fold's own key, our install
    /// PUT landed even though it reported failure, so adopt the fold as the
    /// current base — deleting it would destroy the live snapshot.
    fn discard_superseded_fold(&mut self) {
        let Some(f) = &self.pending_fold else { return };
        if f.lsn >= self.core.image.first_lsn() && f.lsn < self.core.image.next_lsn() {
            return;
        }
        let installed = self
            .core
            .image
            .snapshot
            .as_ref()
            .is_some_and(|s| s.key == f.key);
        let f = self.pending_fold.take().expect("checked above");
        if installed {
            self.core.cache.save_snapshot(&f.key, &f.bytes);
            self.core.snapshot_bytes = SnapBytes::Loaded(f.bytes);
        } else {
            let _ = self.core.store.delete(&f.key);
        }
    }

    fn integrate_compaction(&mut self) {
        let Some(task) = &self.compaction else { return };
        let outcome = match task.rx.try_recv() {
            Ok(outcome) => outcome,
            Err(mpsc::TryRecvError::Empty) => return,
            Err(mpsc::TryRecvError::Disconnected) => {
                CompactOutcome::Failed("compaction thread died".into())
            }
        };
        let task = self.compaction.take().expect("checked above");
        let _ = task.handle.join();
        match outcome {
            CompactOutcome::Done(fold) => self.pending_fold = Some(fold),
            CompactOutcome::Failed(msg) => self.last_compaction_error = Some(msg),
        }
    }

    fn maybe_trigger_compaction(&mut self) {
        self.integrate_compaction();
        self.discard_superseded_fold();
        if self.compaction.is_some()
            || self.pending_fold.is_some()
            || self.core.image.entries.is_empty()
        {
            return;
        }
        if self.core.app.should_compact(&self.core.stats()) {
            self.spawn_compaction();
        }
    }

    fn spawn_compaction(&mut self) {
        let app = self.core.app.clone();
        let store = self.core.store.clone();
        let base = match (&self.core.image.snapshot, &self.core.snapshot_bytes) {
            (None, _) => BaseSource::None,
            (Some(_), SnapBytes::Loaded(b)) => BaseSource::Loaded(b.clone()),
            (Some(sr), _) => BaseSource::Fetch(sr.key.clone()),
        };
        let first = self.core.image.first_lsn();
        let entries: Vec<Entry> = self
            .core
            .image
            .entries
            .iter()
            .enumerate()
            .map(|(i, e)| Entry {
                lsn: first + i as u64,
                data: e.clone(),
            })
            .collect();
        let fold_lsn = self
            .core
            .image
            .tip()
            .expect("caller checked entries are nonempty");
        let snap_key = self.core.opts.snap_key(fold_lsn);
        let (tx, rx) = mpsc::channel();
        let handle = std::thread::spawn(move || {
            let outcome = run_compaction(app, store, base, entries, fold_lsn, snap_key);
            let _ = tx.send(outcome);
        });
        self.compaction = Some(CompactionTask { rx, handle });
    }
}

enum BaseSource {
    None,
    Loaded(Arc<Vec<u8>>),
    Fetch(String),
}

fn run_compaction<A: WalApp>(
    app: Arc<A>,
    store: Arc<dyn ObjectStore>,
    base: BaseSource,
    entries: Vec<Entry>,
    fold_lsn: Lsn,
    snap_key: String,
) -> CompactOutcome {
    let base_bytes: Option<Arc<Vec<u8>>> = match base {
        BaseSource::None => None,
        BaseSource::Loaded(b) => Some(b),
        BaseSource::Fetch(key) => match store.get(&key) {
            Ok(Some(s)) => Some(Arc::new(s.data)),
            Ok(None) => {
                return CompactOutcome::Failed(format!(
                    "base snapshot {key} is gone; a later trigger will retry"
                ));
            }
            Err(e) => return CompactOutcome::Failed(e.to_string()),
        },
    };
    let snapshot = match app.compact(base_bytes.as_deref().map(Vec::as_slice), &entries) {
        Ok(s) => s,
        Err(e) => return CompactOutcome::Failed(e.to_string()),
    };
    match store.put(&snap_key, &snapshot) {
        Ok(_) => CompactOutcome::Done(Fold {
            key: snap_key,
            lsn: fold_lsn,
            bytes: Arc::new(snapshot),
        }),
        Err(e) => CompactOutcome::Failed(e.to_string()),
    }
}

/// A read-only follower. Polls the WAL with conditional GETs; never writes
/// and never compacts.
pub struct Replica<A: WalApp> {
    core: Core<A>,
}

impl<A: WalApp> Replica<A> {
    /// Open a follower. Works before the writer has created the WAL; the
    /// state stays empty until `refresh` sees it.
    pub fn open(store: Arc<dyn ObjectStore>, app: A, opts: Options) -> Result<Self, WalError> {
        Ok(Self {
            core: Core::open(store, Arc::new(app), opts, false)?,
        })
    }

    /// Pull the latest WAL image. Returns whether anything changed. Usually a
    /// single cheap conditional GET.
    pub fn refresh(&mut self) -> Result<bool, WalError> {
        self.core.refresh()
    }

    pub fn state(&self) -> &A::State {
        &self.core.state
    }

    pub fn stats(&self) -> WalStats {
        self.core.stats()
    }

    pub fn tip(&self) -> Option<Lsn> {
        self.core.image.tip()
    }

    /// The underlying store, for app payload objects that live outside the WAL.
    pub fn store(&self) -> Arc<dyn ObjectStore> {
        self.core.store.clone()
    }
}
