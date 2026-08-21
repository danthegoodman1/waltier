//! The writer ([`WalTier`]) and read-only follower ([`Replica`]).

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::JoinHandle;

use crate::cache::Cache;
use crate::image::{SnapshotRef, WalImage};
use crate::store::{CondGet, CondPut, ObjectStore, Stored, nanos_since_epoch};
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
        let count = COUNTER.fetch_add(1, Ordering::SeqCst);
        format!(
            "{}snap/{:020}-{:x}-{:x}",
            self.prefix,
            lsn,
            nanos_since_epoch(),
            count
        )
    }
}

/// A completed compaction waiting to be installed by the writer's next PUT.
struct Fold {
    key: String,
    lsn: Lsn,
    bytes: Arc<Vec<u8>>,
}

impl Fold {
    /// Whether this fold covers a prefix of the image's live entries — the
    /// condition for riding on the next PUT. Negated, it means the image has
    /// moved past the fold.
    fn applies_to(&self, image: &WalImage) -> bool {
        self.lsn >= image.first_lsn() && self.lsn < image.next_lsn()
    }
}

enum CompactOutcome {
    Done(Fold),
    Failed(String),
}

struct CompactionTask {
    rx: mpsc::Receiver<CompactOutcome>,
    handle: JoinHandle<()>,
}

/// Read-through snapshot fetch: disk cache first, then the store (filling
/// the cache). `None` means the object is gone — replaced by a newer fold.
fn fetch_snapshot(
    cache: &Cache,
    store: &dyn ObjectStore,
    key: &str,
) -> Result<Option<Vec<u8>>, WalError> {
    if let Some(bytes) = cache.load_snapshot(key) {
        return Ok(Some(bytes));
    }
    match store.get(key)? {
        Some(s) => {
            cache.save_snapshot(key, &s.data);
            Ok(Some(s.data))
        }
        None => Ok(None),
    }
}

/// State shared by writer and replica: the synced image, the app state built
/// from it, and the local cache. Snapshot bytes live in the disk cache, never
/// in memory.
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
                    });
                }
            };
            let image = WalImage::decode(&stored.data)?;
            let mut state = match &image.snapshot {
                None => app.init(),
                Some(sr) => match fetch_snapshot(&cache, store.as_ref(), &sr.key)? {
                    Some(bytes) => app.restore(&bytes)?,
                    // Snapshot replaced under us; re-read the WAL.
                    None => continue,
                },
            };
            for (lsn, entry) in image.entries_from(image.first_lsn()) {
                app.apply(&mut state, lsn, entry);
            }
            let mut core = Self {
                app,
                store,
                opts,
                cache,
                state,
                image,
                etag: None,
                image_len: 0,
            };
            core.record_image_put(stored.etag, &stored.data);
            return Ok(core);
        }
        Err(WalError::Corrupt(
            "open kept racing with concurrent compactions".into(),
        ))
    }

    /// The one place an accepted image version is recorded: cache, etag, size.
    fn record_image_put(&mut self, etag: String, data: &[u8]) {
        self.cache.save_wal(&etag, data);
        self.image_len = data.len() as u64;
        self.etag = Some(etag);
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
            if remote.first_lsn() <= my_next {
                for (lsn, entry) in remote.entries_from(my_next) {
                    self.app.apply(&mut self.state, lsn, entry);
                }
            } else {
                let sr = remote.snapshot.as_ref().ok_or_else(|| {
                    WalError::Corrupt("entries start past lsn 0 with no snapshot".into())
                })?;
                let Some(bytes) = fetch_snapshot(&self.cache, self.store.as_ref(), &sr.key)? else {
                    // Snapshot replaced under us; re-read the WAL.
                    continue;
                };
                let mut state = self.app.restore(&bytes)?;
                for (lsn, entry) in remote.entries_from(sr.lsn + 1) {
                    self.app.apply(&mut state, lsn, entry);
                }
                self.state = state;
            }
            self.image = remote;
            self.record_image_put(stored.etag, &stored.data);
            return Ok(true);
        }
        Err(WalError::Corrupt(
            "refresh kept racing with concurrent compactions".into(),
        ))
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
        self.sync_compaction();
        let mut attempts = 0u32;
        loop {
            let lsn = self.core.image.next_lsn();
            if self.try_install(Some(&entry))? {
                self.core.app.apply(&mut self.core.state, lsn, &entry);
                self.core.image.entries.push_back(entry);
                self.maybe_trigger_compaction();
                return Ok(lsn);
            }
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

    /// Install a pending fold without appending an entry. A no-op when
    /// nothing is pending. Useful on idle logs and before shutdown; a busy
    /// writer installs folds as a side effect of its next `write`.
    pub fn flush(&mut self) -> Result<(), WalError> {
        self.sync_compaction();
        let mut attempts = 0u32;
        while self.pending_fold.is_some() {
            if self.try_install(None)? {
                break;
            }
            attempts += 1;
            if attempts >= self.core.opts.max_write_attempts {
                break;
            }
        }
        Ok(())
    }

    /// Pull changes other instances have written. Returns whether anything
    /// changed.
    pub fn refresh(&mut self) -> Result<bool, WalError> {
        self.sync_compaction();
        let changed = self.core.refresh()?;
        self.discard_superseded_fold();
        Ok(changed)
    }

    /// Start a compaction regardless of [`WalApp::should_compact`]. Returns
    /// whether one was started (`false` when one is already running, a fold
    /// is pending, or there are no entries to fold).
    pub fn compact_now(&mut self) -> bool {
        self.sync_compaction();
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
        let outcome = match &self.compaction {
            None => return self.pending_fold.is_some(),
            Some(task) => task
                .rx
                .recv()
                .unwrap_or_else(|_| CompactOutcome::Failed("compaction thread died".into())),
        };
        self.finish_compaction(outcome);
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

    /// One CAS attempt: PUT the current image with any pending fold applied
    /// and `extra` appended. Returns whether the PUT was accepted. A lost CAS
    /// refreshes the state and re-checks the pending fold; the caller decides
    /// whether to try again. After a win the caller pushes `extra` into the
    /// image itself.
    fn try_install(&mut self, extra: Option<&[u8]>) -> Result<bool, WalError> {
        let data = match &self.pending_fold {
            Some(f) => {
                debug_assert!(f.applies_to(&self.core.image));
                let skip = (f.lsn + 1 - self.core.image.first_lsn()) as usize;
                let sr = SnapshotRef {
                    key: f.key.clone(),
                    lsn: f.lsn,
                };
                self.core.image.encode_view(Some(&sr), skip, extra)
            }
            None => self.core.image.encode_view(None, 0, extra),
        };
        let etag = self
            .core
            .etag
            .clone()
            .expect("writer always holds the wal etag");
        match self
            .core
            .store
            .put_if_match(&self.core.opts.wal_key(), Some(&etag), &data)?
        {
            CondPut::Ok { etag } => {
                self.core.record_image_put(etag, &data);
                self.install_pending_fold();
                Ok(true)
            }
            CondPut::PreconditionFailed => {
                self.core.refresh()?;
                self.discard_superseded_fold();
                Ok(false)
            }
        }
    }

    /// After a winning PUT that carried a fold: drop the folded entries, swap
    /// the snapshot ref, garbage-collect the previous snapshot, and keep the
    /// new bytes in the disk cache as the base for future compactions.
    fn install_pending_fold(&mut self) {
        let Some(f) = self.pending_fold.take() else {
            return;
        };
        let image = &mut self.core.image;
        for _ in 0..=(f.lsn - image.first_lsn()) {
            image.entries.pop_front();
        }
        let old = image.snapshot.replace(SnapshotRef {
            key: f.key.clone(),
            lsn: f.lsn,
        });
        if let Some(old) = old
            && old.key != f.key
        {
            let _ = self.core.store.delete(&old.key);
            self.core.cache.remove_snapshot(&old.key);
        }
        self.core.cache.save_snapshot(&f.key, &f.bytes);
    }

    /// Method preamble: fold in a finished compaction and re-check the
    /// pending fold against the current image.
    fn sync_compaction(&mut self) {
        self.integrate_compaction();
        self.discard_superseded_fold();
    }

    /// Drop a pending fold the image has moved past. Normally a remote
    /// compaction won and our snapshot object is an orphan to delete. The
    /// exception: when the image references the fold's own key, our install
    /// PUT landed even though it reported failure, so keep its bytes cached —
    /// deleting it would destroy the live snapshot.
    fn discard_superseded_fold(&mut self) {
        let Some(f) = &self.pending_fold else { return };
        if f.applies_to(&self.core.image) {
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
        } else {
            let _ = self.core.store.delete(&f.key);
        }
    }

    fn integrate_compaction(&mut self) {
        let outcome = match &self.compaction {
            None => return,
            Some(task) => match task.rx.try_recv() {
                Err(mpsc::TryRecvError::Empty) => return,
                Ok(outcome) => outcome,
                Err(mpsc::TryRecvError::Disconnected) => {
                    CompactOutcome::Failed("compaction thread died".into())
                }
            },
        };
        self.finish_compaction(outcome);
    }

    fn finish_compaction(&mut self, outcome: CompactOutcome) {
        let task = self.compaction.take().expect("a compaction is running");
        let _ = task.handle.join();
        match outcome {
            CompactOutcome::Done(fold) => {
                self.pending_fold = Some(fold);
                self.discard_superseded_fold();
            }
            CompactOutcome::Failed(msg) => self.last_compaction_error = Some(msg),
        }
    }

    /// Called after each successful write. `compact_now` re-verifies
    /// eligibility, so the guard lives in one place.
    fn maybe_trigger_compaction(&mut self) {
        if self.compaction.is_none()
            && self.pending_fold.is_none()
            && !self.core.image.entries.is_empty()
            && self.core.app.should_compact(&self.core.stats())
        {
            self.compact_now();
        }
    }

    fn spawn_compaction(&mut self) {
        let app = self.core.app.clone();
        let store = self.core.store.clone();
        let cache = self.core.cache.clone();
        let base_key = self.core.image.snapshot.as_ref().map(|s| s.key.clone());
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
            let _ = tx.send(run_compaction(
                app, store, cache, base_key, entries, fold_lsn, snap_key,
            ));
        });
        self.compaction = Some(CompactionTask { rx, handle });
    }
}

fn run_compaction<A: WalApp>(
    app: Arc<A>,
    store: Arc<dyn ObjectStore>,
    cache: Cache,
    base_key: Option<String>,
    entries: Vec<Entry>,
    fold_lsn: Lsn,
    snap_key: String,
) -> CompactOutcome {
    let base = match &base_key {
        None => None,
        Some(key) => match fetch_snapshot(&cache, store.as_ref(), key) {
            Ok(Some(bytes)) => Some(bytes),
            Ok(None) => {
                return CompactOutcome::Failed(format!(
                    "base snapshot {key} is gone; a later trigger will retry"
                ));
            }
            Err(e) => return CompactOutcome::Failed(e.to_string()),
        },
    };
    let snapshot = match app.compact(base.as_deref(), &entries) {
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
