//! The writer ([`WalTier`]) and read-only follower ([`Replica`]).

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, mpsc};
use std::thread::JoinHandle;

use crate::cache::Cache;
use crate::image::{ImageLimits, SnapshotRef, WalImage, check_limit};
use crate::store::{CondGet, CondPut, ObjectStore, Stored, unique_id};
use crate::{Entry, Lsn, MutationOutcome, ReconcileBatch, WalApp, WalError, WalStats, WriteError};

/// Bound on re-reading the WAL when a concurrent compaction keeps replacing
/// the snapshot mid-operation.
const MAX_RACES: u32 = 8;

/// Persistent cache writes are optional and never the durability point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CachePolicy {
    Disabled,
    EveryCommit,
    /// Save the WAL at explicit flush/close or `checkpoint_cache`. Snapshots
    /// are still cached after fetch/upload; stale WAL checkpoints are validated.
    OnFlush,
}

pub struct Options {
    /// Directory for the local warm-start cache (WAL image + snapshot).
    pub cache_dir: PathBuf,
    pub cache_policy: CachePolicy,
    /// Maximum obsolete snapshot identities retained for explicit deletion.
    /// Overflow leaves safe orphans and increments the reported sweep debt.
    pub max_pending_deletes: usize,
    /// Object-key prefix, e.g. `"logs/mylog/"`. The WAL lives at
    /// `{prefix}wal`, snapshots under `{prefix}snap/`.
    pub prefix: String,
    /// Bound on CAS attempts per `write` or `flush` call.
    pub max_write_attempts: u32,
    /// Maximum encoded WAL body, including snapshot reference and framing.
    /// Default: 64 MiB. Capped to the store's advertised object limit.
    pub max_image_bytes: usize,
    /// Maximum live entries independent of the application's compaction trigger.
    /// Default: 1,000,000. Exhaustion rejects the entire append before CAS.
    pub max_live_entries: usize,
    /// Maximum snapshot body. Default: 256 MiB; capped to the store's limit.
    /// All writers and readers of a log should use compatible limits.
    pub max_snapshot_bytes: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            cache_dir: PathBuf::new(),
            cache_policy: CachePolicy::Disabled,
            max_pending_deletes: 128,
            prefix: String::new(),
            max_write_attempts: 8,
            max_image_bytes: 64 << 20,
            max_live_entries: 1_000_000,
            max_snapshot_bytes: 256 << 20,
        }
    }
}

impl Options {
    /// Enable best-effort caching at every observed image version.
    pub fn new(cache_dir: impl Into<PathBuf>) -> Self {
        Self {
            cache_dir: cache_dir.into(),
            cache_policy: CachePolicy::EveryCommit,
            ..Self::default()
        }
    }

    fn limits(&self) -> ImageLimits {
        ImageLimits {
            bytes: self.max_image_bytes,
            entries: self.max_live_entries,
        }
    }

    fn validate(&mut self, store: &dyn ObjectStore) -> Result<(), WalError> {
        if let Some(limit) = store.max_object_bytes() {
            self.max_image_bytes = self.max_image_bytes.min(limit);
            self.max_snapshot_bytes = self.max_snapshot_bytes.min(limit);
        }
        if self.max_write_attempts == 0
            || self.max_image_bytes < 9
            || self.max_live_entries == 0
            || self.max_live_entries > u32::MAX as usize
            || self.max_snapshot_bytes == 0
            || self.max_image_bytes > isize::MAX as usize
            || self.max_snapshot_bytes > isize::MAX as usize
        {
            return Err(WalError::InvalidOptions(
                "positive retry/count/snapshot budgets and an image budget of at least 9 bytes are required; counts must fit u32 and byte budgets must fit isize".into(),
            ));
        }
        Ok(())
    }

    fn wal_key(&self) -> String {
        format!("{}wal", self.prefix)
    }
}

/// A completed compaction waiting to be installed by the writer's next PUT.
struct Fold {
    key: String,
    lsn: Lsn,
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

/// Current maintenance state. A ready fold is uploaded but not yet referenced
/// by the WAL; only `Installed` means this handle observed its installation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionStatus {
    Idle,
    Running,
    Ready,
    Installed,
    Superseded,
}

/// Cleanup observations for this handle, not a global inventory of orphans.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GarbageStatus {
    pub pending: usize,
    /// Cumulative candidates omitted because the queue was full. These require
    /// an offline sweep after all handles and uploads have been drained.
    /// Repeated candidates may be counted more than once.
    pub overflowed: u64,
    pub last_error: Option<String>,
}

/// Explicit maintenance drains tracked cleanup and reports any untracked debt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceStatus {
    pub compaction: CompactionStatus,
    pub garbage: GarbageStatus,
}

enum Compaction {
    Idle,
    Running(CompactionTask),
    Ready(Fold),
    Installed,
    Superseded,
    Failed(String),
}

/// An error before or during a CAS. Only the PUT boundary may mark the append
/// unknown: a failed subsequent refresh still means this attempt did not land.
struct InstallError {
    source: WalError,
    outcome: MutationOutcome,
}

impl From<WalError> for InstallError {
    fn from(source: WalError) -> Self {
        Self {
            source,
            outcome: MutationOutcome::NotApplied,
        }
    }
}

/// What a refresh did: whether the image advanced, and the snapshot ref it
/// stopped pointing at. The superseded object is unreachable from the WAL,
/// so a writer collects it; a replica only drops its cached copy.
struct Refreshed {
    changed: bool,
    superseded: Option<SnapshotRef>,
}

struct CompactionTask {
    rx: mpsc::Receiver<CompactOutcome>,
    handle: JoinHandle<()>,
}

/// A second authoritative read naming the same missing immutable snapshot is
/// broken storage/history, whereas changing references are ordinary contention.
fn record_missing_snapshot(
    previous: &mut Option<(String, String)>,
    etag: &str,
    key: &str,
) -> Result<(), WalError> {
    if previous
        .as_ref()
        .is_some_and(|(old_etag, old_key)| old_etag == etag && old_key == key)
    {
        return Err(WalError::Corrupt(format!(
            "WAL references missing snapshot {key}"
        )));
    }
    *previous = Some((etag.into(), key.into()));
    Ok(())
}

/// Read-through snapshot fetch: disk cache first, then the store (filling
/// the cache). `None` means the object is gone — replaced by a newer fold.
fn fetch_snapshot(
    cache: &Cache,
    store: &dyn ObjectStore,
    key: &str,
    max_bytes: usize,
) -> Result<Option<Vec<u8>>, WalError> {
    if let Some(bytes) = cache.load_snapshot(key, max_bytes) {
        check_limit("snapshot bytes", bytes.len(), max_bytes)?;
        return Ok(Some(bytes));
    }
    match store.get(key)? {
        Some(s) => {
            check_limit("snapshot bytes", s.data.len(), max_bytes)?;
            cache.save_snapshot(key, &s.data);
            Ok(Some(s.data))
        }
        None => Ok(None),
    }
}

/// State shared by writer and replica: the synced image, the app state built
/// from it, and the optional local cache. Snapshot bytes are allocated during
/// fetch/compaction but are not retained in Core between operations.
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
    cache_dirty: bool,
}

impl<A: WalApp> Core<A> {
    fn open(
        store: Arc<dyn ObjectStore>,
        app: Arc<A>,
        mut opts: Options,
        create_if_missing: bool,
    ) -> Result<Self, WalError> {
        opts.validate(store.as_ref())?;
        let wal_key = opts.wal_key();
        let cache = match opts.cache_policy {
            CachePolicy::Disabled => Cache::disabled(),
            _ => Cache::new(
                &opts.cache_dir,
                store.cache_namespace().as_deref(),
                &wal_key,
            ),
        };
        // A cached image that no longer decodes is not offered as a cache
        // entry at all, so the store copy is fetched instead.
        let cached = cache
            .load_wal(opts.max_image_bytes)
            .filter(|(_, image)| WalImage::decode(image, opts.limits()).is_ok());
        let mut missing_snapshot = None;
        for _ in 0..MAX_RACES {
            let stored =
                match store.get_if_changed(&wal_key, cached.as_ref().map(|(e, _)| e.as_str()))? {
                    CondGet::NotModified => {
                        let (etag, data) = cached.clone().ok_or_else(|| {
                            WalError::Store(
                                crate::StoreError::new("unconditional GET returned NotModified")
                                    .with_context(crate::StoreOperation::Get, &wal_key, None)
                                    .not_applied(),
                            )
                        })?;
                        Stored { data, etag }
                    }
                    CondGet::Changed(s) => s,
                    CondGet::Missing if create_if_missing => {
                        let data = WalImage::empty().encode(opts.limits())?;
                        match store.put_if_match(&wal_key, None, &data)? {
                            CondPut::Ok { etag } => Stored { data, etag },
                            // Lost the creation race; re-read whoever won.
                            CondPut::PreconditionFailed => continue,
                        }
                    }
                    CondGet::Missing => {
                        let state = app.init();
                        cache.retain_snapshot(None);
                        return Ok(Self {
                            app,
                            store,
                            opts,
                            cache,
                            state,
                            image: WalImage::empty(),
                            etag: None,
                            image_len: 0,
                            cache_dirty: false,
                        });
                    }
                };
            let image = WalImage::decode(&stored.data, opts.limits())?;
            let mut state = match &image.snapshot {
                None => app.init(),
                Some(sr) => {
                    match fetch_snapshot(&cache, store.as_ref(), &sr.key, opts.max_snapshot_bytes)?
                    {
                        Some(bytes) => app.restore(&bytes)?,
                        None => {
                            record_missing_snapshot(&mut missing_snapshot, &stored.etag, &sr.key)?;
                            continue;
                        }
                    }
                }
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
                cache_dirty: false,
            };
            core.cache
                .retain_snapshot(core.image.snapshot.as_ref().map(|sr| sr.key.as_str()));
            core.record_image_put(stored.etag, &stored.data);
            return Ok(core);
        }
        Err(WalError::Contention {
            operation: "open",
            attempts: MAX_RACES,
        })
    }

    /// The one place an accepted image version is recorded: cache, etag, size.
    fn record_image_put(&mut self, etag: String, data: &[u8]) {
        if self.opts.cache_policy == CachePolicy::EveryCommit {
            self.cache.save_wal(&etag, data);
        }
        self.cache_dirty = self.opts.cache_policy == CachePolicy::OnFlush && self.cache.enabled();
        self.image_len = data.len() as u64;
        self.etag = Some(etag);
    }

    fn checkpoint_cache(&mut self) {
        if !self.cache_dirty {
            return;
        }
        if let Some(etag) = &self.etag
            && let Ok(data) = self.image.encode(self.opts.limits())
        {
            self.cache.save_wal(etag, &data);
            self.cache_dirty = false;
        }
    }

    /// Pull the latest WAL image and advance the state. When entries we never
    /// applied have been folded away, rebuilds the state from the snapshot,
    /// and the snapshot the image leaves behind is dropped from the cache and
    /// reported to the caller.
    fn refresh(&mut self) -> Result<Refreshed, WalError> {
        let unchanged = Refreshed {
            changed: false,
            superseded: None,
        };
        let wal_key = self.opts.wal_key();
        let mut missing_snapshot = None;
        for _ in 0..MAX_RACES {
            let stored = match self.store.get_if_changed(&wal_key, self.etag.as_deref())? {
                CondGet::NotModified if self.etag.is_some() => return Ok(unchanged),
                CondGet::NotModified => {
                    return Err(WalError::Store(
                        crate::StoreError::new("unconditional GET returned NotModified")
                            .with_context(crate::StoreOperation::Get, &wal_key, None)
                            .not_applied(),
                    ));
                }
                CondGet::Missing => {
                    if self.etag.is_some() {
                        return Err(WalError::Corrupt("wal object disappeared".into()));
                    }
                    return Ok(unchanged);
                }
                CondGet::Changed(s) => s,
            };
            let remote = WalImage::decode(&stored.data, self.opts.limits())?;
            let my_next = self.image.next_lsn();
            if remote.first_lsn() <= my_next {
                for (lsn, entry) in remote.entries_from(my_next) {
                    self.app.apply(&mut self.state, lsn, entry);
                }
            } else {
                let sr = remote.snapshot.as_ref().ok_or_else(|| {
                    WalError::Corrupt("entries start past lsn 0 with no snapshot".into())
                })?;
                let Some(bytes) = fetch_snapshot(
                    &self.cache,
                    self.store.as_ref(),
                    &sr.key,
                    self.opts.max_snapshot_bytes,
                )?
                else {
                    record_missing_snapshot(&mut missing_snapshot, &stored.etag, &sr.key)?;
                    continue;
                };
                let mut state = self.app.restore(&bytes)?;
                for (lsn, entry) in remote.entries_from(sr.lsn + 1) {
                    self.app.apply(&mut state, lsn, entry);
                }
                self.state = state;
            }
            let superseded = self.image.snapshot.clone().filter(|old| {
                remote
                    .snapshot
                    .as_ref()
                    .is_none_or(|new| new.key != old.key)
            });
            if let Some(old) = &superseded {
                self.cache.remove_snapshot(&old.key);
            }
            self.image = remote;
            self.record_image_put(stored.etag, &stored.data);
            return Ok(Refreshed {
                changed: true,
                superseded,
            });
        }
        Err(WalError::Contention {
            operation: "refresh",
            attempts: MAX_RACES,
        })
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

/// A writer for one log. Multiple writers are allowed: CAS fences stale writes,
/// not ownership. Owns the app state, appends entries with a
/// CAS on the WAL object's etag, and orchestrates compaction.
pub struct WalTier<A: WalApp> {
    core: Core<A>,
    compaction: Compaction,
    garbage: VecDeque<String>,
    garbage_overflowed: u64,
    garbage_error: Option<String>,
}

impl<A: WalApp> WalTier<A> {
    /// Open the log, creating the WAL object if it does not exist.
    pub fn open(store: Arc<dyn ObjectStore>, app: A, opts: Options) -> Result<Self, WalError> {
        let core = Core::open(store, Arc::new(app), opts, true)?;
        Ok(Self {
            core,
            compaction: Compaction::Idle,
            garbage: VecDeque::new(),
            garbage_overflowed: 0,
            garbage_error: None,
        })
    }

    /// Append one entry and return its LSN. Uses whole-batch reconciliation but
    /// rejects a replacement containing anything other than one entry.
    pub fn write(&mut self, entry: impl Into<Vec<u8>>) -> Result<Lsn, WriteError> {
        Ok(self.append(vec![entry.into()], true)?.start)
    }

    /// Append an atomic batch in one CAS and return the final accepted range.
    /// On a conflict, `reconcile_batch` sees refreshed committed state and the
    /// entire pending batch. Replacements may change its length. Empty input or
    /// an empty replacement is a no-op returning the current empty LSN range.
    /// Errors retain the final attempted batch, including any replacements.
    pub fn write_batch(
        &mut self,
        entries: Vec<Vec<u8>>,
    ) -> Result<std::ops::Range<Lsn>, WriteError> {
        self.append(entries, false)
    }

    fn append(
        &mut self,
        mut entries: Vec<Vec<u8>>,
        single: bool,
    ) -> Result<std::ops::Range<Lsn>, WriteError> {
        self.sync_compaction();
        for attempt in 1..=self.core.opts.max_write_attempts {
            let first = self.core.image.next_lsn();
            if entries.is_empty() {
                return Ok(first..first);
            }
            match self.try_install(&entries) {
                Ok(true) => {
                    let count = entries.len() as u64;
                    for (i, entry) in entries.into_iter().enumerate() {
                        self.core
                            .app
                            .apply(&mut self.core.state, first + i as u64, &entry);
                        self.core.image.entries.push_back(entry);
                    }
                    self.maybe_trigger_compaction();
                    return Ok(first..first + count);
                }
                Err(error) => {
                    return Err(WriteError {
                        entries,
                        source: error.source,
                        outcome: error.outcome,
                    });
                }
                Ok(false) => {}
            }
            // No callback on the final attempt: no rewritten batch will be
            // attempted. Return the last batch actually submitted to CAS.
            if attempt == self.core.opts.max_write_attempts {
                return Err(WriteError {
                    entries,
                    source: WalError::Contention {
                        operation: "write",
                        attempts: attempt,
                    },
                    outcome: MutationOutcome::NotApplied,
                });
            }
            match self.core.app.reconcile_batch(&self.core.state, &entries) {
                ReconcileBatch::Retry => {}
                ReconcileBatch::Replace(replacement) => {
                    entries = replacement;
                    if single && entries.len() != 1 {
                        return Err(WriteError {
                            source: WalError::InvalidReplacement {
                                actual: entries.len(),
                            },
                            entries,
                            outcome: MutationOutcome::NotApplied,
                        });
                    }
                }
                ReconcileBatch::Abort => {
                    return Err(WriteError {
                        entries,
                        source: WalError::ReconcileAborted,
                        outcome: MutationOutcome::NotApplied,
                    });
                }
            }
        }
        unreachable!("options require a positive attempt budget")
    }

    /// Wait for compaction, install its fold, checkpoint cache and drain tracked
    /// garbage. Returns the fold state and any untracked offline-sweep debt.
    /// CAS exhaustion is an error and retains the ready fold for retry. Compactor failures remain errors until another
    /// compaction is started; successful appends are not undone by them. Cleanup
    /// errors may be returned after the fold has already been installed.
    pub fn flush(&mut self) -> Result<MaintenanceStatus, WalError> {
        self.wait_for_compaction()?;
        for attempt in 1..=self.core.opts.max_write_attempts {
            if !self.has_pending_fold() {
                break;
            }
            self.try_install(&[]).map_err(|error| error.source)?;
            if self.has_pending_fold() && attempt == self.core.opts.max_write_attempts {
                return Err(WalError::Contention {
                    operation: "flush",
                    attempts: attempt,
                });
            }
        }
        self.core.checkpoint_cache();
        let garbage = self.collect_garbage()?;
        Ok(MaintenanceStatus {
            compaction: self.current_compaction_status()?,
            garbage,
        })
    }

    /// Pull changes other instances have written. Returns whether anything
    /// changed.
    pub fn refresh(&mut self) -> Result<bool, WalError> {
        self.sync_compaction();
        let refreshed = self.core.refresh()?;
        self.discard_superseded_fold();
        self.queue_superseded(refreshed.superseded);
        Ok(refreshed.changed)
    }

    /// Start a compaction regardless of `should_compact`. False means work is
    /// running/ready, no entries need folding, or thread creation failed (visible
    /// through `compaction_status`). Starting new work clears a previous failure.
    pub fn compact_now(&mut self) -> bool {
        self.sync_compaction();
        if self.compaction_running()
            || self.has_pending_fold()
            || self.core.image.entries.is_empty()
        {
            return false;
        }
        self.spawn_compaction()
    }

    /// Wait for running work, returning ready, idle, installed, or superseded,
    /// or its failure. Does not install the fold; use `flush` or the next append.
    pub fn wait_for_compaction(&mut self) -> Result<CompactionStatus, WalError> {
        if let Compaction::Running(task) = &self.compaction {
            let outcome = task.rx.recv().unwrap_or_else(|_| {
                CompactOutcome::Failed("compaction thread panicked or disconnected".into())
            });
            self.finish_compaction(outcome);
        }
        self.discard_superseded_fold();
        self.current_compaction_status()
    }

    /// Observe completed background work without waiting for running work.
    pub fn compaction_status(&mut self) -> Result<CompactionStatus, WalError> {
        self.sync_compaction();
        self.current_compaction_status()
    }

    fn current_compaction_status(&self) -> Result<CompactionStatus, WalError> {
        Ok(match &self.compaction {
            Compaction::Idle => CompactionStatus::Idle,
            Compaction::Running(_) => CompactionStatus::Running,
            Compaction::Ready(_) => CompactionStatus::Ready,
            Compaction::Installed => CompactionStatus::Installed,
            Compaction::Superseded => CompactionStatus::Superseded,
            Compaction::Failed(error) => return Err(WalError::Compaction(error.clone())),
        })
    }

    /// Wait for compaction and flush, returning the fold/cleanup report, then
    /// consume the handle even on error. Inspect `garbage.overflowed` for debt.
    /// Use `flush(&mut self)` first when maintenance retries are wanted. A failed
    /// close never rolls back acknowledged appends; an uninstalled snapshot may
    /// remain as an orphan. Plain drop detaches a running compactor, which may
    /// finish its upload but cannot install a WAL reference.
    pub fn close(mut self) -> Result<MaintenanceStatus, WalError> {
        self.flush()
    }

    /// Best-effort checkpoint of the last observed WAL image for OnFlush.
    /// Cache errors never affect committed history.
    pub fn checkpoint_cache(&mut self) {
        self.core.checkpoint_cache();
    }

    pub fn garbage_status(&self) -> GarbageStatus {
        GarbageStatus {
            pending: self.garbage.len(),
            overflowed: self.garbage_overflowed,
            last_error: self.garbage_error.clone(),
        }
    }

    /// Delete queued, proven-obsolete snapshots. A failed DELETE remains queued
    /// for retry, including ambiguous failures. Never called by append/refresh.
    pub fn collect_garbage(&mut self) -> Result<GarbageStatus, crate::StoreError> {
        while let Some(key) = self.garbage.front() {
            if let Err(error) = self.core.store.delete(key) {
                self.garbage_error = Some(error.to_string());
                return Err(error);
            }
            self.garbage.pop_front();
        }
        self.garbage_error = None;
        Ok(self.garbage_status())
    }

    fn queue_obsolete(&mut self, key: String) {
        if self.garbage.contains(&key) {
            return;
        }
        if self.garbage.len() == self.core.opts.max_pending_deletes {
            self.garbage_overflowed = self.garbage_overflowed.saturating_add(1);
        } else {
            self.garbage.push_back(key);
        }
    }

    /// Last observed committed prefix. Call `refresh` to observe newer commits.
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
        matches!(self.compaction, Compaction::Running(_))
    }

    pub fn has_pending_fold(&self) -> bool {
        matches!(self.compaction, Compaction::Ready(_))
    }

    /// A recorded compaction failure, if any. Explicitly start new work with
    /// `compact_now` or acknowledge it with `take_compaction_error`; automatic
    /// triggers preserve failures until the caller handles them.
    pub fn last_compaction_error(&self) -> Option<&str> {
        match &self.compaction {
            Compaction::Failed(message) => Some(message),
            _ => None,
        }
    }

    /// Acknowledge and clear a recorded compactor failure. Already committed
    /// data is unaffected. Use this when abandoning maintenance rather than
    /// starting another compaction; it does not delete possible orphan uploads.
    pub fn take_compaction_error(&mut self) -> Option<String> {
        self.integrate_compaction();
        if !matches!(self.compaction, Compaction::Failed(_)) {
            return None;
        }
        let Compaction::Failed(message) = std::mem::replace(&mut self.compaction, Compaction::Idle)
        else {
            unreachable!("checked failed")
        };
        Some(message)
    }

    /// One CAS attempt: PUT the current image with any pending fold applied
    /// and `extra` appended. Returns whether the PUT was accepted. A lost CAS
    /// refreshes the state and re-checks the pending fold; the caller decides
    /// whether to try again. After a win the caller pushes `extra` into the
    /// image itself.
    fn try_install(&mut self, extra: &[Vec<u8>]) -> Result<bool, InstallError> {
        let data = match &self.compaction {
            Compaction::Ready(f) => {
                debug_assert!(f.applies_to(&self.core.image));
                let skip = (f.lsn + 1 - self.core.image.first_lsn()) as usize;
                let sr = SnapshotRef {
                    key: f.key.clone(),
                    lsn: f.lsn,
                };
                self.core
                    .image
                    .encode_view(Some(&sr), skip, extra, self.core.opts.limits())?
            }
            _ => self
                .core
                .image
                .encode_view(None, 0, extra, self.core.opts.limits())?,
        };
        let etag = self
            .core
            .etag
            .clone()
            .expect("writer always holds the wal etag");
        match self
            .core
            .store
            .put_if_match(&self.core.opts.wal_key(), Some(&etag), &data)
            .map_err(|source| InstallError {
                outcome: source.mutation_outcome,
                source: WalError::Store(source),
            })? {
            CondPut::Ok { etag } => {
                self.core.record_image_put(etag, &data);
                self.install_pending_fold();
                Ok(true)
            }
            CondPut::PreconditionFailed => {
                let refreshed = self.core.refresh()?;
                self.discard_superseded_fold();
                self.queue_superseded(refreshed.superseded);
                Ok(false)
            }
        }
    }

    /// After a winning PUT that carried a fold: drop the folded entries, swap
    /// the snapshot ref and queue the previous snapshot for explicit cleanup.
    /// The compactor already cached the new snapshot before reporting ready.
    fn install_pending_fold(&mut self) {
        if !self.has_pending_fold() {
            return;
        }
        let Compaction::Ready(f) = std::mem::replace(&mut self.compaction, Compaction::Installed)
        else {
            unreachable!("checked ready")
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
            self.core.cache.remove_snapshot(&old.key);
            self.queue_obsolete(old.key);
        }
    }

    /// Queue a snapshot object the WAL has stopped referencing — ours when
    /// an install PUT landed but reported failure, or a remote writer's that
    /// its own fold replaced. Idempotent: whoever folded may have collected
    /// it already.
    fn queue_superseded(&mut self, superseded: Option<SnapshotRef>) {
        if let Some(sr) = superseded {
            self.queue_obsolete(sr.key);
        }
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
    /// PUT landed even though it reported failure, so keep that snapshot —
    /// deleting it would destroy the live snapshot.
    fn discard_superseded_fold(&mut self) {
        let Compaction::Ready(f) = &self.compaction else {
            return;
        };
        if f.applies_to(&self.core.image) {
            return;
        }
        let installed = self
            .core
            .image
            .snapshot
            .as_ref()
            .is_some_and(|s| s.key == f.key);
        let status = if installed {
            Compaction::Installed
        } else {
            Compaction::Superseded
        };
        let Compaction::Ready(f) = std::mem::replace(&mut self.compaction, status) else {
            unreachable!("checked ready")
        };
        if !installed {
            self.core.cache.remove_snapshot(&f.key);
            self.queue_obsolete(f.key);
        }
    }

    fn integrate_compaction(&mut self) {
        let outcome = match &self.compaction {
            Compaction::Running(task) => match task.rx.try_recv() {
                Err(mpsc::TryRecvError::Empty) => return,
                Ok(outcome) => outcome,
                Err(mpsc::TryRecvError::Disconnected) => {
                    CompactOutcome::Failed("compaction thread panicked or disconnected".into())
                }
            },
            _ => return,
        };
        self.finish_compaction(outcome);
    }

    fn finish_compaction(&mut self, outcome: CompactOutcome) {
        let Compaction::Running(task) = std::mem::replace(&mut self.compaction, Compaction::Idle)
        else {
            unreachable!("a compaction is running")
        };
        self.compaction = if task.handle.join().is_err() {
            Compaction::Failed("compaction thread panicked".into())
        } else {
            match outcome {
                CompactOutcome::Done(fold) => Compaction::Ready(fold),
                CompactOutcome::Failed(message) => Compaction::Failed(message),
            }
        };
        self.discard_superseded_fold();
    }

    /// Called after each successful write. `compact_now` re-verifies
    /// eligibility, so the guard lives in one place.
    fn maybe_trigger_compaction(&mut self) {
        if !self.compaction_running()
            && !self.has_pending_fold()
            && !matches!(self.compaction, Compaction::Failed(_))
            && !self.core.image.entries.is_empty()
            && self.core.app.should_compact(&self.core.stats())
        {
            self.compact_now();
        }
    }

    fn spawn_compaction(&mut self) -> bool {
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
        let snap_prefix = format!("{}snap/{fold_lsn:020}-", self.core.opts.prefix);
        let max_snapshot_bytes = self.core.opts.max_snapshot_bytes;
        let (tx, rx) = mpsc::channel();
        let handle = std::thread::Builder::new()
            .name("waltier-compact".into())
            .spawn(move || {
                let _ = tx.send(run_compaction(
                    app,
                    store,
                    cache,
                    base_key,
                    entries,
                    snap_prefix,
                    max_snapshot_bytes,
                ));
            });
        match handle {
            Ok(handle) => {
                self.compaction = Compaction::Running(CompactionTask { rx, handle });
                true
            }
            Err(error) => {
                self.compaction = Compaction::Failed(format!("start compactor: {error}"));
                false
            }
        }
    }
}

fn run_compaction<A: WalApp>(
    app: Arc<A>,
    store: Arc<dyn ObjectStore>,
    cache: Cache,
    base_key: Option<String>,
    entries: Vec<Entry>,
    snap_prefix: String,
    max_snapshot_bytes: usize,
) -> CompactOutcome {
    let fold_lsn = entries
        .last()
        .expect("compaction requires live entries")
        .lsn;
    let base = match &base_key {
        None => None,
        Some(key) => match fetch_snapshot(&cache, store.as_ref(), key, max_snapshot_bytes) {
            Ok(Some(bytes)) => Some(bytes),
            Ok(None) => {
                return CompactOutcome::Failed(format!(
                    "base snapshot {key} is gone; refresh and explicitly restart compaction"
                ));
            }
            Err(e) => return CompactOutcome::Failed(e.to_string()),
        },
    };
    let snapshot = match app.compact(base.as_deref(), &entries) {
        Ok(s) => s,
        Err(e) => return CompactOutcome::Failed(e.to_string()),
    };
    if let Err(e) = check_limit("snapshot bytes", snapshot.len(), max_snapshot_bytes) {
        return CompactOutcome::Failed(e.to_string());
    }
    match publish_snapshot(store.as_ref(), &snapshot, || {
        unique_id().map(|id| format!("{snap_prefix}{id}"))
    }) {
        Ok(key) => {
            // Publication is confirmed before caching. Finish the optional disk
            // work on the compactor, then release its large snapshot allocation.
            cache.save_snapshot(&key, &snapshot);
            CompactOutcome::Done(Fold { key, lsn: fold_lsn })
        }
        Err(e) => CompactOutcome::Failed(e.to_string()),
    }
}

/// A collision never grants ownership of the existing object. A failed PUT
/// may have landed, so preserve that candidate and name it in the error for
/// offline collection; neither installing it nor deleting it is safe to infer.
fn publish_snapshot(
    store: &dyn ObjectStore,
    snapshot: &[u8],
    mut next_key: impl FnMut() -> std::io::Result<String>,
) -> Result<String, crate::StoreError> {
    for _ in 0..MAX_RACES {
        let key = next_key().map_err(|e| crate::StoreError::new(format!("snapshot ID: {e}")))?;
        match store.put_if_match(&key, None, snapshot) {
            Ok(CondPut::Ok { .. }) => return Ok(key),
            Ok(CondPut::PreconditionFailed) => continue,
            Err(mut e) => {
                e.message = format!("snapshot upload {key} failed (possible orphan retained): {e}");
                e.operation.get_or_insert(crate::StoreOperation::Put);
                e.key.get_or_insert(key);
                return Err(e);
            }
        }
    }
    Err(crate::StoreError::new(
        "snapshot key collision budget exhausted",
    ))
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
        Ok(self.core.refresh()?.changed)
    }

    /// Best-effort checkpoint when using OnFlush on a read-only replica.
    pub fn checkpoint_cache(&mut self) {
        self.core.checkpoint_cache();
    }

    /// Last observed committed prefix. Call `refresh` to observe newer commits.
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

#[cfg(test)]
mod publication_tests {
    use super::*;
    use crate::MemoryStore;

    #[test]
    fn snapshot_collision_retries_without_overwriting_the_existing_object() {
        let store = MemoryStore::new();
        store
            .put("snap/collision", b"acknowledged-history")
            .unwrap();
        let mut keys = ["snap/collision", "snap/new"].into_iter();
        let key =
            publish_snapshot(&store, b"candidate", || Ok(keys.next().unwrap().into())).unwrap();
        assert_eq!(key, "snap/new");
        assert_eq!(
            store.get("snap/collision").unwrap().unwrap().data,
            b"acknowledged-history"
        );
        assert_eq!(store.get(&key).unwrap().unwrap().data, b"candidate");
        assert!(publish_snapshot(&store, b"replacement", || Ok("snap/collision".into())).is_err());
        assert_eq!(
            store.get("snap/collision").unwrap().unwrap().data,
            b"acknowledged-history"
        );
    }
}
