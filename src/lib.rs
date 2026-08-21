//! WalTier: a tiered write-ahead log — memory, local disk, and object storage —
//! with etag-CAS fencing and user-defined compaction.
//!
//! The whole log lives in one small object (the "WAL image") that is rewritten
//! wholesale on every write with a compare-and-swap on its etag. The image
//! carries a pointer to the latest snapshot plus the entries appended since, so
//! it is its own manifest. This gives natural fencing: a stale writer's PUT
//! fails, it re-pulls the image, reconciles, and retries.
//!
//! The contract: entries are small metadata. Every write re-uploads the whole
//! image, so per-write cost is proportional to image size. Store large payloads
//! as separate immutable objects first (via [`ObjectStore`]), then append a WAL
//! entry that references them.
//!
//! Compaction is insert-triggered and never blocks writes. A background thread
//! folds the current entries into a new snapshot object; the writer installs
//! the fold on its next PUT (or an explicit [`WalTier::flush`]). Read-only
//! [`Replica`]s poll with conditional GETs and never compact.

mod cache;
mod error;
mod image;
#[cfg(feature = "s3")]
mod s3;
#[cfg(feature = "sim")]
pub mod sim;
mod store;
mod wal;

pub use error::{StoreError, WalError};
#[cfg(feature = "s3")]
pub use s3::{S3Config, S3Store};
pub use store::{CondGet, CondPut, FsStore, MemoryStore, ObjectStore, Stored};
pub use wal::{Options, Replica, WalTier};

/// Log sequence number. Assigned by the library, starting at 0, contiguous.
pub type Lsn = u64;

/// A WAL entry paired with its sequence number, as handed to [`WalApp::compact`].
#[derive(Debug, Clone)]
pub struct Entry {
    pub lsn: Lsn,
    pub data: Vec<u8>,
}

/// Counters describing the live WAL, passed to [`WalApp::should_compact`].
#[derive(Debug, Clone, Copy)]
pub struct WalStats {
    /// Highest LSN in the log, `None` while the log is empty.
    pub tip: Option<Lsn>,
    /// LSN through which the current snapshot folds, `None` before the first fold.
    pub snapshot_lsn: Option<Lsn>,
    /// Entries in the image (appended since the snapshot).
    pub live_entries: u64,
    /// Total bytes of those entries.
    pub live_entry_bytes: u64,
    /// Encoded size of the WAL image as last written or fetched.
    pub image_bytes: u64,
}

/// What to do with a pending entry after a write conflict, once the library has
/// pulled the missed entries and applied them to the state.
#[derive(Debug)]
pub enum Reconcile {
    /// Append the entry unchanged at the new tip.
    Retry,
    /// Append this rewritten entry instead.
    Replace(Vec<u8>),
    /// Give up; `write` returns [`WalError::Conflict`] with the entry.
    Abort,
}

/// The application half of the log: how entries build state, how state folds
/// into snapshots, and when to fold.
///
/// `init` and `apply` are the minimum. `restore` and `compact` become
/// reachable once compaction is enabled (via `should_compact` or
/// [`WalTier::compact_now`]); their defaults return an error so a basic
/// append-only app can skip them.
pub trait WalApp: Send + Sync + 'static {
    /// In-memory state built by applying entries in LSN order.
    type State;

    /// Empty state, before any entry.
    fn init(&self) -> Self::State;

    /// Fold one entry into the state. Called exactly once per LSN, in order.
    fn apply(&self, state: &mut Self::State, lsn: Lsn, entry: &[u8]);

    /// Rebuild state from a snapshot produced by `compact`.
    fn restore(&self, _snapshot: &[u8]) -> Result<Self::State, WalError> {
        Err(WalError::App(
            "restore is not implemented for this WalApp".into(),
        ))
    }

    /// Fold the base snapshot plus the given entries into a new snapshot.
    /// Runs on a background thread; must not touch `State`.
    fn compact(&self, _base: Option<&[u8]>, _entries: &[Entry]) -> Result<Vec<u8>, WalError> {
        Err(WalError::App(
            "compact is not implemented for this WalApp".into(),
        ))
    }

    /// Whether an insert should trigger compaction. Checked after each
    /// successful write; replicas never call this.
    fn should_compact(&self, _stats: &WalStats) -> bool {
        false
    }

    /// Decide the fate of a pending entry after a write conflict. The state
    /// already reflects the entries that won.
    fn reconcile(&self, _state: &Self::State, _pending: &[u8]) -> Reconcile {
        Reconcile::Abort
    }
}
