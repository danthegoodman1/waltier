use thiserror::Error;

/// Error from an [`crate::ObjectStore`] implementation.
#[derive(Debug, Error)]
#[error("{0}")]
pub struct StoreError(pub String);

#[derive(Debug, Error)]
pub enum WalError {
    /// The WAL changed under a write and the app declined to retry. The
    /// state has been refreshed; `entries` are the pending entries (one for
    /// `write`, the whole batch for `write_batch`, with any `Replace`
    /// rewrites applied), returned so the caller can re-validate and
    /// resubmit.
    #[error("write conflict: the WAL changed and the app declined to retry")]
    Conflict { entries: Vec<Vec<u8>> },

    #[error("storage: {0}")]
    Store(#[from] StoreError),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("corrupt wal: {0}")]
    Corrupt(String),

    /// Error surfaced by a [`crate::WalApp`] method.
    #[error("app: {0}")]
    App(String),
}
