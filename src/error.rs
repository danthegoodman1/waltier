use thiserror::Error;

/// The operation that failed at the storage boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreOperation {
    Get,
    Put,
    Delete,
}

/// Whether a failed mutation could have reached the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationOutcome {
    /// The store guarantees the mutation was not applied.
    NotApplied,
    /// The mutation may have been applied. Refresh before deciding to resubmit.
    Unknown,
}

/// Error from an [`crate::ObjectStore`] implementation. The default constructor
/// is conservative: a custom-store error never implies a failed PUT did not land.
#[derive(Debug, Error)]
#[error("{message}")]
pub struct StoreError {
    pub message: String,
    pub operation: Option<StoreOperation>,
    pub key: Option<String>,
    pub status: Option<u16>,
    pub mutation_outcome: MutationOutcome,
}

impl StoreError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            operation: None,
            key: None,
            status: None,
            mutation_outcome: MutationOutcome::Unknown,
        }
    }

    /// Use only when the backend knows the failed mutation was not applied.
    pub fn not_applied(mut self) -> Self {
        self.mutation_outcome = MutationOutcome::NotApplied;
        self
    }

    pub fn with_context(
        mut self,
        operation: StoreOperation,
        key: &str,
        status: Option<u16>,
    ) -> Self {
        self.operation = Some(operation);
        self.key = Some(key.into());
        self.status = status;
        self
    }
}

#[derive(Debug, Error)]
pub enum WalError {
    /// A valid image or snapshot exceeds the configured resource budget.
    /// Append rejection occurs before the WAL CAS and never commits part of a batch.
    #[error("{resource} limit exceeded: {actual} > {limit}")]
    LimitExceeded {
        resource: &'static str,
        limit: usize,
        actual: usize,
    },

    #[error("invalid options: {0}")]
    InvalidOptions(String),

    #[error("LSN space exhausted")]
    LsnExhausted,

    #[error("application aborted batch reconciliation")]
    ReconcileAborted,

    #[error("{operation} contention budget exhausted after {attempts} attempts")]
    Contention {
        operation: &'static str,
        attempts: u32,
    },

    #[error("write requires exactly one replacement entry, got {actual}")]
    InvalidReplacement { actual: usize },

    #[error("compaction: {0}")]
    Compaction(String),

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

/// A failed append, with ownership of the final attempted batch. A replacement
/// is retained even when it subsequently fails validation or storage access.
#[derive(Debug, Error)]
#[error("{source} (append outcome: {outcome:?})")]
pub struct WriteError {
    pub entries: Vec<Vec<u8>>,
    #[source]
    pub source: WalError,
    /// Refers to this append, not to earlier appends or maintenance work.
    pub outcome: MutationOutcome,
}
