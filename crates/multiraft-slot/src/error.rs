//! Errors at configuration and persistent identity boundaries.

/// A rejected configuration, routing request, or data directory.
#[derive(Debug, thiserror::Error)]
pub enum SlotError {
    #[error("invalid slot configuration: {0}")]
    Config(String),
    #[error("key length {actual} exceeds maximum {max}")]
    KeyTooLong { actual: usize, max: usize },
    #[error("unknown slot {0}")]
    UnknownSlot(u32),
    #[error("unknown group {0}")]
    UnknownGroup(u64),
    #[error("unknown node {0}")]
    UnknownNode(u64),
    #[error("manifest identity does not match: {0}")]
    ManifestMismatch(&'static str),
    #[error("invalid persisted manifest: {0}")]
    InvalidManifest(String),
    #[error("data directory is nonempty but has no manifest")]
    DataDirectoryNotEmpty,
    #[error("manifest creation is in progress or was interrupted")]
    CreationInProgress,
    #[error("manifest I/O failed: {0}")]
    Io(#[from] std::io::Error),
}
