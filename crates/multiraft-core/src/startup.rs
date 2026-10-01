//! Native provider evidence captured before construction changes storage.
/// Pristine means every owned native namespace was recognized and validated empty.
/// Unknown namespaces are errors, never this enum's pristine variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupProvenance {
    Pristine,
    Persisted,
}

/// Raw local initialization reply, independent of quorum and leader causality.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitializeDisposition {
    NotDispatched,
    InitOk,
    NotAllowed,
    Unknown,
}
