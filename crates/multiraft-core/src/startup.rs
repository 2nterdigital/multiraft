//! Native provider evidence captured before construction changes storage.
/// Pristine means every owned native namespace was recognized and validated empty.
/// Unknown namespaces are errors, never this enum's pristine variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupProvenance {
    Pristine,
    Persisted,
}

impl StartupProvenance {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Pristine => "pristine",
            Self::Persisted => "persisted",
        }
    }
}

/// Raw local initialization reply, independent of quorum and leader causality.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitializeDisposition {
    NotDispatched,
    InitOk,
    NotAllowed {
        /// Raw last-log identity returned by native initialization refusal.
        last_log_id: Option<InitializationLogId>,
        /// Raw vote returned in the same refusal, not a later observation.
        vote: InitializationVote,
    },
    Unknown,
}

/// Complete raw log identity from an initialization refusal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InitializationLogId {
    pub term: u64,
    pub node_id: u64,
    pub index: u64,
}

/// Complete raw vote from an initialization refusal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InitializationVote {
    pub term: u64,
    pub node_id: u64,
    pub committed: bool,
}

impl InitializeDisposition {
    /// Bounded source code; detailed refusal facts remain separate scalars.
    pub const fn code(self) -> &'static str {
        match self {
            Self::NotDispatched => "not_dispatched",
            Self::InitOk => "init_ok",
            Self::NotAllowed { .. } => "not_allowed",
            Self::Unknown => "unknown",
        }
    }
}
