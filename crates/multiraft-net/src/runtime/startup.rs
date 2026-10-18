//! Public business-neutral batch startup contracts.
pub(super) mod admission;
mod operation;
use super::*;
pub use multiraft_core::{InitializeDisposition, StartupProvenance};

/// One local declaration; preference changes initialization order only.
#[derive(Debug, Clone)]
pub struct StartupGroup {
    pub group: GroupConfig,
    pub preferred_initializer: Option<NodeId>,
}
/// Opt-in batch on an empty owner. All registration precedes any initialization.
#[derive(Debug, Clone)]
pub struct StartupBatch {
    /// Opaque consumer correlation, never library/cohort verified.
    pub input_digest: Option<[u8; 32]>,
    pub groups: Vec<StartupGroup>,
    /// Explicit candidate, accepted range 100..=2000ms.
    pub grace: Duration,
    /// Per Group from actual registration; validation is afterward.
    pub recovery_timeout: Duration,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupPhase {
    Admission,
    Construct,
    Registered,
    Grace,
    Eligibility,
    Initialize,
    NativeWait,
    Validate,
    Ready,
}
impl StartupPhase {
    /// Bounded operational source label.
    pub const fn code(self) -> &'static str {
        match self {
            Self::Admission => "admission",
            Self::Construct => "construct",
            Self::Registered => "registered",
            Self::Grace => "grace",
            Self::Eligibility => "eligibility",
            Self::Initialize => "initialize",
            Self::NativeWait => "native_wait",
            Self::Validate => "validate",
            Self::Ready => "ready",
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupCleanup {
    NotRequired,
    Released,
    ReleaseUnconfirmed,
}
/// Observed source facts. Registration and deadlines are local monotonic instants.
#[derive(Debug, Clone)]
pub struct GroupStartupReport {
    pub group_id: GroupId,
    pub node_id: NodeId,
    pub preferred_initializer: Option<NodeId>,
    pub provenance: Option<StartupProvenance>,
    pub registered_at: Option<Instant>,
    pub deadline: Option<Instant>,
    pub phase: StartupPhase,
    pub initialization: InitializeDisposition,
    pub fallback: bool,
}
/// Local startup/validation facts; neither cohort agreement nor quorum readiness.
#[derive(Debug, Clone)]
pub struct StartupReport {
    pub input_digest: Option<[u8; 32]>,
    pub node_id: NodeId,
    /// Always false: the library has no remote cohort verification authority.
    pub configuration_verified: bool,
    pub groups: Vec<GroupStartupReport>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupRejection {
    Closed,
    Busy,
    OwnerNotEmpty,
    InvalidInput,
}
/// Source chain and partial source facts survive rollback, including its uncertainty.
#[derive(Debug)]
pub struct StartupFailure {
    pub rejection: Option<StartupRejection>,
    pub group_id: Option<GroupId>,
    pub phase: StartupPhase,
    pub outcome_unknown: bool,
    pub report: Box<StartupReport>,
    pub cleanup: StartupCleanup,
    pub source: RuntimeError,
    pub cleanup_error: Option<RuntimeError>,
}
impl std::fmt::Display for StartupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "node {} startup {:?} group {:?}: {}; outcome_unknown={}; cleanup={:?}",
            self.report.node_id,
            self.phase,
            self.group_id,
            self.source,
            self.outcome_unknown,
            self.cleanup
        )
    }
}
impl std::error::Error for StartupFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
#[derive(Default)]
pub(super) struct StartupAdmission {
    legacy: usize,
    batch: bool,
}

impl StartupReport {
    fn new(node_id: NodeId, batch: &StartupBatch) -> Self {
        Self {
            node_id,
            input_digest: batch.input_digest,
            configuration_verified: false,
            groups: batch
                .groups
                .iter()
                .map(|input| GroupStartupReport {
                    group_id: input.group.group_id,
                    node_id,
                    preferred_initializer: input.preferred_initializer,
                    provenance: None,
                    registered_at: None,
                    deadline: None,
                    phase: StartupPhase::Construct,
                    initialization: InitializeDisposition::NotDispatched,
                    fallback: false,
                })
                .collect(),
        }
    }
    fn failure(
        self,
        group_id: Option<GroupId>,
        phase: StartupPhase,
        source: RuntimeError,
    ) -> StartupFailure {
        // A transaction can construct native workers, restore/application state
        // or receive peer writes before a later local failure. Cleanup never
        // retracts those effects. Admission rejection alone is known undispatched.
        let outcome_unknown = phase != StartupPhase::Admission
            && (self.groups.iter().any(|g| {
                g.registered_at.is_some() || g.initialization == InitializeDisposition::Unknown
            }) || phase == StartupPhase::Construct);
        StartupFailure {
            rejection: None,
            group_id,
            phase,
            outcome_unknown,
            report: Box::new(self),
            cleanup: StartupCleanup::ReleaseUnconfirmed,
            source,
            cleanup_error: None,
        }
    }
}
