//! External durable startup admission; native Raft configuration remains the membership authority.
//!
//! Implementations belong to the application's shared durable catalog (TiKV in the delivery
//! application). There is deliberately no production process-local implementation here.
use multiraft_core::{
    typ::{LogId, StoredMembership},
    GroupId, NodeId, OperationId, OperationRecord, ReplacementSpec,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt, future::Future, pin::Pin};

/// An object-safe asynchronous external catalog operation.
pub type StartupAuthorityFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Immutable identity of one physical startup attempt; retry only to recover its outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartupRequest {
    pub group_id: GroupId,
    pub node_id: NodeId,
    pub generation: u64,
    pub request_id: [u8; 16],
}

/// Durable permission for one physical core open, never a business or quorum authority token.
///
/// The caller owns one-shot consumption. An idempotent retry recovers this same permission;
/// it does not authorize a second physical core. Admissions preceding binding remain
/// potentially live even after source retirement; exclusion follows native committed config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartupAdmission {
    pub request: StartupRequest,
    pub serial: u64,
    pub active_operation: Option<OperationId>,
}

/// Immutable durable target generation consumption identity, validated against its registry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartupTargetGeneration {
    pub node_id: NodeId,
    pub generation: u64,
    pub consumption_id: Vec<u8>,
}

/// Immutable active replacement binding, installed before proposing native Begin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartupBinding {
    pub group_id: GroupId,
    pub specification: ReplacementSpec,
    pub source: NodeId,
    /// Exact old source generation retained for cleanup after retirement.
    pub source_generation: u64,
    pub target: StartupTargetGeneration,
}

/// Exact native committed/applied terminal evidence, verified by the controlled runtime.
///
/// Implementations must validate the full native operation, final exact-three voter config
/// with no learners,
/// complete committed/applied membership identity, native terminal barrier and target
/// durable Opened receipt. `native_record.terminal` must be present and match this proof. A
/// management status, local stop, effective-only config, or source acknowledgement is
/// insufficient. The adapter accepts this only from the trusted native proof verifier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartupTerminalProof {
    pub group_id: GroupId,
    /// Retired source generation; old terminal evidence cannot clean a later generation.
    pub source_generation: u64,
    pub native_record: OperationRecord,
    pub committed_membership: StoredMembership,
    pub applied_membership: StoredMembership,
    pub applied_log: LogId,
    pub target_opened: StartupTargetGeneration,
}

/// Readback of one group's durable startup catalog, never a replacement for Raft membership.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartupCatalog {
    pub group_id: GroupId,
    pub revision: u64,
    pub membership: StoredMembership,
    pub authorized: BTreeMap<NodeId, u64>,
    pub active_operation: Option<StartupBinding>,
    pub admissions: Vec<StartupAdmission>,
    pub completed: Vec<StartupTerminalProof>,
}

/// Deterministic denial; replaying a request identity cannot change its immutable arguments.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StartupAuthorityRejection {
    UnknownGroup,
    UnauthorizedGeneration,
    ActiveOperation,
    IdentityConflict,
    ConfigurationChanged,
    InvalidProof,
    StaleOperation,
}

/// A durable outcome or uncertainty requiring authoritative readback before further work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupAuthorityResult<T> {
    Applied(T),
    Rejected(StartupAuthorityRejection),
    Unknown,
}

/// Transport/storage failure; callers must conservatively treat writes as uncertain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupAuthorityError(pub String);
impl fmt::Display for StartupAuthorityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for StartupAuthorityError {}

/// Shared, durable compare-and-set authority for physical startup and replacement binding.
///
/// Every mutation must linearize in the same external durable serialization domain.
/// `admit` and `bind_active` must conflict atomically: after binding, the moving source
/// is denied; retained old members remain admitted and only the consumed target generation
/// is admitted. Every admission before binding is retained as potentially live. No lease
/// timeout or missing source acknowledgement can bypass this rule.
///
/// `bind_active` requires an initialized uniform three-member native starting configuration
/// with no learners and compares its exact identity, authorizations and immutable
/// consumption receipt. Same-operation retries replay; changed identities and stale sequences
/// reject. The application's catalog provisioning is separate from ordinary startup and
/// must not recreate a missing catalog from local disk or management intent.
///
/// `terminal_handoff` atomically retires source, authorizes target, advances exact native
/// membership and clears active operation while retaining immutable terminal evidence.
/// Unknown must be read back before any subsequent move/start. Previously admitted source
/// cores may still physically run: native committed membership prevents restored authority;
/// local stop remains unknown. Catalog permission itself never establishes quorum authority.
///
/// Implementations retain immutable request outcomes (including denials) across restart.
/// Storage/transport errors and cancellation may leave mutations committed; never retry with
/// changed identities. Callers serialize local one-shot opens, while this boundary serializes
/// admission against all controllers and app cold starts.
pub trait StartupAuthority: Send + Sync {
    fn admit(
        &self,
        request: StartupRequest,
    ) -> StartupAuthorityFuture<
        '_,
        Result<StartupAuthorityResult<StartupAdmission>, StartupAuthorityError>,
    >;
    fn bind_active(
        &self,
        binding: StartupBinding,
    ) -> StartupAuthorityFuture<
        '_,
        Result<StartupAuthorityResult<StartupBinding>, StartupAuthorityError>,
    >;
    fn terminal_handoff(
        &self,
        proof: StartupTerminalProof,
    ) -> StartupAuthorityFuture<
        '_,
        Result<StartupAuthorityResult<StartupTerminalProof>, StartupAuthorityError>,
    >;
    fn read(
        &self,
        group_id: GroupId,
    ) -> StartupAuthorityFuture<'_, Result<StartupCatalog, StartupAuthorityError>>;
}
