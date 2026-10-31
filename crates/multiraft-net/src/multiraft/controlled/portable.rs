//! Portable native target boundary. Decoded messages are observations, never sealed native proofs.
use super::*;
use crate::controlled_startup::StartupAuthorityFuture;
use multiraft_core::{
    typ::{LogId, StoredMembership},
    OperationRecord, TargetOpenReceipt,
};
use multiraft_store::disk_snapshot::DiskChunk;
use serde::{Deserialize, Serialize};

mod client;
mod endpoint;
pub use endpoint::NativeTargetEndpoint;

/// Native protocol version; unsupported versions fail before dispatch.
pub const NATIVE_TARGET_VERSION: u16 = 1;
/// Authenticated transport peer and data-space identity supplied by a trusted transport adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeTargetIdentity {
    pub node: NodeId,
    pub domain: Option<[u8; 32]>,
    pub group: GroupId,
    pub generation: u64,
    pub consumption: Vec<u8>,
}
/// Complete immutable operation scope, echoed by every response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeTargetScope {
    pub version: u16,
    pub target: NativeTargetIdentity,
    pub specification: ReplacementSpec,
    pub correlation: [u8; 16],
}
/// Untrusted seed header; target verifies received body, catalog, vote and log basis before opening.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeSeedHeader {
    pub record: OperationRecord,
    pub vote: multiraft_core::typ::Vote,
    pub meta: multiraft_core::typ::SnapshotMeta,
    pub manifest: Vec<u8>,
}
/// Versioned requests; chunks are bounded and streamed into a target-owned workspace.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeTargetRequest {
    pub scope: NativeTargetScope,
    pub command: NativeTargetCommand,
}
/// Target-side native operations. No command accepts executor or cleanup authority from bytes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NativeTargetCommand {
    Observe {
        configuration: Option<StoredMembership>,
        barrier: Option<LogId>,
    },
    StartSeed {
        header: NativeSeedHeader,
        admission: StartupAdmission,
    },
    SeedChunk {
        sequence: u64,
        chunk: Option<DiskChunk>,
    },
    FinishSeed,
}
/// Delivery uncertainty is explicit; retry using the same operation and authoritative observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeTargetOutcome {
    Unsent,
    InProgress,
    Durable,
    Rejected,
    Unknown,
}
/// Durable stage observed on the target; decoded stages convey no authority by themselves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeTargetStage {
    Reserved,
    Opening,
    Opened,
    Retired,
    LogsCleaned,
    Cleaned,
}
/// Actual target observation, checked again against the source's pinned operation and membership.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeTargetObservation {
    pub stage: NativeTargetStage,
    pub configuration: Option<StoredMembership>,
    pub applied: Option<LogId>,
    pub record: Option<OperationRecord>,
}
/// Reply echoes complete scope and requested barrier/configuration to prevent cross-request reuse.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeTargetReply {
    pub scope: NativeTargetScope,
    pub command: NativeTargetCommand,
    pub outcome: NativeTargetOutcome,
    pub observation: Option<NativeTargetObservation>,
    pub error: Option<String>,
}
/// Failure from an authenticated transport; delivery state cannot establish durable native facts.
#[derive(Debug, Clone, thiserror::Error)]
#[error("native target transport ({outcome:?}): {message}")]
pub struct NativeTargetTransportError {
    pub outcome: NativeTargetOutcome,
    pub message: String,
}
/// Trusted adapter for a channel authenticated to exactly one target node and data space.
///
/// Implementations MUST authenticate the remote process as `peer()` independently of decoded
/// input, and bind every request/reply to that channel. A request header, claimed node id,
/// network address, or caller-provided trusted flag is insufficient. The application supplies
/// this authority just as it supplies `StartupAuthority`; native still checks all wire scope,
/// operation, generation, configuration and application facts. Implementations must bound frames
/// before decoding, preserve request bytes, and report uncertain delivery conservatively.
pub trait NativeTargetTransport: Send + Sync {
    fn peer(&self) -> (NodeId, Option<[u8; 32]>);
    fn exchange(
        &self,
        request: Vec<u8>,
    ) -> StartupAuthorityFuture<'_, Result<Vec<u8>, NativeTargetTransportError>>;
}
/// A source-owned, immutable target identity and authenticated asynchronous port; no remote FSM.
#[derive(Clone)]
pub struct SourceTargetPort {
    identity: NativeTargetIdentity,
    specification: ReplacementSpec,
    transport: Arc<dyn NativeTargetTransport>,
}
impl std::fmt::Debug for SourceTargetPort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceTargetPort")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}
impl SourceTargetPort {
    /// Bind a trusted authenticated channel to immutable target and operation arguments.
    pub fn new(
        identity: NativeTargetIdentity,
        specification: ReplacementSpec,
        transport: Arc<dyn NativeTargetTransport>,
    ) -> Result<Self, ControlledError> {
        let old: BTreeSet<_> = specification
            .expected_start
            .membership()
            .voter_ids()
            .collect();
        let added: Vec<_> = specification
            .target_voters
            .difference(&old)
            .copied()
            .collect();
        if transport.peer() != (identity.node, identity.domain)
            || added != [identity.node]
            || identity.generation != specification.target_generation
            || identity.consumption.is_empty()
        {
            return Err(ControlledError::Rejected(
                "authenticated target identity mismatch",
            ));
        }
        Ok(Self {
            identity,
            specification,
            transport,
        })
    }
    pub fn identity(&self) -> &NativeTargetIdentity {
        &self.identity
    }
    pub fn specification(&self) -> &ReplacementSpec {
        &self.specification
    }
    pub(super) fn validate_source<S: StateMachine>(
        &self,
        node: &MultiRaft<S>,
        group: GroupId,
        specification: &ReplacementSpec,
    ) -> Result<(), ControlledError> {
        if self.identity.domain != node.config.raft_transport_fingerprint
            || self.identity.group != group
            || self.specification != *specification
        {
            return Err(ControlledError::Rejected(
                "target scope differs from source operation",
            ));
        }
        Ok(())
    }
    fn scope(&self, correlation: [u8; 16]) -> NativeTargetScope {
        NativeTargetScope {
            version: NATIVE_TARGET_VERSION,
            target: self.identity.clone(),
            specification: self.specification.clone(),
            correlation,
        }
    }
}

/// Local compatibility adapter uses the same encoded request dispatcher and response validation.
impl<S: StateMachine> ControlledGroup<S> {
    pub fn target_port(
        &self,
        specification: ReplacementSpec,
    ) -> Result<SourceTargetPort, ControlledError> {
        let endpoint = NativeTargetEndpoint::new(self.clone(), specification.clone())?;
        SourceTargetPort::new(
            endpoint.identity().clone(),
            specification,
            Arc::new(endpoint),
        )
    }
}

/// A bounded actual checkpoint export. Dropping the receiver releases the source-owned lease.
/// The source registry retains and joins blocking reads/destruction before source cleanup.
pub struct NativeSeedExport {
    header: NativeSeedHeader,
    chunks: super::seed_stream::ChunkReceiver,
}
impl NativeSeedExport {
    pub fn header(&self) -> &NativeSeedHeader {
        &self.header
    }
    /// Read one bounded chunk; an interrupted stream fails rather than implying completion.
    pub async fn next_chunk(&mut self) -> Result<Option<DiskChunk>, ControlledError> {
        self.chunks
            .recv()
            .await
            .ok_or(ControlledError::Rejected("source seed stream incomplete"))?
            .map_err(ControlledError::Native)
    }
}
impl ControlledSeed {
    /// Export this actual checkpoint in its source process; never opens source files on a target.
    pub async fn export(&self, group: GroupId) -> Result<NativeSeedExport, ControlledError> {
        let chunks = self.input.open(self.manifest.clone(), group).await?;
        Ok(NativeSeedExport {
            header: NativeSeedHeader {
                record: self.record.clone(),
                vote: self.vote,
                meta: self.meta.clone(),
                manifest: self.manifest.clone(),
            },
            chunks,
        })
    }
}
