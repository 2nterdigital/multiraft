//! Fixed, durable slot routing for opaque application keys.
//!
//! Keys hash into 256 fixed buckets; a persisted table maps buckets to Raft
//! slots ([`KeyRouter`]). Layouts are generated deterministically by [`placement`].
//!
//! Slot groups carry Raft replication order; applications define their own business sequence.
//! Layout changes require new data. There is no online resharding in this format.

pub mod config;
pub mod error;
pub mod manifest;
pub mod placement;
pub mod router;

pub use config::{
    ClusterLayout, GroupId, LayoutConfig, ManifestDigest, NodeAddress, NodeId, RouteDigest,
    RouteVersion, SlotAssignment, SlotId, FORMAT_VERSION, MAX_NODES, MAX_SLOTS, MAX_VOTERS,
};
pub use error::SlotError;
pub use manifest::{SlotManifest, MANIFEST_FILE_NAME};
pub use placement::{
    contiguous_bucket_table, plan as plan_layout, PlacementInput, PlacementNode, REPLICATION_FACTOR,
};
pub use router::{HashBucketId, KeyRouter, BUCKET_COUNT, MAX_KEY_LEN};

pub mod limits;
pub mod rpc;
pub mod runtime;
pub mod runtime_error;
pub mod startup;
pub mod status;
pub use limits::{
    Inflight, InflightStatus, SlotLimits, MAX_COMMAND_BYTES, MAX_EFFECTS_BYTES, MAX_QUERY_BYTES,
};
pub use multiraft_core::StartupProvenance;
pub use multiraft_net::{AppliedView, ObservedLogId};
pub use multiraft_net::{
    GroupStartupReport, InitializeDisposition, InitializePolicy, StartupPhase, StartupReport,
};
pub use runtime::{SlotApplied, SlotHandle, SlotNodeConfig, SlotNodeOwner, SlotStateMachine};
pub use runtime_error::{MaintenanceFailure, RuntimeError};
pub use startup::{ProbeError, SlotStartupPlan, BOOTSTRAP_GRACE_RANGE, MAX_RECOVERY_TIMEOUT};
pub use status::{ShutdownReport, SlotStatus};
