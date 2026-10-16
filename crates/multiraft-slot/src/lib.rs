//! Fixed, durable slot routing for opaque application keys.
//!
//! Slot groups carry Raft replication order; applications define their own business sequence.
//! Layout changes require new data. There is no online resharding in this format.

pub mod config;
pub mod error;
pub mod manifest;

pub use config::{
    ClusterLayout, GroupId, HashVersion, LayoutConfig, ManifestDigest, NodeAddress, NodeId,
    SlotAssignment, SlotId, FORMAT_VERSION, MAX_KEY_LEN, MAX_NODES, MAX_SLOTS, MAX_VOTERS,
};
pub use error::SlotError;
pub use manifest::{SlotManifest, MANIFEST_FILE_NAME};

pub mod limits;
pub mod rpc;
pub mod runtime;
pub mod runtime_error;
pub mod status;
pub use limits::{
    Inflight, InflightStatus, SlotLimits, MAX_COMMAND_BYTES, MAX_EFFECTS_BYTES, MAX_QUERY_BYTES,
};
pub use multiraft_net::{AppliedView, ObservedLogId};
pub use runtime::{SlotApplied, SlotHandle, SlotNodeConfig, SlotNodeOwner, SlotStateMachine};
pub use runtime_error::{MaintenanceFailure, RuntimeError};
pub use status::{ShutdownReport, SlotStatus};
