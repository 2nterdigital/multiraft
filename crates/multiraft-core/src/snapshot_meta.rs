//! Snapshot metadata that still carries a snapshot id.
//!
//! OpenRaft 0.10.1 dropped `snapshot_id` from `SnapshotMeta`. multiraft keeps
//! writing a non-empty id wherever the metadata is persisted or sent to a peer:
//! the snapshot RPC tuple, the disk manifest, the native catalog metadata and
//! the controlled seed. Nodes on OpenRaft 0.10.0-alpha.30 read these bytes as
//! their own `SnapshotMeta` and require the id (the `disk-v1-` prefix of disk
//! snapshots, a non-empty native id), so the field order, names and serde
//! layout here must stay exactly those of the alpha.30 type.
use serde::{Deserialize, Serialize};

use crate::typ::{LogId, SnapshotMeta, StoredMembership};

/// The alpha.30 `SnapshotMeta` layout: OpenRaft's metadata plus its id.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMetaWithId {
    pub last_log_id: Option<LogId>,
    pub last_membership: StoredMembership,
    pub snapshot_id: String,
}

impl SnapshotMetaWithId {
    pub fn new(meta: SnapshotMeta, snapshot_id: String) -> Self {
        Self {
            last_log_id: meta.last_log_id,
            last_membership: meta.last_membership,
            snapshot_id,
        }
    }

    /// OpenRaft's metadata, without the id.
    pub fn into_meta(self) -> SnapshotMeta {
        SnapshotMeta {
            last_log_id: self.last_log_id,
            last_membership: self.last_membership,
        }
    }

    /// OpenRaft's metadata, without the id.
    pub fn meta(&self) -> SnapshotMeta {
        self.clone().into_meta()
    }

    /// Whether this describes the same snapshot cut as OpenRaft's `meta`.
    pub fn matches(&self, meta: &SnapshotMeta) -> bool {
        self.last_log_id == meta.last_log_id && self.last_membership == meta.last_membership
    }
}
