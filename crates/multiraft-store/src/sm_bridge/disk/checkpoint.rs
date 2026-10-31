//! Inspect the native ledger inside an owned published disk checkpoint.
use super::{invalid, unsupported, AppliedRecord};
use crate::{disk_snapshot::DiskManifest, StateMachineStore};
use multiraft_core::{
    typ::{LogId, StoredMembership},
    NativeLedger, TypeConfig,
};
use multiraft_fsm::StateMachine;
use openraft::alias::SnapshotMetaOf;
use std::io;

/// Durable applied facts read from the checkpoint database, not the live FSM.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlledCheckpointState {
    pub applied: LogId,
    pub membership: StoredMembership,
    pub ledger: NativeLedger,
}

impl<S: StateMachine> StateMachineStore<S> {
    /// Inspect this Group's exact current published checkpoint and native ledger.
    ///
    /// Publication/install is serialized through the inspection. Replaced or
    /// missing descriptors fail closed; callers must separately retain a bounded
    /// sender lease when subsequently transferring the checkpoint files.
    pub async fn inspect_controlled_checkpoint(
        &self,
        meta: &SnapshotMetaOf<TypeConfig>,
        data: &[u8],
    ) -> io::Result<ControlledCheckpointState> {
        if !self.disk {
            return Err(unsupported("controlled checkpoint inspection"));
        }
        if self.native_is_closing() {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "native checkpoint owner closed",
            ));
        }
        let descriptor = DiskManifest::decode(data, self.group_id)?;
        if descriptor.meta != *meta {
            return Err(invalid("controlled checkpoint metadata mismatch"));
        }
        let _transition = self.disk_rt.transition.lock().await;
        let workspace = self.disk_workspace().await?;
        let group = self.group_id;
        let published = tokio::task::spawn_blocking(move || workspace.latest(group))
            .await
            .map_err(io::Error::other)??
            .ok_or_else(|| invalid("controlled checkpoint is not published"))?;
        if published.bytes != data || published.manifest != descriptor {
            return Err(invalid(
                "controlled checkpoint descriptor is no longer current",
            ));
        }
        let directory = std::fs::symlink_metadata(&published.manifest.source)?;
        if !directory.file_type().is_dir() {
            return Err(invalid(
                "controlled checkpoint directory is not owned regular storage",
            ));
        }
        for file in &published.manifest.files {
            let actual = std::fs::symlink_metadata(published.manifest.source.join(&file.name))?;
            if !actual.file_type().is_file() || actual.len() != file.size {
                return Err(invalid(
                    "controlled checkpoint file differs from its manifest",
                ));
            }
        }
        let bytes = self
            .inner
            .lock()
            .await
            .fsm
            .disk_state()
            .ok_or_else(|| invalid("state machine left disk-backed mode"))?
            .inspect_checkpoint(&published.manifest.source)?
            .ok_or_else(|| invalid("controlled checkpoint has no applied record"))?;
        let record = AppliedRecord::decode(&bytes)?;
        if Some(record.last_applied) != meta.last_log_id
            || record.membership != meta.last_membership
        {
            return Err(invalid(
                "controlled checkpoint database disagrees with its metadata",
            ));
        }
        let ledger = record
            .control
            .ok_or_else(|| invalid("checkpoint has no native control ledger"))?;
        Ok(ControlledCheckpointState {
            applied: record.last_applied,
            membership: record.membership,
            ledger,
        })
    }
}
