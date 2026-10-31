//! Inspect actual native applied records in live stores and published checkpoints.
use super::{invalid, unsupported, AppliedRecord};
use crate::{disk_snapshot::DiskManifest, StateMachineStore};
use multiraft_core::{
    typ::{LogId, StoredMembership},
    NativeLedger, TypeConfig,
};
use multiraft_fsm::StateMachine;
use openraft::alias::SnapshotMetaOf;
use std::collections::BTreeMap;
use std::io;

fn validate_files(manifest: &DiskManifest) -> io::Result<()> {
    let expected: BTreeMap<_, _> = manifest
        .files
        .iter()
        .map(|file| (file.name.as_str(), file.size))
        .collect();
    let mut count = 0;
    for entry in std::fs::read_dir(&manifest.source)? {
        let entry = entry?;
        let name = entry.file_name();
        let expected_size = name
            .to_str()
            .and_then(|name| expected.get(name))
            .ok_or_else(|| invalid("controlled checkpoint contains an undeclared entry"))?;
        let actual = std::fs::symlink_metadata(entry.path())?;
        if !actual.file_type().is_file() || actual.len() != *expected_size {
            return Err(invalid(
                "controlled checkpoint file differs from its manifest",
            ));
        }
        count += 1;
    }
    if count != expected.len() {
        return Err(invalid("controlled checkpoint is missing a declared file"));
    }
    Ok(())
}

/// Durable native facts decoded from an actual disk applied record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlledCheckpointState {
    pub applied: LogId,
    pub membership: StoredMembership,
    pub ledger: NativeLedger,
}

impl<S: StateMachine> StateMachineStore<S> {
    /// Reread native facts from the live durable store, independently of cached state.
    ///
    /// Bootstrap v1 records have an empty native ledger. A pristine store, missing
    /// membership identity or malformed applied record cannot authorize recovery.
    pub async fn inspect_controlled_applied(&self) -> io::Result<ControlledCheckpointState> {
        if !self.disk {
            return Err(unsupported("controlled applied inspection"));
        }
        let _transition = self.disk_rt.transition.lock().await;
        if self.native_is_closing() {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "native applied owner closed",
            ));
        }
        let bytes = self
            .inner
            .lock()
            .await
            .fsm
            .disk_state()
            .ok_or_else(|| invalid("state machine left disk-backed mode"))?
            .durable_applied()?
            .ok_or_else(|| invalid("store has no durable applied record"))?;
        let record = AppliedRecord::decode(&bytes)?;
        let membership_id = record
            .membership
            .log_id()
            .ok_or_else(|| invalid("durable applied record has no native membership identity"))?;
        if membership_id.index > record.last_applied.index
            || membership_id > record.last_applied
            || (membership_id.index == record.last_applied.index
                && membership_id != record.last_applied)
            || record.membership.membership().voter_ids().next().is_none()
        {
            return Err(invalid(
                "durable native membership is inconsistent with applied position",
            ));
        }
        Ok(ControlledCheckpointState {
            applied: record.last_applied,
            membership: record.membership,
            ledger: record.control.unwrap_or_default(),
        })
    }

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
        validate_files(&published.manifest)?;
        let bytes = self
            .inner
            .lock()
            .await
            .fsm
            .disk_state()
            .ok_or_else(|| invalid("state machine left disk-backed mode"))?
            .inspect_checkpoint(&published.manifest.source)?
            .ok_or_else(|| invalid("controlled checkpoint has no applied record"))?;
        validate_files(&published.manifest)?;
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
