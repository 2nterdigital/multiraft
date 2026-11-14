//! Persist the validated concrete storage capability, bound to its replica generation.
use super::*;
use multiraft_fsm::DiskStoragePaths;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StorageOwner {
    node: NodeId,
    group: GroupId,
    generation: u64,
    identity: Vec<u8>,
    fingerprint: Option<[u8; 32]>,
    live: PathBuf,
    workspace: PathBuf,
}
fn record_path(root: &std::path::Path, group: GroupId, generation: u64) -> PathBuf {
    root.join("controlled-storage")
        .join(format!("group-{group}"))
        .join(format!("generation-{generation}.json"))
}
impl<S: StateMachine> MultiRaft<S> {
    pub(in crate::multiraft) fn controlled_validate_fsm(
        &self,
        group: GroupId,
        fsm: &mut S,
    ) -> Result<(), ControlledError> {
        let live = multiraft_store::DiskFsmDb::group_path(&self.config.data_dir, group);
        let workspace = multiraft_store::DiskFsmDb::workspace_path(&live)?;
        let disk = fsm.disk_state().ok_or(ControlledError::Rejected(
            "controlled group requires a disk-backed FSM",
        ))?;
        let declared = disk.storage_paths()?;
        if declared.live != live
            || declared.workspace != workspace
            || disk.snapshot_workspace()? != workspace
        {
            return Err(ControlledError::Rejected(
                "controlled FSM must declare the canonical owned live and snapshot namespaces",
            ));
        }
        for path in [
            self.config.data_dir.join("fsm-rocksdb"),
            live.clone(),
            workspace.clone(),
        ] {
            match std::fs::symlink_metadata(path) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err(ControlledError::Rejected(
                        "controlled FSM namespace may not be a symlink",
                    ))
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        let receipt =
            self.controlled_db()?
                .lifecycle_receipt(group)?
                .ok_or(ControlledError::Rejected(
                    "storage generation receipt missing",
                ))?;
        let record = StorageOwner {
            node: self.node_id,
            group,
            generation: receipt.generation,
            identity: receipt.identity,
            fingerprint: self.config.raft_transport_fingerprint,
            live,
            workspace,
        };
        let bytes = serde_json::to_vec(&record).map_err(std::io::Error::other)?;
        super::durable_record::publish(
            &self.config.data_dir,
            &record_path(&self.config.data_dir, group, record.generation),
            &bytes,
        )?;
        Ok(())
    }
}
impl<S: StateMachine> ControlledGroup<S> {
    pub(super) fn storage_owner(&self) -> Result<DiskStoragePaths, ControlledError> {
        let node = self.node()?;
        self.checked_receipt()?;
        let bytes = std::fs::read(record_path(
            &node.config.data_dir,
            self.group,
            self.generation,
        ))?;
        let record: StorageOwner = serde_json::from_slice(&bytes).map_err(std::io::Error::other)?;
        let live = multiraft_store::DiskFsmDb::group_path(&node.config.data_dir, self.group);
        let workspace = multiraft_store::DiskFsmDb::workspace_path(&live)?;
        if record.node != node.node_id
            || record.group != self.group
            || record.generation != self.generation
            || record.identity != self.identity
            || record.fingerprint != node.config.raft_transport_fingerprint
            || record.live != live
            || record.workspace != workspace
        {
            return Err(ControlledError::Rejected(
                "durable storage ownership identity mismatch",
            ));
        }
        Ok(DiskStoragePaths {
            live: record.live,
            workspace: record.workspace,
        })
    }
}
