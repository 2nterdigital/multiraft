//! Generation-fenced observations never construct storage or retain application handles.
use super::*;
use crate::ReplicaStorageUsage;
use std::{io, path::Path};

impl<S: StateMachine> ControlledGroup<S> {
    pub(crate) async fn storage_usage(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<ReplicaStorageUsage, ControlledError> {
        let this = self.clone();
        self.node()?
            .controlled_cleanup_run(self.group, deadline, move |node| async move {
                let receipt = this.checked_receipt()?;
                let db = node.controlled_db()?;
                let retained_log_value_bytes =
                    db.observe_retained_log_bytes_generation(this.group, this.generation)?;
                let paths = this.storage_owner()?;
                let snapshots = node
                    .config
                    .data_dir
                    .join("snapshots")
                    .join(this.group.to_string());
                let (fsm_file_bytes, workspace_file_bytes, snapshot_file_bytes) =
                    tokio::task::spawn_blocking(move || {
                        (
                            file_bytes(&paths.live).ok(),
                            file_bytes(&paths.workspace).ok(),
                            file_bytes(&snapshots).ok(),
                        )
                    })
                    .await
                    .map_err(|error| ControlledError::Native(error.to_string()))?;
                // Cleaned permits native rejoin to replace the command owner. A scan which
                // overlaps that transition must never label new files with the old receipt.
                if this.checked_receipt()? != receipt {
                    return Err(ControlledError::Rejected(
                        "storage lifecycle changed during observation",
                    ));
                }
                Ok(ReplicaStorageUsage {
                    receipt,
                    retained_log_value_bytes,
                    fsm_file_bytes,
                    workspace_file_bytes,
                    snapshot_file_bytes,
                })
            })
            .await
    }
}
fn checked_sum(a: u64, b: u64) -> io::Result<u64> {
    a.checked_add(b)
        .ok_or_else(|| io::Error::other("observed file lengths overflow"))
}
fn file_bytes(path: &Path) -> io::Result<u64> {
    let meta = match std::fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    };
    if meta.file_type().is_symlink() {
        return Err(io::Error::other(
            "owned storage observation refuses symlinks",
        ));
    }
    if meta.is_file() {
        return Ok(meta.len());
    }
    let mut total = 0;
    for entry in std::fs::read_dir(path)? {
        total = checked_sum(total, file_bytes(&entry?.path())?)?;
    }
    Ok(total)
}
