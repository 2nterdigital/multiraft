//! Per-generation retirement waits for every native, task and application owner before deletion.
use super::*;
use multiraft_store::ControlledGroupStage;

impl<S: StateMachine> ControlledGroup<S> {
    /// Retire and remove only the excluded local generation proven by this sealed terminal.
    /// The retained job continues through all barriers if its caller leaves or times out.
    pub async fn retire_and_cleanup(
        &self,
        terminal: &ControlledTerminal,
        deadline: tokio::time::Instant,
    ) -> Result<(), ControlledError> {
        let this = self.clone();
        let terminal = terminal.clone();
        self.node()?
            .controlled_run(self.group, deadline, move |node| async move {
                if terminal.group != this.group || terminal.source != node.node_id {
                    return Err(ControlledError::Rejected(
                        "terminal does not exclude this local replica",
                    ));
                }
                let catalog = this.authority.read(this.group).await?;
                if !catalog.completed.iter().any(|completed| {
                    completed.native_record.specification == terminal.specification
                        && completed.native_record.terminal.as_ref() == Some(&terminal.proof)
                }) {
                    return Err(ControlledError::Rejected("terminal handoff is not durable"));
                }
                let db = node.controlled_db()?;
                let receipt = this.checked_receipt()?;
                if receipt.stage == ControlledGroupStage::Cleaned {
                    return Ok(());
                }
                let proof = serde_json::to_vec(&terminal.proof)
                    .map_err(|error| ControlledError::Native(error.to_string()))?;
                if receipt.stage == ControlledGroupStage::Opened {
                    db.retire(this.group, this.generation, &proof)?;
                }
                let receipt = this.checked_receipt()?;
                if !matches!(
                    receipt.stage,
                    ControlledGroupStage::Retired | ControlledGroupStage::LogsCleaned
                ) || receipt.terminal_proof.as_ref() != Some(&proof)
                {
                    return Err(ControlledError::Rejected("retirement identity mismatch"));
                }
                let owner = node.group_owner(this.group);
                owner.close();
                let app = node
                    .groups
                    .lock()
                    .expect("native group lock poisoned")
                    .remove(&this.group);
                if let Some(app) = app {
                    let release = app.state_machine.release_observer();
                    app.state_machine.close_native_intake();
                    app.raft
                        .shutdown()
                        .await
                        .map_err(|error| ControlledError::Native(error.to_string()))?;
                    owner
                        .watchers
                        .join()
                        .await
                        .map_err(|error| ControlledError::Native(error.to_string()))?;
                    owner
                        .maintenance
                        .join()
                        .await
                        .map_err(|error| ControlledError::Native(error.to_string()))?;
                    app.state_machine.wait_native_quiescent().await;
                    drop(app);
                    release.wait().await;
                } else {
                    owner
                        .watchers
                        .join()
                        .await
                        .map_err(|error| ControlledError::Native(error.to_string()))?;
                    owner
                        .maintenance
                        .join()
                        .await
                        .map_err(|error| ControlledError::Native(error.to_string()))?;
                }
                node.construction_recovery
                    .lock()
                    .expect("construction basis lock poisoned")
                    .remove(&this.group);
                let root = node.config.data_dir.clone();
                let group = this.group;
                let generation = this.generation;
                tokio::task::spawn_blocking(move || {
                    db.cleanup_group(group, generation, &proof)?;
                    for path in [
                        multiraft_store::DiskFsmDb::group_path(&root, group),
                        root.join("snapshots").join(group.to_string()),
                        root.join(format!("group-{group}")),
                    ] {
                        match std::fs::remove_dir_all(&path) {
                            Ok(()) => {
                                if let Some(parent) = path.parent() {
                                    std::fs::File::open(parent)?.sync_all()?;
                                }
                            }
                            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                            Err(error) => return Err(error),
                        }
                    }
                    db.finish_cleanup(group, generation, &proof)
                })
                .await
                .map_err(|error| ControlledError::Native(error.to_string()))??;
                Ok(())
            })
            .await
    }
}
