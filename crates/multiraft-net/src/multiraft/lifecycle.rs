//! Bounded shutdown of existing native storage and operation owners.
use super::*;

impl<S: StateMachine> MultiRaft<S> {
    pub(super) async fn shutdown_owned_groups(&self) -> Result<(), MultiRaftError> {
        self.snapshot_rt
            .stopping
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let rafts: Vec<(GroupId, Raft<S>, StateMachineStore<S>)> = self
            .groups
            .lock()
            .unwrap()
            .iter()
            .map(|(&group_id, group)| (group_id, group.raft.clone(), group.state_machine.clone()))
            .collect();
        tracing::info!(
            target: "multiraft::recovery",
            operation = "node_shutdown",
            phase = "start",
            node_id = self.node_id,
            group_count = rafts.len() as u64,
            "shutting down Multi-Raft node"
        );
        for (_, _, sm) in &rafts {
            sm.close_native_intake();
        }
        for (group_id, raft, _) in &rafts {
            if let Err(error) = raft.shutdown().await {
                tracing::error!(
                    target: "multiraft::recovery",
                    operation = "node_shutdown",
                    phase = "error",
                    node_id = self.node_id,
                    group_id,
                    error = %error,
                    error_debug = ?error,
                    "failed to shut down Raft group"
                );
                return Err(MultiRaftError::Other(anyhow::anyhow!(
                    "shutdown group {group_id}: {error}"
                )));
            }
        }
        let operations: Vec<_> = self
            .snapshot_rt
            .operations
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect();
        for operation in operations {
            let task = operation.task.lock().unwrap().take();
            if let Some(task) = task {
                task.await
                    .map_err(|error| MultiRaftError::Other(error.into()))?;
            }
        }
        for (_, _, sm) in &rafts {
            sm.wait_native_quiescent().await;
        }
        let _builds_quiescent = self
            .snapshot_rt
            .build_budget
            .clone()
            .acquire_owned()
            .await
            .map_err(|error| MultiRaftError::Other(error.into()))?;
        self.groups.lock().unwrap().clear();
        if let NetBackend::InProcess { router, .. } = &self.net {
            let _ = router.unregister_node(self.node_id);
        }
        tracing::info!(
            target: "multiraft::recovery",
            operation = "node_shutdown",
            phase = "complete",
            node_id = self.node_id,
            remaining_groups = 0_u64,
            "shut down Multi-Raft node"
        );
        Ok(())
    }
}
