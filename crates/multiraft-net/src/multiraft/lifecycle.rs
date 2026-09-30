//! Shutdown of the existing native storage, transport and operation owners.
use super::*;

impl<S: StateMachine> MultiRaft<S> {
    pub(crate) fn abort_background(&self) {
        self.snapshot_rt
            .stopping
            .store(true, std::sync::atomic::Ordering::Release);
        self.snapshot_rt.maintenance_tasks.close();
        self.ingress_accepting
            .store(false, std::sync::atomic::Ordering::Release);
        if let Some(stop) = self.listener_stop.lock().unwrap().take() {
            let _ = stop.send(());
        }
        self.tasks.abort();
        self.ingress_tasks.abort();
        if let NetBackend::InProcess { router, .. } = &self.net {
            let _ = router.unregister_node(self.node_id);
        }
    }

    pub(super) async fn shutdown_owned_groups(&self) -> Result<(), MultiRaftError> {
        self.snapshot_rt
            .stopping
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.snapshot_rt.maintenance_tasks.close();
        self.ingress_accepting
            .store(false, std::sync::atomic::Ordering::Release);
        if let Some(stop) = self.listener_stop.lock().unwrap().take() {
            let _ = stop.send(());
        }
        if let NetBackend::InProcess { router, .. } = &self.net {
            let _ = router.unregister_node(self.node_id);
        }
        let rafts: Vec<(GroupId, Raft<S>, StateMachineStore<S>)> = self
            .groups
            .lock()
            .unwrap()
            .iter()
            .map(|(&group_id, group)| (group_id, group.raft.clone(), group.state_machine.clone()))
            .collect();
        tracing::info!(target: "multiraft::recovery", operation = "node_shutdown", phase = "start",
            node_id = self.node_id, group_count = rafts.len() as u64, "shutting down Multi-Raft node");
        let mut first_error = None;
        for (_, _, sm) in &rafts {
            sm.close_native_intake();
        }
        for (group_id, raft, _) in &rafts {
            if let Err(error) = raft.shutdown().await {
                first_error.get_or_insert_with(|| {
                    MultiRaftError::Other(anyhow::anyhow!("shutdown group {group_id}: {error}"))
                });
            }
        }
        // Stop/join listener, watchers and in-process ingress INCLUDING their children.
        if let Err(error) = self.tasks.join().await {
            first_error.get_or_insert(MultiRaftError::Other(error));
        }
        if let Err(error) = self.ingress_tasks.join().await {
            first_error.get_or_insert(MultiRaftError::Other(error));
        }
        // Retained operation and sampler jobs include their blocking provider/log
        // children. A canceled join never drops their handles or releases permits.
        if let Err(error) = self.snapshot_rt.maintenance_tasks.join().await {
            first_error.get_or_insert(MultiRaftError::Other(error));
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
        drop(rafts);
        // Core shutdown does not join the SM worker. Successful stop promises that
        // its application destructor has completed before callers reopen resources.
        let releases = std::mem::take(&mut *self.fsm_releases.lock().unwrap());
        for release in releases {
            release.wait().await;
        }
        tracing::info!(target: "multiraft::recovery", operation = "node_shutdown", phase = "complete",
            node_id = self.node_id, remaining_groups = 0_u64, "shut down Multi-Raft node");
        first_error.map_or(Ok(()), Err)
    }
}
