//! The command guard follows owned native work, independently of its caller's waiter.
use super::*;
use std::future::Future;

pub(in crate::multiraft) struct GroupOwner {
    pub(in crate::multiraft) serial: Arc<tokio::sync::Mutex<()>>,
    pub(in crate::multiraft) commands: tasks::OwnedTasks,
    pub(in crate::multiraft) watchers: tasks::OwnedTasks,
    pub(in crate::multiraft) maintenance: tasks::OwnedTasks,
    stopped: tokio::sync::watch::Sender<bool>,
}
impl Default for GroupOwner {
    fn default() -> Self {
        Self {
            serial: Arc::default(),
            commands: tasks::OwnedTasks::default(),
            watchers: tasks::OwnedTasks::default(),
            maintenance: tasks::OwnedTasks::default(),
            stopped: tokio::sync::watch::channel(false).0,
        }
    }
}
impl GroupOwner {
    pub(in crate::multiraft) fn retire(&self) {
        self.stopped.send_replace(true);
        self.watchers.close();
        self.maintenance.close();
    }
    pub(in crate::multiraft) fn close(&self) {
        self.retire();
        self.commands.close();
    }
    pub(in crate::multiraft) fn stopping(&self) -> tokio::sync::watch::Receiver<bool> {
        self.stopped.subscribe()
    }
    pub(in crate::multiraft) async fn join(&self) -> Result<(), ControlledError> {
        self.commands
            .join()
            .await
            .map_err(|error| ControlledError::Native(error.to_string()))?;
        self.watchers
            .join()
            .await
            .map_err(|error| ControlledError::Native(error.to_string()))?;
        self.maintenance
            .join()
            .await
            .map_err(|error| ControlledError::Native(error.to_string()))
    }
}
impl<S: StateMachine> MultiRaft<S> {
    pub(in crate::multiraft) fn group_owner(&self, group: GroupId) -> Arc<GroupOwner> {
        self.group_owners
            .lock()
            .expect("group owner lock poisoned")
            .entry(group)
            .or_default()
            .clone()
    }
    pub(super) async fn controlled_run<T: Send + 'static, F, Fut>(
        self: &Arc<Self>,
        group: GroupId,
        deadline: tokio::time::Instant,
        operation: F,
    ) -> Result<T, ControlledError>
    where
        F: FnOnce(Arc<Self>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, ControlledError>> + Send + 'static,
    {
        self.controlled_run_inner(group, deadline, false, operation)
            .await
    }
    pub(super) async fn controlled_cleanup_run<T: Send + 'static, F, Fut>(
        self: &Arc<Self>,
        group: GroupId,
        deadline: tokio::time::Instant,
        operation: F,
    ) -> Result<T, ControlledError>
    where
        F: FnOnce(Arc<Self>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, ControlledError>> + Send + 'static,
    {
        self.controlled_run_inner(group, deadline, true, operation)
            .await
    }
    async fn controlled_run_inner<T: Send + 'static, F, Fut>(
        self: &Arc<Self>,
        group: GroupId,
        deadline: tokio::time::Instant,
        cleanup: bool,
        operation: F,
    ) -> Result<T, ControlledError>
    where
        F: FnOnce(Arc<Self>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, ControlledError>> + Send + 'static,
    {
        let owner = self.group_owner(group);
        let guard = tokio::time::timeout_at(deadline, owner.serial.clone().lock_owned())
            .await
            .map_err(|_| ControlledError::Deadline {
                outcome_unknown: false,
            })?;
        if (!cleanup && *owner.stopping().borrow())
            || self
                .snapshot_rt
                .stopping
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(ControlledError::Closed);
        }
        let (reply, receiver) = tokio::sync::oneshot::channel();
        let node = self.clone();
        if !owner.commands.spawn(async move {
            let _guard = guard;
            let result = operation(node).await;
            let _ = reply.send(result);
        }) {
            return Err(ControlledError::Closed);
        }
        tokio::time::timeout_at(deadline, receiver)
            .await
            .map_err(|_| ControlledError::Deadline {
                outcome_unknown: true,
            })?
            .map_err(|_| ControlledError::Closed)?
    }
    pub(super) fn controlled_parts(
        &self,
        group: GroupId,
    ) -> Result<(Raft<S>, StateMachineStore<S>), ControlledError> {
        if let Some(app) = self
            .groups
            .lock()
            .expect("native group lock poisoned")
            .get(&group)
        {
            return Ok((app.raft.clone(), app.state_machine.clone()));
        }
        if let Some(app) = self
            .quarantined_groups
            .lock()
            .expect("quarantined group lock poisoned")
            .get(&group)
        {
            return Ok((app.raft.clone(), app.state_machine.clone()));
        }
        Err(ControlledError::Rejected("local core is not running"))
    }
    pub(super) async fn controlled_wait_ready(
        &self,
        group: GroupId,
    ) -> Result<(), ControlledError> {
        let (raft, sm) = self.controlled_parts(group)?;
        let target = self
            .construction_recovery
            .lock()
            .expect("construction basis lock poisoned")
            .get(&group)
            .copied()
            .ok_or(ControlledError::Rejected(
                "native constructor basis missing",
            ))?;
        raft.wait(Some(Duration::from_secs(30)))
            .applied_index_at_least(
                target.map(|id| id.index),
                "quarantined durable constructor basis applied",
            )
            .await
            .map_err(|error| ControlledError::Native(error.to_string()))?;
        self.validate_recovered_store(group, sm).await?;
        raft.metrics()
            .borrow_watched()
            .running_state
            .clone()
            .map_err(|error| ControlledError::Native(error.to_string()))
    }
    pub(super) async fn publish_controlled_group(
        &self,
        group: GroupId,
    ) -> Result<(), ControlledError> {
        let _construction = self.controlled_construction.read().await;
        if self
            .snapshot_rt
            .stopping
            .load(std::sync::atomic::Ordering::Acquire)
            || *self.group_owner(group).stopping().borrow()
        {
            return Err(ControlledError::Closed);
        }
        let app = self
            .quarantined_groups
            .lock()
            .expect("quarantined group lock poisoned")
            .remove(&group);
        if let Some(app) = app {
            let raft = app.raft.clone();
            self.groups
                .lock()
                .expect("native group lock poisoned")
                .insert(group, app);
            raft.runtime_config().elect(true);
            self.spawn_leader_watch(group, raft.clone(), self.leader_cbs.clone());
            self.spawn_standby_throttle_watch(group, raft, self.standby_throttle.clone());
        }
        Ok(())
    }
    pub(in crate::multiraft) fn is_controlled_group(
        &self,
        group: GroupId,
    ) -> Result<bool, MultiRaftError> {
        let db = self
            .rocks_log_db
            .lock()
            .expect("Raft log owner lock poisoned")
            .clone();
        db.map(|db| db.lifecycle_receipt(group))
            .transpose()
            .map(|receipt| receipt.flatten().is_some())
            .map_err(|error| MultiRaftError::Other(error.into()))
    }
    pub(in crate::multiraft) fn controlled_db(
        &self,
    ) -> Result<RocksLogDb<TypeConfig>, ControlledError> {
        self.rocks_log_db
            .lock()
            .expect("Raft log owner lock poisoned")
            .clone()
            .ok_or(ControlledError::Rejected(
                "controlled groups require RocksDB",
            ))
    }
}
