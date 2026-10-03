//! Consumer validation of the native-recovered local application image.
use super::*;

impl<S: StateMachine> MultiRaft<S> {
    /// Validate the recovered local application generation. Low-level consumers
    /// first await `wait_for_recovery`; owned runtime startup does this automatically.
    /// Rejection fences the application until shutdown/restart.
    pub async fn validate_recovered(&self, group: GroupId) -> Result<(), MultiRaftError> {
        let context = FsmFactoryContext {
            node_id: self.node_id,
            group_id: group,
        };
        let sm = self
            .groups
            .lock()
            .unwrap()
            .get(&group)
            .map(|group| group.state_machine.clone())
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        let result = sm
            .validate_recovery(|fsm| {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    self.fsm_factory.validate_recovered(context, fsm)
                }))
                .unwrap_or_else(|_| Err(anyhow::anyhow!("recovery validation callback panicked")))
                .map_err(std::io::Error::other)
            })
            .await;
        result.map_err(|source| {
            tracing::warn!(target: "multiraft::recovery", operation = "recovery_validation",
                phase = "rejected", node_id = self.node_id, group_id = group,
                "consumer rejected recovered application image");
            MultiRaftError::Other(anyhow::Error::new(source).context(format!(
                "validate recovered FSM for node {}, group {}",
                self.node_id, group
            )))
        })
    }
}

/// Classify native facts without exporting or logging application payload text.
pub(super) fn native_recovery_error(
    group: GroupId,
    stage: multiraft_core::RecoveryStage,
    source: multiraft_core::typ::Fatal,
) -> MultiRaftError {
    use multiraft_core::{NativeFailure, RecoveryError, RecoveryFailure};
    let failure = match &source {
        openraft::error::Fatal::Stopped => RecoveryFailure::Closed,
        openraft::error::Fatal::Panicked => RecoveryFailure::Backend(NativeFailure::Panicked),
        openraft::error::Fatal::StorageError(_) => RecoveryFailure::Backend(NativeFailure::Storage),
    };
    RecoveryError::new(group, stage, failure, Some(source.into())).into()
}

pub(super) fn durable_local_mode(config: &multiraft_core::ClusterConfig) -> bool {
    !config.data_dir.as_os_str().is_empty()
        && config.file_log_sync_level != multiraft_core::FileLogSyncLevel::Os
}

/// Snapshot metadata and forced native commit record, before the constructor can
/// expose this Group to any peer. This is construction provenance, not a new
/// application watermark/current authority. Native construction still performs
/// all restoration, strict required-basis checks and committed suffix replay.
pub(super) async fn construction_basis<S: StateMachine>(
    group: GroupId,
    log: &mut FileLogStoreOf,
    sm: &StateMachineStore<S>,
) -> Result<Option<openraft::alias::LogIdOf<TypeConfig>>, MultiRaftError> {
    use openraft::storage::RaftLogStorage;
    let storage_error = |source: std::io::Error| {
        MultiRaftError::from(multiraft_core::RecoveryError::new(
            group,
            multiraft_core::RecoveryStage::Construct,
            multiraft_core::RecoveryFailure::Backend(multiraft_core::NativeFailure::Storage),
            Some(source.into()),
        ))
    };
    let committed = log.read_committed().await.map_err(storage_error)?;
    let snapshot = sm
        .native_snapshot_info()
        .await
        .map_err(storage_error)?
        .and_then(|snapshot| snapshot.meta.last_log_id);
    Ok(committed.max(snapshot))
}

impl<S: StateMachine> MultiRaft<S> {
    /// Owned startup's durable-local seam is distinct from the legacy cluster-tail
    /// confirmation. Weak/memory grades retain that existing stronger wait.
    pub(crate) async fn wait_for_owned_recovery(
        &self,
        group: GroupId,
        timeout: Duration,
    ) -> Result<(), MultiRaftError> {
        if !durable_local_mode(&self.config) {
            return self.wait_for_recovery(group, timeout).await;
        }
        let target = self
            .construction_recovery
            .lock()
            .unwrap()
            .get(&group)
            .copied()
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        self.ensure_recovery_running(group)?;
        let result = raft
            .wait(Some(timeout))
            .applied_index_at_least(
                target.map(|id| id.index),
                "durable native constructor basis applied",
            )
            .await;
        if let Err(source) = result {
            self.ensure_recovery_running(group)?;
            let failure = match &source {
                openraft::metrics::WaitError::Timeout(..) => {
                    multiraft_core::RecoveryFailure::Deadline
                }
                openraft::metrics::WaitError::ShuttingDown => {
                    multiraft_core::RecoveryFailure::Closed
                }
            };
            return Err(multiraft_core::RecoveryError::new(
                group,
                multiraft_core::RecoveryStage::Await,
                failure,
                Some(source.into()),
            )
            .into());
        }
        self.ensure_recovery_running(group)?;
        tracing::info!(target: "multiraft::recovery", operation = "owned_recovery", phase = "complete",
            node_id = self.node_id, group_id = group, recovery_basis = "durable_native_constructor",
            applied_target_index = ?target.map(|id| id.index), "native local recovery basis confirmed");
        Ok(())
    }

    pub(crate) fn ensure_recovery_running(&self, group: GroupId) -> Result<(), MultiRaftError> {
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        let state = raft.metrics().borrow_watched().running_state.clone();
        state.map_err(|source| {
            native_recovery_error(group, multiraft_core::RecoveryStage::Await, source)
        })
    }
}

/// Finite source log labels come from the typed recovery result, never its text.
pub(super) fn diagnostic_fields(error: &MultiRaftError) -> (&'static str, &'static str) {
    use multiraft_core::{NativeFailure, RecoveryFailure, RecoveryStage};
    let MultiRaftError::Recovery(recovery) = error else {
        return ("unknown", "unknown");
    };
    let phase = match recovery.stage {
        RecoveryStage::Construct => "construct",
        RecoveryStage::Await => "await",
        _ => "unknown",
    };
    let failure = match recovery.failure {
        RecoveryFailure::Deadline => "deadline",
        RecoveryFailure::Closed => "closed",
        RecoveryFailure::Backend(NativeFailure::Storage) => "native_storage",
        RecoveryFailure::Backend(NativeFailure::Panicked) => "native_panicked",
        _ => "unknown",
    };
    (phase, failure)
}
