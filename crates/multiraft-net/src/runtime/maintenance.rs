//! Weak, deadline-bounded maintenance over the existing native owner/classifier.
use super::requests::Admitted;
use super::*;
use crate::{CompactionRejection, CompactionSubmission, LocalStorageStatus};

impl<S: StateMachine> RuntimeHandle<S> {
    /// Submit one local native snapshot/compaction using the original absolute
    /// budget. Success is submission, not completion. After dispatch, deadline or
    /// cancellation never retracts native work; observe storage status separately.
    pub async fn request_compaction(
        &self,
        group: GroupId,
        deadline: Instant,
    ) -> Result<CompactionSubmission, RuntimeError> {
        let admitted = self.admit(deadline).await?;
        admitted.ensure_group(group)?;
        admitted
            .run_maintenance(
                deadline,
                RuntimePhase::Compaction,
                true,
                admitted
                    .shared
                    .node
                    .request_compaction_until(group, deadline),
            )
            .await
    }

    /// Read bounded local native/provider/log facts, without leadership authority.
    /// One retained sampler per Node returns Busy while actual work is unfinished,
    /// even after its original waiter was canceled or its deadline expired.
    pub async fn local_storage_status(
        &self,
        group: GroupId,
        deadline: Instant,
    ) -> Result<LocalStorageStatus, RuntimeError> {
        let admitted = self.admit(deadline).await?;
        admitted.ensure_group(group)?;
        admitted
            .run_maintenance(
                deadline,
                RuntimePhase::StorageStatus,
                false,
                admitted.shared.node.local_storage_status(group),
            )
            .await
    }
}

impl<S: StateMachine> Admitted<S> {
    async fn run_maintenance<R>(
        &self,
        deadline: Instant,
        phase: RuntimePhase,
        outcome_unknown: bool,
        future: impl std::future::Future<Output = Result<R, CompactionRejection>>,
    ) -> Result<R, RuntimeError> {
        if Instant::now() >= deadline {
            return Err(RuntimeError::Deadline {
                phase: RuntimePhase::Admission,
                outcome_unknown: false,
            });
        }
        let mut abort = self.shared.abort_requests.subscribe();
        if *abort.borrow_and_update() {
            return Err(RuntimeError::Interrupted {
                phase,
                outcome_unknown,
            });
        }
        tokio::select! {
            biased;
            _ = abort.changed() => Err(RuntimeError::Interrupted { phase, outcome_unknown }),
            result = tokio::time::timeout_at(deadline, future) => result
                .map_err(|_| RuntimeError::Deadline { phase, outcome_unknown })?
                .map_err(RuntimeError::MaintenanceRejected),
        }
    }
}
