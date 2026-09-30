//! Recovery and consumer validation before executable Group publication.
use super::*;

impl<S: StateMachine> RuntimeShared<S> {
    pub(super) async fn recover_group(
        &self,
        group: GroupConfig,
        deadline: Instant,
    ) -> Result<(), RuntimeError> {
        if *self.abort_requests.borrow() || !self.accepting.load(Ordering::Acquire) {
            return Err(RuntimeError::Closed);
        }
        if Instant::now() >= deadline {
            return Err(RuntimeError::Deadline {
                phase: RuntimePhase::GroupStart,
                outcome_unknown: false,
            });
        }
        if self.ready.lock().unwrap().contains(&group.group_id) {
            return Ok(());
        }
        // Native construction cancellation is safe before worker/core spawn. Once
        // constructed, the Group remains in the node registry until cleanup joins
        // every native owner and observes the actual application destructor.
        tokio::time::timeout_at(deadline, async {
            self.node
                .create_group(group.group_id, &group.voters)
                .await?;
            self.node
                .wait_for_owned_recovery(
                    group.group_id,
                    deadline.saturating_duration_since(Instant::now()),
                )
                .await?;
            self.node.validate_recovered(group.group_id).await?;
            self.node.ensure_recovery_running(group.group_id)?;
            Ok::<_, MultiRaftError>(())
        })
        .await
        .map_err(|_| RuntimeError::Deadline {
            phase: RuntimePhase::GroupStart,
            outcome_unknown: true,
        })??;
        // Synchronous application validation cannot be preempted by timeout; do
        // not publish readiness if its bounded work exhausted the caller budget.
        if Instant::now() >= deadline {
            return Err(RuntimeError::Deadline {
                phase: RuntimePhase::GroupStart,
                outcome_unknown: false,
            });
        }
        if !self.accepting.load(Ordering::Acquire) {
            return Err(RuntimeError::Closed);
        }
        self.ready.lock().unwrap().insert(group.group_id);
        Ok(())
    }
}
