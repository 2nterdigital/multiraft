//! Recovery and consumer validation before executable Group publication.
use super::*;

/// Which existing startup stages the caller's timeout covers.
#[derive(Clone, Copy)]
pub(super) enum StartupTiming {
    Absolute(Instant),
    NativeWait(Duration),
}

impl<S: StateMachine> RuntimeShared<S> {
    pub(super) async fn recover_group(
        &self,
        group: GroupConfig,
        timing: StartupTiming,
    ) -> Result<(), RuntimeError> {
        if *self.abort_requests.borrow() || !self.accepting.load(Ordering::Acquire) {
            return Err(RuntimeError::Closed);
        }
        if matches!(timing, StartupTiming::Absolute(deadline) if Instant::now() >= deadline) {
            return Err(RuntimeError::Deadline {
                phase: RuntimePhase::GroupStart,
                outcome_unknown: false,
            });
        }
        if self.ready.lock().unwrap().contains(&group.group_id) {
            return Ok(());
        }
        // One constructor/basis/validator pipeline for both contracts. The
        // compatibility timeout covers only the native wait, as legacy consumers
        // required; it never disables validation or changes the native basis.
        let pipeline = self.construct_wait_validate(&group, timing);
        match timing {
            StartupTiming::Absolute(deadline) => {
                tokio::time::timeout_at(deadline, pipeline)
                    .await
                    .map_err(|_| RuntimeError::Deadline {
                        phase: RuntimePhase::GroupStart,
                        outcome_unknown: true,
                    })??;
                // Synchronous application code is not preemptible. An absolute
                // caller still cannot publish after that budget has expired.
                if Instant::now() >= deadline {
                    return Err(RuntimeError::Deadline {
                        phase: RuntimePhase::GroupStart,
                        outcome_unknown: false,
                    });
                }
            }
            StartupTiming::NativeWait(_) => pipeline.await?,
        }
        if !self.accepting.load(Ordering::Acquire) {
            return Err(RuntimeError::Closed);
        }
        self.ready.lock().unwrap().insert(group.group_id);
        Ok(())
    }

    async fn construct_wait_validate(
        &self,
        group: &GroupConfig,
        timing: StartupTiming,
    ) -> Result<(), RuntimeError> {
        self.node
            .create_group(group.group_id, &group.voters)
            .await?;
        let timeout = match timing {
            StartupTiming::Absolute(deadline) => deadline.saturating_duration_since(Instant::now()),
            StartupTiming::NativeWait(timeout) => timeout,
        };
        self.node
            .wait_for_owned_recovery(group.group_id, timeout)
            .await?;
        let mut closed = self.closed.subscribe();
        if *closed.borrow() {
            return Err(RuntimeError::Closed);
        }
        tokio::select! {
            biased;
            _ = closed.changed() => return Err(RuntimeError::Closed),
            result = self.node.validate_recovered(group.group_id) => result?,
        }
        self.node.ensure_recovery_running(group.group_id)?;
        Ok(())
    }
}
