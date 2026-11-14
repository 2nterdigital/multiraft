//! Cancellation-independent native proposal ownership.
use super::*;
use futures::FutureExt;
use multiraft_core::ProposeApplied;
use tokio::sync::oneshot;

/// Completion of an admitted proposal. Dropping this receipt abandons only observation;
/// the runtime retains its admission slot until native completion or shutdown interruption.
#[must_use = "observe completion or explicitly drop the receipt to abandon observation"]
#[derive(Debug)]
pub struct ProposalReceipt {
    completion: oneshot::Receiver<Result<ProposeApplied, RuntimeError>>,
}

impl ProposalReceipt {
    /// Wait for the exact native applied result. Caller cancellation does not cancel work.
    /// An interrupted write has an unknown outcome and must not be blindly retried.
    pub async fn completion(self) -> Result<ProposeApplied, RuntimeError> {
        self.completion
            .await
            .map_err(|_| RuntimeError::Interrupted {
                phase: RuntimePhase::Propose,
                outcome_unknown: true,
            })?
    }
}

impl<S: StateMachine> RuntimeHandle<S> {
    /// Admit unchanged command bytes and transfer their execution to the runtime.
    /// The deadline bounds admission and first native dispatch only. Completion has
    /// no caller deadline; timing out or dropping its receipt leaves admission held.
    pub async fn submit_owned(
        &self,
        group: GroupId,
        command: Vec<u8>,
        admission_deadline: Instant,
    ) -> Result<ProposalReceipt, RuntimeError> {
        let admitted = self.admit(admission_deadline).await?;
        admitted.ensure_group(group)?;
        let shared = admitted.shared.clone();
        let task_shared = shared.clone();
        let (reply, completion) = oneshot::channel();
        // Registration and receipt return have no intervening cancellation point.
        if !shared.proposals.spawn(async move {
            let shared = task_shared;
            let _admitted = admitted;
            let execution = async {
                let mut abort = shared.abort_requests.subscribe();
                if Instant::now() >= admission_deadline {
                    return Err(RuntimeError::Deadline {
                        phase: RuntimePhase::Admission,
                        outcome_unknown: false,
                    });
                }
                if *abort.borrow_and_update() {
                    return Err(RuntimeError::Interrupted {
                        phase: RuntimePhase::Propose,
                        outcome_unknown: false,
                    });
                }
                tokio::select! {
                    biased;
                    _ = abort.changed() => Err(RuntimeError::Interrupted {
                        phase: RuntimePhase::Propose,
                        outcome_unknown: true,
                    }),
                    result = shared.node.propose_with_effects(group, command) => {
                        result.map_err(RuntimeError::Source)
                    }
                }
            };
            let result = match std::panic::AssertUnwindSafe(execution).catch_unwind().await {
                Ok(result) => result,
                Err(_) => {
                    // Fence before releasing admission: no later request may use a
                    // runtime whose proposal task unexpectedly unwound.
                    shared.begin_cleanup();
                    Err(RuntimeError::Source(MultiRaftError::Other(
                        anyhow::anyhow!("owned proposal task panicked"),
                    )))
                }
            };
            // Completion is published only once its native admission is released.
            drop(_admitted);
            let _ = reply.send(result);
        }) {
            return Err(RuntimeError::Closed);
        }
        Ok(ProposalReceipt { completion })
    }
}
