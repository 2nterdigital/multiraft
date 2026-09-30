//! Bounded admission and existing opaque byte/ReadIndex operations.
use super::*;
use multiraft_core::ProposeApplied;
use tokio::sync::{oneshot, OwnedRwLockReadGuard, OwnedSemaphorePermit};

pub(super) struct Admitted<S: StateMachine> {
    pub(super) shared: Arc<RuntimeShared<S>>,
    _admission: OwnedRwLockReadGuard<()>,
    _slot: OwnedSemaphorePermit,
}

impl<S: StateMachine> RuntimeHandle<S> {
    /// Static identity of this capability; it is not a liveness observation.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub(super) async fn admit(&self, deadline: Instant) -> Result<Admitted<S>, RuntimeError> {
        let shared = self.shared.upgrade().ok_or(RuntimeError::Closed)?;
        if !shared.accepting.load(Ordering::Acquire) {
            return Err(RuntimeError::Closed);
        }
        if Instant::now() >= deadline {
            return Err(RuntimeError::Deadline {
                phase: RuntimePhase::Admission,
                outcome_unknown: false,
            });
        }
        let slot = shared
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::Closed => RuntimeError::Closed,
                tokio::sync::TryAcquireError::NoPermits => RuntimeError::Busy,
            })?;
        let admission = tokio::time::timeout_at(deadline, shared.admission.clone().read_owned())
            .await
            .map_err(|_| RuntimeError::Deadline {
                phase: RuntimePhase::Admission,
                outcome_unknown: false,
            })?;
        if !shared.accepting.load(Ordering::Acquire) {
            return Err(RuntimeError::Closed);
        }
        if Instant::now() >= deadline {
            return Err(RuntimeError::Deadline {
                phase: RuntimePhase::Admission,
                outcome_unknown: false,
            });
        }
        Ok(Admitted {
            shared,
            _admission: admission,
            _slot: slot,
        })
    }

    /// Construct/recover a Group using the retained factory. A canceled caller
    /// never interrupts native construction: the owner drains it before stopping.
    /// Construction shares the absolute caller deadline and a maximum 30-second
    /// runtime budget. Application callbacks must be bounded.
    pub async fn create_group(
        &self,
        group: GroupConfig,
        deadline: Instant,
    ) -> Result<(), RuntimeError> {
        let admitted = self.admit(deadline).await?;
        let deadline = deadline.min(Instant::now() + CLEANUP_TIMEOUT);
        let shared = admitted.shared.clone();
        let (reply, receiver) = oneshot::channel();
        let task_shared = shared.clone();
        let registered = shared.startups.spawn(async move {
            let shared = task_shared;
            let _admitted = admitted;
            let _serialized = shared.group_creation.lock().await;
            let result = async {
                if *shared.abort_requests.borrow() {
                    return Err(RuntimeError::Closed);
                }
                if Instant::now() >= deadline {
                    return Err(RuntimeError::Deadline {
                        phase: RuntimePhase::GroupStart,
                        outcome_unknown: false,
                    });
                }
                // The native constructor's only suspension points precede worker/core
                // spawn; cancellation drops its RAII tick. The facade registered an
                // FSM release witness before entering it, even without a Group handle.
                tokio::time::timeout_at(
                    deadline,
                    shared.node.create_group(group.group_id, &group.voters),
                )
                .await
                .map_err(|_| RuntimeError::Deadline {
                    phase: RuntimePhase::GroupStart,
                    outcome_unknown: true,
                })??;
                let remaining = deadline
                    .saturating_duration_since(Instant::now())
                    .min(CLEANUP_TIMEOUT);
                shared
                    .node
                    .wait_for_recovery(group.group_id, remaining)
                    .await?;
                if !shared.accepting.load(Ordering::Acquire) {
                    return Err(RuntimeError::Closed);
                }
                shared.ready.lock().unwrap().insert(group.group_id);
                Ok(())
            }
            .await;
            let _ = reply.send(result);
        });
        if !registered {
            return Err(RuntimeError::Closed);
        }
        tokio::time::timeout_at(deadline, receiver)
            .await
            .map_err(|_| RuntimeError::Deadline {
                phase: RuntimePhase::GroupStart,
                outcome_unknown: true,
            })?
            .map_err(|_| {
                RuntimeError::Source(MultiRaftError::Other(anyhow::anyhow!(
                    "owned Group startup task stopped without a reply"
                )))
            })?
    }

    /// Propose unchanged bytes. Effects correspond to the exact committed and applied entry.
    /// Deadline after dispatch is an unknown outcome; no retry is performed.
    pub async fn propose(
        &self,
        group: GroupId,
        command: Vec<u8>,
        deadline: Instant,
    ) -> Result<ProposeApplied, RuntimeError> {
        let admitted = self.admit(deadline).await?;
        admitted.ensure_group(group)?;
        admitted
            .run(
                deadline,
                RuntimePhase::Propose,
                true,
                admitted.shared.node.propose_with_effects(group, command),
            )
            .await
    }
}
impl<S: StateMachine> Admitted<S> {
    pub(super) async fn run<R>(
        &self,
        deadline: Instant,
        phase: RuntimePhase,
        outcome_unknown: bool,
        future: impl std::future::Future<Output = Result<R, MultiRaftError>>,
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
            _ = abort.changed() => Err(RuntimeError::Interrupted {phase, outcome_unknown}),
            result = tokio::time::timeout_at(deadline, future) => result
                .map_err(|_| RuntimeError::Deadline {phase, outcome_unknown})?
                .map_err(RuntimeError::Source),
        }
    }

    pub(super) fn ensure_group(&self, group: GroupId) -> Result<(), RuntimeError> {
        if self.shared.ready.lock().unwrap().contains(&group) {
            Ok(())
        } else {
            Err(RuntimeError::Source(MultiRaftError::UnknownGroup(group)))
        }
    }
}
