//! Bounded admission and existing opaque byte/ReadIndex operations.
use super::recovery::StartupTiming;
use super::*;
use futures::FutureExt;
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
    /// runtime budget. Application callbacks must be bounded. Startup failure,
    /// including recovery validation rejection, fences the whole owned node.
    /// Successful rollback waits for actual FSM destruction before returning the
    /// startup error; cancellation of this wait retains cleanup.
    pub async fn create_group(
        &self,
        group: GroupConfig,
        deadline: Instant,
    ) -> Result<(), RuntimeError> {
        let admitted = self.admit(deadline).await?;
        let deadline = deadline.min(Instant::now() + CLEANUP_TIMEOUT);
        self.create_group_registered(group, admitted, StartupTiming::Absolute(deadline))
            .await
    }

    /// Construct a Group, then bound only its native recovery wait.
    ///
    /// This stage-compatible contract preserves consumers whose native constructor
    /// and application validation were outside their recovery-wait timeout. The
    /// same local basis, validator and owned rollback are used. It does not bound
    /// total startup: synchronous application callbacks must themselves be bounded.
    /// Cancellation drops only this waiter; construction and cleanup remain owned.
    /// The existing absolute-deadline [`Self::create_group`] contract is unchanged.
    pub async fn create_group_with_recovery_timeout(
        &self,
        group: GroupConfig,
        recovery_timeout: Duration,
    ) -> Result<(), RuntimeError> {
        let admitted = self.admit(Instant::now() + CLEANUP_TIMEOUT).await?;
        self.create_group_registered(group, admitted, StartupTiming::NativeWait(recovery_timeout))
            .await
    }

    async fn create_group_registered(
        &self,
        group: GroupConfig,
        admitted: Admitted<S>,
        timing: StartupTiming,
    ) -> Result<(), RuntimeError> {
        let shared = admitted.shared.clone();
        let reservation = shared.reserve_legacy()?;
        let (reply, receiver) = oneshot::channel();
        let task_shared = shared.clone();
        let registered = shared.startups.spawn(async move {
            let shared = task_shared;
            let _admitted = admitted;
            let _reservation = reservation;
            let _serialized = shared.group_creation.lock().await;
            let result = std::panic::AssertUnwindSafe(shared.recover_group(group, timing))
                .catch_unwind()
                .await
                .unwrap_or_else(|_| {
                    Err(RuntimeError::Source(MultiRaftError::Other(
                        anyhow::anyhow!("owned Group startup callback panicked"),
                    )))
                });
            if result.is_err() {
                // No partially recovered Group survives a failed public startup.
                // Cleanup runs outside this admission guard/startup task, then the
                // caller observes actual resource release before retrying.
                shared.begin_cleanup();
            }
            let _ = reply.send(result);
        });
        if !registered {
            return Err(RuntimeError::Closed);
        }
        let reply = match timing {
            StartupTiming::Absolute(deadline) => tokio::time::timeout_at(deadline, receiver)
                .await
                .map_err(|_| RuntimeError::Deadline {
                    phase: RuntimePhase::GroupStart,
                    outcome_unknown: true,
                }),
            StartupTiming::NativeWait(_) => Ok(receiver.await),
        };
        let result = reply
            .and_then(|reply| {
                reply.map_err(|_| {
                    RuntimeError::Source(MultiRaftError::Other(anyhow::anyhow!(
                        "owned Group startup task stopped without a reply"
                    )))
                })
            })
            .and_then(|result| result);
        if result.is_err() {
            shared.begin_cleanup();
            // This rollback budget is independent of the expired request budget.
            // A canceled waiter abandons only the wait; cleanup remains owned.
            shared
                .wait_cleanup(Instant::now() + CLEANUP_TIMEOUT)
                .await?;
        }
        result
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
