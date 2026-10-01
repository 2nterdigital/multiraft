//! Atomic owner reservation includes legacy calls waiting for creation serialization.
use super::*;
use crate::runtime::requests::Admitted;
use futures::FutureExt;
use tokio::sync::oneshot;

pub(crate) struct Reservation<S: StateMachine> {
    shared: Arc<RuntimeShared<S>>,
    batch: bool,
}
impl<S: StateMachine> Drop for Reservation<S> {
    fn drop(&mut self) {
        let mut admission = self.shared.startup_admission.lock().unwrap();
        if self.batch {
            admission.batch = false;
        } else {
            admission.legacy -= 1;
        }
    }
}
impl<S: StateMachine> RuntimeShared<S> {
    pub(crate) fn reserve_legacy(self: &Arc<Self>) -> Result<Reservation<S>, RuntimeError> {
        let mut state = self.startup_admission.lock().unwrap();
        if !self.accepting.load(Ordering::Acquire) {
            return Err(RuntimeError::Closed);
        }
        if state.batch {
            return Err(RuntimeError::Busy);
        }
        state.legacy += 1;
        Ok(Reservation {
            shared: self.clone(),
            batch: false,
        })
    }
}

impl<S: StateMachine> RuntimeHandle<S> {
    /// Retained opt-in transaction: register all local Groups, then initialize/recover,
    /// validate every image and publish executable capabilities together. Canceling
    /// this waiter does not release its reservation or reset a Group's deadline.
    pub async fn start_groups_with_preference(
        &self,
        batch: StartupBatch,
    ) -> Result<StartupReport, StartupFailure> {
        let report = StartupReport::new(self.node_id, &batch);
        let reject = |kind, source| {
            let mut failure = report
                .clone()
                .failure(None, StartupPhase::Admission, source);
            failure.rejection = Some(kind);
            failure.cleanup = StartupCleanup::NotRequired;
            failure
        };
        let shared = self
            .shared
            .upgrade()
            .ok_or_else(|| reject(StartupRejection::Closed, RuntimeError::Closed))?;
        if !shared.accepting.load(Ordering::Acquire) {
            return Err(reject(StartupRejection::Closed, RuntimeError::Closed));
        }
        validate(&shared, &batch).map_err(|e| reject(StartupRejection::InvalidInput, e))?;
        let admitted = self
            .admit(Instant::now() + CLEANUP_TIMEOUT)
            .await
            .map_err(|e| {
                let kind = if matches!(e, RuntimeError::Closed) {
                    StartupRejection::Closed
                } else {
                    StartupRejection::Busy
                };
                reject(kind, e)
            })?;
        let reservation = {
            let mut state = shared.startup_admission.lock().unwrap();
            if !shared.accepting.load(Ordering::Acquire) {
                return Err(reject(StartupRejection::Closed, RuntimeError::Closed));
            }
            if state.batch || state.legacy != 0 {
                return Err(reject(StartupRejection::Busy, RuntimeError::Busy));
            }
            if !shared.node.startup_owner_empty() {
                return Err(reject(
                    StartupRejection::OwnerNotEmpty,
                    RuntimeError::InvalidConfig("startup requires empty owner"),
                ));
            }
            state.batch = true;
            Reservation {
                shared: shared.clone(),
                batch: true,
            }
        };
        self.run_startup_batch(batch, report, admitted, reservation)
            .await
    }
    async fn run_startup_batch(
        &self,
        batch: StartupBatch,
        report: StartupReport,
        admitted: Admitted<S>,
        reservation: Reservation<S>,
    ) -> Result<StartupReport, StartupFailure> {
        let shared = admitted.shared.clone();
        let progress = Arc::new(Mutex::new(report));
        let (reply, receiver) = oneshot::channel();
        let task_shared = shared.clone();
        let task_progress = progress.clone();
        if !shared.startups.spawn(async move {
            let _admitted = admitted;
            let _reservation = reservation;
            let _creation = task_shared.group_creation.lock().await;
            let result =
                std::panic::AssertUnwindSafe(task_shared.run_batch(batch, task_progress.clone()))
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|panic| {
                        let snapshot = task_progress.lock().unwrap().clone();
                        let active = snapshot
                            .groups
                            .iter()
                            .find(|g| g.phase == StartupPhase::Construct)
                            .or_else(|| {
                                snapshot
                                    .groups
                                    .iter()
                                    .find(|g| g.phase != StartupPhase::Ready)
                            });
                        let (id, phase) = active.map_or((None, StartupPhase::Construct), |g| {
                            (Some(g.group_id), g.phase)
                        });
                        Err(snapshot.failure(
                            id,
                            phase,
                            RuntimeError::Source(MultiRaftError::Other(anyhow::anyhow!(
                                "startup callback panicked: {}",
                                panic
                                    .downcast_ref::<String>()
                                    .map(String::as_str)
                                    .or_else(|| panic.downcast_ref::<&str>().copied())
                                    .unwrap_or("unknown panic payload")
                            ))),
                        ))
                    });
            match &result {
                Ok(report) => {
                    let digest = crate::multiraft::digest_label(report.input_digest);
                    tracing::info!(target: "multiraft::startup", node_id=report.node_id, startup_digest=digest.as_deref(), digest_known=digest.is_some(), phase="batch_complete", groups=report.groups.len(), "all local validators complete; startup does not prove quorum");
                }
                Err(failure) => {
                    let digest = crate::multiraft::digest_label(failure.report.input_digest);
                    let initialization = failure.group_id.and_then(|id|failure.report.groups.iter().find(|g|g.group_id==id)).map(|g|g.initialization.code());
                    tracing::warn!(target: "multiraft::startup", node_id=failure.report.node_id, startup_digest=digest.as_deref(), digest_known=digest.is_some(), group_id=failure.group_id, failed_group_known=failure.group_id.is_some(), phase=failure.phase.code(), initialization=initialization, initialization_known=initialization.is_some(), outcome_unknown=failure.outcome_unknown, "owned startup failed; rollback retained");
                }
            }
            if result.is_err() {
                task_shared.begin_cleanup();
            }
            let _ = reply.send(result);
        }) {
            return Err(progress.lock().unwrap().clone().failure(
                None,
                StartupPhase::Admission,
                RuntimeError::Closed,
            ));
        }
        let result = receiver.await.unwrap_or_else(|_| {
            Err(progress.lock().unwrap().clone().failure(
                None,
                StartupPhase::Construct,
                RuntimeError::Closed,
            ))
        });
        match result {
            Ok(report) => Ok(report),
            Err(mut failure) => {
                shared.begin_cleanup();
                match shared.wait_cleanup(Instant::now() + CLEANUP_TIMEOUT).await {
                    Ok(()) => failure.cleanup = StartupCleanup::Released,
                    Err(e) => {
                        failure.cleanup_error = Some(e);
                        failure.cleanup = StartupCleanup::ReleaseUnconfirmed;
                    }
                }
                Err(failure)
            }
        }
    }
}
fn validate<S: StateMachine>(
    shared: &RuntimeShared<S>,
    batch: &StartupBatch,
) -> Result<(), RuntimeError> {
    if batch.groups.is_empty()
        || batch.groups.len() > 4096
        || !(Duration::from_millis(100)..=Duration::from_millis(2000)).contains(&batch.grace)
        || batch.recovery_timeout.is_zero()
        || batch.recovery_timeout > CLEANUP_TIMEOUT
    {
        return Err(RuntimeError::InvalidConfig("batch requires 1..=4096 Groups, 100..=2000ms grace and 0..=30s nonzero recovery budget"));
    }
    let peers = shared.node.startup_peer_ids();
    let mut groups = BTreeSet::new();
    for input in &batch.groups {
        let voters: BTreeSet<_> = input.group.voters.iter().copied().collect();
        if !groups.insert(input.group.group_id)
            || voters.is_empty()
            || voters.len() != input.group.voters.len()
            || !voters.is_subset(&peers)
            || (shared.node.startup_voter() && !voters.contains(&shared.node.node_id()))
            || input
                .preferred_initializer
                .is_some_and(|id| !voters.contains(&id))
        {
            return Err(RuntimeError::InvalidConfig(
                "batch Group/voter/preference declarations must be unique and known",
            ));
        }
    }
    Ok(())
}
