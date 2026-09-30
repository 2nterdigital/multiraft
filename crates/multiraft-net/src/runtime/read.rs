//! Application reads use one caller budget and one observer, independent of shared rounds.
use super::*;
use crate::read_observation::{ReadGuard, ReadObserver, ReadOutcome, ReadStage, TryReadError};

impl<S: StateMachine> RuntimeHandle<S> {
    /// Shared ReadIndex then this invocation's local FSM query. No hidden retry.
    /// The synchronous query must be bounded. For fallible queries use `try_read_linearizable`.
    pub async fn read_linearizable<R>(
        &self,
        group: GroupId,
        deadline: Instant,
        query: impl FnOnce(&S) -> R,
    ) -> Result<R, RuntimeError> {
        self.read_with_observer(group, deadline, query, None, |_| ReadOutcome::Completed)
            .await
    }
    /// Per-caller neutral source observations with isolated observer failure.
    pub async fn read_linearizable_observed<R>(
        &self,
        group: GroupId,
        deadline: Instant,
        query: impl FnOnce(&S) -> R,
        observer: &dyn ReadObserver,
    ) -> Result<R, RuntimeError> {
        self.read_with_observer(group, deadline, query, Some(observer), |_| {
            ReadOutcome::Completed
        })
        .await
    }
    /// A query's application Err remains distinct from a ReadIndex/source error.
    pub async fn try_read_linearizable<R, E>(
        &self,
        group: GroupId,
        deadline: Instant,
        query: impl FnOnce(&S) -> Result<R, E>,
        observer: Option<&dyn ReadObserver>,
    ) -> Result<R, TryReadError<E>> {
        self.read_with_observer(group, deadline, query, observer, |r| {
            if r.is_ok() {
                ReadOutcome::Completed
            } else {
                ReadOutcome::ApplicationError
            }
        })
        .await
        .map_err(TryReadError::Runtime)?
        .map_err(TryReadError::Application)
    }
    async fn read_with_observer<R>(
        &self,
        group: GroupId,
        deadline: Instant,
        query: impl FnOnce(&S) -> R,
        observer: Option<&dyn ReadObserver>,
        application_outcome: impl FnOnce(&R) -> ReadOutcome,
    ) -> Result<R, RuntimeError> {
        let mut admission = ReadGuard::new(
            group,
            self.node_id,
            ReadStage::Admission,
            deadline,
            observer,
            None,
        );
        let admitted = match self.admit(deadline).await.and_then(|a| {
            a.ensure_group(group)?;
            Ok(a)
        }) {
            Ok(admitted) => {
                admission.finish(ReadOutcome::Completed, None, None);
                admitted
            }
            Err(error) => {
                admission.finish(
                    match &error {
                        RuntimeError::Closed | RuntimeError::Interrupted { .. } => {
                            ReadOutcome::Closed
                        }
                        RuntimeError::Deadline { .. } => ReadOutcome::Deadline,
                        RuntimeError::Busy => ReadOutcome::Busy,
                        RuntimeError::Source(MultiRaftError::UnknownGroup(_)) => {
                            ReadOutcome::UnknownGroup
                        }
                        _ => ReadOutcome::Backend,
                    },
                    None,
                    None,
                );
                return Err(error);
            }
        };
        admitted
            .run(
                deadline,
                RuntimePhase::Read,
                false,
                admitted.shared.node.read_observed(
                    group,
                    deadline,
                    query,
                    observer,
                    Some(admitted.shared.abort_requests.subscribe()),
                    application_outcome,
                ),
            )
            .await
    }
}
