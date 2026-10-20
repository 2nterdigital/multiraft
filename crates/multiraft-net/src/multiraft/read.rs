//! One native ReadIndex per caller, using its single absolute budget through FSM query.
use super::*;
use crate::read_observation::{ReadGuard, ReadObserver, ReadOutcome, ReadStage};
use multiraft_core::{NativeFailure, ReadIndexFailure};
use tokio::time::Instant;

const READ_BOUND: Duration = Duration::from_secs(10);
/// Pause before re-confirming a ReadIndex round that missed its quorum.
const QUORUM_RETRY: Duration = Duration::from_millis(20);
fn native_failure(error: openraft::error::Fatal<TypeConfig>) -> ReadIndexFailure {
    match error {
        openraft::error::Fatal::Stopped => ReadIndexFailure::Closed,
        openraft::error::Fatal::Panicked => ReadIndexFailure::Backend(NativeFailure::Panicked),
        openraft::error::Fatal::StorageError(_) => {
            ReadIndexFailure::Backend(NativeFailure::Storage)
        }
    }
}
/// Returns the confirmed read point; OpenRaft has applied at least that far.
async fn confirm<S: StateMachine>(
    raft: &Raft<S>,
    deadline: Instant,
) -> Result<Option<openraft::alias::LogIdOf<TypeConfig>>, MultiRaftError> {
    match tokio::time::timeout_at(deadline, raft.ensure_linearizable(ReadPolicy::ReadIndex)).await {
        Ok(Ok(read_log_id)) => Ok(read_log_id),
        Ok(Err(error)) => {
            if let Some(fwd) = error.forward_to_leader() {
                return Err(MultiRaftError::NotLeader {
                    hint: fwd.leader_id,
                });
            }
            Err(match error {
                RaftError::APIError(openraft::error::LinearizableReadError::QuorumNotEnough(
                    error,
                )) => ReadIndexFailure::QuorumUnavailable {
                    responders: error.got,
                }
                .into(),
                RaftError::Fatal(error) => native_failure(error).into(),
                RaftError::APIError(openraft::error::LinearizableReadError::ForwardToLeader(
                    error,
                )) => MultiRaftError::NotLeader {
                    hint: error.leader_id,
                },
            })
        }
        Err(_) => Err(ReadIndexFailure::Deadline.into()),
    }
}
impl<S: StateMachine> MultiRaft<S> {
    /// Independent native ReadIndex confirmation followed by this caller's FSM query.
    /// One ten-second absolute budget covers both stages; no sharing, pool or queue.
    pub async fn read_linearizable<R>(
        &self,
        group: GroupId,
        query: impl FnOnce(&S) -> R,
    ) -> Result<R, MultiRaftError> {
        self.read_linearizable_at(group, Instant::now() + READ_BOUND, query)
            .await
    }
    /// Absolute-deadline ReadIndex + FSM read. The synchronous query must be bounded.
    pub async fn read_linearizable_at<R>(
        &self,
        group: GroupId,
        deadline: Instant,
        query: impl FnOnce(&S) -> R,
    ) -> Result<R, MultiRaftError> {
        self.read_observed_view(group, deadline, query, None, None, |_| {
            ReadOutcome::Completed
        })
        .await
        .map(|view| view.value)
    }
    /// Absolute-deadline ReadIndex + FSM read that also returns the applied log
    /// id sampled under the same FSM lock as the value. The synchronous query
    /// must be bounded.
    pub async fn read_linearizable_with_applied_at<R>(
        &self,
        group: GroupId,
        deadline: Instant,
        query: impl FnOnce(&S) -> R,
    ) -> Result<crate::LinearizableView<R>, MultiRaftError> {
        self.read_observed_view(group, deadline, query, None, None, |_| {
            ReadOutcome::Completed
        })
        .await
    }
    pub(crate) async fn read_observed_view<R>(
        &self,
        group: GroupId,
        deadline: Instant,
        query: impl FnOnce(&S) -> R,
        observer: Option<&dyn ReadObserver>,
        closed: Option<tokio::sync::watch::Receiver<bool>>,
        application_outcome: impl FnOnce(&R) -> ReadOutcome,
    ) -> Result<crate::LinearizableView<R>, MultiRaftError> {
        let mut stage = ReadGuard::new(
            group,
            self.node_id,
            ReadStage::ReadIndex,
            deadline,
            observer,
            closed.clone(),
        );
        let result = async {
            if Instant::now() >= deadline {
                return Err(ReadIndexFailure::Deadline.into());
            }
            let raft = self
                .raft(group)
                .ok_or(MultiRaftError::UnknownGroup(group))?;
            // One ReadIndex round waits only one heartbeat interval for a
            // quorum; a follower that answers a little later (busy applying
            // or syncing) is not lost authority. Retry within this caller's
            // budget; leadership loss and terminal failures still propagate.
            loop {
                match confirm(&raft, deadline).await {
                    Err(MultiRaftError::ReadIndex(ReadIndexFailure::QuorumUnavailable {
                        ..
                    })) if Instant::now() + QUORUM_RETRY < deadline => {
                        tokio::time::sleep(QUORUM_RETRY).await;
                    }
                    result => break result,
                }
            }
        }
        .await;
        stage.finish_source(&result);
        let read_log_id = result?;
        let mut stage = ReadGuard::new(
            group,
            self.node_id,
            ReadStage::StateMachine,
            deadline,
            observer,
            closed,
        );
        let result = tokio::time::timeout_at(deadline, async {
            if Instant::now() >= deadline {
                return Err(ReadIndexFailure::Deadline.into());
            }
            let sm = self
                .groups
                .lock()
                .unwrap()
                .get(&group)
                .map(|g| g.state_machine.clone())
                .ok_or(ReadIndexFailure::FsmUnavailable)?;
            let (value, applied) = sm
                .try_with_fsm_and_applied(query)
                .await
                .map_err(|_| ReadIndexFailure::FsmUnavailable)?;
            // The view must reflect at least the confirmed read point.
            match applied {
                Some(applied) if read_log_id.is_none_or(|read| applied >= read) => {
                    Ok(crate::LinearizableView {
                        value,
                        applied: crate::group_observation::normalize_log_id(&applied),
                    })
                }
                _ => Err(ReadIndexFailure::FsmUnavailable.into()),
            }
        })
        .await
        .map_err(|_| MultiRaftError::from(ReadIndexFailure::Deadline))
        .and_then(|r| r);
        match &result {
            Ok(view) => stage.finish(application_outcome(&view.value), None, None),
            Err(_) => stage.finish_source(&result),
        }
        result
    }
}

impl<S: StateMachine> MultiRaft<S> {
    pub(crate) fn leader_hint_receiver(
        &self,
        group: GroupId,
        closed: Option<tokio::sync::watch::Receiver<bool>>,
    ) -> Result<crate::LeaderHintReceiver, MultiRaftError> {
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        Ok(crate::LeaderHintReceiver::new(
            group,
            raft.metrics(),
            closed,
        ))
    }
}
