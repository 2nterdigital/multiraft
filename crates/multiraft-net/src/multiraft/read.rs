//! One native ReadIndex per caller, using its single absolute budget through FSM query.
use super::*;
use crate::read_observation::{ReadGuard, ReadObserver, ReadOutcome, ReadStage};
use multiraft_core::{NativeFailure, ReadIndexFailure};
#[cfg(feature = "lab-group-version-batch")]
use openraft::ServerState;
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
pub(super) async fn confirm<S: StateMachine>(
    raft: &Raft<S>,
    policy: ReadPolicy,
    deadline: Instant,
) -> Result<Option<openraft::alias::LogIdOf<TypeConfig>>, MultiRaftError> {
    match tokio::time::timeout_at(deadline, raft.ensure_linearizable(policy)).await {
        Ok(Ok(read_log_id)) => Ok(Some(*read_log_id.log_id())),
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
async fn confirm_with_retry<S: StateMachine>(
    raft: &Raft<S>,
    policy: ReadPolicy,
    deadline: Instant,
) -> Result<Option<openraft::alias::LogIdOf<TypeConfig>>, MultiRaftError> {
    // One ReadIndex round waits only one heartbeat interval for a
    // quorum; a follower that answers a little later (busy applying
    // or syncing) is not lost authority. Retry within this caller's
    // budget; leadership loss and terminal failures still propagate.
    let mut policy = policy;
    loop {
        match confirm(raft, policy.clone(), deadline).await {
            // No hint: this node's lease is unconfirmed or expired, so
            // ReadIndex decides leadership. A known other leader is returned as is.
            // Since OpenRaft 0.10.1 a leader whose lease expired answers a
            // LeaseRead with `QuorumNotEnough` instead of an empty forward.
            Err(
                MultiRaftError::NotLeader { hint: None }
                | MultiRaftError::ReadIndex(ReadIndexFailure::QuorumUnavailable { .. }),
            ) if policy == ReadPolicy::LeaseRead => {
                policy = ReadPolicy::ReadIndex;
            }
            Err(MultiRaftError::ReadIndex(ReadIndexFailure::QuorumUnavailable { .. }))
                if Instant::now() + QUORUM_RETRY < deadline =>
            {
                tokio::time::sleep(QUORUM_RETRY).await;
            }
            result => break result,
        }
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
        self.read_observed_view(
            group,
            ReadPolicy::ReadIndex,
            deadline,
            query,
            None,
            None,
            |_| ReadOutcome::Completed,
        )
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
        self.read_observed_view(
            group,
            ReadPolicy::ReadIndex,
            deadline,
            query,
            None,
            None,
            |_| ReadOutcome::Completed,
        )
        .await
    }
    /// Leader lease read: like [`Self::read_linearizable_with_applied_at`], but
    /// a leader whose lease is still valid answers without a quorum round. An
    /// unconfirmed or expired lease falls back once to ReadIndex within the same
    /// deadline. Linearizable only while clock-rate drift between nodes stays
    /// within the lease margin.
    pub async fn read_leased_with_applied_at<R>(
        &self,
        group: GroupId,
        deadline: Instant,
        query: impl FnOnce(&S) -> R,
    ) -> Result<crate::LinearizableView<R>, MultiRaftError> {
        self.read_observed_view(
            group,
            ReadPolicy::LeaseRead,
            deadline,
            query,
            None,
            None,
            |_| ReadOutcome::Completed,
        )
        .await
    }
    #[cfg(feature = "lab-group-version-batch")]
    /// Use the Raft and FSM captured together at the start of this strict read.
    pub(crate) async fn read_pinned_linearizable<R>(
        &self,
        group: GroupId,
        deadline: Instant,
        query: impl FnOnce(&S) -> R,
    ) -> Result<R, MultiRaftError> {
        // Retirement signals this owner before removing its group from the registry.
        let mut retired = self.group_owner(group).stopping();
        let (raft, sm) = self
            .groups
            .lock()
            .unwrap()
            .get(&group)
            .map(|app| (app.raft.clone(), app.state_machine.clone()))
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        if *retired.borrow_and_update() {
            return Err(ReadIndexFailure::Closed.into());
        }
        let read = async {
            let read_log_id = confirm_with_retry(&raft, ReadPolicy::ReadIndex, deadline).await?;
            let (value, applied) = sm
                .try_with_fsm_and_applied(query)
                .await
                .map_err(|_| ReadIndexFailure::FsmUnavailable)?;
            if Instant::now() >= deadline {
                return Err(ReadIndexFailure::Deadline.into());
            }
            let metrics = raft.metrics();
            let metrics = metrics.borrow_watched();
            if *retired.borrow() || metrics.state == ServerState::Shutdown {
                return Err(ReadIndexFailure::Closed.into());
            }
            if let Err(error) = &metrics.running_state {
                return Err(native_failure(error.clone()).into());
            }
            match applied {
                Some(applied) if read_log_id.is_none_or(|read| applied >= read) => Ok(value),
                _ => Err(ReadIndexFailure::FsmUnavailable.into()),
            }
        };
        // Use a separate subscription so the read can check retirement after
        // returning from the synchronous callback, even if both branches became ready.
        let mut stopping = retired.clone();
        tokio::select! {
            biased;
            _ = stopping.changed() => Err(ReadIndexFailure::Closed.into()),
            result = read => result,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn read_observed_view<R>(
        &self,
        group: GroupId,
        policy: ReadPolicy,
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
        let result = if Instant::now() >= deadline {
            Err(ReadIndexFailure::Deadline.into())
        } else if let Some(raft) = self.raft(group) {
            confirm_with_retry(&raft, policy, deadline).await
        } else {
            Err(MultiRaftError::UnknownGroup(group))
        };
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
