//! Recover original persistent state using an adoption anchor; never initialize membership.
use super::*;
use multiraft_store::adoption::{load_adoption_anchor, AdoptionAnchor};
use openraft::storage::{RaftLogReader, RaftLogStorage, RaftStateMachine};

pub(super) fn anchor<S: StateMachine>(
    node: &MultiRaft<S>,
    request: &ControlledOpen,
) -> Result<Option<AdoptionAnchor>, ControlledError> {
    Ok(load_adoption_anchor(
        &node.config.data_dir,
        node.node_id,
        request.group_id,
        request.generation,
        &request.identity,
        node.config.raft_transport_fingerprint,
    )?)
}

pub(super) async fn recover<S: StateMachine>(
    node: &MultiRaft<S>,
    request: &ControlledOpen,
    anchor: AdoptionAnchor,
) -> Result<(), ControlledError> {
    // Re-read completion after construction. A stale preconstruction observation is insufficient.
    if self::anchor(node, request)?.as_ref() != Some(&anchor) {
        return Err(ControlledError::Rejected(
            "adoption anchor changed during recovery",
        ));
    }
    let original = anchor.original;
    let (raft, mut sm) = node.controlled_parts(request.group_id)?;
    let actual = sm.inspect_controlled_applied().await?;
    let floor = original
        .membership
        .log_id()
        .ok_or(ControlledError::Rejected(
            "adopted membership floor missing",
        ))?;
    let configuration = actual.membership.log_id().ok_or(ControlledError::Rejected(
        "recovered membership identity missing",
    ))?;
    if !super::seed::covers(actual.applied, original.applied)
        || !super::seed::covers(configuration, floor)
        || (configuration == floor && actual.membership != original.membership)
    {
        return Err(ControlledError::Rejected(
            "actual adopted state is behind or conflicts with original state",
        ));
    }
    let observed = sm
        .applied_state()
        .await
        .map_err(|e| ControlledError::Native(e.to_string()))?;
    if observed.0 != Some(actual.applied) || observed.1 != actual.membership {
        return Err(ControlledError::Rejected(
            "adopted recovery cache differs from durable state",
        ));
    }
    let membership = actual.membership;
    let native = raft
        .with_raft_state(move |state| {
            let committed = state.membership_state.committed().as_ref();
            let effective = state.membership_state.effective().as_ref();
            committed == &membership
                && match (*effective.log_id(), *membership.log_id()) {
                    (Some(effective_id), Some(applied_id)) => {
                        super::seed::covers(effective_id, applied_id)
                            && (effective_id != applied_id || effective == &membership)
                    }
                    _ => false,
                }
        })
        .await
        .map_err(|e| ControlledError::Native(e.to_string()))?;
    let mut log = node.open_native_log(request.group_id)?;
    let state = log.get_log_state().await?;
    let vote = log.read_vote().await?;
    let committed = log.read_committed().await?;
    if !native
        || state
            .last_log_id
            .is_none_or(|id| !super::seed::covers(id, actual.applied))
        || original
            .vote
            .is_some_and(|floor| vote.is_none_or(|actual| actual < floor))
        || original
            .committed
            .is_some_and(|floor| committed.is_none_or(|actual| !super::seed::covers(actual, floor)))
    {
        return Err(ControlledError::Rejected(
            "native adopted log, vote or committed floor conflicts with original proof",
        ));
    }
    Ok(())
}
