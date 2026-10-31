//! Immutable explicit-new bootstrap intent survives construction and initialization crashes.
use super::*;
use openraft::storage::{RaftLogReader, RaftLogStorage, RaftStateMachine};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct BootstrapIntent {
    version: u8,
    node: NodeId,
    group: GroupId,
    generation: u64,
    identity: Vec<u8>,
    fingerprint: Option<[u8; 32]>,
    members: BTreeMap<NodeId, BasicNode>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BootstrapAnchor {
    intent: BootstrapIntent,
    configuration: multiraft_core::typ::StoredMembership,
    applied: multiraft_core::typ::LogId,
    vote: Option<multiraft_core::typ::Vote>,
}
fn anchor_path(root: &std::path::Path, group: GroupId) -> PathBuf {
    root.join("controlled-bootstrap")
        .join(format!("group-{group}.anchor.json"))
}

fn intent<S: StateMachine>(
    node: &MultiRaft<S>,
    request: &ControlledOpen,
    voters: &BTreeSet<NodeId>,
) -> BootstrapIntent {
    BootstrapIntent {
        version: 1,
        node: node.node_id,
        group: request.group_id,
        generation: request.generation,
        identity: request.identity.clone(),
        fingerprint: node.config.raft_transport_fingerprint,
        members: node.membership_nodes(&voters.iter().copied().collect::<Vec<_>>()),
    }
}
fn path(root: &std::path::Path, group: GroupId) -> PathBuf {
    root.join("controlled-bootstrap")
        .join(format!("group-{group}.json"))
}

pub(super) fn verify<S: StateMachine>(
    node: &MultiRaft<S>,
    request: &ControlledOpen,
    voters: &BTreeSet<NodeId>,
) -> Result<(), ControlledError> {
    let expected =
        serde_json::to_vec(&intent(node, request, voters)).map_err(std::io::Error::other)?;
    if std::fs::read(path(&node.config.data_dir, request.group_id))? != expected {
        return Err(ControlledError::Rejected(
            "bootstrap intent identity mismatch",
        ));
    }
    Ok(())
}
pub(super) async fn persist<S: StateMachine>(
    node: &MultiRaft<S>,
    request: &ControlledOpen,
    voters: &BTreeSet<NodeId>,
) -> Result<(), ControlledError> {
    let bytes =
        serde_json::to_vec(&intent(node, request, voters)).map_err(std::io::Error::other)?;
    let path = path(&node.config.data_dir, request.group_id);
    let root = node.config.data_dir.clone();
    tokio::task::spawn_blocking(move || super::durable_record::publish(&root, &path, &bytes))
        .await
        .map_err(|error| ControlledError::Native(error.to_string()))??;
    Ok(())
}
pub(super) fn owns_generation<S: StateMachine>(
    node: &MultiRaft<S>,
    request: &ControlledOpen,
) -> Result<bool, ControlledError> {
    match std::fs::read(path(&node.config.data_dir, request.group_id)) {
        Ok(bytes) => {
            let intent: BootstrapIntent =
                serde_json::from_slice(&bytes).map_err(std::io::Error::other)?;
            Ok(intent.node == node.node_id
                && intent.group == request.group_id
                && intent.generation == request.generation
                && intent.identity == request.identity)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

pub(super) fn has_anchor<S: StateMachine>(
    node: &MultiRaft<S>,
    group: GroupId,
) -> Result<bool, ControlledError> {
    Ok(anchor_path(&node.config.data_dir, group).try_exists()?)
}

pub(super) async fn reconcile<S: StateMachine>(
    node: &MultiRaft<S>,
    request: &ControlledOpen,
    voters: &BTreeSet<NodeId>,
) -> Result<(), ControlledError> {
    verify(node, request, voters)?;
    if has_anchor(node, request.group_id)? {
        return recover_anchor(node, request).await;
    }
    let (raft, mut sm) = node.controlled_parts(request.group_id)?;
    let members = node.membership_nodes(&voters.iter().copied().collect::<Vec<_>>());
    // Only the native pristine check authorizes initialize. A persisted vote/log is retained.
    if !raft
        .is_initialized()
        .await
        .map_err(|error| ControlledError::Native(error.to_string()))?
    {
        match raft.initialize(members.clone()).await {
            Ok(())
            | Err(openraft::error::RaftError::APIError(
                openraft::error::InitializeError::NotAllowed(_),
            )) => {}
            Err(error) => return Err(ControlledError::Native(error.to_string())),
        }
    }
    let mut stopping = node.group_owner(request.group_id).stopping();
    loop {
        node.ensure_recovery_running(request.group_id)?;
        let (applied, membership) = sm
            .applied_state()
            .await
            .map_err(|error| ControlledError::Native(error.to_string()))?;
        let configs = membership.membership().get_joint_config();
        if let Some(config_log) = *membership.log_id() {
            if configs.len() != 1
                || configs[0] != *voters
                || membership.membership().learner_ids().next().is_some()
                || members
                    .iter()
                    .any(|(id, node)| membership.membership().get_node(id) != Some(node))
            {
                return Err(ControlledError::Rejected(
                    "native initialized membership differs from bootstrap intent",
                ));
            }
            let exact = membership.clone();
            let native = raft
                .with_raft_state(move |state| {
                    state.membership_state.committed().as_ref() == &exact
                        && state.membership_state.effective().as_ref() == &exact
                })
                .await
                .map_err(|error| ControlledError::Native(error.to_string()))?;
            if native && applied.is_some_and(|applied| applied >= config_log) {
                let actual = sm.inspect_controlled_applied().await?;
                if actual.membership != membership
                    || !super::seed::covers(actual.applied, config_log)
                {
                    continue;
                }
                let mut log = node.open_native_log(request.group_id)?;
                let state = log.get_log_state().await?;
                if state
                    .last_log_id
                    .is_none_or(|last| !super::seed::covers(last, actual.applied))
                {
                    continue;
                }
                let anchor = BootstrapAnchor {
                    intent: intent(node, request, voters),
                    configuration: actual.membership,
                    applied: actual.applied,
                    vote: log.read_vote().await?,
                };
                let bytes = serde_json::to_vec(&anchor).map_err(std::io::Error::other)?;
                super::durable_record::publish(
                    &node.config.data_dir,
                    &anchor_path(&node.config.data_dir, request.group_id),
                    &bytes,
                )?;
                return Ok(());
            }
        }
        if *stopping.borrow_and_update() {
            return Err(ControlledError::Closed);
        }
        tokio::select! { _ = stopping.changed() => return Err(ControlledError::Closed), _ = tokio::time::sleep(Duration::from_millis(20)) => {} }
    }
}

/// Prove the actual original generation's native floor without issuing initialize.
pub(super) async fn recover_anchor<S: StateMachine>(
    node: &MultiRaft<S>,
    request: &ControlledOpen,
) -> Result<(), ControlledError> {
    let bytes = std::fs::read(anchor_path(&node.config.data_dir, request.group_id))?;
    let anchor: BootstrapAnchor = serde_json::from_slice(&bytes).map_err(std::io::Error::other)?;
    let original: BootstrapIntent = serde_json::from_slice(&std::fs::read(path(
        &node.config.data_dir,
        request.group_id,
    ))?)
    .map_err(std::io::Error::other)?;
    if anchor.intent != original
        || anchor.intent.node != node.node_id
        || anchor.intent.group != request.group_id
        || anchor.intent.generation != request.generation
        || anchor.intent.identity != request.identity
        || anchor.intent.fingerprint != node.config.raft_transport_fingerprint
    {
        return Err(ControlledError::Rejected(
            "bootstrap recovery anchor identity mismatch",
        ));
    }
    let (raft, mut sm) = node.controlled_parts(request.group_id)?;
    let actual = sm.inspect_controlled_applied().await?;
    let config_floor = anchor
        .configuration
        .log_id()
        .ok_or(ControlledError::Rejected(
            "bootstrap native configuration floor missing",
        ))?;
    let actual_config = actual.membership.log_id().ok_or(ControlledError::Rejected(
        "recovered native configuration identity missing",
    ))?;
    if !super::seed::covers(actual.applied, anchor.applied)
        || !super::seed::covers(actual_config, config_floor)
        || (actual_config == config_floor && actual.membership != anchor.configuration)
    {
        return Err(ControlledError::Rejected(
            "actual durable bootstrap state is behind its native anchor",
        ));
    }
    let observed = sm
        .applied_state()
        .await
        .map_err(|error| ControlledError::Native(error.to_string()))?;
    if observed.0 != Some(actual.applied) || observed.1 != actual.membership {
        return Err(ControlledError::Rejected(
            "bootstrap recovery cache differs from actual durable state",
        ));
    }
    let membership = actual.membership;
    let native = raft
        .with_raft_state(move |state| {
            let committed = state.membership_state.committed().as_ref();
            let effective = state.membership_state.effective().as_ref();
            let compatible = match (*effective.log_id(), *membership.log_id()) {
                (Some(effective_id), Some(applied_id)) => {
                    super::seed::covers(effective_id, applied_id)
                        && (effective_id != applied_id || effective == &membership)
                }
                _ => false,
            };
            committed == &membership && compatible
        })
        .await
        .map_err(|error| ControlledError::Native(error.to_string()))?;
    let mut log = node.open_native_log(request.group_id)?;
    let state = log.get_log_state().await?;
    let vote = log.read_vote().await?;
    if !native
        || state
            .last_log_id
            .is_none_or(|last| !super::seed::covers(last, anchor.applied))
        || !matches!(
            vote.partial_cmp(&anchor.vote),
            Some(std::cmp::Ordering::Equal | std::cmp::Ordering::Greater)
        )
    {
        return Err(ControlledError::Rejected(
            "actual native bootstrap recovery basis is behind its durable anchor",
        ));
    }
    sm.check_disk_log_basis(&mut log).await?;
    super::durable_record::publish(
        &node.config.data_dir,
        &anchor_path(&node.config.data_dir, request.group_id),
        &bytes,
    )?;
    Ok(())
}
