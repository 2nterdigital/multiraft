//! Immutable explicit-new bootstrap intent survives construction and initialization crashes.
use super::*;
use openraft::storage::RaftStateMachine;
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

pub(super) async fn reconcile<S: StateMachine>(
    node: &MultiRaft<S>,
    request: &ControlledOpen,
    voters: &BTreeSet<NodeId>,
) -> Result<(), ControlledError> {
    verify(node, request, voters)?;
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
                return Ok(());
            }
        }
        if *stopping.borrow_and_update() {
            return Err(ControlledError::Closed);
        }
        tokio::select! { _ = stopping.changed() => return Err(ControlledError::Closed), _ = tokio::time::sleep(Duration::from_millis(20)) => {} }
    }
}
