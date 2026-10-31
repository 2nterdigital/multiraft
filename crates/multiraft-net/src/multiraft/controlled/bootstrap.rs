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
    tokio::task::spawn_blocking(move || persist_intent(&path, &bytes))
        .await
        .map_err(|error| ControlledError::Native(error.to_string()))??;
    Ok(())
}
fn persist_intent(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    use sha2::Digest;
    use std::io::Write;
    match std::fs::read(path) {
        Ok(existing) if existing == bytes => return Ok(()),
        Ok(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "bootstrap intent identity conflict",
            ))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let directory = path
        .parent()
        .ok_or_else(|| std::io::Error::other("bootstrap directory missing"))?;
    std::fs::create_dir_all(directory)?;
    // The unique content address permits recovery of a torn pre-publication write.
    let suffix = format!("{:x}", sha2::Sha256::digest(bytes));
    let pending = directory.join(format!(".{suffix}.pending"));
    match std::fs::remove_file(&pending) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&pending)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    match std::fs::hard_link(&pending, path) {
        Ok(()) => {}
        Err(error)
            if error.kind() == std::io::ErrorKind::AlreadyExists
                && std::fs::read(path)? == bytes => {}
        Err(error) => return Err(error),
    }
    std::fs::remove_file(pending)?;
    std::fs::File::open(directory)?.sync_all()?;
    if let Some(parent) = directory.parent() {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
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
