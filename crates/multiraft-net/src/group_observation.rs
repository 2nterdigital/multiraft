//! Backend-neutral observations of one local Raft Group.

use std::collections::BTreeSet;

use multiraft_core::GroupId;
use multiraft_core::NodeId;
use multiraft_core::ObservationClosed;
use multiraft_core::TypeConfig;
use openraft::async_runtime::WatchReceiver as _;
use openraft::metrics::RaftServerMetrics;
use openraft::type_config::alias::WatchReceiverOf;
use openraft::vote::RaftLeaderId;
use openraft::BasicNode;
use openraft::StoredMembership;

type RawStoredMembership =
    StoredMembership<<TypeConfig as openraft::RaftTypeConfig>::LeaderId, NodeId, BasicNode>;
type RawLogId = openraft::alias::LogIdOf<TypeConfig>;
type RawServerMetricsReceiver = WatchReceiverOf<TypeConfig, RaftServerMetrics<TypeConfig>>;

/// Latest normalized server-side observation for one local Raft Group.
///
/// This is a sampled observation, not an authorization token for business
/// reads or writes.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupObservation {
    /// The observed Raft Group.
    pub group_id: GroupId,
    /// This process' local Raft node id.
    pub local_node_id: NodeId,
    /// Local node role derived from the effective membership.
    pub local_membership_role: LocalMembershipRole,
    /// Current local OpenRaft server state, normalized without exposing OpenRaft.
    pub server_state: GroupServerState,
    /// Latest leader id hinted by the local Raft instance.
    pub leader_hint: Option<NodeId>,
    /// Latest vote flushed by the local Raft instance.
    pub flushed_vote: VoteObservation,
    /// Latest effective membership known locally.
    pub effective_membership: MembershipObservation,
    /// Latest committed membership known locally.
    pub committed_membership: MembershipObservation,
}

/// Normalized Raft vote observation.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VoteObservation {
    /// Vote term.
    pub term: u64,
    /// Node id attached to the vote's leader id.
    pub node_id: NodeId,
    /// Whether the vote has been granted by a quorum.
    pub committed: bool,
}

impl VoteObservation {
    /// Creates a stable normalized vote observation.
    pub const fn new(term: u64, node_id: NodeId, committed: bool) -> Self {
        Self {
            term,
            node_id,
            committed,
        }
    }
}

/// Normalized membership plus the log id that carried it.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembershipObservation {
    /// Complete log id for this membership entry, when initialized.
    pub log_id: Option<ObservedLogId>,
    /// Joint-consensus voter configurations, preserved without flattening.
    pub voter_configs: Vec<BTreeSet<NodeId>>,
    /// Learner ids separate from voter ids.
    pub learner_ids: BTreeSet<NodeId>,
}

impl MembershipObservation {
    /// Creates a normalized membership observation without flattening joint consensus.
    pub fn new(
        log_id: Option<ObservedLogId>,
        voter_configs: Vec<BTreeSet<NodeId>>,
        learner_ids: BTreeSet<NodeId>,
    ) -> Self {
        Self {
            log_id,
            voter_configs,
            learner_ids,
        }
    }
}

/// Complete committed log identity for an observed membership entry.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservedLogId {
    /// Committed leader term.
    pub term: u64,
    /// Committed leader node id.
    pub node_id: NodeId,
    /// Log index.
    pub index: u64,
}

impl ObservedLogId {
    /// Creates a complete observed log identity.
    pub const fn new(term: u64, node_id: NodeId, index: u64) -> Self {
        Self {
            term,
            node_id,
            index,
        }
    }
}

/// Local node role derived from effective membership only.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalMembershipRole {
    /// The local node is present in at least one effective voter config.
    Voter,
    /// The local node is a learner in the effective membership.
    Learner,
    /// The local node is absent from the effective membership.
    NotMember,
}

/// Normalized active local Raft server state.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupServerState {
    /// Passive learner state.
    Learner,
    /// Follower state.
    Follower,
    /// Candidate state.
    Candidate,
    /// Leader state.
    Leader,
}

/// Receiver for latest-value group observations.
#[must_use = "dropping the receiver stops observing this Raft instance"]
pub struct GroupObservationReceiver {
    pub(crate) group_id: GroupId,
    raw: RawServerMetricsReceiver,
}

impl GroupObservationReceiver {
    /// Wait for the next latest-value observation from this exact local Raft instance.
    ///
    /// Intermediate states may be coalesced by the underlying watch channel.
    ///
    /// # Errors
    ///
    /// Returns [`ObservationClosed`] when the underlying local Raft instance has
    /// ended, or when the next raw sample is terminal.
    pub async fn changed(&mut self) -> Result<GroupObservation, ObservationClosed> {
        self.raw
            .changed()
            .await
            .map_err(|_| ObservationClosed::new(self.group_id))?;
        let metrics = {
            let borrowed = self.raw.borrow_and_update();
            borrowed.clone()
        };
        normalize_server_metrics(self.group_id, &metrics)
    }
}

pub(crate) fn initial_group_observation(
    group_id: GroupId,
    mut raw: RawServerMetricsReceiver,
) -> Result<(GroupObservation, GroupObservationReceiver), ObservationClosed> {
    let metrics = {
        let borrowed = raw.borrow_and_update();
        borrowed.clone()
    };
    let initial = normalize_server_metrics(group_id, &metrics)?;
    Ok((initial, GroupObservationReceiver { group_id, raw }))
}

pub(crate) fn normalize_server_metrics(
    group_id: GroupId,
    metrics: &RaftServerMetrics<TypeConfig>,
) -> Result<GroupObservation, ObservationClosed> {
    let server_state = normalize_server_state(group_id, metrics.state)?;
    let effective_membership = normalize_membership(&metrics.membership_config);
    let local_membership_role = local_membership_role(metrics.id, &effective_membership);

    Ok(GroupObservation {
        group_id,
        local_node_id: metrics.id,
        local_membership_role,
        server_state,
        leader_hint: metrics.current_leader,
        flushed_vote: VoteObservation {
            term: metrics.vote.leader_id().term(),
            node_id: *metrics.vote.leader_id().node_id(),
            committed: metrics.vote.committed,
        },
        effective_membership,
        committed_membership: normalize_membership(&metrics.committed_membership_config),
    })
}

pub(crate) fn normalize_server_state(
    group_id: GroupId,
    state: openraft::ServerState,
) -> Result<GroupServerState, ObservationClosed> {
    match state {
        openraft::ServerState::Learner => Ok(GroupServerState::Learner),
        openraft::ServerState::Follower => Ok(GroupServerState::Follower),
        openraft::ServerState::Candidate => Ok(GroupServerState::Candidate),
        openraft::ServerState::Leader => Ok(GroupServerState::Leader),
        openraft::ServerState::Shutdown => Err(ObservationClosed::new(group_id)),
    }
}

pub(crate) fn normalize_membership(membership: &RawStoredMembership) -> MembershipObservation {
    MembershipObservation {
        log_id: membership.log_id().as_ref().map(normalize_log_id),
        voter_configs: membership.membership().get_joint_config().clone(),
        learner_ids: membership.membership().learner_ids().collect(),
    }
}

pub(crate) fn normalize_log_id(log_id: &RawLogId) -> ObservedLogId {
    let leader_id = log_id.committed_leader_id();
    ObservedLogId {
        term: leader_id.term(),
        node_id: *leader_id.node_id(),
        index: log_id.index(),
    }
}

fn local_membership_role(
    local_node_id: NodeId,
    normalized: &MembershipObservation,
) -> LocalMembershipRole {
    if normalized
        .voter_configs
        .iter()
        .any(|config| config.contains(&local_node_id))
    {
        LocalMembershipRole::Voter
    } else if normalized.learner_ids.contains(&local_node_id) {
        LocalMembershipRole::Learner
    } else {
        LocalMembershipRole::NotMember
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use futures::FutureExt;
    use multiraft_core::TypeConfig;
    use openraft::async_runtime::WatchSender as _;
    use openraft::metrics::RaftServerMetrics;
    use openraft::testing::log_id;
    use openraft::type_config::TypeConfigExt;
    use openraft::BasicNode;
    use openraft::Membership;
    use openraft::ServerState;
    use openraft::StoredMembership;
    use openraft::Vote;

    fn set(ids: &[NodeId]) -> BTreeSet<NodeId> {
        ids.iter().copied().collect()
    }

    fn stored_membership(
        term: u64,
        node_id: NodeId,
        index: u64,
        voter_configs: Vec<BTreeSet<NodeId>>,
        all_nodes: &[NodeId],
    ) -> Arc<StoredMembership<<TypeConfig as openraft::RaftTypeConfig>::LeaderId, NodeId, BasicNode>>
    {
        Arc::new(StoredMembership::new(
            Some(log_id::<TypeConfig>(term, node_id, index)),
            Membership::new_with_defaults(voter_configs, all_nodes.iter().copied()),
        ))
    }

    fn metrics_with(
        local_node_id: NodeId,
        state: ServerState,
        effective_membership: Arc<
            StoredMembership<<TypeConfig as openraft::RaftTypeConfig>::LeaderId, NodeId, BasicNode>,
        >,
        committed_membership: Arc<
            StoredMembership<<TypeConfig as openraft::RaftTypeConfig>::LeaderId, NodeId, BasicNode>,
        >,
    ) -> RaftServerMetrics<TypeConfig> {
        RaftServerMetrics {
            id: local_node_id,
            vote: Vote::new_committed(9, 2),
            state,
            current_leader: Some(2),
            membership_config: effective_membership,
            committed_membership_config: committed_membership,
        }
    }

    #[test]
    fn public_observation_constructors_preserve_joint_identity() {
        let log_id = ObservedLogId::new(7, 2, 11);
        let vote = VoteObservation::new(9, 2, true);
        let membership =
            MembershipObservation::new(Some(log_id), vec![set(&[1, 2]), set(&[2, 3])], set(&[4]));

        assert_eq!(vote.term, 9);
        assert_eq!(vote.node_id, 2);
        assert!(vote.committed);
        assert_eq!(membership.log_id, Some(log_id));
        assert_eq!(membership.voter_configs, vec![set(&[1, 2]), set(&[2, 3])]);
        assert_eq!(membership.learner_ids, set(&[4]));
    }

    #[test]
    fn normalization_preserves_advanced_log_id_and_joint_memberships() {
        let effective =
            stored_membership(7, 2, 11, vec![set(&[1, 2]), set(&[2, 3])], &[1, 2, 3, 4, 5]);
        let committed = stored_membership(6, 3, 8, vec![set(&[1, 2, 3])], &[1, 2, 3, 6]);
        let metrics = metrics_with(4, ServerState::Follower, effective, committed);

        let observed = normalize_server_metrics(5, &metrics).expect("active snapshot");

        assert_eq!(observed.group_id, 5);
        assert_eq!(observed.local_node_id, 4);
        assert_eq!(observed.leader_hint, Some(2));
        assert_eq!(
            observed.flushed_vote,
            VoteObservation {
                term: 9,
                node_id: 2,
                committed: true,
            }
        );
        assert_eq!(
            observed.effective_membership.log_id,
            Some(ObservedLogId {
                term: 7,
                node_id: 2,
                index: 11,
            })
        );
        assert_eq!(
            observed.effective_membership.voter_configs,
            vec![set(&[1, 2]), set(&[2, 3])]
        );
        assert_eq!(observed.effective_membership.learner_ids, set(&[4, 5]));
        assert_eq!(
            observed.committed_membership.log_id,
            Some(ObservedLogId {
                term: 6,
                node_id: 3,
                index: 8,
            })
        );
        assert_eq!(
            observed.committed_membership.voter_configs,
            vec![set(&[1, 2, 3])]
        );
        assert_eq!(observed.committed_membership.learner_ids, set(&[6]));
    }

    #[test]
    fn normalization_derives_local_role_from_effective_membership() {
        let effective = stored_membership(7, 2, 11, vec![set(&[1, 2, 3])], &[1, 2, 3, 4]);
        let committed = effective.clone();

        let voter = metrics_with(
            2,
            ServerState::Follower,
            effective.clone(),
            committed.clone(),
        );
        let learner = metrics_with(
            4,
            ServerState::Follower,
            effective.clone(),
            committed.clone(),
        );
        let absent = metrics_with(9, ServerState::Follower, effective, committed);

        assert_eq!(
            normalize_server_metrics(5, &voter)
                .expect("voter snapshot")
                .local_membership_role,
            LocalMembershipRole::Voter
        );
        assert_eq!(
            normalize_server_metrics(5, &learner)
                .expect("learner snapshot")
                .local_membership_role,
            LocalMembershipRole::Learner
        );
        assert_eq!(
            normalize_server_metrics(5, &absent)
                .expect("absent snapshot")
                .local_membership_role,
            LocalMembershipRole::NotMember
        );
    }

    #[test]
    fn normalization_maps_active_states_and_rejects_shutdown() {
        let membership = stored_membership(7, 2, 11, vec![set(&[1, 2, 3])], &[1, 2, 3]);
        let active_states = [
            (ServerState::Learner, GroupServerState::Learner),
            (ServerState::Follower, GroupServerState::Follower),
            (ServerState::Candidate, GroupServerState::Candidate),
            (ServerState::Leader, GroupServerState::Leader),
        ];

        for (raw, expected) in active_states {
            let metrics = metrics_with(1, raw, membership.clone(), membership.clone());
            assert_eq!(
                normalize_server_metrics(5, &metrics)
                    .expect("active snapshot")
                    .server_state,
                expected
            );
        }

        let shutdown = metrics_with(
            1,
            ServerState::Shutdown,
            membership.clone(),
            membership.clone(),
        );
        let err = normalize_server_metrics(5, &shutdown).expect_err("shutdown is terminal");
        assert_eq!(err.group_id(), 5);
    }

    #[tokio::test]
    async fn initial_sample_is_marked_seen_before_waiting_for_change() {
        let membership = stored_membership(7, 2, 11, vec![set(&[1, 2, 3])], &[1, 2, 3]);
        let initial_metrics = metrics_with(
            1,
            ServerState::Follower,
            membership.clone(),
            membership.clone(),
        );
        let latest_metrics = metrics_with(1, ServerState::Leader, membership.clone(), membership);
        let (tx, rx) = TypeConfig::watch_channel(initial_metrics);
        tx.send(latest_metrics).expect("send latest metrics");

        let (initial, mut receiver) = initial_group_observation(5, rx).expect("initial sample");

        assert_eq!(initial.server_state, GroupServerState::Leader);
        assert_eq!(initial.local_membership_role, LocalMembershipRole::Voter);
        assert!(
            receiver.changed().now_or_never().is_none(),
            "initial sample must already be marked seen"
        );
    }

    #[tokio::test]
    async fn cancelled_wait_does_not_consume_the_next_control_change() {
        let initial_membership = stored_membership(7, 2, 11, vec![set(&[1, 2, 3])], &[1, 2, 3]);
        let learner_membership = stored_membership(8, 2, 12, vec![set(&[1, 2, 3])], &[1, 2, 3, 4]);
        let initial_metrics = metrics_with(
            4,
            ServerState::Learner,
            initial_membership.clone(),
            initial_membership,
        );
        let latest_metrics = metrics_with(
            4,
            ServerState::Learner,
            learner_membership.clone(),
            learner_membership,
        );
        let (tx, rx) = TypeConfig::watch_channel(initial_metrics);
        let (_initial, mut receiver) = initial_group_observation(5, rx).expect("initial sample");

        assert!(receiver.changed().now_or_never().is_none());

        tx.send(latest_metrics).expect("send latest metrics");
        let observed = receiver.changed().await.expect("next control change");

        assert_eq!(observed.local_membership_role, LocalMembershipRole::Learner);
        assert_eq!(observed.effective_membership.learner_ids, set(&[4]));
        assert_eq!(
            observed.effective_membership.log_id,
            Some(ObservedLogId {
                term: 8,
                node_id: 2,
                index: 12,
            })
        );
    }
}
