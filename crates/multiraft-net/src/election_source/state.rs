//! Public state point reads. No shadow eligibility or timeout calculation.
use super::*;
use crate::group_observation::{normalize_log_id, normalize_membership, project_server_state};
use multiraft_core::{MultiRaftError, TypeConfig};
use multiraft_fsm::StateMachine;
use multiraft_store::Raft;
use openraft::{async_runtime::WatchReceiver as _, Instant as _};

const MAX_MEMBERSHIP_ITEMS: usize = 1024;
fn membership(
    value: &openraft::StoredMembership<
        <TypeConfig as openraft::RaftTypeConfig>::LeaderId,
        NodeId,
        openraft::BasicNode,
    >,
) -> SourceFact<MembershipObservation> {
    let config = value.membership().get_joint_config();
    let mut items = config.len();
    if items > MAX_MEMBERSHIP_ITEMS {
        return SourceFact::Unknown(SourceUnknown::MembershipLimit);
    }
    for voters in config {
        items = items.saturating_add(voters.len());
    }
    if items > MAX_MEMBERSHIP_ITEMS {
        return SourceFact::Unknown(SourceUnknown::MembershipLimit);
    }
    // Iterator traversal is itself bounded, including a huge learner set.
    items = items.saturating_add(
        value
            .membership()
            .learner_ids()
            .take(MAX_MEMBERSHIP_ITEMS + 1)
            .count(),
    );
    if items > MAX_MEMBERSHIP_ITEMS {
        SourceFact::Unknown(SourceUnknown::MembershipLimit)
    } else {
        SourceFact::Known(normalize_membership(value))
    }
}
pub(crate) async fn read_state<S: StateMachine>(
    raft: &Raft<S>,
    source: Arc<SourceHub>,
) -> Result<ElectionStatePoint, MultiRaftError> {
    let receiver = raft.metrics();
    let (ack_age, metrics_elapsed) = {
        let now = openraft::type_config::alias::InstantOf::<TypeConfig>::now();
        let metrics = receiver.borrow_watched();
        (
            metrics
                .last_quorum_acked
                .map(|v| now.saturating_duration_since(v.into_inner())),
            source.elapsed_at(now),
        )
    };
    raft.with_raft_state(move |state| {
        let now = openraft::type_config::alias::InstantOf::<TypeConfig>::now();
        ElectionStatePoint {
            state_sample_elapsed: source.elapsed_at(now),
            vote: network::vote(state.vote_ref()),
            vote_last_modified_age: state
                .vote_last_modified()
                .map(|v| now.saturating_duration_since(v)),
            role: project_server_state(state.server_state),
            effective_membership: membership(state.membership_state.effective()),
            committed_membership: membership(state.membership_state.committed()),
            local_committed: state.local_committed().map(normalize_log_id),
            cluster_committed: state.cluster_committed().map(normalize_log_id),
            actual_random_timeout: SourceFact::Unknown(SourceUnknown::PublicPointNotExposed),
            lease_enabled: SourceFact::Unknown(SourceUnknown::PublicPointNotExposed),
            lease_duration: SourceFact::Unknown(SourceUnknown::PublicPointNotExposed),
            greater_log: SourceFact::Unknown(SourceUnknown::PublicPointNotExposed),
            committed_append_quorum_ack_age: ack_age,
            ack_metrics_sample_elapsed: metrics_elapsed,
        }
    })
    .await
    .map_err(|e| MultiRaftError::Other(e.into()))
}
