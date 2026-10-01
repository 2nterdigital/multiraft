//! Projection of facts emitted at actual native branches; no copied eligibility.
use super::*;
use crate::group_observation::normalize_log_id;
use multiraft_core::TypeConfig;
use openraft::election_observer::{
    ElectionEvent, ElectionEventKind, ElectionObserver, ElectionTiming,
};
use openraft::{type_config::alias::InstantOf, Instant as _};
use std::collections::BTreeSet;

pub use openraft::election_observer::{
    AutomaticElectionDecision, CampaignOrigin, CampaignPhase, VoteRequestDisposition,
    VoteResponseDisposition,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeElectionTiming {
    pub last_vote_update_age: Option<Duration>,
    pub lease: Duration,
    pub lease_enabled: bool,
    pub election_timeout: Duration,
    pub seen_greater_log: bool,
    pub greater_log_delay: Duration,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeElectionFact {
    pub source_elapsed: Duration,
    pub local_vote: VoteObservation,
    pub kind: NativeElectionKind,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeElectionKind {
    Started {
        timing: NativeElectionTiming,
    },
    AutomaticElection {
        timing: NativeElectionTiming,
        is_voter: bool,
        voter_count: usize,
        is_leader: bool,
        election_enabled: Option<bool>,
        pre_vote_enabled: Option<bool>,
        decision: AutomaticElectionDecision,
    },
    CampaignStarted {
        campaign_id: u64,
        origin: CampaignOrigin,
        phase: CampaignPhase,
        vote: VoteObservation,
        last_log_id: Option<ObservedLogId>,
        membership_log_id: Option<ObservedLogId>,
        joint_voters: SourceFact<Vec<BTreeSet<NodeId>>>,
        election_timeout_before: Duration,
        election_timeout_after: Duration,
    },
    VoteRequestProcessed {
        phase: CampaignPhase,
        vote: VoteObservation,
        last_log_id: Option<ObservedLogId>,
        leadership_transfer: bool,
        previous_vote: VoteObservation,
        local_last_log_id: Option<ObservedLogId>,
        timing: NativeElectionTiming,
        disposition: VoteRequestDisposition,
    },
    VoteResponse {
        campaign_id: Option<u64>,
        phase: CampaignPhase,
        target: NodeId,
        candidate_vote: VoteObservation,
        response_vote: VoteObservation,
        response_last_log_id: Option<ObservedLogId>,
        response_granted: bool,
        disposition: VoteResponseDisposition,
    },
    SelfPreVoteGranted {
        campaign_id: Option<u64>,
        vote: VoteObservation,
        quorum: bool,
    },
    QuorumGranted {
        campaign_id: Option<u64>,
        phase: CampaignPhase,
        vote: VoteObservation,
        granters: SourceFact<Vec<NodeId>>,
    },
    LeaderEstablished {
        campaign_id: Option<u64>,
        vote: VoteObservation,
    },
    Unknown,
}
fn timing(value: ElectionTiming<TypeConfig>, at: InstantOf<TypeConfig>) -> NativeElectionTiming {
    NativeElectionTiming {
        last_vote_update_age: value
            .last_vote_update
            .map(|v| at.saturating_duration_since(v)),
        lease: value.lease,
        lease_enabled: value.lease_enabled,
        election_timeout: value.election_timeout,
        seen_greater_log: value.seen_greater_log,
        greater_log_delay: value.greater_log_delay,
    }
}
pub(crate) struct NativeSourceObserver {
    pub(crate) group: GroupId,
    pub(crate) hub: Arc<SourceHub>,
}
impl ElectionObserver<TypeConfig> for NativeSourceObserver {
    fn on_event(&self, event: ElectionEvent<TypeConfig>) {
        self.hub.submit_native(self.group, event);
    }
}

pub(crate) fn project(
    event: ElectionEvent<TypeConfig>,
    hub: &SourceHub,
) -> (Option<u64>, NativeElectionFact) {
    let at = event.at;
    let mut round = None;
    let kind = match event.kind {
        ElectionEventKind::Started { timing: value } => NativeElectionKind::Started {
            timing: timing(value, at),
        },
        ElectionEventKind::AutomaticElection {
            timing: value,
            is_voter,
            voter_count,
            is_leader,
            election_enabled,
            pre_vote_enabled,
            decision,
        } => NativeElectionKind::AutomaticElection {
            timing: timing(value, at),
            is_voter,
            voter_count,
            is_leader,
            election_enabled,
            pre_vote_enabled,
            decision,
        },
        ElectionEventKind::CampaignStarted {
            campaign_id,
            origin,
            phase,
            vote,
            last_log_id,
            membership_log_id,
            joint_voters,
            election_timeout_before,
            election_timeout_after,
        } => {
            round = Some(campaign_id);

            NativeElectionKind::CampaignStarted {
                campaign_id,
                origin,
                phase,
                vote: network::vote(&vote),
                last_log_id: last_log_id.as_ref().map(normalize_log_id),
                membership_log_id: membership_log_id.as_ref().map(normalize_log_id),
                joint_voters: joint_voters
                    .map(SourceFact::Known)
                    .unwrap_or(SourceFact::Unknown(SourceUnknown::MembershipLimit)),
                election_timeout_before,
                election_timeout_after,
            }
        }
        ElectionEventKind::VoteRequestProcessed {
            phase,
            vote,
            last_log_id,
            leadership_transfer,
            previous_vote,
            local_last_log_id,
            timing: value,
            disposition,
        } => NativeElectionKind::VoteRequestProcessed {
            phase,
            vote: network::vote(&vote),
            last_log_id: last_log_id.as_ref().map(normalize_log_id),
            leadership_transfer,
            previous_vote: network::vote(&previous_vote),
            local_last_log_id: local_last_log_id.as_ref().map(normalize_log_id),
            timing: timing(value, at),
            disposition,
        },
        ElectionEventKind::VoteResponse {
            campaign_id,
            phase,
            target,
            candidate_vote,
            response_vote,
            response_last_log_id,
            response_granted,
            disposition,
        } => {
            round = campaign_id;
            NativeElectionKind::VoteResponse {
                campaign_id,
                phase,
                target,
                candidate_vote: network::vote(&candidate_vote),
                response_vote: network::vote(&response_vote),
                response_last_log_id: response_last_log_id.as_ref().map(normalize_log_id),
                response_granted,
                disposition,
            }
        }
        ElectionEventKind::SelfPreVoteGranted {
            campaign_id,
            vote,
            quorum,
        } => {
            round = campaign_id;
            NativeElectionKind::SelfPreVoteGranted {
                campaign_id,
                vote: network::vote(&vote),
                quorum,
            }
        }
        ElectionEventKind::QuorumGranted {
            campaign_id,
            phase,
            vote,
            granters,
        } => {
            round = campaign_id;
            NativeElectionKind::QuorumGranted {
                campaign_id,
                phase,
                vote: network::vote(&vote),
                granters: granters
                    .map(SourceFact::Known)
                    .unwrap_or(SourceFact::Unknown(SourceUnknown::MembershipLimit)),
            }
        }
        ElectionEventKind::LeaderEstablished { campaign_id, vote } => {
            round = campaign_id;
            NativeElectionKind::LeaderEstablished {
                campaign_id,
                vote: network::vote(&vote),
            }
        }
        _ => NativeElectionKind::Unknown,
    };
    (
        round,
        NativeElectionFact {
            source_elapsed: hub.elapsed_at(at),
            local_vote: network::vote(&event.local_vote),
            kind,
        },
    )
}
