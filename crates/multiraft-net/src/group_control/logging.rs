//! Finite source summaries and cancellation evidence. No arbitrary error/debug payloads.
use super::*;

pub(crate) struct ControlGuard {
    pub(crate) context: ControlContext,
    echo: GroupControlRequestEcho,
    local: NodeId,
    stage: ControlStage,
    started: tokio::time::Instant,
    done: bool,
}
impl ControlGuard {
    pub(crate) fn new(
        context: ControlContext,
        echo: GroupControlRequestEcho,
        local: NodeId,
    ) -> Self {
        let guard = Self::admission(context, echo, local);
        guard.log("started", "none");
        guard
    }
    /// The weak boundary logs refusals/cancellation; successful work uses the facade start.
    pub(crate) fn admission(
        context: ControlContext,
        echo: GroupControlRequestEcho,
        local: NodeId,
    ) -> Self {
        Self {
            context,
            echo,
            local,
            stage: ControlStage::Admission,
            started: tokio::time::Instant::now(),
            done: false,
        }
    }
    pub(crate) fn disarm(&mut self) {
        self.done = true;
    }
    pub(crate) fn stage(&mut self, stage: ControlStage) {
        self.stage = stage;
    }
    fn log(&self, result: &'static str, reason_code: &'static str) {
        tracing::info!(target: "multiraft::control", invocation_id = %self.context.invocation_id,
            group_id = self.echo.group_id, local_node_id = self.local,
            source_node_id = self.echo.source, target_node_id = self.echo.target,
            stage = ?self.stage, result, reason_code,
            duration_ms = self.started.elapsed().as_millis() as u64,
            "control invocation source fact");
    }
    pub(crate) fn finish(&mut self, result: &'static str, reason: &'static str) {
        self.done = true;
        self.log(result, reason);
    }
    pub(crate) fn sample(&self, sample: &GroupControlSample) {
        tracing::debug!(target: "multiraft::control", invocation_id = %self.context.invocation_id,
            group_id = sample.group_id, local_node_id = sample.local_node_id,
            vote_term = sample.flushed_vote.term, vote_node_id = sample.flushed_vote.node_id,
            vote_committed = sample.flushed_vote.committed, sample_age_ms = sample.sample_age.as_millis() as u64,
            target_detail_truncated = sample.target_qualifications.len()>16,
            "fresh control sample");
        // Bounded details; complete memberships/qualifications stay in typed return values.
        for (target, qualification) in sample.target_qualifications.iter().take(16) {
            let (code, age, max, matched, required) = match *qualification {
                TargetQualification::Source => ("source", None, None, None, None),
                TargetQualification::Qualified { ack_age, matched } => {
                    ("qualified", Some(ack_age), None, Some(matched), None)
                }
                TargetQualification::NotVoter => ("not_voter", None, None, None, None),
                TargetQualification::MissingAck => ("missing_ack", None, None, None, None),
                TargetQualification::AckTooOld { age, max } => {
                    ("ack_too_old", Some(age), Some(max), None, None)
                }
                TargetQualification::MissingLocalCommitted => {
                    ("missing_local_committed", None, None, None, None)
                }
                TargetQualification::MissingMatched => ("missing_matched", None, None, None, None),
                TargetQualification::Lagging { matched, required } => {
                    ("lagging", None, None, Some(matched), Some(required))
                }
            };
            tracing::debug!(target: "multiraft::control", invocation_id = %self.context.invocation_id,
                group_id = sample.group_id, target_node_id = target, qualification = code,
                target_ack_age_ms = ?age.map(|age|age.as_millis()), target_ack_max_age_ms = ?max.map(|age|age.as_millis()),
                matched_term = ?matched.map(|id|id.term), matched_node_id = ?matched.map(|id|id.node_id), matched_index = ?matched.map(|id|id.index),
                required_term = ?required.map(|id|id.term), required_node_id = ?required.map(|id|id.node_id), required_index = ?required.map(|id|id.index),
                "sampled target qualification");
        }
    }
}
impl Drop for ControlGuard {
    fn drop(&mut self) {
        if !self.done {
            self.log(
                if self.stage == ControlStage::Trigger {
                    "outcome_unknown"
                } else {
                    "not_submitted"
                },
                "cancelled",
            );
        }
    }
}
pub(crate) fn sample_reason(error: &GroupControlSampleError) -> &'static str {
    match error {
        GroupControlSampleError::Busy { .. } => "busy",
        GroupControlSampleError::Deadline { .. } => "deadline",
        GroupControlSampleError::WrongNode { .. } => "wrong_node",
        GroupControlSampleError::InvalidOptions { .. } => "invalid_options",
        GroupControlSampleError::UnknownGroup { .. } => "unknown_group",
        GroupControlSampleError::Closed { .. } => "closed",
        GroupControlSampleError::NotLeader { .. } => "not_leader",
        GroupControlSampleError::ReadIndexFailed { source, .. } => match source {
            ReadIndexFailure::QuorumUnavailable { .. } => "read_index_quorum_unavailable",
            ReadIndexFailure::Backend(NativeFailure::Storage) => "native_storage",
            ReadIndexFailure::Backend(NativeFailure::Panicked) => "native_panicked",
            ReadIndexFailure::Closed => "closed",
            ReadIndexFailure::Deadline => "deadline",
            _ => "read_index_failed",
        },
        GroupControlSampleError::InconsistentIdentity { .. } => "identity_changed",
        GroupControlSampleError::UnexpectedMembership { .. } => "unexpected_membership",
        GroupControlSampleError::SampleTooOld { .. } => "sample_too_old",
    }
}

pub(crate) fn precheck_reason(error: &GroupControlPrecheckError) -> &'static str {
    match error {
        GroupControlPrecheckError::GroupChanged { .. } => "group_changed",
        GroupControlPrecheckError::SourceChanged { .. } => "source_changed",
        GroupControlPrecheckError::VoteChanged { .. } => "vote_changed",
        GroupControlPrecheckError::MembershipChanged {
            kind: MembershipChangeKind::Effective,
            ..
        } => "effective_membership_changed",
        GroupControlPrecheckError::MembershipChanged {
            kind: MembershipChangeKind::Committed,
            ..
        } => "committed_membership_changed",
        GroupControlPrecheckError::TargetNotVoter { .. } => "target_not_voter",
        GroupControlPrecheckError::TargetIsSource { .. } => "target_is_source",
        GroupControlPrecheckError::TargetNotRecentlyAcked { .. } => "target_not_recently_acked",
        GroupControlPrecheckError::TargetProgressUnavailable { .. } => {
            "target_progress_unavailable"
        }
        GroupControlPrecheckError::TargetLagging { .. } => "target_lagging",
    }
}
impl ControlGuard {
    pub(crate) fn rejection(&self, error: &GroupControlPrecheckError) {
        match error {
            GroupControlPrecheckError::VoteChanged { expected, actual } => {
                tracing::debug!(target: "multiraft::control", invocation_id = %self.context.invocation_id,
                    group_id = self.echo.group_id, expected_vote_term = expected.term, expected_vote_node_id = expected.node_id,
                    expected_vote_committed = expected.committed, observed_vote_term = actual.term,
                    observed_vote_node_id = actual.node_id, observed_vote_committed = actual.committed, "vote precheck evidence");
            }
            GroupControlPrecheckError::MembershipChanged {
                kind,
                expected,
                actual,
            } => {
                self.membership("expected", *kind, expected);
                self.membership("observed", *kind, actual);
            }
            _ => {} // Complete structured rejection and sample remain in the returned outcome.
        }
    }
    fn membership(
        &self,
        scope: &'static str,
        kind: MembershipChangeKind,
        value: &MembershipObservation,
    ) {
        tracing::debug!(target: "multiraft::control", invocation_id = %self.context.invocation_id,
            group_id = self.echo.group_id, membership_scope = scope, membership_kind = ?kind,
            membership_log_term = ?value.log_id.map(|id|id.term), membership_log_node_id = ?value.log_id.map(|id|id.node_id),
            membership_log_index = ?value.log_id.map(|id|id.index), voter_config_count = value.voter_configs.len(),
            learner_count = value.learner_ids.len(), detail_truncated = value.voter_configs.len()>2 || value.voter_configs.iter().any(|v|v.len()>16) || value.learner_ids.len()>16,
            "membership precheck evidence");
        for (config, voters) in value.voter_configs.iter().take(2).enumerate() {
            for voter in voters.iter().take(16) {
                tracing::debug!(target: "multiraft::control", invocation_id = %self.context.invocation_id,
                    group_id = self.echo.group_id, membership_scope = scope, membership_kind = ?kind,
                    voter_config_index = config, voter_node_id = voter, "membership voter evidence");
            }
        }
        for learner in value.learner_ids.iter().take(16) {
            tracing::debug!(target: "multiraft::control", invocation_id = %self.context.invocation_id,
                group_id = self.echo.group_id, membership_scope = scope, membership_kind = ?kind,
                learner_node_id = learner, "membership learner evidence");
        }
    }
}

pub(crate) fn submission_reason(reason: ControlSubmissionError) -> &'static str {
    match reason {
        ControlSubmissionError::Stopped => "native_stopped",
        ControlSubmissionError::Backend(NativeFailure::Storage) => "native_storage",
        ControlSubmissionError::Backend(NativeFailure::Panicked) => "native_panicked",
        ControlSubmissionError::Deadline => "deadline",
        ControlSubmissionError::Cancelled => "cancelled",
        ControlSubmissionError::Closed => "closed",
        ControlSubmissionError::Backend(_) => "native_backend_unknown",
        ControlSubmissionError::Unknown => "unknown",
    }
}
