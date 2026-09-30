//! Existing qualification-based precheck; no native algorithm change.
use super::*;
impl GroupControlSample {
    pub fn observed_preconditions_for(&self, target: NodeId) -> GroupControlPreconditions {
        GroupControlPreconditions {
            group_id: self.group_id,
            expected_source: self.leader_id,
            observed_vote: self.flushed_vote,
            effective_membership: self.effective_membership.clone(),
            committed_membership: self.committed_membership.clone(),
            target,
        }
    }

    pub fn check_transfer_preconditions(
        &self,
        preconditions: &GroupControlPreconditions,
    ) -> Result<(), GroupControlPrecheckError> {
        if preconditions.group_id != self.group_id {
            return Err(GroupControlPrecheckError::GroupChanged {
                expected: preconditions.group_id,
                actual: self.group_id,
            });
        }
        if preconditions.expected_source != self.leader_id {
            return Err(GroupControlPrecheckError::SourceChanged {
                expected: preconditions.expected_source,
                actual: self.leader_id,
            });
        }
        if preconditions.observed_vote != self.flushed_vote {
            return Err(GroupControlPrecheckError::VoteChanged {
                expected: preconditions.observed_vote,
                actual: self.flushed_vote,
            });
        }
        if preconditions.effective_membership != self.effective_membership {
            return Err(GroupControlPrecheckError::MembershipChanged {
                kind: MembershipChangeKind::Effective,
                expected: Box::new(preconditions.effective_membership.clone()),
                actual: Box::new(self.effective_membership.clone()),
            });
        }
        if preconditions.committed_membership != self.committed_membership {
            return Err(GroupControlPrecheckError::MembershipChanged {
                kind: MembershipChangeKind::Committed,
                expected: Box::new(preconditions.committed_membership.clone()),
                actual: Box::new(self.committed_membership.clone()),
            });
        }

        match self.target_qualifications.get(&preconditions.target) {
            Some(TargetQualification::Qualified { .. }) => Ok(()),
            Some(TargetQualification::Source) => Err(GroupControlPrecheckError::TargetIsSource {
                target: preconditions.target,
            }),
            Some(
                qualification @ (TargetQualification::MissingAck
                | TargetQualification::AckTooOld { .. }),
            ) => Err(GroupControlPrecheckError::TargetNotRecentlyAcked {
                target: preconditions.target,
                qualification: *qualification,
            }),
            Some(
                qualification @ (TargetQualification::MissingLocalCommitted
                | TargetQualification::MissingMatched),
            ) => Err(GroupControlPrecheckError::TargetProgressUnavailable {
                target: preconditions.target,
                qualification: *qualification,
            }),
            Some(TargetQualification::Lagging { matched, required }) => {
                Err(GroupControlPrecheckError::TargetLagging {
                    target: preconditions.target,
                    matched: *matched,
                    required: *required,
                })
            }
            Some(TargetQualification::NotVoter) | None => {
                Err(GroupControlPrecheckError::TargetNotVoter {
                    target: preconditions.target,
                })
            }
        }
    }
}
