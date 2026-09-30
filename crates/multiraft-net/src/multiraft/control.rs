//! Normative local control admission and absolute-deadline facade over native rules.
use super::*;
use crate::group_control::{
    logging::{sample_reason, ControlGuard},
    ControlContext, ControlInvocationId, ControlStage, ControlTransferOutcome,
    GroupControlPrecheckRejection,
};
use tokio::time::Instant;

impl<S: StateMachine> MultiRaft<S> {
    /// Legacy bounded entry; use `_at` to propagate an invocation and inherited deadline.
    pub async fn read_group_control_sample(
        &self,
        group: GroupId,
        expected_voters: &[NodeId],
        max_sample_age: Duration,
        max_target_ack_age: Duration,
    ) -> Result<GroupControlSample, GroupControlSampleError> {
        self.read_group_control_sample_at(
            group,
            expected_voters,
            legacy_context(max_sample_age, max_target_ack_age),
        )
        .await
    }
    pub async fn read_group_control_sample_at(
        &self,
        group: GroupId,
        expected_voters: &[NodeId],
        context: ControlContext,
    ) -> Result<GroupControlSample, GroupControlSampleError> {
        self.read_control_owned(group, expected_voters, context, None)
            .await
    }
    pub(crate) async fn read_control_owned(
        &self,
        group: GroupId,
        expected_voters: &[NodeId],
        context: ControlContext,
        abort: Option<tokio::sync::watch::Receiver<bool>>,
    ) -> Result<GroupControlSample, GroupControlSampleError> {
        let echo = GroupControlRequestEcho {
            group_id: group,
            source: self.node_id,
            target: self.node_id,
        };
        let mut guard = ControlGuard::observation(context, echo, self.node_id);
        let result = async {
            let _permit = self.control_admit(group, context)?;
            self.sample_control(group, expected_voters, &mut guard, abort)
                .await
        }
        .await;
        match &result {
            Ok(sample) => {
                guard.sample(sample);
                guard.finish_observation("sampled", "none");
            }
            Err(error) => guard.finish("sample_unavailable", sample_reason(error)),
        }
        result
    }
    fn control_admit(
        &self,
        group_id: GroupId,
        context: ControlContext,
    ) -> Result<tokio::sync::SemaphorePermit<'_>, GroupControlSampleError> {
        if Instant::now() >= context.deadline {
            return Err(GroupControlSampleError::Deadline {
                group_id,
                stage: ControlStage::Admission,
            });
        }
        if !self
            .ingress_accepting
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(GroupControlSampleError::Closed { group_id });
        }
        self.control_slot
            .try_acquire()
            .map_err(|_| GroupControlSampleError::Busy { group_id })
    }
    async fn sample_control(
        &self,
        group: GroupId,
        voters: &[NodeId],
        guard: &mut ControlGuard,
        mut abort: Option<tokio::sync::watch::Receiver<bool>>,
    ) -> Result<GroupControlSample, GroupControlSampleError> {
        let raft = self
            .raft(group)
            .ok_or(GroupControlSampleError::UnknownGroup { group_id: group })?;
        guard.stage(ControlStage::Sampling);
        let context = guard.context;
        let sampling = async {
            tokio::time::timeout_at(
                context.deadline,
                crate::group_control::read_group_control_sample(
                    &raft,
                    group,
                    self.node_id,
                    voters,
                    context.max_sample_age,
                    context.max_target_ack_age,
                ),
            )
            .await
            .unwrap_or(Err(GroupControlSampleError::Deadline {
                group_id: group,
                stage: ControlStage::Sampling,
            }))
        };
        tokio::select! { biased;
            _ = interrupted(&mut abort) => Err(GroupControlSampleError::Closed { group_id: group }),
            sample = sampling => sample,
        }
    }

    /// Rechecks and submits at most one native trigger. Queued is not completion.
    pub async fn try_transfer_group_leader(
        &self,
        preconditions: &GroupControlPreconditions,
        voters: &[NodeId],
        max_sample_age: Duration,
        max_target_ack_age: Duration,
    ) -> GroupControlRequestResult {
        self.try_transfer_group_leader_at(
            preconditions,
            voters,
            legacy_context(max_sample_age, max_target_ack_age),
        )
        .await
        .result
    }
    pub async fn try_transfer_group_leader_at(
        &self,
        preconditions: &GroupControlPreconditions,
        voters: &[NodeId],
        context: ControlContext,
    ) -> ControlTransferOutcome {
        self.transfer_control_owned(preconditions, voters, context, None)
            .await
    }
    pub(crate) async fn transfer_control_owned(
        &self,
        preconditions: &GroupControlPreconditions,
        voters: &[NodeId],
        context: ControlContext,
        abort: Option<tokio::sync::watch::Receiver<bool>>,
    ) -> ControlTransferOutcome {
        let group = preconditions.group_id;
        let mut guard = ControlGuard::new(context, preconditions.echo(), self.node_id);
        let admitted = self.control_admit(group, context).and_then(|permit| {
            if preconditions.expected_source != self.node_id {
                return Err(GroupControlSampleError::WrongNode {
                    group_id: group,
                    expected: preconditions.expected_source,
                    actual: self.node_id,
                });
            }
            let raft = self
                .raft(group)
                .ok_or(GroupControlSampleError::UnknownGroup { group_id: group })?;
            Ok((permit, raft))
        });
        let (_permit, raft) = match admitted {
            Ok(admitted) => admitted,
            Err(error) => {
                guard.finish("precheck_rejected", sample_reason(&error));
                return ControlTransferOutcome {
                    invocation_id: context.invocation_id,
                    local_node_id: self.node_id,
                    sample: None,
                    result: GroupControlRequestResult::PrecheckRejected {
                        echo: preconditions.echo(),
                        reason: GroupControlPrecheckRejection::Sample(error),
                    },
                };
            }
        };
        crate::group_control::submit_group_control_transfer(
            &raft,
            self.node_id,
            preconditions,
            voters,
            &mut guard,
            abort,
        )
        .await
    }
    /// Independent layout sample; historical source may differ from this current leader.
    pub async fn observe_group_control_layout(
        &self,
        echo: GroupControlRequestEcho,
        voters: &[NodeId],
        max_sample_age: Duration,
        max_target_ack_age: Duration,
    ) -> GroupControlLayoutObservation {
        self.observe_group_control_layout_at(
            echo,
            voters,
            legacy_context(max_sample_age, max_target_ack_age),
        )
        .await
    }
    pub async fn observe_group_control_layout_at(
        &self,
        echo: GroupControlRequestEcho,
        voters: &[NodeId],
        context: ControlContext,
    ) -> GroupControlLayoutObservation {
        self.layout_control_owned(echo, voters, context, None).await
    }
    pub(crate) async fn layout_control_owned(
        &self,
        echo: GroupControlRequestEcho,
        voters: &[NodeId],
        context: ControlContext,
        abort: Option<tokio::sync::watch::Receiver<bool>>,
    ) -> GroupControlLayoutObservation {
        let mut guard = ControlGuard::observation(context, echo, self.node_id);
        guard.stage(ControlStage::Layout);
        let sample = async {
            let _permit = self.control_admit(echo.group_id, context)?;
            self.sample_control(echo.group_id, voters, &mut guard, abort)
                .await
        }
        .await;
        let result = crate::group_control::classify_group_control_layout(echo, sample.as_ref());
        guard.stage(ControlStage::Layout);
        match &result {
            GroupControlLayoutObservation::Unavailable { reason, .. } => {
                guard.finish("layout_unavailable", sample_reason(reason))
            }
            GroupControlLayoutObservation::TargetObserved { .. } => {
                guard.finish_observation("target_observed", "causality_unknown")
            }
            GroupControlLayoutObservation::SourceObserved { .. } => {
                guard.finish_observation("source_observed", "causality_unknown")
            }
            GroupControlLayoutObservation::DifferentLeaderObserved { .. } => {
                guard.finish_observation("different_leader_observed", "causality_unknown")
            }
        }
        result
    }
}
fn legacy_context(sample: Duration, ack: Duration) -> ControlContext {
    let mut context = ControlContext::new(
        ControlInvocationId::fresh(),
        Instant::now() + Duration::from_secs(10),
    );
    context.max_sample_age = sample;
    context.max_target_ack_age = ack;
    context
}

async fn interrupted(abort: &mut Option<tokio::sync::watch::Receiver<bool>>) {
    match abort {
        Some(receiver) => {
            if !*receiver.borrow_and_update() {
                let _ = receiver.changed().await;
            }
        }
        None => std::future::pending::<()>().await,
    }
}
