//! Weak normative control access. Project/business RPC routing stays above this boundary.
use super::*;
use crate::group_control::logging::{sample_reason, ControlGuard};
use crate::{
    ControlContext, ControlStage, ControlTransferOutcome, GroupControlLayoutObservation,
    GroupControlPrecheckRejection, GroupControlPreconditions, GroupControlRequestEcho,
    GroupControlRequestResult, GroupControlSample, GroupControlSampleError,
};

impl<S: StateMachine> RuntimeHandle<S> {
    pub async fn read_group_control_sample(
        &self,
        group: GroupId,
        voters: &[NodeId],
        context: ControlContext,
    ) -> Result<GroupControlSample, GroupControlSampleError> {
        let echo = GroupControlRequestEcho {
            group_id: group,
            source: self.node_id,
            target: self.node_id,
        };
        let mut guard = ControlGuard::admission(context, echo, self.node_id);
        let admitted = match self.control_admit(group, context).await {
            Ok(admitted) => admitted,
            Err(error) => {
                guard.finish("sample_unavailable", sample_reason(&error));
                return Err(error);
            }
        };
        guard.disarm();
        admitted
            .shared
            .node
            .read_control_owned(
                group,
                voters,
                context,
                Some(admitted.shared.abort_requests.subscribe()),
            )
            .await
    }

    /// One fresh precheck and at most one trigger. Unknown outcomes never cause a retry.
    pub async fn try_transfer_group_leader(
        &self,
        preconditions: &GroupControlPreconditions,
        voters: &[NodeId],
        context: ControlContext,
    ) -> ControlTransferOutcome {
        let mut guard = ControlGuard::admission(context, preconditions.echo(), self.node_id);
        let admitted = match self.control_admit(preconditions.group_id, context).await {
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
        guard.disarm();
        admitted
            .shared
            .node
            .transfer_control_owned(
                preconditions,
                voters,
                context,
                Some(admitted.shared.abort_requests.subscribe()),
            )
            .await
    }
    /// Source/target/different leader classification does not prove request causality.
    pub async fn observe_group_control_layout(
        &self,
        echo: GroupControlRequestEcho,
        voters: &[NodeId],
        context: ControlContext,
    ) -> GroupControlLayoutObservation {
        let mut guard = ControlGuard::admission(context, echo, self.node_id);
        let admitted = match self.control_admit(echo.group_id, context).await {
            Ok(admitted) => admitted,
            Err(reason) => {
                guard.finish("layout_unavailable", sample_reason(&reason));
                return GroupControlLayoutObservation::Unavailable { echo, reason };
            }
        };
        guard.disarm();
        admitted
            .shared
            .node
            .layout_control_owned(
                echo,
                voters,
                context,
                Some(admitted.shared.abort_requests.subscribe()),
            )
            .await
    }
    async fn control_admit(
        &self,
        group: GroupId,
        context: ControlContext,
    ) -> Result<requests::Admitted<S>, GroupControlSampleError> {
        self.admit(context.deadline)
            .await
            .and_then(|admitted| {
                admitted.ensure_group(group)?;
                Ok(admitted)
            })
            .map_err(|error| match error {
                RuntimeError::Busy => GroupControlSampleError::Busy { group_id: group },
                RuntimeError::Deadline { .. } => GroupControlSampleError::Deadline {
                    group_id: group,
                    stage: ControlStage::Admission,
                },
                RuntimeError::Source(MultiRaftError::UnknownGroup(_)) => {
                    GroupControlSampleError::UnknownGroup { group_id: group }
                }
                _ => GroupControlSampleError::Closed { group_id: group },
            })
    }
}
