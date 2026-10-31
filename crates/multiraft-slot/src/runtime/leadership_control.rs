//! Fixed-Slot adapters for the native expected-leader transfer guards.
use super::{SlotControlError, SlotHandle, SlotStateMachine};
use crate::{RuntimeError, SlotId};
use multiraft_net::{
    ControlContext, ControlTransferOutcome, GroupControlLayoutObservation,
    GroupControlPreconditions, GroupControlRequestEcho, GroupControlSample,
};
impl<S: SlotStateMachine> SlotHandle<S> {
    /// Capture the native leader/configuration precheck using actual routing voters.
    pub async fn read_group_control_sample(
        &self,
        slot: SlotId,
        context: ControlContext,
    ) -> Result<GroupControlSample, SlotControlError> {
        self.control_call(slot, 0, context.deadline, move |shared| async move {
            let group = shared
                .layout
                .group_for_slot(slot)
                .map_err(|_| RuntimeError::InvalidRequest)?;
            let voters = shared.routing_voters(slot, context.deadline).await?;
            shared
                .native
                .read_group_control_sample(group, &voters, context)
                .await
                .map_err(|_| RuntimeError::Unavailable.into())
        })
        .await
    }
    /// Trigger at most one transfer after native rechecks the expected leader and configuration.
    pub async fn try_transfer_group_leader(
        &self,
        slot: SlotId,
        preconditions: GroupControlPreconditions,
        context: ControlContext,
    ) -> Result<ControlTransferOutcome, SlotControlError> {
        self.control_call(slot, 0, context.deadline, move |shared| async move {
            let group = shared
                .layout
                .group_for_slot(slot)
                .map_err(|_| RuntimeError::InvalidRequest)?;
            if preconditions.group_id != group {
                return Err(RuntimeError::InvalidRequest.into());
            }
            let voters = shared.routing_voters(slot, context.deadline).await?;
            Ok(shared
                .native
                .try_transfer_group_leader(&preconditions, &voters, context)
                .await)
        })
        .await
    }
    /// Observe a transfer result without inferring that the request caused the observed election.
    pub async fn observe_group_control_layout(
        &self,
        slot: SlotId,
        echo: GroupControlRequestEcho,
        context: ControlContext,
    ) -> Result<GroupControlLayoutObservation, SlotControlError> {
        self.control_call(slot, 0, context.deadline, move |shared| async move {
            let group = shared
                .layout
                .group_for_slot(slot)
                .map_err(|_| RuntimeError::InvalidRequest)?;
            if echo.group_id != group {
                return Err(RuntimeError::InvalidRequest.into());
            }
            let voters = shared.routing_voters(slot, context.deadline).await?;
            Ok(shared
                .native
                .observe_group_control_layout(echo, &voters, context)
                .await)
        })
        .await
    }
}
