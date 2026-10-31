//! Sample actual local native sources without inferring membership from the frozen layout.
use super::*;
use crate::{SlotGroupObservation, SlotObservedGroup};
impl<S: SlotStateMachine> SlotHandle<S> {
    /// Read local memberships and progress; samples are not atomic or authority tokens.
    ///
    /// # Errors
    /// Returns Closed when the owner is fenced, InvalidRequest for an unknown Slot,
    /// or Deadline when the request budget is already exhausted.
    pub async fn local_group_observation(
        &self,
        slot: SlotId,
        deadline: Instant,
    ) -> Result<SlotGroupObservation, RuntimeError> {
        let shared = self.shared()?;
        let group = self
            .layout
            .group_for_slot(slot)
            .map_err(|_| RuntimeError::InvalidRequest)?;
        if Instant::now() >= deadline {
            return Err(RuntimeError::Deadline);
        }
        let membership = match shared.native.observe_group(group, deadline).await {
            Ok((sample, _receiver)) => sample,
            Err(multiraft_net::RuntimeError::Source(
                multiraft_core::MultiRaftError::UnknownGroup(_),
            )) => return Ok(SlotGroupObservation::LocalMissing),
            Err(multiraft_net::RuntimeError::Closed) => return Err(RuntimeError::Closed),
            Err(_) => return Ok(SlotGroupObservation::Unavailable),
        };
        let (progress, storage) = tokio::join!(
            shared.native.local_group_status(group, deadline),
            shared.native.local_storage_status(group, deadline)
        );
        match (progress, storage) {
            (Ok(progress), Ok(storage)) => {
                Ok(SlotGroupObservation::Present(Box::new(SlotObservedGroup {
                    membership,
                    progress,
                    storage,
                })))
            }
            _ => Ok(SlotGroupObservation::Unavailable),
        }
    }
}
