//! Slot-addressed local native authority, independent of Genesis placement and business readiness.
use super::{SlotHandle, SlotStateMachine};
use crate::SlotId;
use multiraft_net::{LocalAuthoritativeMembership, LocalAuthorityError};
use tokio::time::Instant;
impl<S: SlotStateMachine> SlotHandle<S> {
    /// Capture this Slot's actual local native membership and control ledger after ReadIndex.
    ///
    /// Does not forward or initialize missing groups. Candidates can inspect local
    /// native authority even while business traffic is disabled. Genesis voters do
    /// not gate this method; native registration and consensus decide authority.
    ///
    /// # Errors
    /// Returns typed invalid-slot, missing, nonleader, unavailable, admission,
    /// deadline and shutdown outcomes. No diagnostic topology fallback is used.
    pub async fn local_authoritative_membership(
        &self,
        slot: SlotId,
        deadline: Instant,
    ) -> Result<LocalAuthoritativeMembership, LocalAuthorityError> {
        let shared = self.shared().map_err(|_| LocalAuthorityError::Closed)?;
        let group = self
            .layout
            .group_for_slot(slot)
            .map_err(|_| LocalAuthorityError::InvalidSlot)?;
        if Instant::now() >= deadline {
            return Err(LocalAuthorityError::Deadline);
        }
        let _guard = shared
            .budget
            .acquire(slot, 0)
            .map_err(|_| LocalAuthorityError::Busy)?;
        shared
            .native
            .local_authoritative_membership(group, deadline)
            .await
    }
}
