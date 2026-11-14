//! Bounded owned admission and shutdown cancellation for read-only local authority.
use super::*;
use crate::{LocalAuthoritativeMembership, LocalAuthorityError};
impl<S: StateMachine> RuntimeHandle<S> {
    /// Read this registered Group's authoritative membership and native ledger locally.
    ///
    /// Uses immediate bounded admission, the caller's deadline and the owner's
    /// shutdown fence. No detached work, group construction, forwarding or writes.
    /// Registration is checked by native, not the original startup voter declarations.
    ///
    /// # Errors
    /// Returns local missing/nonleader/unavailable outcomes without substituting a
    /// diagnostic sample. Busy, deadline and shutdown remain distinct typed outcomes.
    pub async fn local_authoritative_membership(
        &self,
        group: GroupId,
        deadline: Instant,
    ) -> Result<LocalAuthoritativeMembership, LocalAuthorityError> {
        let admitted = self.admit(deadline).await.map_err(|error| match error {
            RuntimeError::Closed | RuntimeError::Interrupted { .. } => LocalAuthorityError::Closed,
            RuntimeError::Busy => LocalAuthorityError::Busy,
            RuntimeError::Deadline { .. } => LocalAuthorityError::Deadline,
            _ => LocalAuthorityError::Unavailable,
        })?;
        let mut abort = admitted.shared.abort_requests.subscribe();
        if *abort.borrow_and_update() {
            return Err(LocalAuthorityError::Closed);
        }
        tokio::select! {
            biased;
            _ = abort.changed() => Err(LocalAuthorityError::Closed),
            result = admitted.shared.node.local_authoritative_membership(group, deadline) => {
                // Fencing racing a successful read cannot revive the request capability.
                if !admitted.shared.accepting.load(Ordering::Acquire) {
                    Err(LocalAuthorityError::Closed)
                } else {
                    result
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
