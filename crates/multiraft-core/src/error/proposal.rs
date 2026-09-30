//! Single native write failures retain bounded facts and their opaque source chain.
use super::NativeFailure;
use thiserror::Error;

#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ProposalFailure {
    #[error("native proposal owner is closed")]
    Closed,
    #[error(transparent)]
    Backend(NativeFailure),
    #[error("native membership request was rejected")]
    MembershipRejected,
}

/// A native write error supplies no committed/applied receipt. The caller must
/// preserve outcome uncertainty and cannot use it as permission to resubmit.
/// Display is bounded; original native/application text stays in the source chain.
#[derive(Error)]
#[error("proposal failed: {failure}; outcome_unknown=true")]
pub struct ProposalError {
    pub failure: ProposalFailure,
    #[source]
    source: anyhow::Error,
}
impl ProposalError {
    pub fn new(failure: ProposalFailure, source: anyhow::Error) -> Self {
        Self { failure, source }
    }
    pub const fn outcome_unknown(&self) -> bool {
        true
    }
}

impl std::fmt::Debug for ProposalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProposalError")
            .field("failure", &self.failure)
            .field("outcome_unknown", &true)
            .finish_non_exhaustive()
    }
}
