//! Business-neutral native startup/recovery facts with an opaque source chain.
use super::*;

#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryStage {
    Construct,
    Await,
}

#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum RecoveryFailure {
    #[error("recovery deadline expired")]
    Deadline,
    #[error("native recovery owner is closed")]
    Closed,
    #[error(transparent)]
    Backend(NativeFailure),
}

/// The display contains bounded facts only. Native/source errors remain available
/// through std::error::Error::source and are never formatted into recovery logs.
#[derive(Debug, Error)]
#[error("group {group_id} recovery failed during {stage:?}: {failure}")]
pub struct RecoveryError {
    pub group_id: GroupId,
    pub stage: RecoveryStage,
    pub failure: RecoveryFailure,
    #[source]
    source: Option<anyhow::Error>,
}
impl RecoveryError {
    pub fn new(
        group_id: GroupId,
        stage: RecoveryStage,
        failure: RecoveryFailure,
        source: Option<anyhow::Error>,
    ) -> Self {
        Self {
            group_id,
            stage,
            failure,
            source,
        }
    }
}
