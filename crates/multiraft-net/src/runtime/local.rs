//! Weak local observation/query access: never a substitute for ReadIndex.
use super::*;
use crate::{LocalGroupStatus, TryReadError};

impl<S: StateMachine> RuntimeHandle<S> {
    /// Query this replica's applied FSM under its application lock, without
    /// confirming authority. It may be stale and must not authorize business work.
    /// Applications own query meaning; callbacks must be bounded and nonblocking.
    pub async fn try_read_applied<R, E>(
        &self,
        group: GroupId,
        deadline: Instant,
        query: impl FnOnce(&S) -> Result<R, E>,
    ) -> Result<R, TryReadError<E>> {
        let admitted = self.admit(deadline).await.map_err(TryReadError::Runtime)?;
        admitted
            .ensure_group(group)
            .map_err(TryReadError::Runtime)?;
        let read = async {
            admitted
                .shared
                .node
                .with_fsm(group, query)
                .await
                .ok_or(MultiRaftError::ReadIndex(
                    multiraft_core::ReadIndexFailure::FsmUnavailable,
                ))
        };
        admitted
            .run(deadline, RuntimePhase::LocalRead, false, read)
            .await
            .map_err(TryReadError::Runtime)?
            .map_err(TryReadError::Application)
    }

    /// Query this replica and capture its applied watermark in the same FSM view.
    /// No authority or freshness is implied. Callbacks must be bounded and
    /// nonblocking, and execute under the local application lock.
    pub async fn try_read_applied_with_watermark<R, E>(
        &self,
        group: GroupId,
        deadline: Instant,
        query: impl FnOnce(&S) -> Result<R, E>,
    ) -> Result<crate::AppliedView<R>, TryReadError<E>> {
        let admitted = self.admit(deadline).await.map_err(TryReadError::Runtime)?;
        admitted
            .ensure_group(group)
            .map_err(TryReadError::Runtime)?;
        let view = admitted
            .run(
                deadline,
                RuntimePhase::LocalRead,
                false,
                admitted
                    .shared
                    .node
                    .read_applied_with_watermark(group, query),
            )
            .await
            .map_err(TryReadError::Runtime)?;
        Ok(crate::AppliedView {
            value: view.value.map_err(TryReadError::Application)?,
            applied: view.applied,
        })
    }

    /// Copy one local full-metrics point. No ReadIndex, network work or scan of
    /// other Groups is performed. Holding the value retains no runtime owner.
    pub async fn local_group_status(
        &self,
        group: GroupId,
        deadline: Instant,
    ) -> Result<LocalGroupStatus, RuntimeError> {
        let admitted = self.admit(deadline).await?;
        admitted.ensure_group(group)?;
        admitted
            .run(deadline, RuntimePhase::LocalStatus, false, async {
                admitted.shared.node.local_group_status(group)
            })
            .await
    }
}
