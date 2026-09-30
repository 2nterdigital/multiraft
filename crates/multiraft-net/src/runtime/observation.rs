//! Weak observation admission is released before awaiting any source notification.
use super::*;
use crate::{GroupObservation, GroupObservationReceiver, LeaderHint, LeaderHintReceiver};

impl<S: StateMachine> RuntimeHandle<S> {
    /// Subscribe before inspection. The initial sample is local and may immediately be stale.
    pub async fn observe_leader_hint(
        &self,
        group: GroupId,
        deadline: Instant,
    ) -> Result<(LeaderHint, LeaderHintReceiver), RuntimeError> {
        let admitted = self.admit(deadline).await?;
        admitted.ensure_group(group)?;
        let mut receiver = admitted
            .shared
            .node
            .leader_hint_receiver(group, Some(admitted.shared.closed.subscribe()))?;
        let initial = receiver.latest()?;
        Ok((initial, receiver))
    }
    /// Latest source-bearing hint. Destination still must validate its own authority.
    pub async fn leader_hint(
        &self,
        group: GroupId,
        deadline: Instant,
    ) -> Result<LeaderHint, RuntimeError> {
        self.observe_leader_hint(group, deadline)
            .await
            .map(|(hint, _)| hint)
    }
    /// Wait for a hint using an absolute deadline. Expiry is None, shutdown/source errors typed.
    pub async fn wait_leader_hint(
        &self,
        group: GroupId,
        deadline: Instant,
    ) -> Result<Option<LeaderHint>, RuntimeError> {
        self.wait_hint(group, None, deadline).await
    }
    /// An unchanged previous candidate counts as absent; budget is never renewed.
    pub async fn wait_leader_hint_other_than(
        &self,
        group: GroupId,
        previous: NodeId,
        deadline: Instant,
    ) -> Result<Option<LeaderHint>, RuntimeError> {
        self.wait_hint(group, Some(previous), deadline).await
    }
    async fn wait_hint(
        &self,
        group: GroupId,
        excluded: Option<NodeId>,
        deadline: Instant,
    ) -> Result<Option<LeaderHint>, RuntimeError> {
        if Instant::now() >= deadline {
            let shared = self.shared.upgrade().ok_or(RuntimeError::Closed)?;
            if !shared.accepting.load(Ordering::Acquire) {
                return Err(RuntimeError::Closed);
            }
            if !shared.ready.lock().unwrap().contains(&group) {
                return Err(MultiRaftError::UnknownGroup(group).into());
            }
            return Ok(None);
        }
        // No strong runtime reference or admitted operation survives this acquisition block.
        let mut receiver = {
            let admitted = self.admit(deadline).await?;
            admitted.ensure_group(group)?;
            admitted
                .shared
                .node
                .leader_hint_receiver(group, Some(admitted.shared.closed.subscribe()))?
        };
        let hint = receiver.wait_until(deadline, excluded).await?;
        // Shutdown racing a final native update must not revive fenced handles.
        let shared = self.shared.upgrade().ok_or(RuntimeError::Closed)?;
        if !shared.accepting.load(Ordering::Acquire) {
            return Err(RuntimeError::Closed);
        }
        Ok(if Instant::now() >= deadline {
            None
        } else {
            hint
        })
    }
    /// Read-only local Group observation, with receiver lifetime independent from the owner.
    pub async fn observe_group(
        &self,
        group: GroupId,
        deadline: Instant,
    ) -> Result<(GroupObservation, GroupObservationReceiver), RuntimeError> {
        let admitted = self.admit(deadline).await?;
        admitted.ensure_group(group)?;
        Ok(admitted.shared.node.observe_group(group)?)
    }
}
