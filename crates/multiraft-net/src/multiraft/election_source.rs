//! Native point read through the registered Group, not a consumer-labelled Raft.
use super::*;
impl<S: StateMachine> MultiRaft<S> {
    pub(crate) async fn election_state(
        &self,
        group: GroupId,
    ) -> Result<crate::ElectionStatePoint, MultiRaftError> {
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        let source = self.election_source.clone().ok_or_else(|| {
            MultiRaftError::Other(anyhow::anyhow!("election source is not enabled"))
        })?;
        crate::election_source::read_state(&raft, source).await
    }
}
