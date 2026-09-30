//! Consumer validation of the native-recovered local application image.
use super::*;

impl<S: StateMachine> MultiRaft<S> {
    pub(crate) async fn validate_recovered(&self, group: GroupId) -> Result<(), MultiRaftError> {
        let context = FsmFactoryContext {
            node_id: self.node_id,
            group_id: group,
        };
        let result = self
            .with_fsm(group, |fsm| {
                // Convert consumer panic to a startup rejection while keeping the
                // native Group in the owner registry for cancellation-safe cleanup.
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    self.fsm_factory.validate_recovered(context, fsm)
                }))
                .unwrap_or_else(|_| Err(anyhow::anyhow!("recovery validation callback panicked")))
            })
            .await
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        result.map_err(|source| {
            tracing::warn!(target: "multiraft::recovery", operation = "recovery_validation",
                phase = "rejected", node_id = self.node_id, group_id = group,
                "consumer rejected recovered application image");
            MultiRaftError::Other(source.context(format!(
                "validate recovered FSM for node {}, group {}",
                self.node_id, group
            )))
        })
    }
}
