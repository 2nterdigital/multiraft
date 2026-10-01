//! Accepted point-read jobs stay owned when the caller abandons its waiter.
use super::*;
use crate::{ElectionSourceEvent, ElectionStatePoint};
use tokio::sync::oneshot;
impl<S: StateMachine> RuntimeHandle<S> {
    /// Serialized public native point read. This is local evidence, not authority.
    /// Cancellation abandons the waiter; the source job is retained and drained.
    pub async fn sample_election_state(
        &self,
        group: GroupId,
        deadline: Instant,
    ) -> Result<ElectionStatePoint, RuntimeError> {
        let admitted = self.admit(deadline).await?;
        admitted.ensure_group(group)?;
        let source =
            admitted
                .shared
                .node
                .election_source
                .clone()
                .ok_or(RuntimeError::InvalidConfig(
                    "election source is not enabled",
                ))?;
        let shared = admitted.shared.clone();
        let attempt = source.begin(group, ElectionSourceEvent::StateRequested);
        let (reply, receiver) = oneshot::channel();
        let registered = shared.source_jobs.spawn(async move {
            let result = admitted
                .run(
                    deadline,
                    RuntimePhase::LocalStatus,
                    false,
                    admitted.shared.node.election_state(group),
                )
                .await;
            attempt.finish(match &result {
                Ok(point) => ElectionSourceEvent::StatePoint {
                    point: Box::new(point.clone()),
                },
                Err(_) => ElectionSourceEvent::StateRequestFailed,
            });
            let _ = reply.send(result);
        });
        if !registered {
            return Err(RuntimeError::Closed);
        }
        tokio::time::timeout_at(deadline, receiver)
            .await
            .map_err(|_| RuntimeError::Deadline {
                phase: RuntimePhase::LocalStatus,
                outcome_unknown: false,
            })?
            .map_err(|_| RuntimeError::Closed)?
    }
}
