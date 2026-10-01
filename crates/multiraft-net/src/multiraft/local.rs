//! Non-authoritative local facts from one native full-metrics point sample.
use super::*;
use crate::group_observation::{normalize_log_id, project_server_state};
use crate::{GroupServerState, ObservedLogId};

/// One local Group point sample. It can be stale immediately and provides no
/// authority or cross-Group atomicity. No query, ReadIndex or network work runs.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalGroupStatus {
    pub group_id: GroupId,
    pub local_node_id: NodeId,
    pub leader_hint: Option<NodeId>,
    pub current_term: u64,
    pub server_state: GroupServerState,
    pub running: bool,
    pub last_log_index: Option<u64>,
    pub last_applied: Option<ObservedLogId>,
}
impl<S: StateMachine> MultiRaft<S> {
    pub(crate) fn local_group_status(
        &self,
        group: GroupId,
    ) -> Result<LocalGroupStatus, MultiRaftError> {
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        let receiver = raft.metrics();
        // Borrow a single source sample and copy only fixed-size facts. Avoid
        // cloning membership/replication collections or scanning all Groups.
        let metrics = receiver.borrow_watched();
        if metrics.id != self.node_id {
            return Err(MultiRaftError::ObservationIdentityMismatch {
                group_id: group,
                expected: self.node_id,
                observed: metrics.id,
            });
        }
        Ok(LocalGroupStatus {
            group_id: group,
            local_node_id: metrics.id,
            leader_hint: metrics.current_leader,
            current_term: metrics.current_term,
            server_state: project_server_state(metrics.state),
            running: metrics.running_state.is_ok(),
            last_log_index: metrics.last_log_index,
            last_applied: metrics.last_applied.as_ref().map(normalize_log_id),
        })
    }
}
