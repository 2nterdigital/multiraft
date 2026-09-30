//! Notification-driven, source-bearing routing hints. No ReadIndex is performed here.
use multiraft_core::{
    GroupId, MultiRaftError, NativeFailure, NodeId, ObservationClosed, TypeConfig,
};
use openraft::async_runtime::WatchReceiver as _;
use openraft::type_config::alias::WatchReceiverOf;
use openraft::RaftMetrics;
use tokio::sync::watch;
use tokio::time::{timeout_at, Instant};

#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HintSource {
    LocalRaft,
}
/// Latest local hint; its sampling timestamp is not an authority/freshness lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaderHint {
    pub group_id: GroupId,
    pub local_node_id: NodeId,
    pub candidate: Option<NodeId>,
    pub source: HintSource,
    pub observed_at: Instant,
}
/// Receiver retains only watch subscriptions; it cannot extend node or Raft lifetime.
pub struct LeaderHintReceiver {
    group: GroupId,
    raw: WatchReceiverOf<TypeConfig, RaftMetrics<TypeConfig>>,
    closed: Option<watch::Receiver<bool>>,
}
impl LeaderHintReceiver {
    pub(crate) fn new(
        group: GroupId,
        raw: WatchReceiverOf<TypeConfig, RaftMetrics<TypeConfig>>,
        closed: Option<watch::Receiver<bool>>,
    ) -> Self {
        Self { group, raw, closed }
    }
    /// Inspect the latest local source state, without proving authority.
    pub fn latest(&mut self) -> Result<LeaderHint, MultiRaftError> {
        if self.closed.as_ref().is_some_and(|c| *c.borrow()) {
            return Err(ObservationClosed::new(self.group).into());
        }
        let metrics = self.raw.borrow_and_update();
        if let Err(fatal) = &metrics.running_state {
            return Err(match fatal {
                openraft::error::Fatal::Stopped => ObservationClosed::new(self.group).into(),
                openraft::error::Fatal::Panicked => MultiRaftError::ObservationFailed {
                    group_id: self.group,
                    source: NativeFailure::Panicked,
                },
                openraft::error::Fatal::StorageError(_) => MultiRaftError::ObservationFailed {
                    group_id: self.group,
                    source: NativeFailure::Storage,
                },
            });
        }
        Ok(LeaderHint {
            group_id: self.group,
            local_node_id: metrics.id,
            candidate: metrics.current_leader,
            source: HintSource::LocalRaft,
            observed_at: Instant::now(),
        })
    }
    /// Wait for a latest-value source change. Intermediate updates may coalesce.
    pub async fn changed(&mut self) -> Result<LeaderHint, MultiRaftError> {
        if self.closed.as_ref().is_some_and(|c| *c.borrow()) {
            return Err(ObservationClosed::new(self.group).into());
        }
        tokio::select! {
            biased;
            _ = wait_closed(&mut self.closed) => return Err(ObservationClosed::new(self.group).into()),
            result = self.raw.changed() => result.map_err(|_| MultiRaftError::from(ObservationClosed::new(self.group)))?,
        }
        self.latest()
    }
    /// Wait until a hint exists and differs from `excluded`, within the original deadline.
    /// Expiry returns None. Hint change never refreshes the budget and never authorizes work.
    pub async fn wait_until(
        &mut self,
        deadline: Instant,
        excluded: Option<NodeId>,
    ) -> Result<Option<LeaderHint>, MultiRaftError> {
        loop {
            if Instant::now() >= deadline {
                return Ok(None);
            }
            let current = self.latest()?;
            if current.candidate.is_some() && current.candidate != excluded {
                return Ok(Some(current));
            }
            match timeout_at(deadline, self.changed()).await {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => return Err(error),
                Err(_) => return Ok(None),
            }
        }
    }
}
async fn wait_closed(closed: &mut Option<watch::Receiver<bool>>) {
    let Some(closed) = closed else {
        std::future::pending::<()>().await;
        return;
    };
    if *closed.borrow_and_update() {
        return;
    }
    let _ = closed.changed().await;
}

#[cfg(test)]
mod tests;
