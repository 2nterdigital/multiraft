//! Per-caller source observations; the shared round contains no application context.
use multiraft_core::{GroupId, MultiRaftError, NodeId, ReadIndexFailure};
use std::time::Duration;
use tokio::time::Instant;

#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadStage {
    Admission,
    ReadIndex,
    StateMachine,
}
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadOutcome {
    Completed,
    NotLeader,
    QuorumUnavailable,
    Backend,
    RoundTimeout,
    Abandoned,
    ApplicationError,
    UnknownGroup,
    Closed,
    Deadline,
    Cancelled,
    Busy,
}
/// An observation, never an authority token. Evidence is bounded and contains no query data.
#[derive(Debug, Clone)]
pub struct ReadEvent {
    pub group_id: GroupId,
    pub local_node_id: NodeId,
    pub stage: ReadStage,
    pub outcome: ReadOutcome,
    pub elapsed: Duration,
    pub remaining: Duration,
    pub leader_hint: Option<NodeId>,
    pub source: Option<ReadIndexFailure>,
}
/// A synchronous bounded callback. Panic is isolated from application results.
pub trait ReadObserver: Send + Sync {
    fn observe(&self, event: ReadEvent);
}
impl<F: Fn(ReadEvent) + Send + Sync> ReadObserver for F {
    fn observe(&self, event: ReadEvent) {
        self(event);
    }
}
/// Application query rejection is separate from HA failure, preserving the original E.
#[derive(Debug)]
pub enum TryReadError<E> {
    Runtime(crate::RuntimeError),
    Application(E),
}
impl<E: std::fmt::Display> std::fmt::Display for TryReadError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Runtime(e) => e.fmt(f),
            Self::Application(e) => e.fmt(f),
        }
    }
}
impl<E: std::error::Error + 'static> std::error::Error for TryReadError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Runtime(e) => Some(e),
            Self::Application(e) => Some(e),
        }
    }
}

pub(crate) struct ReadGuard<'a> {
    group: GroupId,
    node: NodeId,
    stage: ReadStage,
    deadline: Instant,
    started: Instant,
    observer: Option<&'a dyn ReadObserver>,
    closed: Option<tokio::sync::watch::Receiver<bool>>,
    finished: bool,
}
impl<'a> ReadGuard<'a> {
    pub(crate) fn new(
        group: GroupId,
        node: NodeId,
        stage: ReadStage,
        deadline: Instant,
        observer: Option<&'a dyn ReadObserver>,
        closed: Option<tokio::sync::watch::Receiver<bool>>,
    ) -> Self {
        Self {
            group,
            node,
            stage,
            deadline,
            started: Instant::now(),
            observer,
            closed,
            finished: false,
        }
    }
    pub(crate) fn finish_source<R>(&mut self, result: &Result<R, MultiRaftError>) {
        match result {
            Ok(_) => self.finish(ReadOutcome::Completed, None, None),
            Err(MultiRaftError::NotLeader { hint }) => {
                self.finish(ReadOutcome::NotLeader, None, *hint)
            }
            Err(MultiRaftError::UnknownGroup(_)) => {
                self.finish(ReadOutcome::UnknownGroup, None, None)
            }
            Err(MultiRaftError::ReadIndex(error)) => self.finish(
                match error {
                    ReadIndexFailure::QuorumUnavailable { .. } => ReadOutcome::QuorumUnavailable,
                    ReadIndexFailure::Backend(_) | ReadIndexFailure::FsmUnavailable => {
                        ReadOutcome::Backend
                    }
                    ReadIndexFailure::RoundTimeout => ReadOutcome::RoundTimeout,
                    ReadIndexFailure::Abandoned => ReadOutcome::Abandoned,
                    ReadIndexFailure::Closed => ReadOutcome::Closed,
                    ReadIndexFailure::Deadline => ReadOutcome::Deadline,
                    _ => ReadOutcome::Backend,
                },
                Some(error.clone()),
                None,
            ),
            Err(_) => self.finish(ReadOutcome::Backend, None, None),
        }
    }
    pub(crate) fn finish(
        &mut self,
        outcome: ReadOutcome,
        source: Option<ReadIndexFailure>,
        hint: Option<NodeId>,
    ) {
        self.finished = true;
        let Some(observer) = self.observer else {
            return;
        };
        let event = ReadEvent {
            group_id: self.group,
            local_node_id: self.node,
            stage: self.stage,
            outcome,
            elapsed: self.started.elapsed(),
            remaining: self.deadline.saturating_duration_since(Instant::now()),
            source,
            leader_hint: hint,
        };
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| observer.observe(event)));
    }
}
impl Drop for ReadGuard<'_> {
    fn drop(&mut self) {
        if !self.finished {
            let outcome = if self.closed.as_ref().is_some_and(|c| *c.borrow()) {
                ReadOutcome::Closed
            } else if Instant::now() >= self.deadline {
                ReadOutcome::Deadline
            } else {
                ReadOutcome::Cancelled
            };
            self.finish(outcome, None, None);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    #[tokio::test(start_paused = true)]
    async fn caller_deadline_cancellation_and_owner_close_remain_distinct() {
        let events = Mutex::new(Vec::<ReadEvent>::new());
        let observer = |e| events.lock().unwrap().push(e);
        let deadline = Instant::now() + Duration::from_secs(1);
        let (closed, closing) = tokio::sync::watch::channel(false);
        let mut cancelled = Box::pin(async {
            let _guard = ReadGuard::new(
                7,
                101,
                ReadStage::ReadIndex,
                deadline,
                Some(&observer),
                Some(closing.clone()),
            );
            std::future::pending::<()>().await;
        });
        assert!(futures::poll!(&mut cancelled).is_pending());
        drop(cancelled);
        let mut interrupted = Box::pin(async {
            let _guard = ReadGuard::new(
                7,
                101,
                ReadStage::ReadIndex,
                deadline,
                Some(&observer),
                Some(closing),
            );
            std::future::pending::<()>().await;
        });
        assert!(futures::poll!(&mut interrupted).is_pending());
        closed.send_replace(true);
        drop(interrupted);
        let expired = tokio::time::timeout_at(deadline, async {
            let _guard = ReadGuard::new(
                7,
                101,
                ReadStage::ReadIndex,
                deadline,
                Some(&observer),
                None,
            );
            std::future::pending::<()>().await;
        })
        .await;
        assert!(expired.is_err());
        let events = events.lock().unwrap();
        assert_eq!(
            events.iter().map(|e| e.outcome).collect::<Vec<_>>(),
            [
                ReadOutcome::Cancelled,
                ReadOutcome::Closed,
                ReadOutcome::Deadline
            ]
        );
        assert!(events
            .iter()
            .all(|e| e.group_id == 7 && e.local_node_id == 101 && e.stage == ReadStage::ReadIndex));
        assert_eq!(events[2].remaining, Duration::ZERO);
    }
}
