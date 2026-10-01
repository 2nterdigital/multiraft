//! An in-memory bounded ring; receivers retain no Raft, FSM, listener or task.
use super::*;
use crate::RuntimeError;
use multiraft_core::TypeConfig;
use openraft::{type_config::alias::InstantOf, Instant as _};
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
};
use tokio::sync::{mpsc, Notify};

#[derive(Debug, Clone)]
pub struct ElectionSourceConfig {
    pub run_id: String,
    pub boot_id: String,
    pub capacity: usize,
}
struct Ring {
    node: Option<NodeId>,
    records: VecDeque<Arc<ElectionSourceRecord>>,
    native_rx: mpsc::Receiver<NativePacket>,
    sequence: u64,
    evicted: u64,
    next_attempt: u64,
    active_attempts: usize,
    receivers: usize,
    closed: bool,
}
type NativePacket = (
    GroupId,
    openraft::election_observer::ElectionEvent<TypeConfig>,
);

pub(crate) struct SourceHub {
    run_id: Arc<str>,
    boot_id: Arc<str>,
    capacity: usize,
    started: InstantOf<TypeConfig>,
    ring: Mutex<Ring>,
    notify: Notify,
    native_tx: mpsc::Sender<NativePacket>,
    native_received: AtomicU64,
    native_dropped: AtomicU64,
}

/// Create before starting the node, so construction failures remain observable.
/// A source can attach to exactly one owned node and cannot be reopened.
#[derive(Clone)]
pub struct ElectionSource {
    pub(crate) hub: Arc<SourceHub>,
}
impl ElectionSource {
    pub fn new(config: ElectionSourceConfig) -> Result<Self, RuntimeError> {
        let identity = |value: &str| {
            !value.is_empty()
                && value.len() <= 128
                && value
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b))
        };
        if !identity(&config.run_id)
            || !identity(&config.boot_id)
            || !(1..=8192).contains(&config.capacity)
        {
            return Err(RuntimeError::InvalidConfig(
                "source identities must be 1..=128 safe ASCII bytes and capacity 1..=8192",
            ));
        }
        let (native_tx, native_rx) = mpsc::channel(config.capacity);
        Ok(Self {
            hub: Arc::new(SourceHub {
                run_id: config.run_id.into(),
                boot_id: config.boot_id.into(),
                capacity: config.capacity,
                started: InstantOf::<TypeConfig>::now(),
                ring: Mutex::new(Ring {
                    node: None,
                    native_rx,
                    records: VecDeque::with_capacity(config.capacity),
                    sequence: 0,
                    evicted: 0,
                    next_attempt: 0,
                    active_attempts: 0,
                    receivers: 0,
                    closed: false,
                }),
                notify: Notify::new(),
                native_tx,
                native_received: AtomicU64::new(0),
                native_dropped: AtomicU64::new(0),
            }),
        })
    }
    /// Starts at the oldest retained record. Earlier eviction is reported as lag.
    pub fn subscribe(&self) -> ElectionSourceReceiver {
        self.hub.ring.lock().unwrap().receivers += 1;
        ElectionSourceReceiver {
            hub: self.hub.clone(),
            next: 1,
            native_dropped_seen: 0,
        }
    }
    pub fn status(&self) -> ElectionSourceStatus {
        let mut ring = self.hub.ring.lock().unwrap();
        self.hub.drain(&mut ring);
        ElectionSourceStatus {
            attached_node: ring.node,
            capacity: self.hub.capacity,
            retained: ring.records.len(),
            last_sequence: ring.sequence,
            evicted: ring.evicted,
            native_received: self.hub.native_received.load(Ordering::Acquire),
            native_dropped: self.hub.native_dropped.load(Ordering::Acquire),
            native_pending: ring.native_rx.len(),
            active_attempts: ring.active_attempts,
            receivers: ring.receivers,
            closed: ring.closed,
            capabilities: ElectionSourceCapabilities::default(),
        }
    }
}
#[must_use]
pub struct ElectionSourceReceiver {
    hub: Arc<SourceHub>,
    next: u64,
    native_dropped_seen: u64,
}
impl ElectionSourceReceiver {
    /// Cancellation-safe. No source task is spawned by subscribing or waiting.
    pub async fn recv(&mut self) -> ElectionSourceRead {
        let hub = self.hub.clone();
        loop {
            let notified = hub.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(value) = self.try_recv() {
                return value;
            }
            notified.await;
        }
    }
    pub fn try_recv(&mut self) -> Option<ElectionSourceRead> {
        let total = self.hub.native_dropped.load(Ordering::Acquire);
        if total != self.native_dropped_seen {
            let read = ElectionSourceRead::NativeDropped {
                total,
                new_since_previous: total - self.native_dropped_seen,
            };
            self.native_dropped_seen = total;
            return Some(read);
        }
        let mut ring = self.hub.ring.lock().unwrap();
        self.hub.drain(&mut ring);
        let oldest = ring
            .records
            .front()
            .map(|r| r.sequence)
            .unwrap_or(ring.sequence + 1);
        if self.next < oldest {
            let result = ElectionSourceRead::Lagged {
                first_missing: self.next,
                last_missing: oldest - 1,
            };
            self.next = oldest;
            return Some(result);
        }
        if let Some(record) = ring.records.get((self.next - oldest) as usize) {
            let record = record.clone();
            self.next += 1;
            drop(ring);
            return Some(ElectionSourceRead::Record(Box::new(
                record.as_ref().clone(),
            )));
        }
        ring.closed.then_some(ElectionSourceRead::Closed)
    }
}
impl Drop for ElectionSourceReceiver {
    fn drop(&mut self) {
        self.hub.ring.lock().unwrap().receivers -= 1;
    }
}
impl SourceHub {
    pub(crate) fn attach(self: &Arc<Self>, node: NodeId) -> Result<Attachment, RuntimeError> {
        {
            let mut ring = self.ring.lock().unwrap();
            if ring.node.is_some() || ring.closed {
                return Err(RuntimeError::InvalidConfig(
                    "election source already attached or closed",
                ));
            }
            ring.node = Some(node);
        }
        self.emit(
            None,
            None,
            ElectionSourceEvent::Attached {
                capabilities: ElectionSourceCapabilities::default(),
            },
        );
        Ok(Attachment {
            hub: self.clone(),
            transferred: false,
        })
    }
    pub(crate) fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }
    pub(crate) fn elapsed_at(&self, now: InstantOf<TypeConfig>) -> Duration {
        now.saturating_duration_since(self.started)
    }
    pub(crate) fn emit(
        &self,
        group: Option<GroupId>,
        attempt: Option<u64>,
        event: ElectionSourceEvent,
    ) {
        let mut ring = self.ring.lock().unwrap();
        self.drain(&mut ring);
        if ring.closed {
            return;
        }
        let Some(node) = ring.node else {
            return;
        };
        ring.sequence += 1;
        let record = ElectionSourceRecord {
            run_id: self.run_id.clone(),
            boot_id: self.boot_id.clone(),
            local_node_id: node,
            group_id: group,
            sequence: ring.sequence,
            local_elapsed: self.elapsed(),
            attempt_id: attempt,
            native_round: SourceFact::Unknown(SourceUnknown::NotObserved),
            event,
        };
        if ring.records.len() == self.capacity {
            ring.records.pop_front();
            ring.evicted += 1;
        }
        ring.records.push_back(Arc::new(record));
        drop(ring);
        self.notify.notify_waiters();
    }
    /// A native callback never takes the ring lock, projects, serializes or waits.
    pub(crate) fn submit_native(
        &self,
        group: GroupId,
        event: openraft::election_observer::ElectionEvent<TypeConfig>,
    ) {
        self.native_received.fetch_add(1, Ordering::AcqRel);
        if self.native_tx.try_send((group, event)).is_err() {
            self.native_dropped.fetch_add(1, Ordering::AcqRel);
        }
        self.notify.notify_waiters();
    }
    // The ordinary ring owner drains at most capacity packets per call. No task
    // or native resource is needed; the queue and ring are independently bounded.
    fn drain(&self, ring: &mut Ring) {
        for _ in 0..self.capacity {
            let Ok((group, raw)) = ring.native_rx.try_recv() else {
                break;
            };
            let (round, event) = super::native::project(raw, self);
            let Some(node) = ring.node.filter(|_| !ring.closed) else {
                continue;
            };
            ring.sequence += 1;
            let record = ElectionSourceRecord {
                run_id: self.run_id.clone(),
                boot_id: self.boot_id.clone(),
                local_node_id: node,
                group_id: Some(group),
                sequence: ring.sequence,
                local_elapsed: self.elapsed(),
                attempt_id: None,
                native_round: round
                    .map(SourceFact::Known)
                    .unwrap_or(SourceFact::Unknown(SourceUnknown::NotObserved)),
                event: ElectionSourceEvent::Native { event },
            };
            if ring.records.len() == self.capacity {
                ring.records.pop_front();
                ring.evicted += 1;
            }
            ring.records.push_back(Arc::new(record));
        }
    }
    pub(crate) fn begin(self: &Arc<Self>, group: GroupId, event: ElectionSourceEvent) -> Attempt {
        let id = {
            let mut ring = self.ring.lock().unwrap();
            ring.next_attempt += 1;
            ring.active_attempts += 1;
            ring.next_attempt
        };
        self.emit(Some(group), Some(id), event);
        Attempt {
            hub: self.clone(),
            group,
            id,
            finished: false,
        }
    }
    pub(crate) fn close(&self) {
        {
            let mut ring = self.ring.lock().unwrap();
            ring.native_rx.close();
            self.drain(&mut ring);
        }
        self.emit(None, None, ElectionSourceEvent::Closed);
        self.ring.lock().unwrap().closed = true;
        self.notify.notify_waiters();
    }
}
pub(crate) struct Attempt {
    hub: Arc<SourceHub>,
    group: GroupId,
    id: u64,
    finished: bool,
}
impl Attempt {
    pub(crate) fn finish(mut self, event: ElectionSourceEvent) {
        self.hub.emit(Some(self.group), Some(self.id), event);
        self.finished = true;
    }
}
impl Drop for Attempt {
    fn drop(&mut self) {
        if !self.finished {
            self.hub.emit(
                Some(self.group),
                Some(self.id),
                ElectionSourceEvent::AttemptCancelled,
            );
        }
        self.hub.ring.lock().unwrap().active_attempts -= 1;
    }
}

// Armed only across transport construction. There is no native Group yet;
// cancellation here must still seal the source's startup coverage.
pub(crate) struct Attachment {
    hub: Arc<SourceHub>,
    transferred: bool,
}
impl Attachment {
    pub(crate) fn transfer(&mut self) {
        self.transferred = true;
    }
}
impl Drop for Attachment {
    fn drop(&mut self) {
        if !self.transferred {
            self.hub.close();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::election_observer::{ElectionEvent, ElectionEventKind, ElectionTiming};
    fn event() -> ElectionEvent<TypeConfig> {
        let at = InstantOf::<TypeConfig>::now();
        ElectionEvent {
            at,
            local_vote: openraft::Vote::new(1, 1),
            kind: ElectionEventKind::Started {
                timing: ElectionTiming {
                    last_vote_update: Some(at),
                    lease: Duration::from_millis(600),
                    lease_enabled: true,
                    election_timeout: Duration::from_millis(400),
                    seen_greater_log: false,
                    greater_log_delay: Duration::from_millis(1200),
                },
            },
        }
    }
    fn source(capacity: usize) -> ElectionSource {
        let source = ElectionSource::new(ElectionSourceConfig {
            run_id: "run".into(),
            boot_id: "boot".into(),
            capacity,
        })
        .unwrap();
        source.hub.attach(1).unwrap().transfer();
        source
    }
    #[test]
    fn held_ring_cannot_block_or_drop_native_ingress_and_source_time_survives_deferred_projection()
    {
        let source = source(3);
        let raw = event();
        let at = raw.at;
        let busy = source.hub.ring.lock().unwrap();
        let worker_source = source.clone();
        let (done, completion) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            worker_source.hub.submit_native(7, raw);
            let _ = done.send(());
        });
        let result = completion.recv_timeout(Duration::from_secs(2));
        drop(busy);
        worker.join().unwrap();
        assert!(result.is_ok(), "native callback waited for ring owner");
        let status = source.status();
        assert_eq!(status.native_received, 1);
        assert_eq!(status.native_dropped, 0);
        assert_eq!(status.native_pending, 0);
        let mut rx = source.subscribe();
        assert!(matches!(rx.try_recv(), Some(ElectionSourceRead::Record(_))));
        let Some(ElectionSourceRead::Record(record)) = rx.try_recv() else {
            panic!("native fact")
        };
        let ElectionSourceEvent::Native { event } = record.event else {
            panic!("native fact")
        };
        assert_eq!(event.source_elapsed, source.hub.elapsed_at(at));
        source.hub.close();
        assert!(source.status().closed);
    }
    #[test]
    fn full_ingress_is_explicit_loss_for_each_receiver_and_close_drains_accepted_packets() {
        let source = source(2);
        let mut first = source.subscribe();
        let mut second = source.subscribe();
        source.hub.submit_native(7, event());
        source.hub.submit_native(7, event());
        source.hub.submit_native(7, event());
        let expected = Some(ElectionSourceRead::NativeDropped {
            total: 1,
            new_since_previous: 1,
        });
        assert_eq!(first.try_recv(), expected);
        assert_eq!(second.try_recv(), expected);
        source.hub.close();
        let status = source.status();
        assert!(status.closed);
        assert_eq!(status.native_pending, 0);
        assert_eq!(status.native_received, 3);
        assert_eq!(status.native_dropped, 1);
        assert!(matches!(
            first.try_recv(),
            Some(ElectionSourceRead::Lagged { .. })
        ));
        while matches!(first.try_recv(), Some(ElectionSourceRead::Record(_))) {}
        assert_eq!(first.try_recv(), Some(ElectionSourceRead::Closed));
    }
}
