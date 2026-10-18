//! Independent application factory with owned file leases and bounded gates.
#![allow(dead_code)]
use futures::{stream::FuturesUnordered, StreamExt};
use multiraft_core::{ClusterConfig, FileLogSyncLevel, MultiRaftError};
use multiraft_fsm::{ApplyOut, GroupId, StateMachine};
use multiraft_net::{
    FsmFactoryContext, GroupConfig, InitializePolicy, NodeOwner, RuntimeConfig, RuntimeError,
    RuntimeHandle, StartupBatch, StartupGroup, StateMachineFactory,
};
use std::{
    fs::{self, OpenOptions},
    future::Future,
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};
use tokio::{
    sync::Notify,
    time::{timeout_at, Instant},
};
pub const GRACE: Duration = Duration::from_millis(100);
pub fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
pub fn addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}
#[derive(Clone, Default)]
pub struct Gate {
    pub entered: Arc<Notify>,
    state: Arc<(Mutex<bool>, Condvar)>,
}
impl Gate {
    pub fn block(&self) {
        self.entered.notify_one();
        let (lock, signal) = &*self.state;
        let (released, _) = signal
            .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(4), |v| !*v)
            .unwrap();
        assert!(*released, "bounded consumer gate not released");
    }
    pub fn release(&self) {
        *self.state.0.lock().unwrap() = true;
        self.state.1.notify_all();
    }
}
pub struct Release(pub Vec<Gate>);
impl Drop for Release {
    fn drop(&mut self) {
        for g in &self.0 {
            g.release();
        }
    }
}
pub struct Bytes {
    pub value: Vec<u8>,
    lease: PathBuf,
    restore_gate: Option<Gate>,
}
impl Drop for Bytes {
    fn drop(&mut self) {
        fs::remove_file(&self.lease).unwrap();
    }
}
impl StateMachine for Bytes {
    type Error = std::io::Error;
    fn apply(&mut self, _: GroupId, _: u64, bytes: &[u8]) -> Result<ApplyOut, Self::Error> {
        self.value = bytes.to_vec();
        Ok(ApplyOut {
            effects: bytes.to_vec(),
        })
    }
    fn snapshot(&self, _: GroupId) -> Result<Vec<u8>, Self::Error> {
        Ok(self.value.clone())
    }
    fn freeze_bounded(
        &self,
        _: GroupId,
        max_bytes: usize,
    ) -> Result<Vec<u8>, multiraft_fsm::CaptureError<Self::Error>> {
        if self.value.len() > max_bytes {
            return Err(multiraft_fsm::CaptureError::Refused(
                multiraft_fsm::CaptureRefusal::SizeLimit,
            ));
        }
        Ok(self.value.clone())
    }
    fn restore(&mut self, _: GroupId, data: &[u8]) -> Result<(), Self::Error> {
        if let Some(gate) = &self.restore_gate {
            gate.block();
        }
        self.value = data.to_vec();
        Ok(())
    }
}
#[derive(Clone)]
pub struct Factory {
    pub root: PathBuf,
    pub calls: Arc<Mutex<Vec<GroupId>>>,
    pub construct: Option<(GroupId, Gate)>,
    pub validate: Option<(GroupId, Gate)>,
    pub reject: Option<GroupId>,
    pub panic: Option<GroupId>,
    pub reject_validation: Option<GroupId>,
    /// Blocks the first snapshot restore of this Group until released.
    pub restore: Option<(GroupId, Gate)>,
}
impl Factory {
    pub fn new(root: &Path) -> Self {
        Self {
            root: root.to_owned(),
            calls: Default::default(),
            construct: None,
            validate: None,
            reject: None,
            panic: None,
            reject_validation: None,
            restore: None,
        }
    }
}
impl StateMachineFactory<Bytes> for Factory {
    fn create(&self, context: FsmFactoryContext) -> anyhow::Result<Bytes> {
        self.calls.lock().unwrap().push(context.group_id());
        let lease = self
            .root
            .join(format!("consumer-{}.lease", context.group_id()));
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&lease)?;
        let fsm = Bytes {
            value: vec![],
            lease,
            restore_gate: self
                .restore
                .as_ref()
                .filter(|(id, _)| *id == context.group_id())
                .map(|(_, gate)| gate.clone()),
        };
        if let Some((id, gate)) = &self.construct {
            if *id == context.group_id() {
                gate.block();
            }
        }
        anyhow::ensure!(
            self.reject != Some(context.group_id()),
            "original constructor failure"
        );
        assert_ne!(
            self.panic,
            Some(context.group_id()),
            "original constructor panic"
        );
        Ok(fsm)
    }
    fn validate_recovered(&self, context: FsmFactoryContext, _: &Bytes) -> anyhow::Result<()> {
        if let Some((id, gate)) = &self.validate {
            if *id == context.group_id() {
                gate.block();
            }
        }
        anyhow::ensure!(
            self.reject_validation != Some(context.group_id()),
            "original validator failure"
        );
        Ok(())
    }
}
pub fn batch(
    groups: &[u64],
    voters: &[u64],
    preferred: Option<u64>,
    budget: Duration,
) -> StartupBatch {
    batch_with_policy(groups, voters, preferred, budget, InitializePolicy::Allowed)
}
pub fn batch_with_policy(
    groups: &[u64],
    voters: &[u64],
    preferred: Option<u64>,
    budget: Duration,
    initialize: InitializePolicy,
) -> StartupBatch {
    StartupBatch {
        input_digest: Some([7; 32]),
        groups: groups
            .iter()
            .map(|id| StartupGroup {
                group: GroupConfig {
                    group_id: *id,
                    voters: voters.to_vec(),
                },
                preferred_initializer: preferred,
                initialize,
            })
            .collect(),
        grace: GRACE,
        recovery_timeout: budget,
    }
}
pub async fn start(
    root: &Path,
    id: u64,
    peers: &[(u64, SocketAddr)],
    factory: Factory,
    durable: bool,
) -> NodeOwner<Bytes> {
    let ids: Vec<_> = peers.iter().map(|(id, _)| *id).collect();
    let mut config = ClusterConfig::for_test(id, &ids);
    config.peers = peers.to_vec();
    if durable {
        config.data_dir = root.to_owned();
        config.file_log_sync_level = FileLogSyncLevel::Data;
    }
    NodeOwner::start(RuntimeConfig::new(config, vec![]), factory, deadline())
        .await
        .unwrap()
}
pub async fn start_native(
    root: &Path,
    id: u64,
    peers: &[(u64, SocketAddr)],
    factory: Factory,
) -> NodeOwner<Bytes> {
    let ids: Vec<_> = peers.iter().map(|(id, _)| *id).collect();
    let mut config = ClusterConfig::for_test(id, &ids);
    config.peers = peers.to_vec();
    config.data_dir = root.to_owned();
    config.file_log_sync_level = FileLogSyncLevel::Data;
    config.snapshot_mode = multiraft_core::SnapshotMode::NativeDurable;
    config.retain_log_entries = 0;
    NodeOwner::start(RuntimeConfig::new(config, vec![]), factory, deadline())
        .await
        .unwrap()
}
pub fn reusable(root: &Path, peers: &[(u64, SocketAddr)]) {
    for entry in fs::read_dir(root).unwrap() {
        assert!(!entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".lease"));
    }
    for (_, address) in peers {
        TcpListener::bind(address).unwrap();
    }
}
/// OpenRaft's normal transient outcomes while a leader is elected or changes:
/// no leader yet or a stale one (`NotLeader`), a ReadIndex that could not reach
/// a quorum in one heartbeat interval or did not finish in time, and a request
/// whose deadline passed. Tests retry these within their own budget; production
/// code never does. For writes `Deadline` and `NotLeader` leave the outcome
/// unknown, so only idempotent test writes (the set-value [`Bytes`]) may retry.
pub fn retryable(error: &RuntimeError) -> bool {
    use multiraft_core::ReadIndexFailure;
    matches!(
        error,
        RuntimeError::Source(MultiRaftError::NotLeader { .. })
            | RuntimeError::Source(MultiRaftError::ReadIndex(
                ReadIndexFailure::QuorumUnavailable { .. } | ReadIndexFailure::Deadline
            ))
            | RuntimeError::Deadline { .. }
    )
}

pub async fn leader(handles: &[RuntimeHandle<Bytes>], group: u64) -> &RuntimeHandle<Bytes> {
    let nodes: Vec<_> = handles.iter().map(RuntimeHandle::node_id).collect();
    let index = confirmed_leader(&nodes, group, |index, until| {
        handles[index].read_linearizable(group, until, |_| ())
    })
    .await;
    &handles[index]
}

async fn confirmed_leader<F, Probe>(nodes: &[u64], group: u64, probe: F) -> usize
where
    F: Fn(usize, Instant) -> Probe,
    Probe: Future<Output = Result<(), RuntimeError>>,
{
    let until = deadline();
    let outcomes: Vec<_> = nodes
        .iter()
        .map(|_| Mutex::new(String::from("not probed")))
        .collect();
    let result = timeout_at(until, async {
        // Each node retries independently: a stalled follower must not conceal
        // either a completed ReadIndex or a later election on another node.
        let mut pending = FuturesUnordered::new();
        for (index, outcome) in outcomes.iter().enumerate() {
            let probe = &probe;
            pending.push(async move {
                let mut attempts = 0_u32;
                loop {
                    // Never start a probe at or after the shared deadline: its
                    // admission would fail with `Deadline` and race the outer
                    // timeout. Park instead so the probes diagnostic is reported.
                    if Instant::now() >= until {
                        std::future::pending::<()>().await;
                    }
                    attempts += 1;
                    let last = std::mem::take(&mut *outcome.lock().unwrap());
                    *outcome.lock().unwrap() = format!("probing #{attempts}; previous: {last}");
                    match probe(index, until).await {
                        Ok(()) => return index,
                        Err(error) if retryable(&error) => {
                            *outcome.lock().unwrap() = format!("#{attempts}: {error:?}")
                        }
                        Err(error) => panic!(
                            "unexpected leader read: group={group} node={} error={error:?}",
                            nodes[index]
                        ),
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            });
        }
        pending.next().await.expect("leader probe requires nodes")
    })
    .await;
    result.unwrap_or_else(|_| {
        let probes: Vec<_> = nodes
            .iter()
            .zip(&outcomes)
            .map(|(node, outcome)| (node, outcome.lock().unwrap().clone()))
            .collect();
        panic!("leader read timed out: group={group} probes={probes:?}")
    })
}

#[tokio::test(start_paused = true)]
async fn confirmed_leader_does_not_wait_for_an_earlier_pending_read() {
    let confirmed = tokio::time::timeout(
        Duration::from_secs(1),
        confirmed_leader(&[1, 2], 7, |index, _| async move {
            if index == 0 {
                std::future::pending().await
            } else {
                Ok(())
            }
        }),
    )
    .await
    .expect("a pending read must not hide another node's confirmed ReadIndex");
    assert_eq!(confirmed, 1);
}

#[tokio::test(start_paused = true)]
async fn confirmed_leader_retries_while_another_read_is_pending() {
    let calls = Mutex::new([0; 2]);
    let confirmed = tokio::time::timeout(
        Duration::from_secs(1),
        confirmed_leader(&[1, 2], 7, |index, _| {
            let attempt = {
                let mut calls = calls.lock().unwrap();
                calls[index] += 1;
                calls[index]
            };
            async move {
                if index == 0 {
                    std::future::pending().await
                } else if attempt == 1 {
                    Err(RuntimeError::Source(MultiRaftError::NotLeader {
                        hint: None,
                    }))
                } else {
                    Ok(())
                }
            }
        }),
    )
    .await
    .expect("a pending read must not prevent another node's next ReadIndex attempt");
    assert_eq!(confirmed, 1);
    assert_eq!(*calls.lock().unwrap(), [1, 2]);
}

#[tokio::test(start_paused = true)]
async fn confirmed_leader_reports_probes_instead_of_panicking_at_the_deadline() {
    // Every probe runs until the shared deadline and then fails its admission,
    // as a real read does when `until` passes; the helper must report the
    // probes diagnostic rather than panic on the Deadline error.
    let outcome = tokio::spawn(confirmed_leader(&[1, 2], 7, |_, until| async move {
        tokio::time::sleep_until(until).await;
        Err(RuntimeError::Deadline {
            phase: multiraft_net::RuntimePhase::Admission,
            outcome_unknown: false,
        })
    }))
    .await;
    let panic = outcome.expect_err("no node confirmed").into_panic();
    let message = panic.downcast_ref::<String>().cloned().unwrap_or_default();
    assert!(
        message.starts_with("leader read timed out: group=7 probes="),
        "{message}"
    );
}
