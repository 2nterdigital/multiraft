//! Restarting one voter at its old gRPC address, as a rolling restart does.
use multiraft_core::{ClusterConfig, FileLogSyncLevel};
use multiraft_fsm::{ApplyOut, GroupId, StateMachine};
use multiraft_net::{GroupConfig, GroupServerState, NodeOwner, RuntimeConfig, RuntimeHandle};
use std::{
    collections::BTreeSet,
    io,
    net::{SocketAddr, TcpListener},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::Instant;

const GROUP: GroupId = 7;

#[derive(Default)]
struct Bytes(Vec<u8>);
impl StateMachine for Bytes {
    type Error = io::Error;
    fn apply(&mut self, _: GroupId, _: u64, bytes: &[u8]) -> Result<ApplyOut, Self::Error> {
        self.0 = bytes.to_vec();
        Ok(ApplyOut {
            effects: Vec::new(),
        })
    }
    fn snapshot(&self, _: GroupId) -> Result<Vec<u8>, Self::Error> {
        Ok(self.0.clone())
    }
    fn restore(&mut self, _: GroupId, bytes: &[u8]) -> Result<(), Self::Error> {
        self.0 = bytes.to_vec();
        Ok(())
    }
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}

async fn start(config: ClusterConfig) -> NodeOwner<Bytes> {
    let groups = vec![GroupConfig {
        group_id: GROUP,
        voters: vec![1, 2, 3],
    }];
    NodeOwner::start(
        RuntimeConfig::new(config, groups),
        |_| Ok(Bytes::default()),
        deadline(),
    )
    .await
    .unwrap()
}

/// (term, leader) from whichever live node reports itself Leader.
async fn leader(handles: &[RuntimeHandle<Bytes>]) -> Option<(u64, u64)> {
    for handle in handles {
        let soon = Instant::now() + Duration::from_millis(200);
        if let Ok(s) = handle.local_group_status(GROUP, soon).await {
            if s.server_state == GroupServerState::Leader {
                return Some((s.current_term, s.local_node_id));
            }
        }
    }
    None
}

async fn write(handles: &[RuntimeHandle<Bytes>], bytes: &[u8]) {
    let until = deadline();
    loop {
        for handle in handles {
            let soon = Instant::now() + Duration::from_millis(500);
            if handle.propose(GROUP, bytes.to_vec(), soon).await.is_ok() {
                return;
            }
        }
        assert!(Instant::now() < until, "no write within deadline");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[derive(Default)]
struct Seen {
    candidates: BTreeSet<u64>,
    leaders: BTreeSet<u64>,
}

/// Poll every live node so a Candidate interval (at least one election
/// timeout long) is not missed between samples.
fn sample(handles: Arc<Mutex<Vec<RuntimeHandle<Bytes>>>>, seen: Arc<Mutex<Seen>>) {
    tokio::spawn(async move {
        loop {
            let live = handles.lock().unwrap().clone();
            for handle in live {
                let soon = Instant::now() + Duration::from_millis(50);
                if let Ok(s) = handle.local_group_status(GROUP, soon).await {
                    let mut seen = seen.lock().unwrap();
                    match s.server_state {
                        GroupServerState::Candidate => seen.candidates.insert(s.local_node_id),
                        GroupServerState::Leader => seen.leaders.insert(s.local_node_id),
                        _ => false,
                    };
                }
            }
            if Arc::strong_count(&seen) == 1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
}

struct Outcome {
    term: (u64, u64),
    leader: (u64, u64),
}

async fn restart_once(restart_leader: bool, down: Duration) -> Outcome {
    let root = tempfile::tempdir().unwrap();
    let listeners: Vec<_> = (0..3)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let peers: Vec<(u64, SocketAddr)> = listeners
        .iter()
        .enumerate()
        .map(|(i, listener)| ((i + 1) as u64, listener.local_addr().unwrap()))
        .collect();
    drop(listeners);
    let configs: Vec<_> = peers
        .iter()
        .map(|&(node, _)| {
            let mut config = ClusterConfig::for_test(node, &[1, 2, 3]);
            config.peers = peers.clone();
            config.data_dir = root.path().join(format!("node-{node}"));
            config.file_log_sync_level = FileLogSyncLevel::Data;
            config
        })
        .collect();
    let timeout = Duration::from_millis(configs[0].election_timeout_max_ms);
    let (a, b, c) = tokio::join!(
        start(configs[0].clone()),
        start(configs[1].clone()),
        start(configs[2].clone())
    );
    let mut owners = vec![Some(a), Some(b), Some(c)];
    let all: Vec<_> = owners
        .iter()
        .map(|o| o.as_ref().unwrap().handle())
        .collect();
    for i in 0..3u8 {
        write(&all, &[i]).await;
    }
    tokio::time::sleep(timeout * 2).await;
    let (term, before) = leader(&all).await.expect("stable leader");
    let victim = if restart_leader {
        before
    } else {
        (1..=3).find(|id| *id != before).unwrap()
    };
    let index = (victim - 1) as usize;
    let live = Arc::new(Mutex::new(all.clone()));
    let seen = Arc::new(Mutex::new(Seen::default()));
    sample(live.clone(), seen.clone());

    owners[index]
        .take()
        .unwrap()
        .shutdown(deadline())
        .await
        .unwrap();
    live.lock().unwrap().remove(index);
    tokio::time::sleep(down).await;
    let started = Instant::now();
    let restarted = start(configs[index].clone()).await;
    let startup = started.elapsed();
    live.lock().unwrap().insert(index, restarted.handle());
    owners[index] = Some(restarted);
    tokio::time::sleep(timeout * 2).await;

    let handles = live.lock().unwrap().clone();
    let after = leader(&handles).await;
    write(&handles, b"after restart").await;
    let seen = std::mem::take(&mut *seen.lock().unwrap());
    println!(
        "RESTART_TERM victim={} down_ms={} term={term}->{:?} leader={before}->{:?} \
         candidates={:?} leaders_seen={:?} restart_ms={}",
        if restart_leader { "leader" } else { "follower" },
        down.as_millis(),
        after.map(|(t, _)| t),
        after.map(|(_, l)| l),
        seen.candidates,
        seen.leaders,
        startup.as_millis(),
    );
    for owner in owners.into_iter().flatten() {
        owner.shutdown(deadline()).await.unwrap();
    }
    let (term_after, leader_after) = after.expect("leader after restart");
    Outcome {
        term: (term, term_after),
        leader: (before, leader_after),
    }
}

fn runs() -> usize {
    std::env::var("RESTART_TERM_RUNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1)
}

/// Downtimes of about one and three `election_timeout_max`; the listener is
/// bound before the Group starts, so the leader reaches it before it campaigns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_follower_keeps_term_and_leader() {
    for down in [600, 1800] {
        for _ in 0..runs() {
            let outcome = restart_once(false, Duration::from_millis(down)).await;
            assert_eq!(outcome.term.0, outcome.term.1, "term after {down} ms down");
            assert_eq!(
                outcome.leader.0, outcome.leader.1,
                "leader after {down} ms down"
            );
        }
    }
}

/// `enable_leader_restore: Some(false)`: the old leader rejoins as a follower and
/// the Group pays exactly one election (OpenRaft's restore would campaign again).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_leader_costs_one_election() {
    for down in [600, 1800] {
        for _ in 0..runs() {
            let outcome = restart_once(true, Duration::from_millis(down)).await;
            assert_eq!(
                outcome.term.0 + 1,
                outcome.term.1,
                "term after {down} ms down"
            );
        }
    }
}
