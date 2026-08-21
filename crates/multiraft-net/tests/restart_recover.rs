//! 3-node MultiRaft restart with shared file-backed data dirs.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use multiraft_core::ClusterConfig;
use multiraft_fsm::{ApplyOut, CounterFsm, StateMachine};
use multiraft_net::{wait_for_leader, FsmFactoryContext, MultiRaft, StateMachineFactory};

fn temp_data_dirs(root: &tempfile::TempDir, peer_ids: &[u64]) -> Vec<std::path::PathBuf> {
    peer_ids
        .iter()
        .map(|&id| {
            let dir = root.path().join(format!("node-{id}"));
            std::fs::create_dir_all(&dir).unwrap();
            dir
        })
        .collect()
}

fn configs_with_dirs(peer_ids: &[u64], dirs: &[std::path::PathBuf]) -> Vec<ClusterConfig> {
    peer_ids
        .iter()
        .zip(dirs.iter())
        .map(|(&id, dir)| {
            let mut cfg = ClusterConfig::for_test(id, peer_ids);
            cfg.data_dir = dir.clone();
            cfg
        })
        .collect()
}

async fn propose_on_leader<S: StateMachine>(nodes: &[MultiRaft<S>], group: u64, data: Vec<u8>) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        for n in nodes {
            if n.is_leader(group) {
                match n.propose(group, data.clone()).await {
                    Ok(_) => return,
                    Err(multiraft_core::MultiRaftError::NotLeader { .. }) => {}
                    Err(e) => panic!("propose failed: {e:?}"),
                }
            }
        }
        if std::time::Instant::now() >= deadline {
            panic!("timed out proposing");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_leader_for<S: StateMachine>(
    nodes: &[MultiRaft<S>],
    group: u64,
    timeout: Duration,
) -> Option<u64> {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        for node in nodes {
            if let Some(leader) = node.leader(group) {
                if nodes.iter().any(|peer| peer.is_leader(group)) {
                    return Some(leader);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    None
}

#[derive(Default)]
struct ReplayProbeFsm {
    value: i64,
}

impl ReplayProbeFsm {
    fn encode_add(delta: i64) -> Vec<u8> {
        delta.to_le_bytes().to_vec()
    }
}

impl StateMachine for ReplayProbeFsm {
    type Error = std::io::Error;

    fn apply(&mut self, _group: u64, _index: u64, data: &[u8]) -> Result<ApplyOut, Self::Error> {
        let bytes: [u8; 8] = data.try_into().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "replay probe delta")
        })?;
        self.value += i64::from_le_bytes(bytes);
        Ok(ApplyOut::default())
    }

    fn snapshot(&self, _group: u64) -> Result<Vec<u8>, Self::Error> {
        Ok(self.value.to_le_bytes().to_vec())
    }

    fn restore(&mut self, _group: u64, snapshot: &[u8]) -> Result<(), Self::Error> {
        let bytes: [u8; 8] = snapshot.try_into().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "replay probe snapshot")
        })?;
        self.value = i64::from_le_bytes(bytes);
        Ok(())
    }
}

#[derive(Clone)]
struct CountingReplayFactory {
    calls: Arc<AtomicUsize>,
}

impl StateMachineFactory<ReplayProbeFsm> for CountingReplayFactory {
    fn create(&self, _context: FsmFactoryContext) -> anyhow::Result<ReplayProbeFsm> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(ReplayProbeFsm::default())
    }
}

#[tokio::test]
async fn restart_replays_committed_state() {
    let peer_ids = [1u64, 2, 3];
    let data_root = tempfile::tempdir().expect("create temporary data root");
    let dirs = temp_data_dirs(&data_root, &peer_ids);
    let group = 1u64;
    let members = peer_ids.to_vec();
    let expected: i64 = 1 + 2 + 3 + 4 + 5;

    {
        let configs = configs_with_dirs(&peer_ids, &dirs);
        let nodes = MultiRaft::start_cluster(configs)
            .await
            .expect("start_cluster");

        for n in &nodes {
            n.create_group(group, &members).await.expect("create_group");
        }

        wait_for_leader(&nodes, group, Duration::from_secs(5))
            .await
            .expect("leader elected");

        for (i, delta) in [1i64, 2, 3, 4, 5].into_iter().enumerate() {
            let data = CounterFsm::encode_add(delta, /*idem=*/ (i as u64) + 1);
            propose_on_leader(&nodes, group, data).await;
        }

        // Spot-check before shutdown.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let mut saw = false;
        for n in &nodes {
            if let Some(v) = n.with_fsm(group, |fsm| fsm.value(group)).await {
                if v == expected {
                    saw = true;
                    break;
                }
            }
        }
        assert!(saw, "at least one node should have applied all cmds");

        for n in &nodes {
            n.shutdown().await.expect("shutdown");
        }
    }

    // Restart with the same per-node data_dir paths.
    let configs = configs_with_dirs(&peer_ids, &dirs);
    let nodes = MultiRaft::start_cluster(configs)
        .await
        .expect("restart cluster");

    for n in &nodes {
        n.create_group(group, &members)
            .await
            .expect("create_group after restart");
    }

    wait_for_leader(&nodes, group, Duration::from_secs(5))
        .await
        .expect("leader after restart");

    for n in &nodes {
        n.wait_for_recovery(group, Duration::from_secs(5))
            .await
            .expect("wait_for_recovery");
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut recovered = None;
    while std::time::Instant::now() < deadline {
        for n in &nodes {
            if let Some(v) = n.with_fsm(group, |fsm| fsm.value(group)).await {
                if v == expected {
                    recovered = Some(v);
                    break;
                }
            }
        }
        if recovered.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert_eq!(
        recovered,
        Some(expected),
        "FSM value must be restored after restart"
    );
    for n in &nodes {
        n.shutdown().await.expect("shutdown after recovery");
    }
}

#[tokio::test]
async fn file_log_replay_restores_committed_state_with_custom_factory() {
    let peer_ids = [1u64, 2, 3];
    let data_root = tempfile::tempdir().expect("create temporary data root");
    let dirs = temp_data_dirs(&data_root, &peer_ids);
    let members = peer_ids.to_vec();
    let calls = Arc::new(AtomicUsize::new(0));
    let factory = CountingReplayFactory {
        calls: calls.clone(),
    };

    {
        let nodes = MultiRaft::start_cluster_with_factory(
            configs_with_dirs(&peer_ids, &dirs),
            factory.clone(),
        )
        .await
        .expect("start custom cluster");
        for node in &nodes {
            node.create_group(1, &members).await.expect("create group");
        }
        wait_for_leader_for(&nodes, 1, Duration::from_secs(5))
            .await
            .expect("leader");
        for delta in [1, 2, 3, 4, 5] {
            propose_on_leader(&nodes, 1, ReplayProbeFsm::encode_add(delta)).await;
        }
        for node in &nodes {
            node.shutdown().await.expect("shutdown");
        }
    }

    let nodes = MultiRaft::start_cluster_with_factory(configs_with_dirs(&peer_ids, &dirs), factory)
        .await
        .expect("restart custom cluster");
    for node in &nodes {
        node.create_group(1, &members)
            .await
            .expect("create after restart");
    }
    for node in &nodes {
        node.wait_for_recovery(1, Duration::from_secs(5))
            .await
            .expect("recovery");
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        let mut values = Vec::new();
        for node in &nodes {
            values.push(node.with_fsm(1, |fsm| fsm.value).await);
        }
        if values.iter().all(|value| *value == Some(15)) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for node in &nodes {
        assert_eq!(node.with_fsm(1, |fsm| fsm.value).await, Some(15));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 6);
    for node in &nodes {
        node.shutdown().await.expect("shutdown after recovery");
    }
}
