//! Independent multi-Group byte FSM consumer, using only owned runtime APIs.
use multiraft_core::{ClusterConfig, FileLogSyncLevel};
use multiraft_fsm::{ApplyOut, GroupId, StateMachine};
use multiraft_net::{GroupConfig, NodeOwner, RuntimeConfig, RuntimeHandle};
use std::{
    io,
    net::{SocketAddr, TcpListener},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::time::Instant;

struct Bytes {
    value: Vec<u8>,
    released: Arc<AtomicUsize>,
}
impl Drop for Bytes {
    fn drop(&mut self) {
        self.released.fetch_add(1, Ordering::SeqCst);
    }
}
impl StateMachine for Bytes {
    type Error = io::Error;
    fn apply(&mut self, _: GroupId, _: u64, bytes: &[u8]) -> Result<ApplyOut, Self::Error> {
        self.value = bytes.to_vec();
        Ok(ApplyOut {
            effects: self.value.clone(),
        })
    }
    fn snapshot(&self, _: GroupId) -> Result<Vec<u8>, Self::Error> {
        Ok(self.value.clone())
    }
    fn restore(&mut self, _: GroupId, bytes: &[u8]) -> Result<(), Self::Error> {
        self.value = bytes.to_vec();
        Ok(())
    }
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
async fn start(config: ClusterConfig, released: Arc<AtomicUsize>) -> NodeOwner<Bytes> {
    NodeOwner::start(
        RuntimeConfig::new(
            config,
            [7, 8]
                .into_iter()
                .map(|group_id| GroupConfig {
                    group_id,
                    voters: vec![1, 2, 3],
                })
                .collect(),
        ),
        move |_| {
            Ok(Bytes {
                value: Vec::new(),
                released: released.clone(),
            })
        },
        deadline(),
    )
    .await
    .unwrap()
}
async fn authoritative(handles: &[RuntimeHandle<Bytes>], group: GroupId) -> RuntimeHandle<Bytes> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            for handle in handles {
                if handle
                    .read_linearizable(group, Instant::now() + Duration::from_secs(1), |_| ())
                    .await
                    .is_ok()
                {
                    return handle.clone();
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("remaining majority confirms authority")
}
async fn write_read(handles: &[RuntimeHandle<Bytes>], group: GroupId, bytes: &[u8]) {
    let handle = authoritative(handles, group).await;
    let receipt = handle
        .propose(group, bytes.to_vec(), deadline())
        .await
        .unwrap();
    assert_eq!(receipt.effects, bytes);
    assert_eq!(
        handle
            .read_linearizable(group, deadline(), |fsm| fsm.value.clone())
            .await
            .unwrap(),
        bytes
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_group_majority_requires_restarted_peer_and_shutdown_releases_resources() {
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
    let released = Arc::new(AtomicUsize::new(0));
    let (first, second, third) = tokio::join!(
        start(configs[0].clone(), released.clone()),
        start(configs[1].clone(), released.clone()),
        start(configs[2].clone(), released.clone()),
    );
    let handles = [first.handle(), second.handle(), third.handle()];
    write_read(&handles, 7, b"first Group").await;
    write_read(&handles, 8, b"second Group").await;
    third.shutdown(deadline()).await.unwrap();
    assert_eq!(released.load(Ordering::SeqCst), 2);
    TcpListener::bind(peers[2].1).unwrap();
    write_read(&handles[..2], 7, b"during peer stop").await;
    write_read(&handles[..2], 8, b"also during stop").await;
    let restarted = start(configs[2].clone(), released.clone()).await;
    // A successful operation now needs the restarted peer: only Node 1 and
    // restarted Node 3 remain. Both Groups require a two-voter majority.
    second.shutdown(deadline()).await.unwrap();
    let majority = [first.handle(), restarted.handle()];
    write_read(&majority, 7, b"restarted peer confirms Group 7").await;
    write_read(&majority, 8, b"restarted peer confirms Group 8").await;
    first.shutdown(deadline()).await.unwrap();
    restarted.shutdown(deadline()).await.unwrap();
    assert_eq!(released.load(Ordering::SeqCst), 8);
    for (_, address) in peers {
        TcpListener::bind(address).unwrap();
    }
}
