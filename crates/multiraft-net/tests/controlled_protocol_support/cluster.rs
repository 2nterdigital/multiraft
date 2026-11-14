//! Fixtures shared by the controlled native protocol trajectories.
#[path = "disk.rs"]
mod disk;

pub use disk::DiskImage;

use multiraft_core::{ClusterConfig, LogStorageBackend, SnapshotMode};
use multiraft_net::{FsmFactoryContext, MultiRaft};
use std::{
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::time::Instant;

pub const GROUP: u64 = 7;
pub const VOTERS: [u64; 3] = [1, 2, 3];

pub fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(20)
}

pub fn scratch(case: &str) -> PathBuf {
    let parent = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/controlled-protocol");
    std::fs::create_dir_all(&parent).expect("create protocol evidence directory");
    tempfile::Builder::new()
        .prefix(case)
        .tempdir_in(parent)
        .expect("allocate protocol evidence")
        .keep()
}

pub fn configurations(root: &Path) -> Vec<ClusterConfig> {
    let listeners: Vec<_> = (0..4)
        .map(|_| TcpListener::bind("127.0.0.1:0").expect("reserve peer address"))
        .collect();
    let peers_path = root.join("peers.json");
    let peers: Vec<(u64, SocketAddr)> = if peers_path.exists() {
        serde_json::from_slice(&std::fs::read(&peers_path).expect("read original peer addresses"))
            .expect("decode original peer addresses")
    } else {
        let peers: Vec<_> = listeners
            .iter()
            .enumerate()
            .map(|(index, listener)| {
                (
                    (index + 1) as u64,
                    listener.local_addr().expect("peer address"),
                )
            })
            .collect();
        std::fs::write(
            &peers_path,
            serde_json::to_vec(&peers).expect("encode peer addresses"),
        )
        .expect("persist peer addresses");
        peers
    };
    peers
        .iter()
        .map(|(id, _)| {
            let mut config = ClusterConfig::for_test(*id, &VOTERS);
            config.peers = peers.clone();
            config.data_dir = root.join(format!("node-{id}"));
            config.log_storage_backend = LogStorageBackend::RocksDb;
            config.snapshot_mode = SnapshotMode::NativeDurable;
            config.retain_log_entries = 0;
            config.enable_stale_queries = true;
            config
        })
        .collect()
}

pub async fn start(
    config: ClusterConfig,
    creations: Arc<AtomicUsize>,
) -> Arc<MultiRaft<DiskImage>> {
    let root = config.data_dir.clone();
    let crash_opening = config.node_id == 4
        && std::env::var("CONTROLLED_PROTOCOL_SCENARIO").as_deref() == Ok("opening")
        && std::env::var("CONTROLLED_PROTOCOL_RESUME").as_deref() == Ok("0");
    Arc::new(
        MultiRaft::start_grpc_with_factory(config, move |context: FsmFactoryContext| {
            if crash_opening && context.group_id() == GROUP {
                // The runtime persists Opening before entering this real factory.
                super::process::persisted_phase("opening");
            }
            let image = DiskImage::open(&root, context.group_id())?;
            creations.fetch_add(1, Ordering::SeqCst);
            Ok(image)
        })
        .await
        .expect("start real gRPC peer"),
    )
}

pub async fn leader(nodes: &[Arc<MultiRaft<DiskImage>>]) -> usize {
    let result = tokio::time::timeout(Duration::from_secs(20), async {
        let mut previous = None;
        let mut confirmations = 0;
        loop {
            for (index, node) in nodes.iter().enumerate() {
                let votes = nodes
                    .iter()
                    .filter(|peer| peer.leader(GROUP) == Some(node.node_id()))
                    .count();
                if node.is_leader(GROUP)
                    && votes >= 2
                    && node.read_linearizable(GROUP, |_| ()).await.is_ok()
                {
                    if previous == Some(index) {
                        confirmations += 1;
                    } else {
                        previous = Some(index);
                        confirmations = 1;
                    }
                    if confirmations >= 3 {
                        return index;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    result.unwrap_or_else(|error| {
        panic!(
            "committed leader with real quorum: {error}; local observations={:?}",
            nodes
                .iter()
                .map(|node| (
                    node.node_id(),
                    node.observe_group(GROUP).map(|(view, _)| view)
                ))
                .collect::<Vec<_>>()
        )
    })
}

pub async fn wait_payloads(node: &MultiRaft<DiskImage>, expected: &[Vec<u8>]) {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if node
                .with_fsm(GROUP, |fsm| {
                    fsm.payloads().expect("read durable application rows")
                })
                .await
                .as_deref()
                == Some(expected)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("exact opaque application payloads applied");
}

pub async fn compact(node: &MultiRaft<DiskImage>) -> u64 {
    let submitted = node
        .request_compaction(GROUP)
        .await
        .expect("request real durable checkpoint and purge");
    let target = submitted
        .target
        .expect("nonempty compaction boundary")
        .index;
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let status = node
                .local_storage_status(GROUP)
                .await
                .expect("observe native storage");
            if status.progress == multiraft_net::CompactionProgress::CompletedObserved
                && status.purged.is_some_and(|id| id.index >= target)
            {
                assert!(
                    status
                        .durable_snapshot
                        .and_then(|snapshot| snapshot.last_log_id)
                        .is_some_and(|id| id.index >= target),
                    "purge requires durable checkpoint covering its boundary"
                );
                return target;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("durable native checkpoint and actual log purge")
}
