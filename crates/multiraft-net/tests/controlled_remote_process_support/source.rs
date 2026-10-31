//! Three source owners and their source-only disk roots.
use super::{
    authority::{StartupAuthority, StartupAuthorityResult, StartupCatalog, StartupRequest},
    catalog::FileAuthority,
    disk::DiskImage,
};
use multiraft_core::{typ::StoredMembership, ClusterConfig, LogStorageBackend, SnapshotMode};
use multiraft_net::{ControlledGroup, ControlledOpen, ControlledOpenMode, MultiRaft};
use openraft::{BasicNode, Membership};
use std::{
    collections::BTreeMap,
    net::TcpListener,
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::time::Instant;
pub const GROUP: u64 = 7;
pub const DOMAIN: [u8; 32] = [71; 32];
pub fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(40)
}
pub fn admitted<T: std::fmt::Debug>(value: StartupAuthorityResult<T>) -> T {
    match value {
        StartupAuthorityResult::Applied(v) => v,
        v => panic!("durable admission: {v:?}"),
    }
}
pub fn configurations(root: &Path) -> Vec<ClusterConfig> {
    let sockets: Vec<_> = (0..4)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let peers: Vec<_> = sockets
        .iter()
        .enumerate()
        .map(|(i, s)| ((i + 1) as u64, s.local_addr().unwrap()))
        .collect();
    (1..=4)
        .map(|id| {
            let mut c = ClusterConfig::for_test(id, &[1, 2, 3]);
            c.peers = peers.clone();
            c.data_dir = root.join(if id == 4 {
                "target".to_owned()
            } else {
                format!("source/node-{id}")
            });
            c.log_storage_backend = LogStorageBackend::RocksDb;
            c.snapshot_mode = SnapshotMode::NativeDurable;
            c.retain_log_entries = 0;
            c.enable_stale_queries = true;
            c.raft_transport_fingerprint = Some(DOMAIN);
            c
        })
        .collect()
}
pub async fn start(config: ClusterConfig, count: Arc<AtomicUsize>) -> Arc<MultiRaft<DiskImage>> {
    let root = config.data_dir.clone();
    Arc::new(
        MultiRaft::start_grpc_with_factory(config, move |ctx: multiraft_net::FsmFactoryContext| {
            let fsm = DiskImage::open(&root, ctx.group_id())?;
            count.fetch_add(1, Ordering::SeqCst);
            Ok(fsm)
        })
        .await
        .expect("start disk-backed real gRPC owner"),
    )
}
pub struct Sources {
    pub nodes: Vec<Arc<MultiRaft<DiskImage>>>,
    pub groups: Vec<ControlledGroup<DiskImage>>,
    pub authority: Arc<FileAuthority>,
}
impl Sources {
    pub async fn fresh(root: &Path, configs: &[ClusterConfig]) -> Self {
        let authority = Arc::new(FileAuthority::provision(
            &root.join("catalog.json"),
            StartupCatalog {
                group_id: GROUP,
                revision: 0,
                membership: StoredMembership::new(
                    None,
                    Membership::new(
                        vec![[1, 2, 3].into()],
                        [1, 2, 3]
                            .into_iter()
                            .map(|id| (id, BasicNode::default()))
                            .collect::<BTreeMap<_, _>>(),
                    )
                    .unwrap(),
                ),
                authorized: [(1, 10), (2, 20), (3, 30)].into(),
                active_operation: None,
                admissions: vec![],
                completed: vec![],
            },
        ));
        let mut nodes = vec![];
        // The parent never constructs node 4, including a placeholder owner.
        for config in configs.iter().take(3) {
            nodes.push(start(config.clone(), Arc::default()).await);
        }
        let mut futures = vec![];
        for node in &nodes {
            let id = node.node_id();
            let authority = authority.clone();
            let node = node.clone();
            let admission = admitted(
                authority
                    .admit(StartupRequest {
                        group_id: GROUP,
                        node_id: id,
                        generation: id * 10,
                        request_id: [id as u8; 16],
                    })
                    .await
                    .unwrap(),
            );
            futures.push(async move {
                node.open_controlled_group(
                    ControlledOpen {
                        group_id: GROUP,
                        generation: id * 10,
                        identity: vec![id as u8],
                        mode: ControlledOpenMode::BootstrapNew {
                            voters: [1, 2, 3].into(),
                        },
                    },
                    authority,
                    admission,
                    deadline(),
                )
                .await
                .unwrap()
            });
        }
        let groups = futures::future::join_all(futures).await;
        let this = Self {
            nodes,
            groups,
            authority,
        };
        let leader = this.leader().await;
        this.authority
            .bootstrap_membership(this.groups[leader].membership(deadline()).await.unwrap())
            .unwrap();
        this
    }
    pub async fn leader(&self) -> usize {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                for (i, n) in self.nodes.iter().enumerate() {
                    if n.is_leader(GROUP) && n.read_linearizable(GROUP, |_| ()).await.is_ok() {
                        return i;
                    }
                }
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
        })
        .await
        .expect("source quorum leader")
    }
    pub async fn stop(self) {
        drop(self.groups);
        for n in self.nodes {
            n.shutdown().await.unwrap();
        }
    }
}
pub async fn compact(node: &MultiRaft<DiskImage>) {
    let boundary = node
        .request_compaction(GROUP)
        .await
        .unwrap()
        .target
        .unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let s = node.local_storage_status(GROUP).await.unwrap();
            if s.progress == multiraft_net::CompactionProgress::CompletedObserved
                && s.purged.is_some_and(|id| id.index >= boundary.index)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("actual durable checkpoint and log purge");
}
pub async fn wait_payloads(node: &MultiRaft<DiskImage>, expected: &[Vec<u8>]) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if node
                .with_fsm(GROUP, |fsm| fsm.payloads().unwrap())
                .await
                .as_deref()
                == Some(expected)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("opaque rows applied to real disk FSM");
}
