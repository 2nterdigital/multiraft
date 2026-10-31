//! Shared real-disk owned-runtime fixtures.
use super::{authority::*, catalog::FileAuthority, disk::DiskImage};
use multiraft_core::{typ::StoredMembership, ClusterConfig, LogStorageBackend, SnapshotMode};
use multiraft_net::{
    ControlledOpen, ControlledOpenMode, FsmFactoryContext, ManagedControlledGroup, NodeOwner,
    RuntimeConfig,
};
use openraft::{BasicNode, Membership};
use std::{collections::BTreeMap, net::TcpListener, sync::Arc, time::Duration};
use tokio::time::Instant;
pub const GROUP: u64 = 7;
pub fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(20)
}
pub struct Fixture {
    pub root: tempfile::TempDir,
    pub authority: Arc<FileAuthority>,
    pub owners: Vec<NodeOwner<DiskImage>>,
    pub groups: Vec<ManagedControlledGroup<DiskImage>>,
    pub configs: Vec<ClusterConfig>,
}
impl Fixture {
    pub async fn fresh() -> Self {
        let parent =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/owned-controlled");
        std::fs::create_dir_all(&parent).unwrap();
        let root = tempfile::tempdir_in(parent).unwrap();
        let authority = Arc::new(FileAuthority::provision(
            &root.path().join("catalog.json"),
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
        let listeners: Vec<_> = (0..4)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let peers: Vec<_> = listeners
            .iter()
            .enumerate()
            .map(|(i, l)| ((i + 1) as u64, l.local_addr().unwrap()))
            .collect();
        drop(listeners);
        let mut configs = Vec::new();
        let mut owners = Vec::new();
        for id in 1..=4 {
            let mut config = ClusterConfig::for_test(id, &[1, 2, 3]);
            config.peers = peers.clone();
            config.data_dir = root.path().join(format!("node-{id}"));
            config.log_storage_backend = LogStorageBackend::RocksDb;
            config.snapshot_mode = SnapshotMode::NativeDurable;
            let path = config.data_dir.clone();
            owners.push(
                NodeOwner::start(
                    RuntimeConfig::new(config.clone(), vec![]),
                    move |ctx: FsmFactoryContext| {
                        DiskImage::open(&path, ctx.group_id()).map_err(Into::into)
                    },
                    deadline(),
                )
                .await
                .unwrap(),
            );
            configs.push(config);
        }
        let mut opens = Vec::new();
        for (i, owner) in owners.iter().take(3).enumerate() {
            let handle = owner.handle();
            let authority = authority.clone();
            opens.push(async move {
                let node_id = (i + 1) as u64;
                let admission = match authority
                    .admit(StartupRequest {
                        group_id: GROUP,
                        node_id,
                        generation: node_id * 10,
                        request_id: [node_id as u8; 16],
                    })
                    .await
                    .unwrap()
                {
                    StartupAuthorityResult::Applied(value) => value,
                    other => panic!("{other:?}"),
                };
                handle
                    .open_controlled_group(
                        ControlledOpen {
                            group_id: GROUP,
                            generation: node_id * 10,
                            identity: vec![node_id as u8],
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
        let groups = futures::future::join_all(opens).await;
        Self {
            root,
            authority,
            owners,
            groups,
            configs,
        }
    }
    pub async fn stop(self) {
        for owner in self.owners {
            owner.shutdown(deadline()).await.unwrap();
        }
    }
}
