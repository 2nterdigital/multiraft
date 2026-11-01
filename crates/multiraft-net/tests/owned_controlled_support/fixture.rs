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
        Self::startup(false).await
    }
    pub async fn unopened() -> Self {
        Self::unopened_known(true).await
    }
    async fn unopened_known(candidate_known: bool) -> Self {
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
            if !candidate_known {
                config.raft_transport_fingerprint = Some([7; 32]);
                if id != 4 {
                    config.peers.retain(|(peer, _)| *peer != 4);
                }
            }
            config.data_dir = root.path().join(format!("node-{id}"));
            config.log_storage_backend = LogStorageBackend::RocksDb;
            config.snapshot_mode = SnapshotMode::NativeDurable;
            config.retain_log_entries = 0;
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
        Self {
            root,
            authority,
            owners,
            configs,
            groups: vec![],
        }
    }
    pub async fn without_candidate() -> Self {
        Self::startup_catalog(false, false).await
    }
    pub async fn startup(cancel_first_waiter: bool) -> Self {
        Self::startup_catalog(cancel_first_waiter, true).await
    }
    async fn startup_catalog(cancel_first_waiter: bool, candidate_known: bool) -> Self {
        let Self {
            root,
            authority,
            owners,
            configs,
            ..
        } = Self::unopened_known(candidate_known).await;
        let mut opens = Vec::new();
        for (i, owner) in owners.iter().take(3).enumerate() {
            let handle = owner.handle();
            let authority_for_inspection = authority.clone();
            let authority = authority.clone();
            opens.push(tokio::spawn(async move {
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
            }));
            if cancel_first_waiter && i == 0 {
                tokio::time::timeout(Duration::from_secs(10), async {
                    loop {
                        if let Ok(group) = owner
                            .handle()
                            .inspect_controlled_group(
                                GROUP,
                                10,
                                vec![1],
                                authority_for_inspection.clone(),
                                deadline(),
                            )
                            .await
                        {
                            if group.receipt(deadline()).await.unwrap().stage
                                == multiraft_store::ControlledGroupStage::Opening
                            {
                                break;
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                })
                .await
                .expect("first source is Opening without quorum");
                opens[0].abort();
            }
        }
        let mut groups = Vec::new();
        for (i, opening) in opens.into_iter().enumerate() {
            if cancel_first_waiter && i == 0 {
                assert!(opening.await.unwrap_err().is_cancelled());
                groups.push(
                    owners[0]
                        .handle()
                        .inspect_controlled_group(GROUP, 10, vec![1], authority.clone(), deadline())
                        .await
                        .unwrap(),
                );
            } else {
                groups.push(opening.await.unwrap());
            }
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            while owners[0]
                .handle()
                .local_group_status(GROUP, deadline())
                .await
                .is_err()
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("actual owned completion publishes readiness after canceled first waiter");
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

impl Fixture {
    pub async fn leader(&self) -> usize {
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                for (i, owner) in self.owners.iter().enumerate().take(3) {
                    if owner
                        .handle()
                        .local_group_status(GROUP, deadline())
                        .await
                        .is_ok_and(|status| status.leader_hint == Some((i + 1) as u64))
                        && self.groups[i].membership(deadline()).await.is_ok()
                    {
                        return i;
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("actual committed leader")
    }
}

pub async fn start_owned(config: ClusterConfig) -> NodeOwner<DiskImage> {
    let root = config.data_dir.clone();
    NodeOwner::start(
        RuntimeConfig::new(config, vec![]),
        move |ctx: FsmFactoryContext| DiskImage::open(&root, ctx.group_id()).map_err(Into::into),
        deadline(),
    )
    .await
    .unwrap()
}
pub async fn recover(
    owner: &NodeOwner<DiskImage>,
    authority: Arc<FileAuthority>,
    node: u64,
    boot: u8,
) -> Result<ManagedControlledGroup<DiskImage>, multiraft_net::ManagedControlError> {
    let admission = match authority
        .admit(StartupRequest {
            group_id: GROUP,
            node_id: node,
            generation: node * 10,
            request_id: [boot; 16],
        })
        .await
        .unwrap()
    {
        StartupAuthorityResult::Applied(value) => value,
        other => panic!("{other:?}"),
    };
    owner
        .handle()
        .open_controlled_group(
            ControlledOpen {
                group_id: GROUP,
                generation: node * 10,
                identity: vec![node as u8],
                mode: ControlledOpenMode::Recover,
            },
            authority,
            admission,
            deadline(),
        )
        .await
}

pub struct LocalTransport {
    pub endpoint: multiraft_net::ManagedTargetEndpoint<DiskImage>,
}
impl multiraft_net::portable::NativeTargetTransport for LocalTransport {
    fn peer(&self) -> (u64, Option<[u8; 32]>) {
        let id = self.endpoint.identity();
        (id.node, id.domain)
    }
    fn exchange(
        &self,
        request: Vec<u8>,
    ) -> StartupAuthorityFuture<
        '_,
        Result<Vec<u8>, multiraft_net::portable::NativeTargetTransportError>,
    > {
        Box::pin(async move {
            self.endpoint
                .dispatch(request, deadline())
                .await
                .map_err(
                    |error| multiraft_net::portable::NativeTargetTransportError {
                        outcome: multiraft_net::portable::NativeTargetOutcome::Unknown,
                        message: error.to_string(),
                    },
                )
        })
    }
}
#[path = "transport.rs"]
pub mod transport;

pub struct Prepared {
    pub leader: usize,
    pub specification: multiraft_core::ReplacementSpec,
    pub target: ManagedControlledGroup<DiskImage>,
    pub endpoint: multiraft_net::ManagedTargetEndpoint<DiskImage>,
    pub executor: multiraft_net::ManagedOperation<DiskImage>,
    pub port: multiraft_net::portable::SourceTargetPort,
    pub admission: StartupAdmission,
}
impl Fixture {
    pub async fn prepare_gated(
        &self,
        gate: transport::Gate,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    ) -> Prepared {
        use multiraft_core::{OperationId, ReplacementSpec};
        use multiraft_net::portable::SourceTargetPort;
        let leader = self.leader().await;
        let source = if leader == 0 { 3 } else { 1 };
        let membership = self.groups[leader].membership(deadline()).await.unwrap();
        self.authority
            .bootstrap_membership(membership.clone())
            .unwrap();
        let specification = ReplacementSpec {
            operation: OperationId {
                sequence: 1,
                nonce: [8; 16],
            },
            expected_start: membership,
            target_voters: [1, 2, 3, 4]
                .into_iter()
                .filter(|id| *id != source)
                .collect(),
            target_generation: 40,
        };
        let target = self.owners[3]
            .handle()
            .reserve_controlled_target(
                GROUP,
                specification.clone(),
                vec![4],
                self.authority.clone(),
                deadline(),
            )
            .await
            .unwrap();
        self.authority
            .consume_target(StartupTargetGeneration {
                node_id: 4,
                generation: 40,
                consumption_id: target.receipt(deadline()).await.unwrap().identity,
            })
            .unwrap();
        let endpoint = target
            .target_endpoint(specification.clone(), deadline())
            .await
            .unwrap();
        let port = SourceTargetPort::new(
            endpoint.identity().clone(),
            specification.clone(),
            Arc::new(transport::GatedTransport {
                endpoint: endpoint.clone(),
                gate,
                entered,
                release,
            }),
        )
        .unwrap();
        let executor = self.groups[leader]
            .begin(specification.clone(), 1, port.clone(), deadline())
            .await
            .unwrap();
        let admission = match self
            .authority
            .admit(StartupRequest {
                group_id: GROUP,
                node_id: 4,
                generation: 40,
                request_id: [40; 16],
            })
            .await
            .unwrap()
        {
            StartupAuthorityResult::Applied(value) => value,
            other => panic!("{other:?}"),
        };
        Prepared {
            leader,
            specification,
            target,
            endpoint,
            executor,
            port,
            admission,
        }
    }
}
#[path = "cancellation.rs"]
pub mod cancellation;
