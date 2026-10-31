//! Public Slot fixtures backed by real durable FSMs and independently persisted catalogs.
#![allow(dead_code)]
#[path = "controlled_support/catalog.rs"]
pub mod catalog;
#[path = "controlled_support/disk.rs"]
pub mod disk;
use authority::*;
use catalog::FileAuthority;
use disk::DiskImage;
use multiraft_core::typ::StoredMembership;
use multiraft_net::controlled_startup as authority;
use multiraft_slot::*;
use openraft::{BasicNode, Membership};
use std::{
    collections::BTreeMap,
    net::{SocketAddr, TcpListener},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::time::Instant;
static NEXT: AtomicU64 = AtomicU64::new(0);
pub fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(30)
}
pub fn address() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}
pub struct Catalogs(pub BTreeMap<u64, Arc<FileAuthority>>);
impl StartupAuthority for Catalogs {
    fn read(
        &self,
        g: u64,
    ) -> StartupAuthorityFuture<'_, Result<StartupCatalog, StartupAuthorityError>> {
        Box::pin(async move {
            self.0
                .get(&g)
                .ok_or_else(|| StartupAuthorityError("missing catalog".into()))?
                .read(g)
                .await
        })
    }
    fn admit(
        &self,
        r: StartupRequest,
    ) -> StartupAuthorityFuture<
        '_,
        Result<StartupAuthorityResult<StartupAdmission>, StartupAuthorityError>,
    > {
        Box::pin(async move {
            self.0
                .get(&r.group_id)
                .ok_or_else(|| StartupAuthorityError("missing catalog".into()))?
                .admit(r)
                .await
        })
    }
    fn bind_active(
        &self,
        b: StartupBinding,
    ) -> StartupAuthorityFuture<
        '_,
        Result<StartupAuthorityResult<StartupBinding>, StartupAuthorityError>,
    > {
        Box::pin(async move {
            self.0
                .get(&b.group_id)
                .ok_or_else(|| StartupAuthorityError("missing catalog".into()))?
                .bind_active(b)
                .await
        })
    }
    fn terminal_handoff(
        &self,
        p: StartupTerminalProof,
    ) -> StartupAuthorityFuture<
        '_,
        Result<StartupAuthorityResult<StartupTerminalProof>, StartupAuthorityError>,
    > {
        Box::pin(async move {
            self.0
                .get(&p.group_id)
                .ok_or_else(|| StartupAuthorityError("missing catalog".into()))?
                .terminal_handoff(p)
                .await
        })
    }
}
pub struct Fixture {
    pub root: PathBuf,
    pub layout: ClusterLayout,
    pub directory: TrustedSlotDirectory,
    pub authority: Arc<Catalogs>,
    pub owners: Vec<SlotNodeOwner<DiskImage>>,
    pub factory_calls: Arc<AtomicU64>,
}
impl Fixture {
    pub async fn fresh() -> Self {
        let parent = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/controlled-slot");
        std::fs::create_dir_all(&parent).unwrap();
        let root = parent.join(format!(
            "{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        let layout = plan_layout(&PlacementInput {
            cluster_id: "controlled".into(),
            slot_count: 12,
            group_id_base: 100,
            nodes: (1..=3)
                .map(|node_id| PlacementNode {
                    node_id,
                    raft: address(),
                    application_rpc: address(),
                    weight: 1,
                })
                .collect(),
        })
        .unwrap();
        let mut nodes = layout.nodes().to_vec();
        nodes.push(NodeAddress {
            node_id: 4,
            raft: address(),
            application_rpc: address(),
        });
        let directory = TrustedSlotDirectory {
            version: 1,
            data_space_id: 77,
            genesis_digest: layout.digest(),
            nodes,
        };
        let mut catalogs = BTreeMap::new();
        for a in &layout.config().slots {
            let group = layout.group_for_slot(a.slot).unwrap();
            let membership = StoredMembership::new(
                None,
                Membership::new(
                    vec![[1, 2, 3].into()],
                    [1, 2, 3]
                        .into_iter()
                        .map(|id| (id, BasicNode::default()))
                        .collect::<BTreeMap<_, _>>(),
                )
                .unwrap(),
            );
            catalogs.insert(
                group,
                Arc::new(FileAuthority::provision(
                    &root.join(format!("catalog-{group}.json")),
                    StartupCatalog {
                        group_id: group,
                        revision: 0,
                        membership,
                        authorized: [(1, 10), (2, 20), (3, 30)].into(),
                        active_operation: None,
                        admissions: vec![],
                        completed: vec![],
                    },
                )),
            );
        }
        let mut fixture = Self {
            root,
            layout,
            directory,
            authority: Arc::new(Catalogs(catalogs)),
            owners: vec![],
            factory_calls: Arc::new(AtomicU64::new(0)),
        };
        let starts = (1..=3)
            .map(|id| fixture.start(id, SlotStartupDisposition::Genesis, 1))
            .collect::<Vec<_>>();
        fixture.owners = futures::future::join_all(starts)
            .await
            .into_iter()
            .map(Result::unwrap)
            .collect();
        fixture
    }
    pub fn startup(
        &self,
        id: u64,
        disposition: SlotStartupDisposition,
        boot: u8,
    ) -> ControlledSlotStartup {
        ControlledSlotStartup {
            startup: StrictSlotStartup {
                disposition,
                identity: SlotLocalIdentity {
                    data_space_id: 77,
                    data_dir_uuid: [id as u8; 16],
                    protocol_version: CONTROLLED_SLOT_PROTOCOL_VERSION,
                    compatibility_digest: [9; 32],
                },
                directory: self.directory.clone(),
            },
            authority: self.authority.clone(),
            replicas: if id <= 3 {
                self.layout
                    .config()
                    .slots
                    .iter()
                    .map(|a| ControlledSlotReplica {
                        slot: a.slot,
                        generation: id * 10,
                        identity: vec![id as u8],
                        request_id: {
                            let mut b = [boot; 16];
                            b[0] = id as u8;
                            b[1] = a.slot.0 as u8;
                            b
                        },
                    })
                    .collect()
            } else {
                vec![]
            },
        }
    }
    pub async fn start(
        &self,
        id: u64,
        disposition: SlotStartupDisposition,
        boot: u8,
    ) -> Result<SlotNodeOwner<DiskImage>, RuntimeError> {
        let path = self.root.join(format!("node-{id}"));
        let fsm_path = path.clone();
        let calls = self.factory_calls.clone();
        let mut config = SlotNodeConfig::new(self.layout.clone(), id, path);
        config.native.heartbeat_interval_ms = 50;
        config.native.election_timeout_min_ms = 150;
        config.native.election_timeout_max_ms = 300;
        SlotNodeOwner::start_controlled(
            config,
            self.startup(id, disposition, boot),
            move |ctx: multiraft_net::FsmFactoryContext| {
                calls.fetch_add(1, Ordering::Relaxed);
                DiskImage::open(&fsm_path, ctx.group_id()).map_err(Into::into)
            },
            deadline(),
        )
        .await
    }
    pub fn handle(&self, id: u64) -> SlotHandle<DiskImage> {
        self.owners[(id - 1) as usize].handle()
    }
    pub async fn stop_owners(&mut self) {
        for owner in self.owners.drain(..) {
            assert!(owner.shutdown(deadline()).await.complete);
        }
    }
    pub async fn stop(mut self) {
        self.stop_owners().await;
        std::fs::remove_dir_all(self.root).unwrap();
    }
}
