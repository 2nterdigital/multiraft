//! Strict transport identity fences native consensus and forwarded Slot operations.
use multiraft_fsm::{ApplyOut, StateMachine};
use multiraft_slot::*;
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
fn address() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}
fn directory() -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/strict-transport");
    std::fs::create_dir_all(&root).unwrap();
    root.join(format!(
        "{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}
fn layout() -> ClusterLayout {
    ClusterLayout::new(LayoutConfig {
        cluster_id: "strict-transport".into(),
        format_version: FORMAT_VERSION,
        route_version: RouteVersion::Sha256Bucket256V1,
        bucket_count: BUCKET_COUNT,
        slot_count: 1,
        group_id_base: 100,
        nodes: (1..=4)
            .map(|node_id| NodeAddress {
                node_id,
                raft: address(),
                application_rpc: address(),
            })
            .collect(),
        buckets: vec![0; 256],
        slots: vec![SlotAssignment {
            slot: SlotId(0),
            voters: vec![1, 2, 3],
            preferred_initializer: 1,
            soft_preferred_leader: 1,
        }],
    })
    .unwrap()
}
fn startup(layout: &ClusterLayout, node: u64, space: u64, compat: u8) -> StrictSlotStartup {
    StrictSlotStartup {
        disposition: SlotStartupDisposition::Genesis,
        identity: SlotLocalIdentity {
            data_space_id: space,
            data_dir_uuid: [node as u8; 16],
            protocol_version: SLOT_PROTOCOL_VERSION,
            compatibility_digest: [compat; 32],
        },
        directory: TrustedSlotDirectory {
            version: node,
            data_space_id: space,
            genesis_digest: layout.digest(),
            nodes: layout.nodes().to_vec(),
        },
    }
}
struct Counter(Arc<AtomicU64>);
impl StateMachine for Counter {
    type Error = std::io::Error;
    fn apply(&mut self, _: u64, _: u64, _: &[u8]) -> Result<ApplyOut, Self::Error> {
        let n = self.0.fetch_add(1, Ordering::Relaxed) + 1;
        Ok(ApplyOut {
            effects: n.to_be_bytes().to_vec(),
        })
    }
    fn snapshot(&self, _: u64) -> Result<Vec<u8>, Self::Error> {
        Ok(self.0.load(Ordering::Relaxed).to_be_bytes().to_vec())
    }
    fn restore(&mut self, _: u64, data: &[u8]) -> Result<(), Self::Error> {
        self.0.store(
            u64::from_be_bytes(data.try_into().unwrap()),
            Ordering::Relaxed,
        );
        Ok(())
    }
}
impl SlotStateMachine for Counter {
    fn query(&self, _: u64, _: &[u8]) -> Result<Vec<u8>, Self::Error> {
        Ok(self.0.load(Ordering::Relaxed).to_be_bytes().to_vec())
    }
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}
async fn start(
    layout: &ClusterLayout,
    node: u64,
    space: u64,
    compat: u8,
) -> (SlotNodeOwner<Counter>, Arc<AtomicU64>, PathBuf) {
    let (owner, counter, dir) = attempt_start(layout, node, space, compat, deadline()).await;
    (owner.unwrap(), counter, dir)
}
async fn attempt_start(
    layout: &ClusterLayout,
    node: u64,
    space: u64,
    compat: u8,
    until: Instant,
) -> (
    Result<SlotNodeOwner<Counter>, RuntimeError>,
    Arc<AtomicU64>,
    PathBuf,
) {
    let dir = directory();
    let counter = Arc::new(AtomicU64::new(0));
    let fsm = counter.clone();
    let mut config = SlotNodeConfig::new(layout.clone(), node, &dir);
    config.native.heartbeat_interval_ms = 30;
    config.native.election_timeout_min_ms = 120;
    config.native.election_timeout_max_ms = 240;
    config.startup.bootstrap_grace = Duration::from_millis(100);
    let owner = SlotNodeOwner::start_strict(
        config,
        startup(layout, node, space, compat),
        move |_| Ok(Counter(fsm.clone())),
        until,
    )
    .await;
    (owner, counter, dir)
}
async fn wait_leader(handle: &SlotHandle<Counter>) {
    let until = deadline();
    while handle
        .leader_hint(SlotId(0), until)
        .await
        .unwrap()
        .is_none()
    {
        assert!(Instant::now() < until);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
async fn stop(owner: SlotNodeOwner<Counter>, dir: PathBuf) {
    assert!(owner.shutdown(deadline()).await.complete);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn foreign_space_or_compatibility_cannot_supply_native_quorum() {
    for (space, compat) in [(78, 9), (77, 8)] {
        let layout = layout();
        let until = Instant::now() + Duration::from_millis(700);
        let (a, b) = tokio::join!(
            attempt_start(&layout, 1, 77, 9, until),
            attempt_start(&layout, 2, space, compat, until)
        );
        // Foreign votes must not allow the committed/applied genesis barrier to complete.
        for (node, (owner, counter, dir)) in [(1, a), (2, b)] {
            assert!(owner.is_err(),"foreign space={space} compatibility={compat} supplied native genesis quorum for node{node}");
            assert_eq!(
                counter.load(Ordering::Relaxed),
                0,
                "foreign native quorum applied a command"
            );
            assert!(!dir.join(".strict-slot.genesis-complete").exists());
            let addresses = layout.node(node).unwrap();
            assert!(
                TcpListener::bind(addresses.raft).is_ok(),
                "failure must join native listener cleanup"
            );
            assert!(TcpListener::bind(addresses.application_rpc).is_ok());
            std::fs::remove_dir_all(dir).unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn common_identity_replication_ignores_local_uuid_and_directory_revision_but_slot_rpc_rejects_foreign_identity(
) {
    let layout = layout();
    let (a, b, c) = tokio::join!(
        start(&layout, 1, 77, 9),
        start(&layout, 2, 77, 9),
        start(&layout, 3, 77, 9)
    );
    let mut nodes = BTreeMap::from([(1, a), (2, b), (3, c)]);
    wait_leader(&nodes[&1].0.handle()).await;
    assert_eq!(
        nodes[&1]
            .0
            .handle()
            .propose(b"key", vec![1], deadline())
            .await
            .unwrap()
            .effects,
        1_u64.to_be_bytes()
    );
    for (space, compat, expected_ok) in [(77, 9, true), (78, 9, false), (77, 8, false)] {
        let router = start(&layout, 4, space, compat).await;
        let result = router
            .0
            .handle()
            .propose(b"key", vec![1], Instant::now() + Duration::from_secs(2))
            .await;
        assert_eq!(
            result.is_ok(),
            expected_ok,
            "Slot RPC space={space} compatibility={compat}: {result:?}"
        );
        stop(router.0, router.2).await;
    }
    for (_, node) in std::mem::take(&mut nodes) {
        let until = deadline();
        while node.1.load(Ordering::Relaxed) < 2 {
            assert!(Instant::now() < until);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            node.1.load(Ordering::Relaxed),
            2,
            "foreign forwarded RPC applied a command"
        );
        stop(node.0, node.2).await;
    }
}
