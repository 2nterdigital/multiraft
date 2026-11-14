//! Strict public startup: candidate identity never admits native Group creation.
use multiraft_fsm::{ApplyOut, StateMachine};
use multiraft_slot::*;
use std::{
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
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/strict-slot");
    std::fs::create_dir_all(&root).unwrap();
    root.join(format!(
        "{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}
fn layout() -> ClusterLayout {
    plan_layout(&PlacementInput {
        cluster_id: "strict".into(),
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
    .unwrap()
}
fn startup(layout: &ClusterLayout) -> StrictSlotStartup {
    let mut nodes = layout.nodes().to_vec();
    nodes.push(NodeAddress {
        node_id: 4,
        raft: address(),
        application_rpc: address(),
    });
    StrictSlotStartup {
        disposition: SlotStartupDisposition::Candidate,
        identity: SlotLocalIdentity {
            data_space_id: 77,
            data_dir_uuid: [4; 16],
            protocol_version: SLOT_PROTOCOL_VERSION,
            compatibility_digest: [9; 32],
        },
        directory: TrustedSlotDirectory {
            version: 1,
            data_space_id: 77,
            genesis_digest: layout.digest(),
            nodes,
        },
    }
}
struct Counter;
impl StateMachine for Counter {
    type Error = std::io::Error;
    fn apply(&mut self, _: u64, _: u64, _: &[u8]) -> Result<ApplyOut, Self::Error> {
        Ok(ApplyOut::default())
    }
    fn snapshot(&self, _: u64) -> Result<Vec<u8>, Self::Error> {
        Ok(vec![])
    }
    fn restore(&mut self, _: u64, _: &[u8]) -> Result<(), Self::Error> {
        Ok(())
    }
}
impl SlotStateMachine for Counter {
    fn query(&self, _: u64, _: &[u8]) -> Result<Vec<u8>, Self::Error> {
        Ok(vec![])
    }
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(15)
}

use multiraft_net::controlled_startup::*;
struct Unavailable;
impl StartupAuthority for Unavailable {
    fn read(
        &self,
        _: u64,
    ) -> StartupAuthorityFuture<'_, Result<StartupCatalog, StartupAuthorityError>> {
        Box::pin(async { Err(StartupAuthorityError("catalog unavailable".into())) })
    }
    fn admit(
        &self,
        _: StartupRequest,
    ) -> StartupAuthorityFuture<
        '_,
        Result<StartupAuthorityResult<StartupAdmission>, StartupAuthorityError>,
    > {
        Box::pin(async { panic!("read must precede admission") })
    }
    fn bind_active(
        &self,
        _: StartupBinding,
    ) -> StartupAuthorityFuture<
        '_,
        Result<StartupAuthorityResult<StartupBinding>, StartupAuthorityError>,
    > {
        Box::pin(async { panic!("startup cannot bind") })
    }
    fn terminal_handoff(
        &self,
        _: StartupTerminalProof,
    ) -> StartupAuthorityFuture<
        '_,
        Result<StartupAuthorityResult<StartupTerminalProof>, StartupAuthorityError>,
    > {
        Box::pin(async { panic!("startup cannot handoff") })
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unavailable_catalog_never_constructs_a_controlled_candidate() {
    let layout = layout();
    let mut strict = startup(&layout);
    strict.identity.protocol_version = CONTROLLED_SLOT_PROTOCOL_VERSION;
    let dir = directory();
    let count = Arc::new(AtomicU64::new(0));
    let calls = count.clone();
    let result = SlotNodeOwner::start_controlled(
        SlotNodeConfig::new(layout, 4, &dir),
        ControlledSlotStartup {
            startup: strict,
            authority: Arc::new(Unavailable),
            replicas: vec![],
        },
        move |_| {
            calls.fetch_add(1, Ordering::Relaxed);
            Ok(Counter)
        },
        deadline(),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(count.load(Ordering::Relaxed), 0);
    assert!(!dir.exists(), "catalog is checked before local birth");
}
