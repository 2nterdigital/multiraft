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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn candidate_and_candidate_restart_keep_zero_groups_and_closed_business() {
    let layout = layout();
    let startup = startup(&layout);
    let dir = directory();
    let count = Arc::new(AtomicU64::new(0));
    for _ in 0..2 {
        let calls = count.clone();
        let owner = SlotNodeOwner::start_strict(
            SlotNodeConfig::new(layout.clone(), 4, &dir),
            startup.clone(),
            move |_| {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(Counter)
            },
            deadline(),
        )
        .await
        .unwrap();
        assert!(owner.startup_report().groups.is_empty());
        let handle = owner.handle();
        for slot in 0..12 {
            assert!(matches!(
                handle
                    .local_group_observation(SlotId(slot), deadline())
                    .await
                    .unwrap(),
                SlotGroupObservation::LocalMissing
            ));
        }
        assert!(matches!(
            handle.propose(b"key", vec![1], deadline()).await,
            Err(RuntimeError::Unavailable)
        ));
        assert_eq!(count.load(Ordering::Relaxed), 0);
        assert!(owner.shutdown(deadline()).await.complete);
        assert!(matches!(
            handle.local_group_observation(SlotId(0), deadline()).await,
            Err(RuntimeError::Closed)
        ));
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn candidate_identity_directory_and_unexpected_storage_fail_closed() {
    let layout = layout();
    let startup = startup(&layout);
    let dir = directory();
    let owner = SlotNodeOwner::start_strict(
        SlotNodeConfig::new(layout.clone(), 4, &dir),
        startup.clone(),
        |_| Ok(Counter),
        deadline(),
    )
    .await
    .unwrap();
    assert!(owner.shutdown(deadline()).await.complete);
    let mut wrong_uuid = startup.clone();
    wrong_uuid.identity.data_dir_uuid[0] = 5;
    let mut wrong_space = startup.clone();
    wrong_space.identity.data_space_id = 78;
    wrong_space.directory.data_space_id = 78;
    let mut wrong_protocol = startup.clone();
    wrong_protocol.identity.protocol_version += 1;
    let mut wrong_compat = startup.clone();
    wrong_compat.identity.compatibility_digest[0] = 8;
    let mut wrong_origin = startup.clone();
    wrong_origin.directory.genesis_digest.0[0] ^= 1;
    let mut missing_source = startup.clone();
    missing_source.directory.nodes.remove(0);
    let mut wrong_node = startup.clone();
    wrong_node.directory.nodes.last_mut().unwrap().node_id = 5;
    for (node, rejected) in [
        (4, wrong_uuid),
        (4, wrong_space),
        (4, wrong_protocol),
        (4, wrong_compat),
        (4, wrong_origin),
        (4, missing_source),
        (5, wrong_node),
    ] {
        let result = SlotNodeOwner::start_strict(
            SlotNodeConfig::new(layout.clone(), node, &dir),
            rejected,
            |_| -> anyhow::Result<Counter> {
                panic!("rejected startup must not construct any FSM")
            },
            deadline(),
        )
        .await;
        assert!(matches!(
            result,
            Err(RuntimeError::InvalidConfig | RuntimeError::ManifestMismatch)
        ));
    }
    std::fs::create_dir(dir.join("snapshots/100")).unwrap();
    let result = SlotNodeOwner::start_strict(
        SlotNodeConfig::new(layout, 4, &dir),
        startup,
        |_| -> anyhow::Result<Counter> { panic!("candidate must not adopt native storage") },
        deadline(),
    )
    .await;
    assert!(matches!(result, Err(RuntimeError::ManifestMismatch)));
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn candidate_directory_revisions_and_independent_listener_binds_preserve_genesis_identity() {
    let layout = layout();
    let mut startup = startup(&layout);
    let dir = directory();
    let raft_bind = address();
    let app_bind = address();
    for revision in 1..=2 {
        startup.directory.version = revision;
        startup.directory.nodes.last_mut().unwrap().raft = address();
        let mut config = SlotNodeConfig::new(layout.clone(), 4, &dir);
        config.transport = Some(SlotTransportConfig {
            nodes: startup
                .directory
                .nodes
                .iter()
                .map(|n| SlotTransportNode {
                    node_id: n.node_id,
                    raft: n.raft.to_string(),
                    application_rpc: n.application_rpc.to_string(),
                })
                .collect(),
            raft_bind,
            application_rpc_bind: app_bind,
        });
        let owner = SlotNodeOwner::start_strict(
            config,
            startup.clone(),
            |_| -> anyhow::Result<Counter> {
                panic!("candidate directory change cannot construct FSMs")
            },
            deadline(),
        )
        .await
        .unwrap();
        assert_eq!(owner.handle().layout(), &layout);
        assert!(TcpListener::bind(raft_bind).is_err());
        assert!(TcpListener::bind(app_bind).is_err());
        assert!(owner.shutdown(deadline()).await.complete);
        assert!(TcpListener::bind(raft_bind).is_ok());
        assert!(TcpListener::bind(app_bind).is_ok());
    }
    std::fs::remove_dir_all(dir).unwrap();
}
