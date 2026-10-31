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
        assert!(!handle.status().accepting);
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
        let addresses = owner.handle().local_transport_addresses().unwrap();
        assert_eq!(addresses.directory_version, revision);
        assert_eq!(
            addresses.advertised,
            *startup.directory.nodes.last().unwrap()
        );
        assert_eq!(addresses.raft_bind, raft_bind);
        assert_eq!(addresses.application_rpc_bind, app_bind);
        assert!(TcpListener::bind(raft_bind).is_err());
        assert!(TcpListener::bind(app_bind).is_err());
        assert!(owner.shutdown(deadline()).await.complete);
        assert!(TcpListener::bind(raft_bind).is_ok());
        assert!(TcpListener::bind(app_bind).is_ok());
    }
    std::fs::remove_dir_all(dir).unwrap();
}

fn source_layout() -> ClusterLayout {
    ClusterLayout::new(LayoutConfig {
        cluster_id: "strict-source".into(),
        format_version: FORMAT_VERSION,
        route_version: RouteVersion::Sha256Bucket256V1,
        bucket_count: BUCKET_COUNT,
        slot_count: 1,
        group_id_base: 100,
        nodes: vec![NodeAddress {
            node_id: 1,
            raft: address(),
            application_rpc: address(),
        }],
        buckets: vec![0; 256],
        slots: vec![SlotAssignment {
            slot: SlotId(0),
            voters: vec![1],
            preferred_initializer: 1,
            soft_preferred_leader: 1,
        }],
    })
    .unwrap()
}
fn source_startup(layout: &ClusterLayout) -> StrictSlotStartup {
    StrictSlotStartup {
        disposition: SlotStartupDisposition::Genesis,
        identity: SlotLocalIdentity {
            data_space_id: 77,
            data_dir_uuid: [1; 16],
            protocol_version: SLOT_PROTOCOL_VERSION,
            compatibility_digest: [9; 32],
        },
        directory: TrustedSlotDirectory {
            version: 1,
            data_space_id: 77,
            genesis_digest: layout.digest(),
            nodes: layout.nodes().to_vec(),
        },
    }
}
#[derive(Default)]
struct DurableCounter(u64);
impl StateMachine for DurableCounter {
    type Error = std::io::Error;
    fn apply(&mut self, _: u64, _: u64, _: &[u8]) -> Result<ApplyOut, Self::Error> {
        self.0 += 1;
        Ok(ApplyOut {
            effects: self.0.to_be_bytes().to_vec(),
        })
    }
    fn snapshot(&self, _: u64) -> Result<Vec<u8>, Self::Error> {
        Ok(self.0.to_be_bytes().to_vec())
    }
    fn restore(&mut self, _: u64, bytes: &[u8]) -> Result<(), Self::Error> {
        self.0 = u64::from_be_bytes(
            bytes
                .try_into()
                .map_err(|_| std::io::Error::other("invalid counter snapshot"))?,
        );
        Ok(())
    }
    fn freeze_bounded(
        &self,
        group: u64,
        max: usize,
    ) -> Result<Vec<u8>, multiraft_fsm::CaptureError<Self::Error>> {
        if max < 8 {
            return Err(multiraft_fsm::CaptureError::Refused(
                multiraft_fsm::CaptureRefusal::SizeLimit,
            ));
        }
        self.snapshot(group)
            .map_err(multiraft_fsm::CaptureError::Application)
    }
}
impl SlotStateMachine for DurableCounter {
    fn query(&self, _: u64, _: &[u8]) -> Result<Vec<u8>, Self::Error> {
        Ok(self.0.to_be_bytes().to_vec())
    }
}
async fn source(
    layout: &ClusterLayout,
    dir: &std::path::Path,
    startup: StrictSlotStartup,
) -> Result<SlotNodeOwner<DurableCounter>, RuntimeError> {
    let mut config = SlotNodeConfig::new(layout.clone(), 1, dir);
    config.native.heartbeat_interval_ms = 50;
    config.native.election_timeout_min_ms = 200;
    config.native.election_timeout_max_ms = 400;
    SlotNodeOwner::start_strict(
        config,
        startup,
        |_| Ok(DurableCounter::default()),
        deadline(),
    )
    .await
}
async fn source_write(owner: &SlotNodeOwner<DurableCounter>) {
    let handle = owner.handle();
    let until = deadline();
    while handle
        .leader_hint(SlotId(0), until)
        .await
        .unwrap()
        .is_none()
    {
        assert!(Instant::now() < until);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        handle
            .propose(b"key", vec![1], until)
            .await
            .unwrap()
            .effects,
        1_u64.to_be_bytes()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_restart_recovers_real_rocksdb_without_initialization_and_reports_actual_membership()
{
    let layout = source_layout();
    let mut startup = source_startup(&layout);
    let dir = directory();
    let owner = source(&layout, &dir, startup.clone()).await.unwrap();
    source_write(&owner).await;
    let observation = owner
        .handle()
        .local_group_observation(SlotId(0), deadline())
        .await
        .unwrap();
    let SlotGroupObservation::Present(observed) = observation else {
        panic!("native source observation required")
    };
    assert_eq!(
        observed.membership.committed_membership.voter_configs,
        vec![std::collections::BTreeSet::from([1])]
    );
    assert!(observed.membership.committed_membership.log_id.is_some());
    assert!(observed
        .membership
        .effective_membership
        .learner_ids
        .is_empty());
    assert!(observed.progress.last_applied.is_some());
    assert!(owner.shutdown(deadline()).await.complete);
    assert!(dir.join("raft-rocksdb/CURRENT").is_file());
    assert!(
        matches!(
            source(&layout, &dir, startup.clone()).await,
            Err(RuntimeError::ManifestMismatch)
        ),
        "existing Genesis may never reinitialize"
    );
    startup.disposition = SlotStartupDisposition::RecoverExisting;
    let owner = source(&layout, &dir, startup).await.unwrap();
    let report = &owner.startup_report().groups[0];
    assert_eq!(report.provenance, Some(StartupProvenance::Persisted));
    assert_eq!(report.initialization, InitializeDisposition::NotDispatched);
    assert!(!report.fallback);
    assert_eq!(
        owner
            .handle()
            .read_applied(SlotId(0), &[], deadline())
            .await
            .unwrap()
            .value,
        1_u64.to_be_bytes()
    );
    assert!(owner.shutdown(deadline()).await.complete);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incomplete_genesis_and_absent_source_identity_never_retry_initialization() {
    let layout = source_layout();
    let mut startup = source_startup(&layout);
    let absent = directory();
    startup.disposition = SlotStartupDisposition::RecoverExisting;
    assert!(matches!(
        source(&layout, &absent, startup.clone()).await,
        Err(RuntimeError::ManifestMismatch)
    ));
    assert!(!absent.exists());
    let dir = directory();
    startup.disposition = SlotStartupDisposition::Genesis;
    let result = SlotNodeOwner::start_strict(
        SlotNodeConfig::new(layout.clone(), 1, &dir),
        startup.clone(),
        |_| -> anyhow::Result<DurableCounter> {
            anyhow::bail!("injected genesis construction failure")
        },
        deadline(),
    )
    .await;
    assert!(matches!(result, Err(RuntimeError::StartupFailed { .. })));
    for disposition in [
        SlotStartupDisposition::Genesis,
        SlotStartupDisposition::RecoverExisting,
    ] {
        startup.disposition = disposition;
        let result = SlotNodeOwner::start_strict(
            SlotNodeConfig::new(layout.clone(), 1, &dir),
            startup.clone(),
            |_| -> anyhow::Result<DurableCounter> {
                panic!("incomplete creation fact cannot retry a factory")
            },
            deadline(),
        )
        .await;
        assert!(matches!(result, Err(RuntimeError::ManifestMismatch)));
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_identity_cannot_reinitialize_when_native_storage_is_wiped() {
    let layout = source_layout();
    let mut startup = source_startup(&layout);
    let dir = directory();
    let owner = source(&layout, &dir, startup.clone()).await.unwrap();
    source_write(&owner).await;
    assert!(owner.shutdown(deadline()).await.complete);
    std::fs::remove_dir_all(dir.join("raft-rocksdb")).unwrap();
    startup.disposition = SlotStartupDisposition::RecoverExisting;
    let count = Arc::new(AtomicU64::new(0));
    for remove_marker in [false, true] {
        if remove_marker {
            std::fs::remove_file(dir.join("raft-log-backend")).unwrap();
        }
        let calls = count.clone();
        let result = SlotNodeOwner::start_strict(
            SlotNodeConfig::new(layout.clone(), 1, &dir),
            startup.clone(),
            move |_| {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(DurableCounter::default())
            },
            deadline(),
        )
        .await;
        assert!(
            result.is_err(),
            "missing native basis must not initialize, marker removed={remove_marker}"
        );
        assert_eq!(
            count.load(Ordering::Relaxed),
            0,
            "check local basis before application factory"
        );
        assert!(dir.join("slot-manifest.json").is_file());
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_restart_restores_a_durable_native_snapshot_without_bootstrap() {
    let layout = source_layout();
    let mut startup = source_startup(&layout);
    let dir = directory();
    let owner = source(&layout, &dir, startup.clone()).await.unwrap();
    source_write(&owner).await;
    let handle = owner.handle();
    let until = deadline();
    handle.request_compaction(SlotId(0), until).await.unwrap();
    loop {
        if handle
            .local_storage_status(SlotId(0), until)
            .await
            .unwrap()
            .durable_snapshot
            .is_some()
        {
            break;
        }
        assert!(
            Instant::now() < until,
            "durable native snapshot was not published"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(owner.shutdown(deadline()).await.complete);
    startup.disposition = SlotStartupDisposition::RecoverExisting;
    let owner = source(&layout, &dir, startup).await.unwrap();
    assert_eq!(
        owner
            .handle()
            .read_applied(SlotId(0), &[], deadline())
            .await
            .unwrap()
            .value,
        1_u64.to_be_bytes()
    );
    assert_eq!(
        owner.startup_report().groups[0].initialization,
        InitializeDisposition::NotDispatched
    );
    assert!(owner.shutdown(deadline()).await.complete);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn successful_rf3_genesis_is_immediately_recoverable_for_every_owned_slot() {
    let layout = layout();
    let dirs: Vec<_> = (0..3).map(|_| directory()).collect();
    let make_startup = |node| StrictSlotStartup {
        disposition: SlotStartupDisposition::Genesis,
        identity: SlotLocalIdentity {
            data_space_id: 77,
            data_dir_uuid: [node as u8; 16],
            protocol_version: SLOT_PROTOCOL_VERSION,
            compatibility_digest: [9; 32],
        },
        directory: TrustedSlotDirectory {
            version: 1,
            data_space_id: 77,
            genesis_digest: layout.digest(),
            nodes: layout.nodes().to_vec(),
        },
    };
    let start = |node: u64, dir: PathBuf, plan: StrictSlotStartup| {
        let mut config = SlotNodeConfig::new(layout.clone(), node, dir);
        config.native.heartbeat_interval_ms = 30;
        config.native.election_timeout_min_ms = 120;
        config.native.election_timeout_max_ms = 240;
        config.startup.bootstrap_grace = Duration::from_millis(100);
        SlotNodeOwner::start_strict(config, plan, |_| Ok(DurableCounter::default()), deadline())
    };
    let (a, b, c) = tokio::join!(
        start(1, dirs[0].clone(), make_startup(1)),
        start(2, dirs[1].clone(), make_startup(2)),
        start(3, dirs[2].clone(), make_startup(3))
    );
    let (a, b, c) = (a.unwrap(), b.unwrap(), c.unwrap());
    assert!(a.shutdown(deadline()).await.complete);
    // Once node 1 is closed, peer shutdown cannot repair its local recovery evidence.
    assert!(b.shutdown(deadline()).await.complete);
    assert!(c.shutdown(deadline()).await.complete);
    let mut recover = make_startup(1);
    recover.disposition = SlotStartupDisposition::RecoverExisting;
    let restarted = start(1, dirs[0].clone(), recover).await;
    assert!(
        restarted.is_ok(),
        "successful genesis must permit immediate local recovery without peers: {:?}",
        restarted.as_ref().err()
    );
    let restarted = restarted.unwrap();
    for group in &restarted.startup_report().groups {
        assert_eq!(group.initialization, InitializeDisposition::NotDispatched);
        assert_eq!(group.provenance, Some(StartupProvenance::Persisted));
    }
    for slot in 0..12 {
        let applied = restarted
            .handle()
            .read_applied(SlotId(slot), &[], deadline())
            .await
            .unwrap();
        assert!(
            applied.applied.is_some(),
            "slot{slot} must retain its actual native applied membership"
        );
        assert_eq!(applied.value, 0_u64.to_be_bytes());
    }
    assert!(restarted.shutdown(deadline()).await.complete);
    for dir in dirs {
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn genesis_cannot_seal_complete_before_local_memberships_are_committed_and_applied() {
    let layout = layout();
    let dir = directory();
    let plan = StrictSlotStartup {
        disposition: SlotStartupDisposition::Genesis,
        identity: SlotLocalIdentity {
            data_space_id: 77,
            data_dir_uuid: [1; 16],
            protocol_version: SLOT_PROTOCOL_VERSION,
            compatibility_digest: [9; 32],
        },
        directory: TrustedSlotDirectory {
            version: 1,
            data_space_id: 77,
            genesis_digest: layout.digest(),
            nodes: layout.nodes().to_vec(),
        },
    };
    let mut config = SlotNodeConfig::new(layout, 1, &dir);
    config.startup.bootstrap_grace = Duration::from_millis(100);
    let result = SlotNodeOwner::start_strict(
        config,
        plan,
        |_| Ok(DurableCounter::default()),
        Instant::now() + Duration::from_millis(600),
    )
    .await;
    assert!(
        matches!(&result,Err(RuntimeError::StartupFailed {phase,..}) if phase=="genesis_basis"),
        "unproven Genesis must fail at its local basis barrier: {:?}",
        result.as_ref().err()
    );
    let accepted = result.is_ok();
    if let Ok(owner) = result {
        assert!(owner.shutdown(deadline()).await.complete);
    }
    assert!(
        !accepted,
        "RF3 without quorum must not report successful durable Genesis completion"
    );
    assert!(
        !dir.join(".strict-slot.genesis-complete").exists(),
        "incomplete genesis must remain explicitly pending"
    );
    std::fs::remove_dir_all(dir).unwrap();
}
