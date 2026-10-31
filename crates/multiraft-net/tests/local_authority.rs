//! Local authority comes from native ReadIndex and a coherent durable FSM capture.
#[path = "local_authority_support/disk.rs"]
mod disk;
use disk::Disk;
use multiraft_core::{ClusterConfig, LogStorageBackend, NativeLedger, SnapshotMode};
use multiraft_net::{
    GroupConfig, LocalAuthorityError, NodeOwner, RuntimeConfig, RuntimeTransport, SharedFabric,
};
use std::{path::Path, time::Duration};
use tokio::time::Instant;
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}
fn config(root: &Path, node: u64, voters: &[u64], fabric: SharedFabric) -> RuntimeConfig {
    let mut cluster = ClusterConfig::for_test(node, voters);
    cluster.data_dir = root.join(format!("node-{node}"));
    cluster.log_storage_backend = LogStorageBackend::RocksDb;
    cluster.snapshot_mode = SnapshotMode::NativeDurable;
    let mut config = RuntimeConfig::new(
        cluster,
        vec![GroupConfig {
            group_id: 7,
            voters: voters.to_vec(),
        }],
    );
    config.transport = RuntimeTransport::InProcess(fabric);
    config
}
fn scratch() -> tempfile::TempDir {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/local-authority");
    std::fs::create_dir_all(&root).unwrap();
    tempfile::tempdir_in(root).unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owned_leader_captures_exact_membership_and_durable_ledger() {
    let dir = scratch();
    let root = dir.path().to_path_buf();
    let owner = NodeOwner::start(
        config(&root, 1, &[1], SharedFabric::new()),
        move |context: multiraft_net::FsmFactoryContext| Ok(Disk::open(&root, context.group_id())?),
        deadline(),
    )
    .await
    .unwrap();
    let handle = owner.handle();
    let until = deadline();
    let snapshot = loop {
        match handle.local_authoritative_membership(7, until).await {
            Ok(snapshot) => break snapshot,
            Err(LocalAuthorityError::NotLeader { .. } | LocalAuthorityError::Unavailable)
                if Instant::now() < until =>
            {
                tokio::time::sleep(Duration::from_millis(10)).await
            }
            other => panic!("native leader authority: {other:?}"),
        }
    };
    assert_eq!(snapshot.group_id, 7);
    assert_eq!(snapshot.node_id, 1);
    assert_eq!(
        snapshot
            .membership
            .membership()
            .voter_ids()
            .collect::<Vec<_>>(),
        vec![1]
    );
    assert!(snapshot.membership.log_id().is_some());
    assert!(snapshot.applied >= snapshot.membership.log_id().unwrap());
    assert!(snapshot
        .read_barrier
        .is_none_or(|barrier| snapshot.applied >= barrier));
    assert_eq!(snapshot.ledger, NativeLedger::default());
    let encoded = serde_json::to_vec(&snapshot).unwrap();
    assert_eq!(
        serde_json::from_slice::<multiraft_net::LocalAuthoritativeMembership>(&encoded).unwrap(),
        snapshot
    );
    owner.shutdown(deadline()).await.unwrap();
    assert_eq!(
        handle.local_authoritative_membership(7, deadline()).await,
        Err(LocalAuthorityError::Closed)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_group_probe_never_constructs_or_initializes() {
    let dir = scratch();
    let mut config = config(dir.path(), 1, &[1], SharedFabric::new());
    config.groups.clear();
    let owner = NodeOwner::<Disk>::start(
        config,
        |_| panic!("read-only probe must not construct an FSM"),
        deadline(),
    )
    .await
    .unwrap();
    let handle = owner.handle();
    assert_eq!(
        handle.local_authoritative_membership(99, deadline()).await,
        Err(LocalAuthorityError::LocalMissing)
    );
    assert_eq!(
        handle
            .local_authoritative_membership(99, Instant::now())
            .await,
        Err(LocalAuthorityError::Deadline)
    );
    owner.shutdown(deadline()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rf3_followers_cannot_serve_false_authority() {
    let dir = scratch();
    let fabric = SharedFabric::new();
    let mut starts = Vec::new();
    for id in 1..=3 {
        let root = dir.path().to_path_buf();
        let config = config(&root, id, &[1, 2, 3], fabric.clone());
        let root = config.cluster.data_dir.clone();
        starts.push(NodeOwner::start(
            config,
            move |context: multiraft_net::FsmFactoryContext| {
                Ok(Disk::open(&root, context.group_id())?)
            },
            deadline(),
        ));
    }
    let owners: Vec<_> = futures::future::join_all(starts)
        .await
        .into_iter()
        .map(Result::unwrap)
        .collect();
    let until = deadline();
    let (leader, snapshot) = loop {
        let mut found = None;
        for (index, owner) in owners.iter().enumerate() {
            if let Ok(snapshot) = owner
                .handle()
                .local_authoritative_membership(7, until)
                .await
            {
                found = Some((index, snapshot));
                break;
            }
        }
        if let Some(found) = found {
            break found;
        }
        assert!(Instant::now() < until, "real RF3 leader must form");
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert_eq!(
        snapshot
            .membership
            .membership()
            .voter_ids()
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    for (index, owner) in owners
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != leader)
    {
        assert!(
            matches!(
                owner
                    .handle()
                    .local_authoritative_membership(7, deadline())
                    .await,
                Err(LocalAuthorityError::NotLeader { .. })
            ),
            "follower {index} must refuse local authority"
        );
    }
    for owner in owners {
        owner.shutdown(deadline()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_memory_fsm_without_durable_control_ledger_is_unavailable() {
    let mut config = RuntimeConfig::new(
        ClusterConfig::for_test(1, &[1]),
        vec![GroupConfig {
            group_id: 7,
            voters: vec![1],
        }],
    );
    config.transport = RuntimeTransport::InProcess(SharedFabric::new());
    let owner = NodeOwner::start(config, |_| Ok(multiraft_fsm::CounterFsm::new()), deadline())
        .await
        .unwrap();
    let handle = owner.handle();
    let until = deadline();
    loop {
        match handle.read_linearizable(7, until, |_| ()).await {
            Ok(()) => break,
            Err(multiraft_net::RuntimeError::Source(
                multiraft_core::MultiRaftError::NotLeader { .. },
            )) if Instant::now() < until => tokio::time::sleep(Duration::from_millis(10)).await,
            other => panic!("single node must form: {other:?}"),
        }
    }
    assert_eq!(
        handle.local_authoritative_membership(7, until).await,
        Err(LocalAuthorityError::Unavailable)
    );
    owner.shutdown(deadline()).await.unwrap();
}
