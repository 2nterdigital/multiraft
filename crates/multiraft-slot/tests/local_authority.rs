//! The Slot facade preserves native authority and serves read-only missing probes on candidates.
#[path = "../../multiraft-net/tests/local_authority_support/disk.rs"]
mod disk;
use disk::Disk;
use multiraft_slot::*;
use std::{
    net::TcpListener,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::time::Instant;
impl SlotStateMachine for Disk {
    fn query(&self, _: u64, _: &[u8]) -> std::io::Result<Vec<u8>> {
        Ok(vec![])
    }
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}
fn address() -> std::net::SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}
fn layout() -> ClusterLayout {
    ClusterLayout::new(LayoutConfig {
        cluster_id: "local-authority".into(),
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
fn root() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../.tmp/slot-local-authority")
        .join(format!(
            "{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
    std::fs::create_dir_all(&root).unwrap();
    root
}
fn startup(layout: &ClusterLayout, candidate: bool) -> StrictSlotStartup {
    let mut nodes = layout.nodes().to_vec();
    if candidate {
        nodes.push(NodeAddress {
            node_id: 2,
            raft: address(),
            application_rpc: address(),
        });
    }
    StrictSlotStartup {
        disposition: if candidate {
            SlotStartupDisposition::Candidate
        } else {
            SlotStartupDisposition::Genesis
        },
        identity: SlotLocalIdentity {
            data_space_id: 77,
            data_dir_uuid: [if candidate { 2 } else { 1 }; 16],
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn candidate_authority_probe_is_local_missing_without_constructing_or_forwarding() {
    let layout = layout();
    let root = root();
    let owner = SlotNodeOwner::<Disk>::start_strict(
        SlotNodeConfig::new(layout.clone(), 2, &root),
        startup(&layout, true),
        |_| panic!("read-only candidate probe must never construct a native group"),
        deadline(),
    )
    .await
    .unwrap();
    let handle = owner.handle();
    assert_eq!(
        handle
            .local_authoritative_membership(SlotId(0), deadline())
            .await,
        Err(LocalAuthorityError::LocalMissing)
    );
    assert_eq!(
        handle
            .local_authoritative_membership(SlotId(1), deadline())
            .await,
        Err(LocalAuthorityError::InvalidSlot)
    );
    assert_eq!(
        handle
            .local_authoritative_membership(SlotId(0), Instant::now())
            .await,
        Err(LocalAuthorityError::Deadline)
    );
    assert_eq!(handle.inflight().global.requests, 0);
    assert!(owner.shutdown(deadline()).await.complete);
    assert_eq!(
        handle
            .local_authoritative_membership(SlotId(0), deadline())
            .await,
        Err(LocalAuthorityError::Closed)
    );
    std::fs::remove_dir_all(root).unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slot_leader_returns_full_native_membership_and_control_facts() {
    let layout = layout();
    let root = root();
    let factory_root = root.clone();
    let owner = SlotNodeOwner::start_strict(
        SlotNodeConfig::new(layout.clone(), 1, &root),
        startup(&layout, false),
        move |context: multiraft_net::FsmFactoryContext| {
            Ok(Disk::open(&factory_root, context.group_id())?)
        },
        deadline(),
    )
    .await
    .unwrap();
    let handle = owner.handle();
    let until = deadline();
    let snapshot = loop {
        match handle
            .local_authoritative_membership(SlotId(0), until)
            .await
        {
            Ok(snapshot) => break snapshot,
            Err(LocalAuthorityError::NotLeader { .. } | LocalAuthorityError::Unavailable)
                if Instant::now() < until =>
            {
                tokio::time::sleep(Duration::from_millis(10)).await
            }
            other => panic!("local Slot authority: {other:?}"),
        }
    };
    assert_eq!(snapshot.group_id, 100);
    assert_eq!(snapshot.node_id, 1);
    assert_eq!(
        snapshot
            .membership
            .membership()
            .voter_ids()
            .collect::<Vec<_>>(),
        vec![1]
    );
    assert_eq!(snapshot.ledger.high_water, 0);
    assert!(snapshot.ledger.current.is_none());
    assert_eq!(
        serde_json::from_slice::<LocalAuthoritativeMembership>(
            &serde_json::to_vec(&snapshot).unwrap()
        )
        .unwrap(),
        snapshot
    );
    assert_eq!(handle.inflight().global.requests, 0);
    assert!(owner.shutdown(deadline()).await.complete);
    assert_eq!(
        handle
            .local_authoritative_membership(SlotId(0), deadline())
            .await,
        Err(LocalAuthorityError::Closed)
    );
    assert_eq!(handle.inflight().global.requests, 0);
    std::fs::remove_dir_all(root).unwrap();
}
