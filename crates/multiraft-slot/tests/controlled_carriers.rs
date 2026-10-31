//! Post-start carrier installation through the controlled Slot boundary.
mod controlled_support;
use controlled_support::disk::DiskImage;
use controlled_support::*;
use multiraft_net::{CarrierCatalog, CarrierPeer};
use multiraft_slot::*;
fn catalog(f: &Fixture, extra: NodeAddress, version: u64) -> CarrierCatalog {
    let mut peers = f
        .directory
        .nodes
        .iter()
        .map(|n| CarrierPeer {
            node_id: n.node_id,
            identity: format!("persistent-passport-{}", n.node_id).into_bytes(),
            raft: n.raft,
            application_rpc: n.application_rpc,
        })
        .collect::<Vec<_>>();
    peers.push(CarrierPeer {
        node_id: extra.node_id,
        identity: b"persistent-passport-4".to_vec(),
        raft: extra.raft,
        application_rpc: extra.application_rpc,
    });
    CarrierCatalog {
        version,
        scope: Some(f.handle(1).transport_domain().unwrap()),
        peers,
    }
}
async fn candidate(f: &Fixture, node: NodeAddress) -> SlotNodeOwner<DiskImage> {
    let mut startup = f.startup(4, SlotStartupDisposition::Candidate, 1);
    startup.startup.directory.nodes = vec![node];
    let path = f.root.join("node-4");
    SlotNodeOwner::start_controlled(
        SlotNodeConfig::new(f.layout.clone(), 4, &path),
        startup,
        |_| panic!("carrier-only candidate must construct no Core"),
        deadline(),
    )
    .await
    .unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn install_ack_requires_both_pools_and_changes_no_native_authority() {
    let mut f = Fixture::fresh_sources().await;
    assert!(f.directory.nodes.iter().all(|node| node.node_id != 4));
    for id in 1..=3 {
        assert_eq!(
            f.handle(id).carrier_catalog(deadline()).await.unwrap(),
            None
        );
    }
    let node = NodeAddress {
        node_id: 4,
        raft: address(),
        application_rpc: address(),
    };
    let added = candidate(&f, node.clone()).await;
    f.owners.push(added);
    assert_eq!(
        f.handle(4)
            .local_role(SlotId(0), deadline())
            .await
            .unwrap()
            .role,
        LocalSlotRole::Missing
    );
    let before = f.factory_calls.load(std::sync::atomic::Ordering::Relaxed);
    let snapshot = catalog(&f, node, 4);
    let expected = snapshot.validated_install().unwrap();
    for id in 1..=4 {
        assert_eq!(
            f.handle(id)
                .install_carrier_catalog(snapshot.clone(), deadline())
                .await
                .unwrap(),
            expected
        );
        assert_eq!(
            f.handle(id).carrier_catalog(deadline()).await.unwrap(),
            Some(expected)
        );
    }
    assert_eq!(
        f.factory_calls.load(std::sync::atomic::Ordering::Relaxed),
        before
    );
    assert!(!f.handle(4).status().accepting);
    assert_eq!(
        f.handle(4)
            .local_role(SlotId(0), deadline())
            .await
            .unwrap()
            .role,
        LocalSlotRole::Missing
    );
    f.stop().await;
}
