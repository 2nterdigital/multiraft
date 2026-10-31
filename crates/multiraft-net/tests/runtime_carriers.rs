//! Verified carrier catalogs add runtime connectivity without constructing native Groups.
use multiraft_net::{CarrierCatalog, CarrierPeer, CarrierService, GrpcPeerChannelPool};
fn peer(id: u64, raft: u16) -> CarrierPeer {
    CarrierPeer {
        node_id: id,
        identity: vec![id as u8],
        raft: format!("127.0.0.1:{raft}").parse().unwrap(),
        application_rpc: format!("127.0.0.1:{}", raft + 100).parse().unwrap(),
    }
}
#[tokio::test]
async fn unknown_runtime_peer_can_be_installed_and_versions_remain_immutable() {
    let pool = GrpcPeerChannelPool::with_carrier_config(
        vec![(1, "127.0.0.1:20001".parse().unwrap())],
        None,
        Some([7; 32]),
        CarrierService::Raft,
    );
    assert!(pool.lease(4).await.is_err());
    let catalog = CarrierCatalog {
        version: 1,
        scope: Some([7; 32]),
        peers: vec![peer(1, 20001), peer(4, 20004)],
    };
    let installed = pool.install_carrier_catalog(&catalog).unwrap();
    assert_eq!(installed.version, 1);
    assert_eq!(pool.install_carrier_catalog(&catalog).unwrap(), installed);
    let mut changed = catalog.clone();
    changed.peers[1].raft = "127.0.0.1:20005".parse().unwrap();
    assert!(pool.install_carrier_catalog(&changed).is_err());
    changed.version = 2;
    pool.install_carrier_catalog(&changed).unwrap();
    assert!(pool.install_carrier_catalog(&catalog).is_err());
    changed.version = 3;
    changed.peers[1].identity = vec![99];
    assert!(pool.install_carrier_catalog(&changed).is_err());
    pool.shutdown().await;
    assert!(pool.install_carrier_catalog(&catalog).is_err());
    assert!(pool.carrier_catalog().is_err());
}
