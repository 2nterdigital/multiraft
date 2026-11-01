//! Fault setup stops between real component installations; assertions use the public Slot facade.
#[path = "../../../tests/controlled_support.rs"]
mod controlled_support;
use crate::{CarrierCatalog, CarrierPeer, SlotCarrierError};
use controlled_support::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn native_only_install_never_acknowledges_and_same_snapshot_reconciles() {
    let f = Fixture::fresh_sources().await;
    let handle = f.handle(1);
    let shared = f.owners[0].shared.clone();
    let catalog = CarrierCatalog {
        version: 4,
        scope: Some(handle.transport_domain().unwrap()),
        peers: f
            .directory
            .nodes
            .iter()
            .map(|peer| CarrierPeer {
                node_id: peer.node_id,
                identity: format!("persistent-passport-{}", peer.node_id).into_bytes(),
                raft: peer.raft,
                application_rpc: peer.application_rpc,
            })
            .collect(),
    };
    let expected = catalog.validated_install().unwrap();
    // Inject the reachable loss window: native committed its component, while the
    // application/discovery planes have not run and the original outcome is unknown.
    shared
        .controlled
        .as_ref()
        .unwrap()
        .directory
        .lock()
        .unwrap()
        .pending = Some(catalog.clone());
    assert_eq!(
        shared
            .native
            .install_carrier_catalog(catalog.clone(), deadline())
            .await
            .unwrap(),
        expected
    );
    match handle.carrier_catalog(deadline()).await {
        Err(SlotCarrierError::Incomplete { components }) => {
            assert_eq!(components.native, Some(expected));
            assert_eq!(components.application_rpc, None);
            assert_eq!(components.discovery, None);
        }
        other => panic!("native-only success cannot acknowledge Slot readiness: {other:?}"),
    }
    let mut changed = catalog.clone();
    changed.version = 5;
    assert!(
        matches!(
            handle.install_carrier_catalog(changed, deadline()).await,
            Err(SlotCarrierError::Pending { version: 4 })
        ),
        "unknown outcome must retain exact snapshot identity"
    );
    assert_eq!(
        handle
            .install_carrier_catalog(catalog, deadline())
            .await
            .unwrap(),
        expected
    );
    assert_eq!(
        handle.carrier_catalog(deadline()).await.unwrap(),
        Some(expected)
    );
    drop(shared);
    f.stop().await;
    assert!(handle.carrier_catalog(deadline()).await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn cold_carrier_barrier_refuses_internal_work_until_composite_publication() {
    let f = Fixture::fresh_sources().await;
    let shared = f.owners[0].shared.clone();
    let handle = f.handle(1);
    let catalog = CarrierCatalog {
        version: 4,
        scope: Some(handle.transport_domain().unwrap()),
        peers: f
            .directory
            .nodes
            .iter()
            .map(|peer| CarrierPeer {
                node_id: peer.node_id,
                identity: format!("persistent-passport-{}", peer.node_id).into_bytes(),
                raft: peer.raft,
                application_rpc: peer.application_rpc,
            })
            .collect(),
    };
    // Hold the same barrier set before cold startup exposes its RPC listener.
    shared
        .controlled
        .as_ref()
        .unwrap()
        .starting_carriers
        .store(true, std::sync::atomic::Ordering::Release);
    assert!(matches!(
        handle.propose(b"not-yet-ready", vec![7], deadline()).await,
        Err(crate::RuntimeError::Unavailable)
    ));
    assert!(matches!(
        handle.leader_hint(crate::SlotId(0), deadline()).await,
        Err(crate::RuntimeError::Unavailable)
    ));
    let expected = catalog.validated_install().unwrap();
    assert_eq!(
        shared.install_carriers(catalog, deadline()).await.unwrap(),
        expected
    );
    assert!(!shared
        .controlled
        .as_ref()
        .unwrap()
        .starting_carriers
        .load(std::sync::atomic::Ordering::Acquire));
    assert_eq!(
        handle.carrier_catalog(deadline()).await.unwrap(),
        Some(expected)
    );
    drop(shared);
    f.stop().await;
}
