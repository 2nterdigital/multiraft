//! Native and application carrier components acknowledge independently through weak owned handles.
use multiraft_core::ClusterConfig;
use multiraft_fsm::CounterFsm;
use multiraft_net::application_rpc::*;
use multiraft_net::{CarrierCatalog, CarrierPeer, FsmFactoryContext, NodeOwner, RuntimeConfig};
use std::{
    net::{SocketAddr, TcpListener},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::time::Instant;
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}
fn address() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}
struct Marker(u8);
impl ApplicationRpcHandler for Marker {
    fn call(&self, _: RpcContext, _: RpcCall) -> RpcFuture<'_> {
        Box::pin(async move { Ok(RpcReply::new(vec![self.0])) })
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_peer_installs_independent_components_and_retargets_application_rpc_without_group_creation(
) {
    let native = address();
    let candidate_raft = address();
    let calls = Arc::new(AtomicUsize::new(0));
    let creations = calls.clone();
    let mut config = ClusterConfig::for_test(1, &[1]);
    config.peers = vec![(1, native)];
    config.raft_transport_fingerprint = Some([7; 32]);
    let owner = NodeOwner::start(
        RuntimeConfig::new(config, vec![]),
        move |_: FsmFactoryContext| {
            creations.fetch_add(1, Ordering::SeqCst);
            Ok(CounterFsm::new())
        },
        deadline(),
    )
    .await
    .unwrap();
    let handle = owner.handle();
    let (app, client) = ApplicationRpcOwner::start_with_carrier_scope(
        ApplicationRpcConfig::new(1, address(), vec![]),
        Arc::new(Marker(1)),
        deadline(),
        Some([7; 32]),
    )
    .await
    .unwrap();
    let (old, _) = ApplicationRpcOwner::start_with_carrier_scope(
        ApplicationRpcConfig::new(4, address(), vec![]),
        Arc::new(Marker(8)),
        deadline(),
        Some([7; 32]),
    )
    .await
    .unwrap();
    let (new, _) = ApplicationRpcOwner::start_with_carrier_scope(
        ApplicationRpcConfig::new(4, address(), vec![]),
        Arc::new(Marker(9)),
        deadline(),
        Some([7; 32]),
    )
    .await
    .unwrap();
    let mut catalog = CarrierCatalog {
        version: 1,
        scope: Some([7; 32]),
        peers: vec![
            CarrierPeer {
                node_id: 1,
                identity: vec![1],
                raft: native,
                application_rpc: app.local_address(),
            },
            CarrierPeer {
                node_id: 4,
                identity: vec![4],
                raft: candidate_raft,
                application_rpc: old.local_address(),
            },
        ],
    };
    let request = || RpcCall::new(1, 1, vec![]);
    assert_eq!(
        client
            .call(4, request(), RpcContext::new(deadline()))
            .await
            .unwrap_err()
            .kind,
        RpcErrorKind::UnknownPeer
    );
    assert!(handle.carrier_catalog(deadline()).await.unwrap().is_none());
    let receipt = handle
        .install_carrier_catalog(catalog.clone(), deadline())
        .await
        .unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "carrier install cannot invoke the factory or initialize any Group"
    );
    assert!(handle.local_group_status(7, deadline()).await.is_err());
    assert!(
        client.carrier_catalog(deadline()).await.unwrap().is_none(),
        "native ACK cannot claim application-plane installation"
    );
    assert_eq!(
        client
            .call(4, request(), RpcContext::new(deadline()))
            .await
            .unwrap_err()
            .kind,
        RpcErrorKind::UnknownPeer
    );
    assert_eq!(
        client
            .install_carrier_catalog(catalog.clone(), deadline())
            .await
            .unwrap(),
        receipt
    );
    assert_eq!(
        client
            .call(4, request(), RpcContext::new(deadline()))
            .await
            .unwrap()
            .payload,
        vec![8]
    );
    catalog.version = 2;
    catalog.peers[1].application_rpc = new.local_address();
    handle
        .install_carrier_catalog(catalog.clone(), deadline())
        .await
        .unwrap();
    client
        .install_carrier_catalog(catalog.clone(), deadline())
        .await
        .unwrap();
    assert_eq!(
        client
            .call(4, request(), RpcContext::new(deadline()))
            .await
            .unwrap()
            .payload,
        vec![9]
    );
    assert_eq!(
        client
            .carrier_catalog(deadline())
            .await
            .unwrap()
            .unwrap()
            .validated_install()
            .unwrap(),
        catalog.validated_install().unwrap()
    );
    app.shutdown(deadline()).await.unwrap();
    assert!(client
        .install_carrier_catalog(catalog.clone(), deadline())
        .await
        .is_err());
    assert!(client.carrier_catalog(deadline()).await.is_err());
    owner.shutdown(deadline()).await.unwrap();
    assert!(handle
        .install_carrier_catalog(catalog, deadline())
        .await
        .is_err());
    assert!(handle.carrier_catalog(deadline()).await.is_err());
    old.shutdown(deadline()).await.unwrap();
    new.shutdown(deadline()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_fences_concurrent_owned_catalog_installers_and_releases_listener_ports() {
    let native = address();
    let app_address = address();
    let mut config = ClusterConfig::for_test(1, &[1]);
    config.peers = vec![(1, native)];
    config.raft_transport_fingerprint = Some([7; 32]);
    let owner = NodeOwner::start(
        RuntimeConfig::new(config, vec![]),
        |_: FsmFactoryContext| Ok(CounterFsm::new()),
        deadline(),
    )
    .await
    .unwrap();
    let handle = owner.handle();
    let (app, client) = ApplicationRpcOwner::start_with_carrier_scope(
        ApplicationRpcConfig::new(1, app_address, vec![]),
        Arc::new(Marker(1)),
        deadline(),
        Some([7; 32]),
    )
    .await
    .unwrap();
    let catalog = CarrierCatalog {
        version: 1,
        scope: Some([7; 32]),
        peers: vec![CarrierPeer {
            node_id: 1,
            identity: vec![1],
            raft: native,
            application_rpc: app_address,
        }],
    };
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let native_handle = handle.clone();
    let rpc_handle = client.clone();
    let mut installing = catalog.clone();
    let entered = started.clone();
    let released = release.clone();
    let task = tokio::spawn(async move {
        native_handle
            .install_carrier_catalog(installing.clone(), deadline())
            .await
            .unwrap();
        rpc_handle
            .install_carrier_catalog(installing.clone(), deadline())
            .await
            .unwrap();
        entered.notify_one();
        released.notified().await;
        let mut native_closed = false;
        let mut rpc_closed = false;
        for version in 2..=128 {
            installing.version = version;
            match native_handle
                .install_carrier_catalog(installing.clone(), deadline())
                .await
            {
                Ok(_) => assert!(
                    !native_closed,
                    "native installer cannot reopen after observing Closed"
                ),
                Err(_) => native_closed = true,
            }
            match rpc_handle
                .install_carrier_catalog(installing.clone(), deadline())
                .await
            {
                Ok(_) => assert!(
                    !rpc_closed,
                    "application installer cannot reopen after observing Closed"
                ),
                Err(_) => rpc_closed = true,
            }
            tokio::task::yield_now().await;
        }
    });
    started.notified().await;
    release.notify_one();
    let (native_stopped, rpc_stopped) =
        tokio::join!(owner.shutdown(deadline()), app.shutdown(deadline()));
    native_stopped.unwrap();
    rpc_stopped.unwrap();
    task.await.unwrap();
    assert!(handle
        .install_carrier_catalog(catalog.clone(), deadline())
        .await
        .is_err());
    assert!(client
        .install_carrier_catalog(catalog, deadline())
        .await
        .is_err());
    assert!(handle.carrier_scope(deadline()).await.is_err());
    assert!(client.carrier_scope(deadline()).await.is_err());
    let native_listener =
        TcpListener::bind(native).expect("idle weak carrier handle cannot retain native listener");
    let app_listener = TcpListener::bind(app_address)
        .expect("idle weak carrier handle cannot retain application listener");
    drop((native_listener, app_listener));
}
