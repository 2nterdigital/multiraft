//! Actual connected channels held immediately before cache publication exercise replacement races.
use super::*;
use crate::application_rpc::*;
use std::time::Duration;
use tokio::{sync::Notify, time::Instant};
pub(super) struct ConnectGate {
    pub version: u64,
    pub entered: Notify,
    pub release: Notify,
}
struct Echo;
impl ApplicationRpcHandler for Echo {
    fn call(&self, _: RpcContext, call: RpcCall) -> RpcFuture<'_> {
        Box::pin(async move { Ok(RpcReply::new(call.payload)) })
    }
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
async fn server() -> ApplicationRpcOwner {
    ApplicationRpcOwner::start(
        ApplicationRpcConfig::new(1, "127.0.0.1:0".parse().unwrap(), vec![]),
        Arc::new(Echo),
        deadline(),
    )
    .await
    .unwrap()
    .0
}
#[tokio::test]
async fn held_old_connect_cannot_publish_after_catalog_install_or_shutdown() {
    for shutdown in [false, true] {
        let old = server().await;
        let new = server().await;
        let gate = Arc::new(ConnectGate {
            version: 0,
            entered: Notify::new(),
            release: Notify::new(),
        });
        let mut pool = GrpcPeerChannelPool::new(vec![(4, old.local_address())]);
        pool.connect_gate = Some(gate.clone());
        let connecting = pool.clone();
        let task = tokio::spawn(async move { connecting.lease(4).await });
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
            .await
            .expect("actual old tonic channel connected before publication");
        let catalog = CarrierCatalog {
            version: 1,
            scope: None,
            peers: vec![crate::CarrierPeer {
                node_id: 4,
                identity: vec![4],
                raft: new.local_address(),
                application_rpc: "127.0.0.1:1".parse().unwrap(),
            }],
        };
        if shutdown {
            pool.shutdown().await;
        } else {
            pool.install_carrier_catalog(&catalog).unwrap();
        }
        gate.release.notify_one();
        let result = task.await.unwrap();
        assert!(
            matches!(
                result,
                Err(GrpcPeerChannelError::CatalogChanged { peer: 4 })
                    | Err(GrpcPeerChannelError::Closed { peer: 4 })
            ),
            "old connected channel must never publish: {result:?}"
        );
        if shutdown {
            assert!(pool.install_carrier_catalog(&catalog).is_err());
            assert!(pool.carrier_catalog().is_err());
        } else {
            assert_eq!(pool.lease(4).await.unwrap().catalog_version(), 1);
            pool.shutdown().await;
        }
        old.shutdown(deadline()).await.unwrap();
        new.shutdown(deadline()).await.unwrap();
    }
}
