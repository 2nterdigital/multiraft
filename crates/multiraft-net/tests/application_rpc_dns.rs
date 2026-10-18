//! DNS overrides retain an already-used application's peer catalog across reconnects.
use multiraft_net::application_rpc::*;
use std::{
    net::{SocketAddr, TcpListener},
    sync::Arc,
    time::Duration,
};
use tokio::time::Instant;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
fn address() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}
struct Echo;
impl ApplicationRpcHandler for Echo {
    fn call(&self, _: RpcContext, call: RpcCall) -> RpcFuture<'_> {
        Box::pin(async move { Ok(RpcReply::new(call.payload)) })
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn previously_open_application_connection_reconnects_to_dns_peer() {
    let remote = address();
    let listen = SocketAddr::from(([0, 0, 0, 0], remote.port()));
    let server = || {
        ApplicationRpcOwner::start(
            ApplicationRpcConfig::new(2, listen, vec![]),
            Arc::new(Echo),
            deadline(),
        )
    };
    let (owner, _) = server().await.unwrap();
    // The numeric identity address intentionally cannot reach this server.
    let identity_address = address();
    let mut config = ApplicationRpcConfig::new(1, address(), vec![(2, identity_address)]);
    config.dial_addresses = Some(vec![(2, format!("localhost:{}", remote.port()))]);
    let (client, handle) = ApplicationRpcOwner::start(config, Arc::new(Echo), deadline())
        .await
        .unwrap();
    let request = |bytes: &[u8]| RpcCall::new(1, 1, bytes.to_vec());
    assert_eq!(
        handle
            .call(2, request(b"before"), RpcContext::new(deadline()))
            .await
            .unwrap()
            .payload,
        b"before"
    );
    owner.shutdown(deadline()).await.unwrap();
    // Explicitly observe a failure on the cached channel before reusing this
    // same client handle. Library failure invalidation must retain the hostname.
    assert!(handle
        .call(2, request(b"offline"), RpcContext::new(deadline()))
        .await
        .is_err());
    let (owner, _) = server().await.unwrap();
    assert_eq!(
        handle
            .call(2, request(b"after"), RpcContext::new(deadline()))
            .await
            .unwrap()
            .payload,
        b"after"
    );
    client.shutdown(deadline()).await.unwrap();
    owner.shutdown(deadline()).await.unwrap();
}
