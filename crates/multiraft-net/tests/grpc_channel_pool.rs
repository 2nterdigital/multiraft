use std::error::Error;
use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use multiraft_net::node_rpc::node_rpc_service_client::NodeRpcServiceClient;
use multiraft_net::node_rpc::node_rpc_service_server::NodeRpcService;
use multiraft_net::node_rpc::node_rpc_service_server::NodeRpcServiceServer;
use multiraft_net::node_rpc::NodeRpcRequest;
use multiraft_net::node_rpc::NodeRpcResponse;
use multiraft_net::GrpcPeerChannelError;
use multiraft_net::GrpcPeerChannelPool;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tonic::transport::Server;
use tonic::Request;
use tonic::Response;
use tonic::Status;

struct MarkerNodeRpc {
    marker: Vec<u8>,
}

#[tonic::async_trait]
impl NodeRpcService for MarkerNodeRpc {
    async fn call(
        &self,
        _request: Request<NodeRpcRequest>,
    ) -> Result<Response<NodeRpcResponse>, Status> {
        Ok(Response::new(NodeRpcResponse {
            payload: self.marker.clone(),
        }))
    }
}

struct TestNodeRpcServer {
    addr: SocketAddr,
    accepted: Arc<AtomicUsize>,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

impl TestNodeRpcServer {
    async fn start(marker: Vec<u8>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind Node RPC marker server");
        let addr = listener.local_addr().expect("Node RPC marker address");
        let accepted = Arc::new(AtomicUsize::new(0));
        let accepted_for_stream = Arc::clone(&accepted);
        let incoming = TcpListenerStream::new(listener).map(move |item| {
            if item.is_ok() {
                accepted_for_stream.fetch_add(1, Ordering::SeqCst);
            }
            item
        });
        let (shutdown, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            Server::builder()
                .add_service(NodeRpcServiceServer::new(MarkerNodeRpc { marker }))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("serve Node RPC marker server");
        });
        Self {
            addr,
            accepted,
            shutdown: Some(shutdown),
            task,
        }
    }

    const fn addr(&self) -> SocketAddr {
        self.addr
    }

    fn accepted_connections(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }

    async fn shutdown(mut self) {
        self.shutdown
            .take()
            .expect("shutdown sender retained")
            .send(())
            .ok();
        self.task.await.expect("join Node RPC marker server");
    }
}

async fn call_marker_for_service(channel: Channel, service_id: u32) -> Vec<u8> {
    NodeRpcServiceClient::new(channel)
        .call(NodeRpcRequest {
            service_id,
            method_id: 1,
            payload: Vec::new(),
        })
        .await
        .expect("marker Node RPC call")
        .into_inner()
        .payload
}

async fn call_marker(channel: Channel) -> Vec<u8> {
    call_marker_for_service(channel, 1).await
}

#[tokio::test]
async fn unknown_peer_returns_the_typed_peer_identity() {
    let pool = GrpcPeerChannelPool::new(Vec::new());
    let error = pool.channel(9).await.expect_err("peer is absent");

    assert!(matches!(
        error,
        GrpcPeerChannelError::UnknownPeer { peer: 9 }
    ));
    assert_eq!(pool.unique_peer_links(), 0);
}

#[tokio::test]
async fn configured_unreachable_peer_returns_the_typed_connect_source() {
    let pool = GrpcPeerChannelPool::new(vec![(3, SocketAddr::from(([127, 0, 0, 1], 0)))]);
    let error = tokio::time::timeout(Duration::from_secs(2), pool.channel(3))
        .await
        .expect("connection attempt must finish")
        .expect_err("unused address must reject connection");

    assert!(matches!(
        error,
        GrpcPeerChannelError::Connect { peer: 3, .. }
    ));
    assert!(error.source().is_some());
    assert_eq!(pool.unique_peer_links(), 0);
}

#[tokio::test]
async fn repeated_peer_lookup_reuses_one_cached_channel() {
    let server = TestNodeRpcServer::start(vec![1]).await;
    let pool = GrpcPeerChannelPool::new(vec![(1, server.addr())]);

    let first = pool.channel(1).await.expect("first peer Channel");
    let second = pool.channel(1).await.expect("cached peer Channel");
    assert_eq!(call_marker(first).await, vec![1]);
    assert_eq!(call_marker(second).await, vec![1]);

    assert_eq!(pool.unique_peer_links(), 1);
    assert_eq!(server.accepted_connections(), 1);
    server.shutdown().await;
}

#[tokio::test]
async fn separate_pools_keep_same_node_id_address_catalogs_isolated() {
    let server_a = TestNodeRpcServer::start(vec![10]).await;
    let server_b = TestNodeRpcServer::start(vec![20]).await;
    let pool_a = GrpcPeerChannelPool::new(vec![(1, server_a.addr())]);
    let pool_b = GrpcPeerChannelPool::new(vec![(1, server_b.addr())]);

    assert_eq!(
        call_marker(pool_a.channel(1).await.expect("pool A Channel")).await,
        vec![10]
    );
    assert_eq!(
        call_marker(pool_b.channel(1).await.expect("pool B Channel")).await,
        vec![20]
    );

    assert_eq!(pool_a.unique_peer_links(), 1);
    assert_eq!(pool_b.unique_peer_links(), 1);
    server_a.shutdown().await;
    server_b.shutdown().await;
}

#[tokio::test]
async fn business_and_control_services_share_cached_peer_channel() {
    let server = TestNodeRpcServer::start(vec![8]).await;
    let pool = GrpcPeerChannelPool::new(vec![(1, server.addr())]);

    assert_eq!(
        call_marker_for_service(pool.channel(1).await.expect("business Channel"), 1).await,
        vec![8]
    );
    assert_eq!(
        call_marker_for_service(pool.channel(1).await.expect("control Channel"), 8).await,
        vec![8]
    );

    assert_eq!(pool.unique_peer_links(), 1);
    assert_eq!(server.accepted_connections(), 1);
    server.shutdown().await;
}
