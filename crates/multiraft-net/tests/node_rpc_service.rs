use std::net::SocketAddr;

use multiraft_net::node_rpc::node_rpc_service_client::NodeRpcServiceClient;
use multiraft_net::node_rpc::node_rpc_service_server::NodeRpcService;
use multiraft_net::node_rpc::node_rpc_service_server::NodeRpcServiceServer;
use multiraft_net::node_rpc::NodeRpcRequest;
use multiraft_net::node_rpc::NodeRpcResponse;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::Request;
use tonic::Response;
use tonic::Status;

struct EchoNodeRpc;

#[tonic::async_trait]
impl NodeRpcService for EchoNodeRpc {
    async fn call(
        &self,
        request: Request<NodeRpcRequest>,
    ) -> Result<Response<NodeRpcResponse>, Status> {
        let request = request.into_inner();
        if request.service_id == 0 || request.method_id == 0 {
            return Err(Status::unimplemented("unknown node RPC operation"));
        }
        Ok(Response::new(NodeRpcResponse {
            payload: request.payload,
        }))
    }
}

struct TestNodeRpcServer {
    addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

impl TestNodeRpcServer {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind Node RPC test server");
        let addr = listener.local_addr().expect("Node RPC test address");
        let incoming = TcpListenerStream::new(listener);
        let (shutdown, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            Server::builder()
                .add_service(NodeRpcServiceServer::new(EchoNodeRpc))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("serve Node RPC test server");
        });
        Self {
            addr,
            shutdown: Some(shutdown),
            task,
        }
    }

    fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }

    async fn shutdown(mut self) {
        self.shutdown
            .take()
            .expect("shutdown sender retained")
            .send(())
            .ok();
        self.task.await.expect("join Node RPC test server");
    }
}

#[tokio::test]
async fn generated_node_rpc_round_trips_numeric_ids_and_opaque_payload() {
    let server = TestNodeRpcServer::start().await;
    let mut client = NodeRpcServiceClient::connect(server.endpoint())
        .await
        .expect("connect Node RPC client");

    let response = client
        .call(NodeRpcRequest {
            service_id: 7,
            method_id: 11,
            payload: vec![0, 1, 2, 255],
        })
        .await
        .expect("Node RPC call")
        .into_inner();

    assert_eq!(response.payload, vec![0, 1, 2, 255]);
    server.shutdown().await;
}

#[tokio::test]
async fn generated_node_rpc_preserves_handler_tonic_status() {
    let server = TestNodeRpcServer::start().await;
    let mut client = NodeRpcServiceClient::connect(server.endpoint())
        .await
        .expect("connect Node RPC client");

    let status = client
        .call(NodeRpcRequest {
            service_id: 0,
            method_id: 11,
            payload: Vec::new(),
        })
        .await
        .expect_err("unknown service must remain a tonic status");

    assert_eq!(status.code(), tonic::Code::Unimplemented);
    server.shutdown().await;
}
