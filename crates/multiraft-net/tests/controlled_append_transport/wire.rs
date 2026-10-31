//! Versioned controlled AppendEntries is negotiated by its explicit path.
use super::*;
use crate::grpc::proto::{
    raft_service_server::{RaftService, RaftServiceServer},
    RaftRequest, RaftResponse,
};
use multiraft_core::{typ, NativeControl, OperationId, Request as NativeRequest, TypeConfig};
use openraft::vote::RaftLeaderIdExt;
use openraft::{
    network::RPCOption,
    raft::{AppendEntriesRequest, AppendEntriesResponse},
};
use openraft_multi::GroupRouter;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

struct Peer {
    paths: Arc<Mutex<Vec<String>>>,
    old: bool,
}
#[tonic::async_trait]
impl RaftService for Peer {
    async fn call(&self, request: Request<RaftRequest>) -> Result<Response<RaftResponse>, Status> {
        let request = request.into_inner();
        self.paths.lock().unwrap().push(request.path.clone());
        let result: Result<AppendEntriesResponse<TypeConfig>, typ::RaftError> =
            Ok(AppendEntriesResponse::Success);
        let payload = if request.path == "/raft/append-json/v1" {
            if self.old {
                return Err(Status::unimplemented("unknown controlled append protocol"));
            }
            let decoded: AppendEntriesRequest<TypeConfig> =
                serde_json::from_slice(&request.payload).unwrap();
            assert!(
                matches!(&decoded.entries[0].payload, openraft::EntryPayload::Normal(request) if request.control.is_some()),
                "typed control must survive JSON transport"
            );
            serde_json::to_vec(&result).unwrap()
        } else {
            let _: AppendEntriesRequest<TypeConfig> =
                bincode::deserialize(&request.payload).unwrap();
            bincode::serialize(&result).unwrap()
        };
        Ok(Response::new(RaftResponse { payload }))
    }
}
fn append(controlled: bool) -> AppendEntriesRequest<TypeConfig> {
    let entries = if controlled {
        vec![typ::Entry {
            log_id: typ::LogId::new(
                openraft::alias::LeaderIdOf::<TypeConfig>::new_committed(1, 1),
                1,
            ),
            payload: openraft::EntryPayload::Normal(NativeRequest::native_control(
                NativeControl::Claim {
                    operation: OperationId {
                        sequence: 1,
                        nonce: [7; 16],
                    },
                    executor_epoch: 1,
                },
            )),
        }]
    } else {
        vec![]
    };
    AppendEntriesRequest {
        vote: typ::Vote::new_committed(1, 1),
        prev_log_id: None,
        entries,
        leader_commit: None,
    }
}

#[tokio::test]
async fn controlled_append_uses_json_and_old_peer_never_receives_binary_fallback() {
    for old in [false, true] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let paths = Arc::new(Mutex::new(Vec::new()));
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let peer = Peer {
            paths: paths.clone(),
            old,
        };
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(RaftServiceServer::new(peer))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = stopped.await;
                }),
        );
        let router = GrpcRouter::new(vec![(2, address)], 1);
        router.enable_controlled_group(7);
        let result = router
            .append_entries(2, 7, append(true), RPCOption::new(Duration::from_secs(1)))
            .await;
        assert_eq!(result.is_err(), old, "unsupported peers must fail closed");
        assert_eq!(*paths.lock().unwrap(), vec!["/raft/append-json/v1"]);
        router
            .append_entries(2, 8, append(false), RPCOption::new(Duration::from_secs(1)))
            .await
            .unwrap();
        assert_eq!(paths.lock().unwrap()[1], "/raft/append");
        router.join().await.unwrap();
        stop.send(()).unwrap();
        server.await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn controlled_ingress_rejects_legacy_binary_without_native_dispatch() {
    use crate::grpc::proto::raft_service_client::RaftServiceClient;
    use crate::grpc::server::IngressProtocol;
    use crate::multiraft::tasks::OwnedTasks;
    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::{atomic::AtomicBool, RwLock},
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let protocol = IngressProtocol {
        fingerprint: None,
        controlled_groups: Arc::new(RwLock::new(BTreeSet::from([7]))),
    };
    let groups: crate::node::GroupMap<multiraft_fsm::CounterFsm> =
        Arc::new(Mutex::new(BTreeMap::new()));
    let server = tokio::spawn(crate::GrpcServer::serve_owned_protocol(
        listener,
        groups,
        1024,
        Arc::new(OwnedTasks::default()),
        Arc::new(AtomicBool::new(true)),
        protocol,
        async {
            let _ = stopped.await;
        },
    ));
    let mut client = RaftServiceClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    let error = client
        .call(RaftRequest {
            group_id: 7,
            path: "/raft/append".into(),
            payload: vec![255],
        })
        .await
        .unwrap_err();
    assert_eq!(
        error.code(),
        tonic::Code::FailedPrecondition,
        "legacy bytes must be rejected before any decoder/native dispatch"
    );
    let error = client
        .call(RaftRequest {
            group_id: 7,
            path: "/raft/append-json/v1".into(),
            payload: vec![],
        })
        .await
        .unwrap_err();
    assert_eq!(
        error.code(),
        tonic::Code::Unavailable,
        "versioned unknown group must not return a binary result"
    );
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}
