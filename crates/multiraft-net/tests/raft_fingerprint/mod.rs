//! Replication-domain wire guards and their ordering relative to native admission.
use super::*;
use crate::grpc::{fingerprint, GrpcRouter};
use multiraft_core::{ClusterConfig, TypeConfig};
use multiraft_fsm::{ApplyOut, GroupId};
use openraft::network::RPCOption;
use openraft::raft::{VoteRequest, VoteResponse};
use openraft_multi::GroupRouter;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

struct Noop;
impl StateMachine for Noop {
    type Error = std::io::Error;
    fn apply(&mut self, _: GroupId, _: u64, _: &[u8]) -> Result<ApplyOut, Self::Error> {
        Ok(ApplyOut::default())
    }
    fn snapshot(&self, _: GroupId) -> Result<Vec<u8>, Self::Error> {
        Ok(Vec::new())
    }
    fn restore(&mut self, _: GroupId, _: &[u8]) -> Result<(), Self::Error> {
        panic!("fingerprint rejection must precede restore")
    }
}
fn service(expected: Option<[u8; 32]>) -> RaftServiceImpl<Noop> {
    RaftServiceImpl {
        groups: Arc::new(Mutex::new(BTreeMap::new())),
        fingerprint: expected,
        snapshot_slots: Arc::new(tokio::sync::Semaphore::new(1)),
        tasks: Arc::new(crate::multiraft::tasks::OwnedTasks::default()),
        accepting: Arc::new(std::sync::atomic::AtomicBool::new(true)),
    }
}
fn request(path: &str, supplied: Option<[u8; 32]>) -> Request<RaftRequest> {
    let mut request = Request::new(RaftRequest {
        group_id: 7,
        path: path.into(),
        payload: vec![255],
    });
    fingerprint::insert(request.metadata_mut(), supplied);
    request
}

#[tokio::test]
#[allow(clippy::await_holding_lock)] // Deliberate proof that rejection precedes registry lookup.
async fn fingerprint_rejection_precedes_lookup_and_snapshot_admission_for_every_native_path() {
    for expected in [None, Some([0xab; 32])] {
        let service = service(expected);
        // If validation consults the registry first it would block on this lock.
        let guard = service.groups.lock().unwrap();
        let slot = service
            .snapshot_slots
            .clone()
            .acquire_owned()
            .await
            .unwrap();
        for path in [
            "/raft/vote",
            "/raft/append",
            "/raft/snapshot",
            "/raft/transfer_leader",
        ] {
            for supplied in [None, Some([0xab; 32]), Some([0xcd; 32])] {
                if supplied == expected {
                    continue;
                }
                let status = tokio::time::timeout(
                    Duration::from_millis(100),
                    service.call(request(path, supplied)),
                )
                .await
                .unwrap()
                .unwrap_err();
                assert_eq!(status.code(), tonic::Code::FailedPrecondition, "{path}");
            }
            let mut duplicate = request(path, Some([0xab; 32]));
            duplicate
                .metadata_mut()
                .append(fingerprint::HEADER, "ab".repeat(32).parse().unwrap());
            assert_eq!(
                service.call(duplicate).await.unwrap_err().code(),
                tonic::Code::FailedPrecondition
            );
            let mut malformed = request(path, None);
            malformed
                .metadata_mut()
                .insert(fingerprint::HEADER, "invalid".parse().unwrap());
            assert_eq!(
                service.call(malformed).await.unwrap_err().code(),
                tonic::Code::FailedPrecondition
            );
        }
        drop(slot);
        drop(guard);
        assert_eq!(service.snapshot_slots.available_permits(), 1);
    }
}

#[tokio::test]
async fn actual_server_echoes_exact_domain_and_preserves_legacy_none() {
    for expected in [None, Some([0xab; 32])] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop, stopping) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(GrpcServer::serve_owned_bound(
            listener,
            service(expected).groups,
            1024,
            Arc::new(crate::multiraft::tasks::OwnedTasks::default()),
            Arc::new(std::sync::atomic::AtomicBool::new(true)),
            expected,
            async {
                let _ = stopping.await;
            },
        ));
        let mut client = crate::grpc::proto::raft_service_client::RaftServiceClient::connect(
            format!("http://{addr}"),
        )
        .await
        .unwrap();
        for supplied in [None, Some([0xab; 32]), Some([0xcd; 32])] {
            for path in [
                "/raft/vote",
                "/raft/append",
                "/raft/snapshot",
                "/raft/transfer_leader",
            ] {
                let response = client.call(request(path, supplied)).await;
                if supplied == expected {
                    fingerprint::validate(response.unwrap().metadata(), expected).unwrap();
                } else {
                    assert_eq!(
                        response.unwrap_err().code(),
                        tonic::Code::FailedPrecondition
                    );
                }
            }
        }
        drop(client);
        stop.send(()).unwrap();
        server.await.unwrap().unwrap();
    }
}

struct EchoPeer {
    echo: Option<[u8; 32]>,
    duplicate: bool,
    malformed: bool,
}
#[tonic::async_trait]
impl RaftService for EchoPeer {
    async fn call(&self, request: Request<RaftRequest>) -> Result<Response<RaftResponse>, Status> {
        // A valid native result must still be rejected if its envelope is wrong.
        if self.echo.is_some() {
            fingerprint::validate(request.metadata(), Some([0xab; 32]))?;
        } // The old-server case ignores request metadata entirely.
        let mut response = Response::new(RaftResponse {
            payload: encode(Result::<VoteResponse<TypeConfig>, typ::RaftError>::Ok(
                VoteResponse::new(typ::Vote::new(1, 2), None, true),
            )),
        });
        fingerprint::insert(response.metadata_mut(), self.echo);
        if self.duplicate {
            response
                .metadata_mut()
                .append(fingerprint::HEADER, "ab".repeat(32).parse().unwrap());
        }
        if self.malformed {
            response
                .metadata_mut()
                .insert(fingerprint::HEADER, "invalid".parse().unwrap());
        }
        Ok(response)
    }
}
#[tokio::test]
async fn client_rejects_old_server_missing_echo_and_invalid_echo_before_native_result() {
    for (echo, duplicate, malformed, success) in [
        (Some([0xab; 32]), false, false, true),
        (None, false, false, false),
        (Some([0xcd; 32]), false, false, false),
        (Some([0xab; 32]), true, false, false),
        (Some([0xab; 32]), false, true, false),
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop, stopping) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(
            Server::builder()
                .add_service(RaftServiceServer::new(EchoPeer {
                    echo,
                    duplicate,
                    malformed,
                }))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = stopping.await;
                }),
        );
        let mut config = ClusterConfig::new(1, vec![(2, addr)]);
        config.raft_transport_fingerprint = Some([0xab; 32]);
        let router = GrpcRouter::from_config(&config);
        let result = router
            .vote(
                2,
                7,
                VoteRequest::new(typ::Vote::new(1, 1), None),
                RPCOption::new(Duration::from_secs(1)),
            )
            .await;
        assert_eq!(result.is_ok(), success);
        if let Err(error) = result {
            assert!(format!("{error:?}").contains("raft fingerprint"));
        }
        router.join().await.unwrap();
        stop.send(()).unwrap();
        server.await.unwrap().unwrap();
    }
}
