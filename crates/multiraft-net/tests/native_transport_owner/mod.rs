//! Deterministic native transport timing seams. No native Raft runtime or probes.
use super::*;
use crate::grpc::proto::raft_service_server::{RaftService, RaftServiceServer};
use crate::grpc::proto::{RaftRequest, RaftResponse};
use openraft::alias::{SnapshotMetaOf, SnapshotOf};
use std::{
    io::Cursor,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    time::Duration,
};
use tokio::sync::{oneshot, Notify, Semaphore};
use tokio_stream::{wrappers::TcpListenerStream, StreamExt};
use tonic::{transport::Server, Request, Response, Status};

struct PeerService {
    stopped: Arc<AtomicBool>,
    entered: Arc<Notify>,
    release: Arc<Semaphore>,
    gated_group: Option<GroupId>,
}
#[tonic::async_trait]
impl RaftService for PeerService {
    async fn call(&self, request: Request<RaftRequest>) -> Result<Response<RaftResponse>, Status> {
        let request = request.into_inner();
        if self.gated_group == Some(request.group_id) {
            self.entered.notify_one();
            self.release.acquire().await.unwrap().forget();
        }
        let payload = if self.stopped.load(Ordering::Acquire) {
            encode(Result::<VoteResponse<TypeConfig>, typ::RaftError>::Err(
                typ::RaftError::Fatal(openraft::error::Fatal::Stopped),
            ))
        } else if request.path == "/raft/snapshot" {
            encode(Result::<SnapshotResponse<TypeConfig>, typ::RaftError>::Ok(
                SnapshotResponse::new(typ::Vote::new_committed(3, 1)),
            ))
        } else {
            encode(Result::<VoteResponse<TypeConfig>, typ::RaftError>::Ok(
                VoteResponse::new(typ::Vote::new_committed(3, 1), None, true),
            ))
        };
        Ok(Response::new(RaftResponse { payload }))
    }
}
fn request() -> VoteRequest<TypeConfig> {
    VoteRequest::new(typ::Vote::new(3, 1), None)
}
fn snapshot() -> SnapshotOf<TypeConfig, typ::SnapshotData> {
    SnapshotOf::<TypeConfig, typ::SnapshotData> {
        meta: SnapshotMetaOf::<TypeConfig>::default(),
        snapshot: Cursor::new(vec![7; 8]),
    }
}
fn service(stopped: Arc<AtomicBool>, gated_group: Option<GroupId>) -> PeerService {
    PeerService {
        stopped,
        entered: Arc::new(Notify::new()),
        release: Arc::new(Semaphore::new(0)),
        gated_group,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_stopped_on_open_old_connection_and_late_failure_do_not_poison_new_peer() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let stopped = Arc::new(AtomicBool::new(false));
    let old_service = service(stopped.clone(), Some(7));
    let entered = old_service.entered.clone();
    let release = old_service.release.clone();
    // Ending only the accept stream frees the listening address while tonic
    // still services the old accepted HTTP/2 connection. This models the old
    // peer's Stopped response without conflating it with TCP connection loss.
    let old_acceptor = tokio::spawn(
        Server::builder()
            .add_service(RaftServiceServer::new(old_service))
            .serve_with_incoming(TcpListenerStream::new(listener).take(1)),
    );
    let router = GrpcRouter::new(vec![(2, address)], 1);
    assert!(
        router
            .vote(2, 6, request(), RPCOption::new(Duration::from_secs(2)))
            .await
            .unwrap()
            .vote_granted
    );
    old_acceptor.await.unwrap().unwrap();
    let late_router = router.clone();
    let late = tokio::spawn(async move {
        late_router
            .vote(2, 7, request(), RPCOption::new(Duration::from_secs(2)))
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    stopped.store(true, Ordering::Release);
    let source = router
        .vote(2, 8, request(), RPCOption::new(Duration::from_secs(2)))
        .await
        .unwrap_err();
    assert!(matches!(source, RPCError::Unreachable(_)));
    assert!(
        format!("{source:?}").to_lowercase().contains("stopped"),
        "native source cause must survive transport"
    );
    let listener = tokio::net::TcpListener::bind(address).await.unwrap();
    let accepted = Arc::new(AtomicUsize::new(0));
    let accepts = accepted.clone();
    let incoming = TcpListenerStream::new(listener).map(move |connection| {
        if connection.is_ok() {
            accepts.fetch_add(1, Ordering::SeqCst);
        }
        connection
    });
    let (stop, stopping) = oneshot::channel();
    let new_acceptor = tokio::spawn(
        Server::builder()
            .add_service(RaftServiceServer::new(service(
                Arc::new(AtomicBool::new(false)),
                None,
            )))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stopping.await;
            }),
    );
    assert!(
        router
            .vote(2, 9, request(), RPCOption::new(Duration::from_secs(2)))
            .await
            .unwrap()
            .vote_granted
    );
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
    release.add_permits(1);
    assert!(matches!(late.await.unwrap(), Err(RPCError::Unreachable(_))));
    // Group 7's old late failure must not force a third connection.
    assert!(
        router
            .vote(2, 7, request(), RPCOption::new(Duration::from_secs(2)))
            .await
            .unwrap()
            .vote_granted
    );
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
    assert_eq!(router.unique_peer_links(), 1);
    router.close();
    router.join().await.unwrap();
    stop.send(()).unwrap();
    new_acceptor.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_snapshot_waiter_retains_node_permit_and_close_joins_owned_send() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = service(Arc::new(AtomicBool::new(false)), Some(7));
    let entered = peer.entered.clone();
    let release = peer.release.clone();
    let (stop, stopping) = oneshot::channel();
    let server = tokio::spawn(
        Server::builder()
            .add_service(RaftServiceServer::new(peer))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = stopping.await;
            }),
    );
    let router = GrpcRouter::new(vec![(2, address)], 1);
    let sending = router.clone();
    let waiter = tokio::spawn(async move {
        sending
            .full_snapshot(
                2,
                7,
                typ::Vote::new_committed(3, 1),
                snapshot(),
                std::future::pending(),
                RPCOption::new(Duration::from_secs(5)),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    let refused = router
        .full_snapshot(
            2,
            8,
            typ::Vote::new_committed(3, 1),
            snapshot(),
            std::future::pending(),
            RPCOption::new(Duration::from_secs(5)),
        )
        .await
        .unwrap_err();
    assert!(
        refused.to_string().contains("busy"),
        "waiter cancellation must retain node-wide send admission"
    );
    router.close();
    // Local stop must end its owned network send without waiting for a remote
    // application callback. The already-dispatched remote effect stays unknown.
    tokio::time::timeout(Duration::from_secs(1), router.join())
        .await
        .unwrap()
        .unwrap();
    let closed = router
        .full_snapshot(
            2,
            8,
            typ::Vote::new_committed(3, 1),
            snapshot(),
            std::future::pending(),
            RPCOption::new(Duration::from_secs(5)),
        )
        .await
        .unwrap_err();
    assert!(closed.to_string().contains("closed"));
    release.add_permits(1);
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dispatched_snapshot_timeout_invalidates_its_generation_and_keeps_outcome_unknown() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = service(Arc::new(AtomicBool::new(false)), Some(7));
    let entered = peer.entered.clone();
    let release = peer.release.clone();
    let accepted = Arc::new(AtomicUsize::new(0));
    let accepts = accepted.clone();
    let incoming = TcpListenerStream::new(listener).map(move |connection| {
        if connection.is_ok() {
            accepts.fetch_add(1, Ordering::SeqCst);
        }
        connection
    });
    let (stop, stopping) = oneshot::channel();
    let server = tokio::spawn(
        Server::builder()
            .add_service(RaftServiceServer::new(peer))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stopping.await;
            }),
    );
    let router = GrpcRouter::new(vec![(2, address)], 1);
    let sending = router.clone();
    let waiter = tokio::spawn(async move {
        sending
            .full_snapshot(
                2,
                7,
                typ::Vote::new_committed(3, 1),
                snapshot(),
                std::future::pending(),
                RPCOption::new(Duration::from_secs(1)),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    let error = waiter.await.unwrap().unwrap_err();
    assert!(
        error.to_string().contains("deadline_unconfirmed"),
        "a dispatched deadline must not claim rollback"
    );
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
    assert!(
        router
            .vote(2, 8, request(), RPCOption::new(Duration::from_secs(2)))
            .await
            .unwrap()
            .vote_granted
    );
    assert_eq!(
        accepted.load(Ordering::SeqCst),
        2,
        "next native attempt reconnects after the failed generation"
    );
    router.close();
    router.join().await.unwrap();
    release.add_permits(1);
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn native_cancel_signal_returns_closed_without_releasing_actual_send_slot() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Semaphore::new(0));
    let service = PeerService {
        stopped: Arc::new(AtomicBool::new(false)),
        entered: entered.clone(),
        release: release.clone(),
        gated_group: Some(7),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopping) = oneshot::channel();
    let server = tokio::spawn(
        Server::builder()
            .add_service(RaftServiceServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = stopping.await;
            }),
    );
    let router = GrpcRouter::new(vec![(2, address)], 1);
    let sending = router.clone();
    let (cancel, canceled) = oneshot::channel();
    let waiter = tokio::spawn(async move {
        sending
            .full_snapshot(
                2,
                7,
                typ::Vote::new_committed(3, 1),
                snapshot(),
                async { canceled.await.unwrap() },
                RPCOption::new(Duration::from_secs(2)),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    cancel
        .send(ReplicationClosed::new("native replication stopped"))
        .unwrap();
    assert!(matches!(
        waiter.await.unwrap(),
        Err(StreamingError::Closed(_))
    ));
    let busy = router
        .full_snapshot(
            2,
            8,
            typ::Vote::new_committed(3, 1),
            snapshot(),
            std::future::pending(),
            RPCOption::new(Duration::from_secs(2)),
        )
        .await
        .unwrap_err();
    assert!(busy.to_string().contains("native snapshot send busy"));
    release.add_permits(1);
    // Join fences all further intake and waits for the originally accepted send,
    // even though its native stream waiter has already observed Closed.
    router.join().await.unwrap();
    let closed = router
        .full_snapshot(
            2,
            8,
            typ::Vote::new_committed(3, 1),
            snapshot(),
            std::future::pending(),
            RPCOption::new(Duration::from_secs(2)),
        )
        .await
        .unwrap_err();
    assert!(closed.to_string().contains("native snapshot owner closed"));
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}
