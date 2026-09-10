//! Inbound tonic server: demux by `group_id` to local Raft handlers.

use std::net::SocketAddr;

use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::Request;
use tonic::Response;
use tonic::Status;

use crate::api;
use crate::encode;
use crate::grpc::proto::raft_service_server::RaftService;
use crate::grpc::proto::raft_service_server::RaftServiceServer;
use crate::grpc::proto::RaftRequest;
use crate::grpc::proto::RaftResponse;
use crate::node::GroupMap;
use multiraft_core::typ;
use multiraft_fsm::StateMachine;

/// Serves the tonic Raft service and dispatches to the same handlers as
/// in-process [`crate::node::Node`].
pub struct GrpcServer;

impl GrpcServer {
    /// Bind `addr` and serve until the accept loop ends.
    pub async fn serve<S: StateMachine + 'static>(
        addr: SocketAddr,
        groups: GroupMap<S>,
    ) -> anyhow::Result<()> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        Self::serve_with_listener(listener, groups).await
    }

    /// Serve on an already-bound listener (so callers can fail-fast on bind).
    pub async fn serve_with_listener<S: StateMachine + 'static>(
        listener: tokio::net::TcpListener,
        groups: GroupMap<S>,
    ) -> anyhow::Result<()> {
        Self::serve_with_listener_and_snapshot_limit(listener, groups, 64 * 1024 * 1024).await
    }

    /// Configure the native snapshot wire envelope while preserving the old non-snapshot floor.
    pub async fn serve_with_listener_and_snapshot_limit<S: StateMachine + 'static>(
        listener: tokio::net::TcpListener,
        groups: GroupMap<S>,
        cap: usize,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(cap > 0 && cap <= 64 * 1024 * 1024, "invalid snapshot cap");
        let incoming = TcpListenerStream::new(listener);
        let wire_limit = (cap + 1024 * 1024).max(4 * 1024 * 1024);
        let svc = RaftServiceServer::new(RaftServiceImpl {
            groups,
            snapshot_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
        })
        .max_decoding_message_size(wire_limit)
        .max_encoding_message_size(wire_limit);
        Server::builder()
            .concurrency_limit_per_connection(32)
            .add_service(svc)
            .serve_with_incoming(incoming)
            .await?;
        Ok(())
    }
}

pub(crate) struct RaftServiceImpl<S: StateMachine> {
    pub(crate) groups: GroupMap<S>,
    snapshot_slots: std::sync::Arc<tokio::sync::Semaphore>,
}

#[tonic::async_trait]
impl<S: StateMachine + 'static> RaftService for RaftServiceImpl<S> {
    async fn call(&self, request: Request<RaftRequest>) -> Result<Response<RaftResponse>, Status> {
        let request = request.into_inner();
        if request.path == "/raft/snapshot" {
            let permit = self
                .snapshot_slots
                .clone()
                .try_acquire_owned()
                .map_err(|_| Status::resource_exhausted("native snapshot install busy"))?;
            let groups = self.groups.clone();
            // The native install owns admission even when its RPC waiter leaves.
            return tokio::spawn(async move {
                let _permit = permit;
                demux_raft_call(&groups, request).await
            })
            .await
            .map_err(|_| Status::internal("native snapshot task stopped"))?;
        }
        demux_raft_call(&self.groups, request).await
    }
}

pub(crate) async fn demux_raft_call<S: StateMachine>(
    groups: &GroupMap<S>,
    req: RaftRequest,
) -> Result<Response<RaftResponse>, Status> {
    let (raft, snapshot_cap) = {
        let groups = groups.lock().unwrap();
        match groups.get(&req.group_id) {
            Some(g) => (g.raft.clone(), g.state_machine.snapshot_byte_limit()),
            None => {
                let payload = encode::<Result<(), typ::RaftError>>(Err(typ::RaftError::Fatal(
                    openraft::error::Fatal::Stopped,
                )));
                return Ok(Response::new(RaftResponse { payload }));
            }
        }
    };

    let res = match req.path.as_str() {
        "/raft/append" => api::append(&raft, &req.payload).await,
        "/raft/snapshot" => api::snapshot(&raft, &req.payload, snapshot_cap).await?,
        "/raft/vote" => api::vote(&raft, &req.payload).await,
        "/raft/transfer_leader" => api::transfer_leader(&raft, &req.payload).await,
        _ => {
            tracing::warn!("unknown grpc path: {}", req.path);
            encode::<Result<(), typ::RaftError>>(Err(typ::RaftError::Fatal(
                openraft::error::Fatal::Stopped,
            )))
        }
    };

    Ok(Response::new(RaftResponse { payload: res }))
}

#[cfg(test)]
#[path = "../../tests/native_service_cancellation/mod.rs"]
mod cancellation_tests;
