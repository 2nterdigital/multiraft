//! Shared application service, required grpc-timeout and versioned neutral failure facts.
use super::owner::{closed, expired, Shared};
use super::*;
use crate::node_rpc::{
    node_rpc_service_server::{NodeRpcService, NodeRpcServiceServer},
    NodeRpcRequest, NodeRpcResponse,
};
use prost::Message;
use std::sync::Weak;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Code, Request, Response, Status};

#[derive(Clone)]
pub(super) struct RpcService {
    pub shared: Weak<Shared>,
    pub node: NodeId,
    pub limit: u32,
}
pub(super) async fn serve(
    listener: tokio::net::TcpListener,
    service: RpcService,
    stopped: oneshot::Receiver<()>,
    io_stop: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    use futures::StreamExt;
    let incoming = TcpListenerStream::new(listener)
        .map(move |accepted| accepted.map(|io| connection::RpcIo::new(io, io_stop.clone())));
    tonic::transport::Server::builder()
        .max_concurrent_streams(Some(service.limit))
        .layer(normalize::NormalizeOutOfRangeLayer)
        .add_service(
            NodeRpcServiceServer::new(service)
                .max_decoding_message_size(APPLICATION_RPC_OUTER_BYTES)
                .max_encoding_message_size(APPLICATION_RPC_OUTER_BYTES),
        )
        .serve_with_incoming_shutdown(incoming, async {
            let _ = stopped.await;
        })
        .await?;
    Ok(())
}
#[tonic::async_trait]
impl NodeRpcService for RpcService {
    async fn call(
        &self,
        request: Request<NodeRpcRequest>,
    ) -> Result<Response<NodeRpcResponse>, Status> {
        let result = async {
            let shared = self
                .shared
                .upgrade()
                .ok_or_else(|| closed(RpcPhase::Admission))?;
            let _permit = shared.core.admit()?;
            let context = context(&request, &shared.core.keys)?;
            let request = request.into_inner();
            let reply = shared
                .core
                .attempt(
                    self.node,
                    RpcCall::new(request.service_id, request.method_id, request.payload),
                    context,
                )
                .await?;
            Ok::<_, RpcError>(Response::new(NodeRpcResponse {
                payload: reply.payload,
            }))
        }
        .await;
        result.map_err(|error| status_from_error(error, self.node))
    }
}
fn context(
    request: &Request<NodeRpcRequest>,
    keys: &BTreeSet<String>,
) -> Result<RpcContext, RpcError> {
    let invalid = || {
        RpcError::new(
            RpcErrorKind::InvalidArgument,
            RpcPhase::RequestValidation,
            RpcDispatch::NotDispatched,
            "application RPC requires valid grpc-timeout",
        )
    };
    let raw = request
        .metadata()
        .get("grpc-timeout")
        .ok_or_else(invalid)?
        .to_str()
        .map_err(|_| invalid())?;
    let budget = parse_timeout(raw).ok_or_else(invalid)?;
    if budget.is_zero() {
        return Err(expired(
            RpcPhase::RequestValidation,
            RpcDispatch::NotDispatched,
        ));
    }
    let deadline = Instant::now().checked_add(budget).ok_or_else(invalid)?;
    let mut metadata = RpcMetadata::default();
    for key in keys {
        let values = request.metadata().get_all(key.as_str());
        let mut values = values.iter();
        if let Some(value) = values.next() {
            if values.next().is_some() {
                return Err(invalid());
            }
            metadata.insert(key, value.to_str().map_err(|_| invalid())?)?;
        }
    }
    Ok(RpcContext { deadline, metadata })
}
fn parse_timeout(raw: &str) -> Option<Duration> {
    if !(2..=9).contains(&raw.len()) {
        return None;
    }
    let (digits, unit) = raw.split_at(raw.len() - 1);
    if digits.len() > 8 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let n = digits.parse::<u64>().ok()?;
    match unit {
        "H" => Some(Duration::from_secs(n * 3600)),
        "M" => Some(Duration::from_secs(n * 60)),
        "S" => Some(Duration::from_secs(n)),
        "m" => Some(Duration::from_millis(n)),
        "u" => Some(Duration::from_micros(n)),
        "n" => Some(Duration::from_nanos(n)),
        _ => None,
    }
}
pub(super) fn ensure_request(call: &RpcCall) -> Result<(), RpcError> {
    if call.payload.len() > APPLICATION_RPC_OUTER_BYTES {
        return ensure_size(
            call.payload.len(),
            RpcPhase::RequestValidation,
            RpcDispatch::NotDispatched,
        );
    }
    let request = NodeRpcRequest {
        service_id: call.service_id,
        method_id: call.method_id,
        payload: call.payload.clone(),
    };
    ensure_size(
        request.encoded_len(),
        RpcPhase::RequestValidation,
        RpcDispatch::NotDispatched,
    )
}
pub(super) fn ensure_response(reply: &RpcReply) -> Result<(), RpcError> {
    if reply.payload.len() > APPLICATION_RPC_OUTER_BYTES {
        return ensure_size(
            reply.payload.len(),
            RpcPhase::Response,
            RpcDispatch::MayHaveDispatched,
        );
    }
    let response = NodeRpcResponse {
        payload: reply.payload.clone(),
    };
    ensure_size(
        response.encoded_len(),
        RpcPhase::Response,
        RpcDispatch::MayHaveDispatched,
    )
}
fn ensure_size(bytes: usize, phase: RpcPhase, dispatch: RpcDispatch) -> Result<(), RpcError> {
    if bytes <= APPLICATION_RPC_OUTER_BYTES {
        Ok(())
    } else {
        Err(RpcError::new(
            RpcErrorKind::ResourceExhausted,
            phase,
            dispatch,
            "application RPC outer message exceeds cap",
        ))
    }
}
fn status_from_error(error: RpcError, node: NodeId) -> Status {
    let code = status_code(error.kind);
    // Fixed12-byte record. No handler/business bytes, raw native error or string classification.
    let mut record = vec![1];
    record.extend_from_slice(&node.to_be_bytes());
    record.push(error.kind as u8);
    record.push(error.phase as u8);
    record.push(match error.dispatch {
        RpcDispatch::NotDispatched => 0,
        RpcDispatch::MayHaveDispatched => 1,
    });
    Status::with_details(code, error.message(), record.into())
}
pub(super) fn error_from_status(status: &Status, target: NodeId) -> RpcError {
    let kind = match status.code() {
        Code::DeadlineExceeded => RpcErrorKind::DeadlineExceeded,
        Code::ResourceExhausted | Code::OutOfRange => RpcErrorKind::ResourceExhausted,
        Code::InvalidArgument => RpcErrorKind::InvalidArgument,
        Code::Unimplemented => RpcErrorKind::Unimplemented,
        Code::Unavailable | Code::Cancelled => RpcErrorKind::Unavailable,
        _ => RpcErrorKind::Internal,
    };
    let mut error = RpcError::new(
        kind,
        RpcPhase::Dispatch,
        RpcDispatch::MayHaveDispatched,
        status.message(),
    );
    error.status_code = u8::try_from(status.code() as i32).ok();
    if let Some((kind, phase, dispatch)) = decode_record(status.details(), target) {
        if status_code(kind) != status.code() {
            return error;
        }
        error.kind = kind;
        error.phase = phase;
        error.dispatch = dispatch;
        error.source_node = Some(target);
    }
    error
}
fn decode_record(raw: &[u8], target: NodeId) -> Option<(RpcErrorKind, RpcPhase, RpcDispatch)> {
    if raw.len() != 12 || raw[0] != 1 || u64::from_be_bytes(raw[1..9].try_into().ok()?) != target {
        return None;
    }
    let kind = match raw[9] {
        1 => RpcErrorKind::InvalidConfiguration,
        2 => RpcErrorKind::Closed,
        3 => RpcErrorKind::UnknownPeer,
        4 => RpcErrorKind::DeadlineExceeded,
        5 => RpcErrorKind::ResourceExhausted,
        6 => RpcErrorKind::InvalidArgument,
        7 => RpcErrorKind::Unimplemented,
        8 => RpcErrorKind::Unavailable,
        9 => RpcErrorKind::Internal,
        _ => return None,
    };
    let phase = match raw[10] {
        1 => RpcPhase::Admission,
        2 => RpcPhase::RequestValidation,
        3 => RpcPhase::Connect,
        4 => RpcPhase::Dispatch,
        5 => RpcPhase::Handler,
        6 => RpcPhase::Response,
        7 => RpcPhase::Shutdown,
        _ => return None,
    };
    let dispatch = match raw[11] {
        0 => RpcDispatch::NotDispatched,
        1 => RpcDispatch::MayHaveDispatched,
        _ => return None,
    };
    // A remote handler/response may never claim transport zero-dispatch.
    if dispatch == RpcDispatch::NotDispatched
        && !matches!(
            phase,
            RpcPhase::Admission | RpcPhase::RequestValidation | RpcPhase::Dispatch
        )
    {
        return None;
    }
    Some((kind, phase, dispatch))
}
mod connection;
mod normalize;

fn status_code(kind: RpcErrorKind) -> Code {
    match kind {
        RpcErrorKind::InvalidArgument => Code::InvalidArgument,
        RpcErrorKind::ResourceExhausted => Code::ResourceExhausted,
        RpcErrorKind::Unimplemented => Code::Unimplemented,
        RpcErrorKind::DeadlineExceeded => Code::DeadlineExceeded,
        RpcErrorKind::Closed | RpcErrorKind::UnknownPeer | RpcErrorKind::Unavailable => {
            Code::Unavailable
        }
        _ => Code::Internal,
    }
}

#[cfg(test)]
mod tests;
