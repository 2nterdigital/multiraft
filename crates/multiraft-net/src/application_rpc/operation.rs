//! Exactly one attempt. Progress moves synchronously before invoking a handler/RPC.
use super::owner::{closed, expired, ApplicationRpcHandle, Core};
use super::*;
use crate::node_rpc::{node_rpc_service_client::NodeRpcServiceClient, NodeRpcRequest};
use crate::{GrpcPeerChannelError, GrpcPeerChannelLease, GrpcPeerChannelPool};
use futures::FutureExt;
use std::sync::atomic::Ordering;
use tonic::{Code, Request};

pub(super) struct Progress {
    phase: RpcPhase,
    dispatch: RpcDispatch,
}
impl ApplicationRpcHandle {
    pub async fn call(
        &self,
        target: NodeId,
        call: RpcCall,
        context: RpcContext,
    ) -> Result<RpcReply, RpcError> {
        let shared = self
            .shared
            .upgrade()
            .ok_or_else(|| closed(RpcPhase::Admission))?;
        let _permit = shared.core.admit()?;
        shared.core.attempt(target, call, context).await
    }
}
impl Core {
    pub(super) async fn attempt(
        &self,
        target: NodeId,
        call: RpcCall,
        context: RpcContext,
    ) -> Result<RpcReply, RpcError> {
        let mut progress = Progress {
            phase: RpcPhase::RequestValidation,
            dispatch: RpcDispatch::NotDispatched,
        };
        let mut cancel = self.cancel.subscribe();
        let deadline = context.deadline;
        if *cancel.borrow_and_update() {
            return Err(closed(progress.phase));
        }
        if Instant::now() >= deadline {
            return Err(expired(progress.phase, progress.dispatch));
        }
        context.metadata.validate(&self.keys)?;
        super::wire::ensure_request(&call)?;
        let outcome = {
            let future = self.call_once(target, call, context, &mut progress);
            tokio::select! {
                biased;
                _=cancel.changed()=>None,
                result=tokio::time::timeout_at(deadline,future)=>Some(result),
            }
        };
        match outcome {
            None => Err(RpcError::new(
                RpcErrorKind::Closed,
                progress.phase,
                progress.dispatch,
                "application RPC owner interrupted call",
            )),
            Some(Err(_)) => Err(expired(progress.phase, progress.dispatch)),
            Some(Ok(result)) => result,
        }
    }
    async fn call_once(
        &self,
        target: NodeId,
        call: RpcCall,
        context: RpcContext,
        progress: &mut Progress,
    ) -> Result<RpcReply, RpcError> {
        if target == self.node {
            if !self.accepting.load(Ordering::Acquire) {
                return Err(closed(RpcPhase::Dispatch));
            }
            if Instant::now() >= context.deadline {
                return Err(expired(RpcPhase::Dispatch, RpcDispatch::NotDispatched));
            }
            progress.phase = RpcPhase::Handler;
            progress.dispatch = RpcDispatch::MayHaveDispatched;
            // The synchronous trait method itself can execute work before returning a Future.
            let future = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.handler.call(context, call)
            }))
            .map_err(|_| handler_panic())?;
            let reply = std::panic::AssertUnwindSafe(future)
                .catch_unwind()
                .await
                .map_err(|_| handler_panic())?
                .map_err(|e| {
                    RpcError::new(
                        e.kind,
                        RpcPhase::Handler,
                        RpcDispatch::MayHaveDispatched,
                        e.message(),
                    )
                })?;
            progress.phase = RpcPhase::Response;
            super::wire::ensure_response(&reply)?;
            return Ok(reply);
        }
        progress.phase = RpcPhase::Connect;
        let lease = self.channels.lease(target).await.map_err(|error| {
            let kind = match error {
                GrpcPeerChannelError::UnknownPeer { .. } => RpcErrorKind::UnknownPeer,
                GrpcPeerChannelError::Closed { .. } => RpcErrorKind::Closed,
                _ => RpcErrorKind::Unavailable,
            };
            RpcError::new(
                kind,
                RpcPhase::Connect,
                RpcDispatch::NotDispatched,
                "application RPC peer connection unavailable",
            )
        })?;
        let mut guard = LeaseGuard {
            pool: &self.channels,
            lease,
            dispatched: false,
            finished: false,
        };
        if Instant::now() >= context.deadline {
            return Err(expired(RpcPhase::Connect, RpcDispatch::NotDispatched));
        }
        if !self.accepting.load(Ordering::Acquire) {
            return Err(closed(RpcPhase::Dispatch));
        }
        let mut request = Request::new(NodeRpcRequest {
            service_id: call.service_id,
            method_id: call.method_id,
            payload: call.payload,
        });
        request.set_timeout(context.deadline.saturating_duration_since(Instant::now()));
        for (key, value) in context.metadata.iter() {
            let name =
                tonic::metadata::MetadataKey::from_bytes(key.as_bytes()).expect("validated key");
            let value = tonic::metadata::MetadataValue::try_from(value.as_str())
                .expect("validated ASCII value");
            request.metadata_mut().insert(name, value);
        }
        let mut client = NodeRpcServiceClient::new(guard.lease.channel())
            .max_decoding_message_size(APPLICATION_RPC_OUTER_BYTES)
            .max_encoding_message_size(APPLICATION_RPC_OUTER_BYTES);
        progress.phase = RpcPhase::Dispatch;
        progress.dispatch = RpcDispatch::MayHaveDispatched;
        guard.dispatched = true;
        let response = client.call(request).await;
        guard.finished = true;
        let response = match response {
            Ok(response) => response.into_inner(),
            Err(status) => {
                let error = super::wire::error_from_status(&status, target);
                if error.kind == RpcErrorKind::Closed
                    || (error.source_node.is_none()
                        && matches!(status.code(), Code::Unavailable | Code::Cancelled))
                {
                    self.channels.invalidate(&guard.lease);
                }
                return Err(error);
            }
        };
        progress.phase = RpcPhase::Response;
        let reply = RpcReply::new(response.payload);
        super::wire::ensure_response(&reply)?;
        Ok(reply)
    }
}
fn handler_panic() -> RpcError {
    RpcError::new(
        RpcErrorKind::Internal,
        RpcPhase::Handler,
        RpcDispatch::MayHaveDispatched,
        "application RPC handler panicked",
    )
}
struct LeaseGuard<'a> {
    pool: &'a GrpcPeerChannelPool,
    lease: GrpcPeerChannelLease,
    dispatched: bool,
    finished: bool,
}
impl Drop for LeaseGuard<'_> {
    fn drop(&mut self) {
        if self.dispatched && !self.finished {
            self.pool.invalidate(&self.lease);
        }
    }
}
