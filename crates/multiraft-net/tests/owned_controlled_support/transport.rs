//! Faults delay real authenticated managed target replies, preserving original request bytes.
use super::*;
use multiraft_net::portable::{
    NativeTargetCommand, NativeTargetOutcome, NativeTargetRequest, NativeTargetTransport,
    NativeTargetTransportError,
};
use tokio::sync::Notify;
#[derive(Clone, Copy)]
pub enum Gate {
    FirstChunk,
    FinishedReply,
}
pub struct GatedTransport {
    pub endpoint: multiraft_net::ManagedTargetEndpoint<DiskImage>,
    pub gate: Gate,
    pub entered: Arc<Notify>,
    pub release: Arc<Notify>,
}
impl NativeTargetTransport for GatedTransport {
    fn peer(&self) -> (u64, Option<[u8; 32]>) {
        let id = self.endpoint.identity();
        (id.node, id.domain)
    }
    fn exchange(
        &self,
        request: Vec<u8>,
    ) -> StartupAuthorityFuture<'_, Result<Vec<u8>, NativeTargetTransportError>> {
        Box::pin(async move {
            let wire: NativeTargetRequest = serde_json::from_slice(&request).unwrap();
            let before = matches!(
                (self.gate, &wire.command),
                (
                    Gate::FirstChunk,
                    NativeTargetCommand::SeedChunk { sequence: 0, .. }
                )
            );
            let after = matches!(
                (self.gate, &wire.command),
                (Gate::FinishedReply, NativeTargetCommand::FinishSeed)
            );
            if before {
                self.entered.notify_one();
                self.release.notified().await;
            }
            let reply = self
                .endpoint
                .dispatch(request, deadline())
                .await
                .map_err(|error| NativeTargetTransportError {
                    outcome: NativeTargetOutcome::Unknown,
                    message: error.to_string(),
                })?;
            if after {
                self.entered.notify_one();
                self.release.notified().await;
            }
            Ok(reply)
        })
    }
}
