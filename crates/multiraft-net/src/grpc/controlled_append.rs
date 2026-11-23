//! Additive JSON AppendEntries protocol for typed native control entries.
use multiraft_core::TypeConfig;
use multiraft_fsm::StateMachine;
use multiraft_store::Raft;
use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse};
use tonic::Status;

pub(super) const PATH: &str = "/raft/append-json/v1";

// Tonic requires the concrete wire status at this boundary.
#[allow(clippy::result_large_err)]
pub(super) async fn receive<S: StateMachine>(
    raft: &Raft<S>,
    payload: &[u8],
) -> Result<Vec<u8>, Status> {
    let request: AppendEntriesRequest<TypeConfig> = serde_json::from_slice(payload)
        .map_err(|_| Status::invalid_argument("invalid controlled append JSON v1"))?;
    // Alpha.30 peers expect an `AppendEntriesResponse` on the wire.
    let result = raft
        .append_entries(request)
        .await
        .map(AppendEntriesResponse::<TypeConfig>::from);
    serde_json::to_vec(&result).map_err(|_| Status::internal("controlled append response encoding"))
}
