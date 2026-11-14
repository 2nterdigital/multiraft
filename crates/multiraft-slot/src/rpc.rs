//! One application-RPC hop. Compatibility fingerprints are not authentication.
use crate::limits::{MAX_COMMAND_BYTES, MAX_EFFECTS_BYTES, MAX_QUERY_BYTES};
use crate::runtime::Shared;
use crate::{
    LinearizableView, ManifestDigest, NodeId, ObservedLogId, RuntimeError, SlotApplied, SlotId,
    SlotStateMachine,
};
use bincode::Options;
use multiraft_net::application_rpc::{
    ApplicationRpcHandler, RpcCall, RpcContext, RpcDispatch, RpcErrorKind, RpcFuture,
    RpcHandlerError, RpcReply, APPLICATION_RPC_OUTER_BYTES,
};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Weak};
use tokio::time::Instant;

const SERVICE: u32 = 0x534c4f54;
const METHOD: u32 = 1;
// 2: 256-bucket route table and compatibility digest (strict startup binds the data space).
const VERSION: u32 = 2;
const CONTROLLED_VERSION: u32 = 3;
// Reserve ample room for protobuf tags, IDs and transport framing.
const MAX_ENVELOPE: usize = APPLICATION_RPC_OUTER_BYTES - 1024;
#[derive(Serialize, Deserialize)]
struct Envelope {
    version: u32,
    cluster: String,
    digest: ManifestDigest,
    slot: SlotId,
    key: Vec<u8>,
    operation: Operation,
}
// These are identity/address data fences against the App-verified catalog. They
// do not cryptographically authenticate callers; trusted-network assumptions remain.
#[derive(Serialize, Deserialize)]
pub(crate) struct CarrierIdentity {
    pub node_id: NodeId,
    pub identity: Option<Vec<u8>>,
}
#[derive(Serialize, Deserialize)]
struct ControlledEnvelope {
    request: Envelope,
    peer: CarrierIdentity,
}
#[derive(Serialize, Deserialize)]
struct ControlledReply {
    reply: Reply,
    peer: CarrierIdentity,
}
fn version<S: SlotStateMachine>(shared: &Shared<S>) -> u32 {
    if shared.controlled_profile() {
        CONTROLLED_VERSION
    } else {
        VERSION
    }
}
#[derive(Serialize, Deserialize)]
enum Operation {
    Hint,
    SlotHint,
    Write(Vec<u8>),
    Read(Vec<u8>),
    // Appended so older variants keep their encoding; a peer that predates it
    // rejects the request as invalid instead of misreading it.
    ReadWithApplied(Vec<u8>),
}
#[derive(Serialize, Deserialize)]
struct Reply {
    version: u32,
    digest: ManifestDigest,
    result: Result<Value, RuntimeError>,
}
#[derive(Serialize, Deserialize)]
enum Value {
    Hint(Option<NodeId>),
    Applied(SlotApplied),
    Read(Vec<u8>),
    ReadWithApplied { value: Vec<u8>, applied: WireLogId },
}
/// `ObservedLogId` on the wire; the public type stays free of serde.
#[derive(Serialize, Deserialize)]
struct WireLogId {
    term: u64,
    node_id: NodeId,
    index: u64,
}
fn read_value(view: LinearizableView<Vec<u8>>, with_applied: bool) -> Value {
    if !with_applied {
        return Value::Read(view.value);
    }
    Value::ReadWithApplied {
        value: view.value,
        applied: WireLogId {
            term: view.applied.term,
            node_id: view.applied.node_id,
            index: view.applied.index,
        },
    }
}
fn codec() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(MAX_ENVELOPE as u64)
        .reject_trailing_bytes()
}
fn encode<T: Serialize>(v: &T) -> Result<Vec<u8>, RuntimeError> {
    let bytes = codec()
        .serialize(v)
        .map_err(|_| RuntimeError::PayloadTooLarge)?;
    if bytes.len() > MAX_ENVELOPE {
        return Err(RuntimeError::PayloadTooLarge);
    }
    Ok(bytes)
}
fn decode<T: serde::de::DeserializeOwned>(v: &[u8]) -> Result<T, RuntimeError> {
    if v.len() > MAX_ENVELOPE {
        return Err(RuntimeError::PayloadTooLarge);
    }
    codec()
        .deserialize(v)
        .map_err(|_| RuntimeError::InvalidRequest)
}

pub(crate) struct Handler<S: SlotStateMachine> {
    pub shared: Weak<Shared<S>>,
}
impl<S: SlotStateMachine> ApplicationRpcHandler for Handler<S> {
    fn call(&self, context: RpcContext, call: RpcCall) -> RpcFuture<'_> {
        Box::pin(async move {
            let shared = self
                .shared
                .upgrade()
                .ok_or_else(|| RpcHandlerError::new(RpcErrorKind::Closed, "slot runtime closed"))?;
            let result = async {
                shared.check(context.deadline)?;
                if call.service_id != SERVICE || call.method_id != METHOD {
                    return Err(RuntimeError::InvalidRequest);
                }
                let request: Envelope = if shared.controlled_profile() {
                    let wire: ControlledEnvelope = decode(&call.payload)?;
                    shared.check_carrier_identity(&wire.peer)?;
                    wire.request
                } else {
                    decode(&call.payload)?
                };
                if request.version != version(&shared)
                    || request.cluster != shared.layout.cluster_id()
                    || request.digest != shared.transport_digest
                {
                    return Err(RuntimeError::LayoutMismatch);
                }
                let slot = if matches!(request.operation, Operation::SlotHint) {
                    // Read-only slot observation has no business key or mutation path.
                    if !request.key.is_empty() {
                        return Err(RuntimeError::InvalidRequest);
                    }
                    shared
                        .layout
                        .group_for_slot(request.slot)
                        .map_err(|_| RuntimeError::InvalidRequest)?;
                    request.slot
                } else {
                    shared
                        .layout
                        .slot_for_key(&request.key)
                        .map_err(|_| RuntimeError::PayloadTooLarge)?
                };
                if slot != request.slot {
                    return Err(RuntimeError::InvalidRequest);
                }
                if !shared.locally_routable(slot, context.deadline).await? {
                    return Err(RuntimeError::NotLeader { hint: None });
                }
                match request.operation {
                    Operation::Hint | Operation::SlotHint => Ok(Value::Hint(
                        local_hint(&shared, slot, context.deadline).await?,
                    )),
                    Operation::Write(command) => {
                        if command.len() > MAX_COMMAND_BYTES {
                            return Err(RuntimeError::PayloadTooLarge);
                        }
                        // The destination does not recursively route or retry this write.
                        shared
                            .accept_write(slot, request.key, command, context.deadline)
                            .await
                            .map(Value::Applied)
                    }
                    Operation::Read(query) | Operation::ReadWithApplied(query)
                        if query.len() > MAX_QUERY_BYTES =>
                    {
                        Err(RuntimeError::PayloadTooLarge)
                    }
                    Operation::Read(query) => shared
                        .local_query(slot, request.key.len(), &query, context.deadline)
                        .await
                        .map(|view| read_value(view, false)),
                    Operation::ReadWithApplied(query) => shared
                        .local_query(slot, request.key.len(), &query, context.deadline)
                        .await
                        .map(|view| read_value(view, true)),
                }
            }
            .await;
            let reply = Reply {
                version: version(&shared),
                digest: shared.transport_digest,
                result,
            };
            let bytes = (if shared.controlled_profile() {
                shared
                    .carrier_identity()
                    .and_then(|peer| encode(&ControlledReply { reply, peer }))
            } else {
                encode(&reply)
            })
            .map_err(|_| {
                RpcHandlerError::new(RpcErrorKind::Internal, "slot reply encoding failed")
            })?;
            Ok(RpcReply::new(bytes))
        })
    }
}
async fn local_hint<S: SlotStateMachine>(
    shared: &Shared<S>,
    slot: SlotId,
    deadline: Instant,
) -> Result<Option<NodeId>, RuntimeError> {
    let group = shared
        .layout
        .group_for_slot(slot)
        .map_err(|_| RuntimeError::InvalidRequest)?;
    shared
        .native
        .wait_leader_hint(group, deadline)
        .await
        .map(|h| h.and_then(|h| h.candidate))
        .map_err(|e| crate::runtime_error::native(e, false))
}
pub(crate) async fn route_hint<S: SlotStateMachine>(
    shared: &Arc<Shared<S>>,
    slot: SlotId,
    deadline: Instant,
) -> Result<Option<NodeId>, RuntimeError> {
    routing_hint(shared, slot, None, deadline).await
}
// A locally present Core can retain an old joint membership after exclusion.
// Wait for its hint only within a slice; absence may discover actual remote
// authority, while local errors and every application dispatch remain unchanged.
async fn routing_hint<S: SlotStateMachine>(
    shared: &Arc<Shared<S>>,
    slot: SlotId,
    key: Option<&[u8]>,
    deadline: Instant,
) -> Result<Option<NodeId>, RuntimeError> {
    shared.check(deadline)?;
    let voters = shared.routing_voters(slot, deadline).await?;
    let hint = if shared.locally_routable(slot, deadline).await? {
        let now = Instant::now();
        let allowance =
            (deadline.saturating_duration_since(now) / 2).min(std::time::Duration::from_secs(1));
        match local_hint(shared, slot, now + allowance).await? {
            Some(candidate) => Some(candidate),
            None => probe_voters(shared, slot, key, deadline).await?,
        }
    } else {
        probe_voters(shared, slot, key, deadline).await?
    };
    if hint.is_some_and(|hint| !voters.contains(&hint)) {
        return Err(RuntimeError::Unavailable);
    }
    Ok(hint)
}
// Only read-only hint probes may fail over. Reserve time for every remaining
// voter and the one eventual application operation under the original deadline.
async fn probe_voters<S: SlotStateMachine>(
    shared: &Shared<S>,
    slot: SlotId,
    key: Option<&[u8]>,
    deadline: Instant,
) -> Result<Option<NodeId>, RuntimeError> {
    let voters = shared.routing_voters(slot, deadline).await?;
    let mut observed = false;
    for (index, voter) in voters.iter().enumerate() {
        shared.check(deadline)?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        let divisor =
            u32::try_from(voters.len() - index + 1).map_err(|_| RuntimeError::InvalidConfig)?;
        let allowance = (remaining / divisor).min(std::time::Duration::from_secs(1));
        let probe_deadline = Instant::now() + allowance;
        let (key, operation) = match key {
            Some(key) => (key.to_vec(), Operation::Hint),
            None => (Vec::new(), Operation::SlotHint),
        };
        match remote(shared, *voter, slot, key, operation, probe_deadline, false).await {
            Ok(Value::Hint(Some(candidate))) if voters.contains(&candidate) => {
                return Ok(Some(candidate))
            }
            Ok(Value::Hint(_)) => observed = true,
            _ => {}
        }
    }
    if observed {
        Ok(None)
    } else {
        Err(RuntimeError::Unavailable)
    }
}
async fn destination<S: SlotStateMachine>(
    shared: &Arc<Shared<S>>,
    slot: SlotId,
    key: &[u8],
    deadline: Instant,
) -> Result<NodeId, RuntimeError> {
    routing_hint(shared, slot, Some(key), deadline)
        .await?
        .ok_or(RuntimeError::Unavailable)
}
pub(crate) async fn route_write<S: SlotStateMachine>(
    shared: &Arc<Shared<S>>,
    slot: SlotId,
    key: Vec<u8>,
    command: Vec<u8>,
    deadline: Instant,
) -> Result<SlotApplied, RuntimeError> {
    let target = destination(shared, slot, &key, deadline).await?;
    if target == shared.node_id {
        return shared.execute_local(slot, command, deadline).await;
    }
    match remote(
        shared,
        target,
        slot,
        key,
        Operation::Write(command),
        deadline,
        true,
    )
    .await?
    {
        Value::Applied(a) if a.slot == slot && a.effects.len() <= MAX_EFFECTS_BYTES => Ok(a),
        _ => Err(RuntimeError::Unknown),
    }
}
pub(crate) async fn route_read<S: SlotStateMachine>(
    shared: &Arc<Shared<S>>,
    slot: SlotId,
    key: Vec<u8>,
    query: Vec<u8>,
    deadline: Instant,
) -> Result<Vec<u8>, RuntimeError> {
    match route_query(shared, slot, key, query, deadline, false).await? {
        Value::Read(v) if v.len() <= MAX_EFFECTS_BYTES => Ok(v),
        _ => Err(RuntimeError::Unavailable),
    }
}
pub(crate) async fn route_read_with_applied<S: SlotStateMachine>(
    shared: &Arc<Shared<S>>,
    slot: SlotId,
    key: Vec<u8>,
    query: Vec<u8>,
    deadline: Instant,
) -> Result<LinearizableView<Vec<u8>>, RuntimeError> {
    match route_query(shared, slot, key, query, deadline, true).await? {
        Value::ReadWithApplied { value, applied } if value.len() <= MAX_EFFECTS_BYTES => {
            Ok(LinearizableView {
                value,
                applied: ObservedLogId::new(applied.term, applied.node_id, applied.index),
            })
        }
        _ => Err(RuntimeError::Unavailable),
    }
}
async fn route_query<S: SlotStateMachine>(
    shared: &Arc<Shared<S>>,
    slot: SlotId,
    key: Vec<u8>,
    query: Vec<u8>,
    deadline: Instant,
    with_applied: bool,
) -> Result<Value, RuntimeError> {
    shared.check(deadline)?;
    let _guard = shared
        .budget
        .acquire(slot, key.len() + query.len() + MAX_EFFECTS_BYTES)?;
    let target = destination(shared, slot, &key, deadline).await?;
    if target == shared.node_id {
        let view = shared.execute_query(slot, &query, deadline).await?;
        return Ok(read_value(view, with_applied));
    }
    let operation = if with_applied {
        Operation::ReadWithApplied(query)
    } else {
        Operation::Read(query)
    };
    remote(shared, target, slot, key, operation, deadline, false).await
}
async fn remote<S: SlotStateMachine>(
    shared: &Shared<S>,
    target: NodeId,
    slot: SlotId,
    key: Vec<u8>,
    operation: Operation,
    deadline: Instant,
    write: bool,
) -> Result<Value, RuntimeError> {
    shared.check(deadline)?;
    let request = Envelope {
        version: version(shared),
        cluster: shared.layout.cluster_id().to_owned(),
        digest: shared.transport_digest,
        slot,
        key,
        operation,
    };
    let payload = if shared.controlled_profile() {
        encode(&ControlledEnvelope {
            request,
            peer: shared.carrier_identity()?,
        })?
    } else {
        encode(&request)?
    };
    let response = shared
        .rpc()?
        .call(
            target,
            RpcCall::new(SERVICE, METHOD, payload),
            RpcContext::new(deadline),
        )
        .await
        .map_err(|e| {
            if write && e.dispatch == RpcDispatch::MayHaveDispatched {
                RuntimeError::Unknown
            } else {
                RuntimeError::Unavailable
            }
        })?;
    let decoded = if shared.controlled_profile() {
        decode::<ControlledReply>(&response.payload).and_then(|wire| {
            if wire.peer.node_id != target {
                return Err(RuntimeError::LayoutMismatch);
            }
            shared.check_carrier_identity(&wire.peer)?;
            Ok(wire.reply)
        })
    } else {
        decode::<Reply>(&response.payload)
    };
    let reply = decoded.map_err(|_| {
        if write {
            RuntimeError::Unknown
        } else {
            RuntimeError::Unavailable
        }
    })?;
    if reply.version != version(shared) || reply.digest != shared.transport_digest {
        return Err(if write {
            RuntimeError::Unknown
        } else {
            RuntimeError::LayoutMismatch
        });
    }
    reply.result
}
