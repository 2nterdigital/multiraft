# Owned application RPC

`multiraft_net::application_rpc` exposes a standalone `ApplicationRpcOwner`, weak
`ApplicationRpcHandle`, numeric `RpcCall`, opaque `RpcReply`, `RpcContext`, bounded
`RpcMetadata` and consumer-supplied `ApplicationRpcHandler`. It reuses the existing
application gRPC service and generic `GrpcPeerChannelPool`. Application and Raft
RPC remain distinct purposes/address catalogs. The library knows no business
service registry, payload schema, routing policy, Conversation or CommandId.

Each call makes one local/remote attempt under its original absolute deadline,
covering admission, request validation, connection acquisition, dispatch and
response. No retry, redirection or broadcast. Local nested contexts preserve the
same Instant. Remote grpc-timeout carries only remaining budget; the receiving
process derives its own absolute Instant. Missing/malformed/zero timeout fails
before invoking the handler. The270336-byte cap is the encoded protobuf outer
size, including scalar/tag/length overhead, on both request and response. Server
and client encoding/decoding limits match. Raw Tonic0.12 OutOfRange normalizes to
ResourceExhausted as in the previous consumer transport.

Transport admission is now finite: default16,384 immediately admitted operations,
configurable1..65,536, independent from consumer business gates. There was no
finite generic admission limit in the old Ech0 implementation; this is explicitly
new resource fencing rather than a claim about a preserved old value. It is not a
memory/capacity bound. HTTP/2 concurrent streams per connection use that limit;
application handlers get immediate ResourceExhausted at full intake, no new queue.
Metadata allows eight configured ASCII keys,64 bytes/key,256 bytes/value and2048
aggregate bytes; reserved transport/auth keys and unconfigured outgoing names
are refused. Unknown incoming headers are ignored. Opaque values and payloads
are redacted in Debug. Display errors are bounded to256 UTF-8 bytes; no arbitrary
handler/native text is emitted by library logging.

`RpcError` retains kind, phase, dispatch fact, optional validated source Node and
original numeric gRPC code. `NotDispatched` requires a known admission/validation/
connection/pre-handler refusal. `MayHaveDispatched` starts synchronously BEFORE
calling the trait method (which itself may do work) or starting Tonic dispatch.
An after-dispatch timeout/closure/cancellation/lost response cannot imply rollback
or no effect. Handler/response errors remain MayHaveDispatched. Native Tonic
Timeout middleware may produce CANCELLED/code1 before the local timer wins;
existing Unavailable mapping and the code are preserved, never inferred from
message text or relabeled as a zero-dispatch result.

A remote server error carries a versioned fixed12-byte neutral source record
(version1,8-byte Node,kind,phase,dispatch). Client validates length/version/Node/
enums/phase-dispatch consistency and gRPC code-kind consistency. Missing or
malformed/foreign records remain conservative MayHaveDispatched. Handler errors
cannot forge transport zero-dispatch. The record is request-local transport fact,
not business control semantics, request deduplication or an authorization token.
Application responses retain their own stronger domain result/evidence.

Request cancellation drops its local/remote handler Future rather than retaining
arbitrary business work; already-accepted native operations retain their separate
native/library owner. Accepted calls retain exact pool leases. A lost/canceled
transport generation invalidates only that lease; stale failures cannot clear a
replacement. Deterministic handler rejection does not evict a healthy generation.
A subsequent explicit caller can reconnect; this call never repeats itself.

Owner shutdown fences new calls, drains operations, closes/drains the pool,
signals/joins the retained listener, and ends accepted connection IO. Cancellation
abandons only the stop waiter; cleanup remains on the originating runtime. Direct
Drop synchronously fences and cancels admitted handler Futures, then retains the
same cleanup. A30-second graceful drain budget signals cancellation for longer
requests; bounded synchronous callbacks cannot be forcibly preempted and success
is not reported before they actually end. Reply/connection shutdown gets a5-second
grace; after it, receiver-only accepted IO is closed to end stalled decoders or
idle prefaces. Tonic's shutdown watcher joins its spawned connections, then the
library registry joins the listener. Successful shutdown is a task/port reuse seam;
an expired stop wait is unconfirmed cleanup, never proof of prior effect reversal.

External `application_rpc_consumer` tests use only the public owner/handle/handler,
plus raw generated clients for negative wire admission. They prove local/remote
bytes/context, single-attempt failures/stages, caps/timeouts, cancellation, weak
Drop and canceled stop, port reuse, restart generations and finite admission.
The lower source-record test verifies missing/malformed/foreign/contradictory
record conservatism. It adds no production probe or consensus/runtime algorithm.
