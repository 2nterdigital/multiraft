# Slot MultiRaft foundation design

**中文：** [2026-10-16-slot-multiraft-foundation.zh-CN.md](./2026-10-16-slot-multiraft-foundation.zh-CN.md)

**Date:** 2026-10-16
**Status:** First verification version completed: final workspace checks, new three-process RF3 all-case acceptance and legacy regression passed at 76ce0dd. Historical observer risk remains open before production integration. See the [evidence ledger](../evidence/2026-10-16-slot-foundation.md).

The delivery is a runnable three-process RF3 foundation serving multiple fixed Slots through a new generic Slot API. It uses RocksDB consensus logs and a minimal deterministic test FSM with snapshots and recovery. Production ech0 Admit/TiKV integration, old-data migration, online resharding and capacity certification are later work. The [feasibility assessment](./2026-10-16-slot-multiraft-feasibility.md) retains fixed-source facts; the [plan](../plans/2026-10-16-slot-multiraft-foundation.md) defines implementation and evidence gates.

The adopted next-stage [Channel fair admission design](./2026-10-16-slot-channel-fair-admission.md) and [implementation plan](../plans/2026-10-16-slot-channel-fair-admission.md) are not implemented or verified. They preserve the fixed layout and one RocksDB per node. Historical 430-pass/A–F evidence and a different-Slot cold request progressing do not prove Channel fairness.

## 1. Baseline and compatibility

Start from C `28e668b676c803838eb64a04ba160fb10ad88316`. Review and integrate necessary general improvements from M `44baed14b3717b6b6f5f7327790cc97299889e19`, including descriptor/shared restart and Jepsen portability. Port the RocksDB log from R `d6a5dbbb6d2f1c68e9e8bddbbfca18f562c1185c` without replacing C's runtime contracts. Keep the exact OpenRaft fork pin and record any dependency change explicitly.

Preserve C's public FSM factory, owned runtime, exact applied effects, absolute deadlines, dispatch classification, native per-call ReadIndex and asynchronous recovery proof/readiness gates. C/M/R historical checks are evidence only for their recorded source, not the combined tree.

Use independent new data directories. Retain existing data untouched. The new Slot API has no automatic compatibility promise with old APIs, FileLog WAL, ech0 OpenRaft 0.9 data, or old snapshots. A RocksDB marker with missing database must fail closed rather than silently recreate an empty history. Unsupported backend changes require a separate migration procedure.

## 2. Responsibilities

| Component | Owns |
| --- | --- |
| `multiraft-slot` (new crate) | Stable partition mapping, canonical manifest and disk binding, server routing checks, group lifecycle orchestration and bounded requests |
| MultiRaft / OpenRaft | Consensus, native groups/membership, Raft transport, log/FSM driving, ReadIndex and recovery proof/readiness gates |
| RocksDB log backend | Node-local consensus entries and metadata in group namespaces; durable callback ownership and database lifetime |
| Test FSM | Deterministic test command/result state, duplicate handling, query and complete snapshot/restore for foundation verification |
| Future ech0 Admit FSM | Atomic dedup/seq/accepted head/pending/result, flush progress and stop state |
| Future ech0 adapter / TiKV | Eligibility inputs, transactional long-term message storage, delivery, collector and application migration contracts |

RocksDB is not the database for all business FSM state. Replicated FSM authority is reconstructed from committed log plus validated snapshot and applied position. Effects are responses, not an alternative durable state store. TiKV remains long-term message transaction storage.

For future SEND, async delivery and async TiKV persistence are parallel paths after admission. SENDACK means admission committed and applied; it does not certify TiKV commit or RECV. This foundation introduces no replacement production SEND path.

## 3. Fixed layout and identity

`businessPartitionKey` is an opaque byte string. The caller supplies stable business normalization and encoding; the library does not parse Channel/User identities. A manifest fixes schema version, cluster identity, slot_count, hash version/parameters, Slot-to-Group mapping, static replica identities and configuration digest. All local groups must be reconstructed from this manifest before serving. Runtime peer hints are routing information, not execution authority or a substitute for committed membership.

The v1 layout has a positive fixed slot_count and one Group per Slot. SlotId ranges over `0..slot_count-1`; GroupId is `group_id_base + slot_id`, with group_id_base bound into the manifest digest and overflow checked. The base must be configured; 0 is valid and there is no production default or protocol reservation of GroupId 0. This three-process harness uses base 100. No split, merge, online remapping or cross-Slot transaction is introduced. Small test layouts such as 4 or 8 Slots are fixtures, not a production sizing recommendation.

The Slot configuration defines `sha256-v1` with SHA-256 and the following tested byte contract:

```text
H = SHA256(UTF8("multiraft-slot/key/sha256-v1\0") || u64_be(key_length) || key)
slot_id = u64_be_decode(H[0..8]) mod slot_count
```

The terminating zero is one zero byte. Length is in bytes, integers are unsigned and big-endian, and empty keys have length zero. This version has no mutable seed. Store the exact hash version and mapping parameters in the manifest; never use DefaultHasher. Keep known vectors and cross-restart mapping tests bound to this implementation. Any change to these bytes is a new incompatible hash version, not an incidental refactor.

The canonical manifest codec/digest must define field order, lengths, integer encoding and replica ordering. The current layout digest uses SHA-256 domain `multiraft-slot/layout/v1\0`; it binds format/hash version, cluster ID, slot_count, group_id_base, sorted node identities with Raft/application addresses, and sorted Slot/voter assignments. Addresses are persistent layout identity in this version; changing them requires new data. Runtime resource budgets are separate from layout identity. The digest detects incompatibility and is not authentication. The JSON disk manifest separately binds local_node_id to that layout. Reject duplicate identities/groups, invalid slot counts, non-bijective mapping and unsupported versions before group startup. Persist and verify cluster identity, manifest digest and local node identity with the data directory; node identity is disk-bound, not inferred from an address. Initial creation must be durable before readiness. Reopening with conflicting config or a copied directory bound to another node fails closed.

Every participating server verifies the manifest digest and recomputes the Slot from the supplied key. A client-supplied Slot or leader hint cannot authorize a different partition. Validate cluster/node/group membership and bounds before proposal dispatch. Cross-node mismatches fail before they can mutate the wrong group. Persisting a local digest alone does not prove other replicas have the same manifest; the application and native transport checks below enforce compatible execution/replication domains.

The exact digest preimage is the following concatenation; every integer is unsigned big-endian. `bytes(x)` means `u64_be(byte_length(x)) || x`. Hash ID is `u32_be(1)`. Nodes, Slot assignments and each voter list are sorted numerically before encoding. Each address is the UTF-8 output of Rust `SocketAddr::to_string()` (Raft then application); IPv6 scope IDs survive and nonzero flowinfo is rejected.

```text
"multiraft-slot/layout/v1\0" || u32(format_version) || u32(1)
|| bytes(cluster_id_utf8) || u32(slot_count) || u64(group_id_base)
|| u64(node_count) || [u64(node_id) || bytes(raft_address) || bytes(application_address)]*
|| u64(assignment_count) || [u32(slot_id) || u64(voter_count) || u64(voter_id)*]*
```

Input ceilings are 65,536 Slots, 1,024 nodes/voters and 65,536 key bytes; these are defense limits, not measured capacity. `slot-manifest.json` stores normalized layout, digest and local node ID. Creation uses create-new files, file/directory sync and `.slot-manifest.creating`; interrupted creation or a nonempty directory without a manifest fails closed and requires explicit operator recovery.

The disk-verified digest also binds native gRPC Raft transport through exactly one `x-multiraft-fingerprint` metadata value: 64 lowercase hex characters. Server rejection precedes group lookup/snapshot admission for all native paths, and clients validate response metadata. Missing, duplicate, malformed or different fingerprints fail closed. Legacy `None` accepts only absent fingerprints; it is not a wildcard. This prevents incompatible domains from replicating together, but is neither authentication nor a startup all-peer agreement certificate. Slot application envelopes separately validate version, cluster ID, digest, key-derived Slot and destination voter membership.

## 4. Implemented API and routing

The public API is in `multiraft-slot`; `S: SlotStateMachine` extends the generic FSM with a bounded, nonblocking `query(group, request)` callback. The application validates any identity inside opaque command/query bytes against its routing key. The following signatures abbreviate Rust types:

```text
SlotNodeConfig::new(layout, node_id, data_dir) -> config
SlotNodeOwner::<S>::start(config, factory, deadline) -> Result<owner, RuntimeError>
owner.handle() -> SlotHandle<S>
handle.propose(key, command: Vec<u8>, deadline) -> Result<SlotApplied, RuntimeError>
handle.propose_expected(key, expected_slot, command, deadline) -> same
handle.read_linearizable(key, query, deadline) -> Result<Vec<u8>, RuntimeError>
handle.read_applied(slot, query, deadline) -> Result<AppliedView<Vec<u8>>, RuntimeError>
owner.shutdown(deadline) -> ShutdownReport
```

The test FSM may carry a command ID inside its command bytes and persist the corresponding result; the generic Slot layer does not invent business dedup semantics. Applied is tied to the exact committed and applied proposal, not the latest observed group index. `SlotApplied` contains slot/index/term/effects. Linearizable queries use the target group's native ReadIndex and required apply visibility and return bytes. `read_applied` is a local Slot-addressed read: its value and optional `ObservedLogId {term, node_id, index}` are sampled under the same FSM lock, with `None` before any applied entry. It neither routes nor establishes quorum freshness. Leader observations are hints, never fences.

`SlotStatus.local_recovery_ready` is a local recovered-owner observation; it does not wait for election or prove leader/quorum availability. A first call may therefore return a classified refusal. A node not hosting the Slot may try bounded read-only hint probes across voters, reserving the same deadline for one eventual application operation; write dispatch itself is never retried.

Any node may accept ingress. Resolve key to Slot/Group, check readiness and acquire bounded capacity; route locally or make at most one application forwarding attempt to the hinted leader. The target repeats manifest/key/Slot checks. A stale hint returns a classified outcome without an unbounded forwarding chain or automatic proposal retry. Application RPC and Raft transport retain their separate purposes; shared channel pools do not imply one physical connection.

Local calls keep one absolute deadline. Remote calls transmit remaining budget; they neither serialize a process-local Instant nor reset the original budget. Reject invalid/expired budgets before dispatch when possible. There is no public `NotDispatched` enum: definite refusals use `RuntimeError` variants such as Busy, Closed, Deadline, InvalidRequest, LayoutMismatch, NotLeader or Unavailable; transport dispatch facts determine Unknown. Once dispatch may have happened, timeout/disconnect/cancellation must not claim NothingApplied; return Unknown unless the exact result is known. Pure query failure is not write Unknown, and a later result miss cannot prove a previous Unknown write did not commit.

## 5. Ownership, recovery and shutdown

A strong owner holds accepted asynchronous work and shutdown responsibility; handles must not accidentally keep the runtime alive. Caller cancellation or timeout releases its waiter, not already accepted execution or a storage completion callback. Preserve ownership through local proposals, forwarded work and RocksDB blocking writes. Slot acceptance registers owned work and its budget guard under the task lock before returning a waiter. Native `submit_owned` returns a `ProposalReceipt`; its deadline bounds admission and first dispatch, while completion retains native admission until completion or shutdown interruption. Slot caller deadlines bound observation and can return Unknown while both layers retain accepted work.

Restore snapshots and replay logs behind the existing recovery gates. Capture proof state under the short FSM lock; perform asynchronous proof outside that lock with its budget, cancellation and panic handling. Activate only after proof and readiness hooks succeed. Pending-startup snapshots remain non-fatally rejected before the Raft core where C requires it. Purging requires sufficient validated recovery evidence, never merely a completed response.

For this foundation, startup/create/recovery proof errors retain C's local-node rollback, including cleanup of groups started by the failed operation. This is not a general assertion about every runtime Raft fatal path. Before production integration, demonstrate that a fault proven local to one Slot can quarantine that Slot without disrupting healthy Slots. Shared database failures and manifest/disk identity mismatch remain node-level failures. Do not advertise runtime fault isolation until evidence establishes its boundary.

Shutdown fences intake and new forwards; native/RPC owners and accepted tasks drain and join during cleanup, retaining group/storage ownership until their shutdown completes. Report unfinished/Unknown work and cleanup errors explicitly. Waiter completion alone is not clean shutdown. Recovery after interruption must distinguish durable committed state from unconfirmed caller outcomes.

## 6. Resource bounds

Defaults are 64 requests/16 MiB per Slot and 1,024 requests/128 MiB globally. Commands, queries and effects are each capped at 128 KiB. Write/routed-read accounting reserves key bytes + payload bytes + 128 KiB maximum response; local applied reads reserve query bytes + 128 KiB. Forwarding ingress and destination each account for their own retained work. `ShutdownReport` contains complete, unknown_writes, remaining and error; unknown_writes counts owned tasks ending Unknown, not every timed-out waiter. Cleanup has its own 120-second native bound and continues when the caller's shutdown wait expires.

Enforce request count and byte limits at both node and Slot scope, including payloads waiting on dispatch or completion. Slot admission has no waiting queue: count/byte acquisition is immediate and all-or-nothing; underlying runtime, RPC and snapshot work retain their separate bounds. Limits are configured and validated rather than derived from transient heap measurements. Acquire/release capacity consistently across success, rejection, timeout, cancellation and shutdown; an accepted task retains the resources it still owns.

Fixed Slot count still entails per-group Raft tasks, heartbeats, log and snapshot costs. Shared RocksDB does not prove fairness, cross-group atomicity or group-commit speedup. Test hot/cold Slot progress, saturated limits and shutdown under load; record all limits in evidence. No capacity or single-hot-key speedup is claimed by this design.

## 7. Future production integration gates

A future ordered Admit command must atomically consult fixed successful dedup results, check deterministic admission inputs, allocate continuous business seq and update accepted head, pending and result. Raft index is not business seq. Preserve flush/stop, exact holder/revision, Unknown and fail-stop contracts. External eligibility reads happen before proposal with the existing accepted stale semantics; deterministic apply performs no TiKV/network/time/random I/O.

Full accepted head, pending, results, flush and matching persisted progress must migrate under a fence that stops old admission and collector work. Reading a possibly lagging TiKV head once is not initialization of an existing Channel. Do not silently narrow migration to head only or relax cross-Channel TiKV transaction semantics when grouping collector work by Slot.

Current command results are retained permanently. This delivery authorizes no dedup TTL/GC, highest-seq-only replacement, new rejection retention policy or other change to long-term dedup semantics. Explicit same-ID retry can recover the original result; Unknown is not permission for automatic retry. Admission runtime overload may still reject before the FSM is reachable.

## 8. Foundation acceptance

Acceptance must run three distinct OS processes with RF3 and multiple Slots. Evidence covers mapping/digest/disk mismatches, exact results through local and remote ingress, snapshots/log replay and full restart, leader loss/change, post-dispatch timeout with Unknown, real response-less socket disconnect and safe explicit retry, cancellation ownership, shutdown and bounded-resource pressure. Inject proof failure and preserve C rollback behavior. Test local-Slot isolation before later production use.

Record exact source, toolchain/features, commands, data paths, process identities, limits, assertions and outcomes. Keep historical C/R evidence separate. A compile, in-process RF3 test or a recorded plan is not this acceptance. Execution status and any gaps belong in the [plan](../plans/2026-10-16-slot-multiraft-foundation.md).
