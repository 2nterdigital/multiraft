# Slot MultiRaft foundation design

**中文：** [2026-10-16-slot-multiraft-foundation.zh-CN.md](./2026-10-16-slot-multiraft-foundation.zh-CN.md)

**Date:** 2026-10-16
**Status:** Design adopted; foundation implementation authorized and in progress. No foundation acceptance result is recorded here yet.

The delivery is a runnable three-process RF3 foundation serving multiple fixed Slots through a new generic Slot API. It uses RocksDB consensus logs and a minimal deterministic test FSM with snapshots and recovery. Production ech0 Admit/TiKV integration, old-data migration, online resharding and capacity certification are later work. The [feasibility assessment](./2026-10-16-slot-multiraft-feasibility.md) retains fixed-source facts; the [plan](../plans/2026-10-16-slot-multiraft-foundation.md) defines implementation and evidence gates.

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

The v1 layout has a positive fixed slot_count and one Group per Slot. SlotId ranges over `0..slot_count-1`; GroupId is `group_id_base + slot_id`, with group_id_base bound into the manifest digest and overflow checked. A base of 1 is the recommended test layout to reserve GroupId 0. No split, merge, online remapping or cross-Slot transaction is introduced. Small test layouts such as 4 or 8 Slots are fixtures, not a production sizing recommendation.

The current Slot configuration implementation defines `sha256-v1` with the existing SHA-256 dependency and the following byte contract (test execution remains pending):

```text
H = SHA256(UTF8("multiraft-slot/key/sha256-v1\0") || u64_be(key_length) || key)
slot_id = u64_be_decode(H[0..8]) mod slot_count
```

The terminating zero is one zero byte. Length is in bytes, integers are unsigned and big-endian, and empty keys have length zero. This version has no mutable seed. Store the exact hash version and mapping parameters in the manifest; never use DefaultHasher. Keep known vectors and cross-restart mapping tests bound to this implementation. Any change to these bytes is a new incompatible hash version, not an incidental refactor.

The canonical manifest codec/digest must define field order, lengths, integer encoding and replica ordering. The current layout digest uses SHA-256 domain `multiraft-slot/layout/v1\0`; it binds format/hash version, cluster ID, slot_count, group_id_base, sorted node identities with Raft/application addresses, and sorted Slot/voter assignments. Addresses are persistent layout identity in this version; changing them requires new data. Runtime resource budgets are separate from layout identity. The digest detects incompatibility and is not authentication. The JSON disk manifest separately binds local_node_id to that layout. Reject duplicate identities/groups, invalid slot counts, non-bijective mapping and unsupported versions before group startup. Persist and verify cluster identity, manifest digest and local node identity with the data directory; node identity is disk-bound, not inferred from an address. Initial creation must be durable before readiness. Reopening with conflicting config or a copied directory bound to another node fails closed.

Every participating server verifies the manifest digest and recomputes the Slot from the supplied key. A client-supplied Slot or leader hint cannot authorize a different partition. Validate cluster/node/group membership and bounds before proposal dispatch. Cross-node mismatches fail before they can mutate the wrong group. Persisting a local digest alone does not prove other replicas have the same manifest; requests and startup/peer verification must enforce that contract.

## 4. API draft and routing

The following is a semantic draft, not a claim that these method names already exist. Public types and adapter details must be reconciled with the final code and tests.

```text
start(manifest, local_identity, limits, storage, fsm_factory) -> owner + handle
propose(key, command_bytes, absolute_deadline)
    -> Applied(exact_effects, position) | NotDispatched(reason) | Unknown(phase)
query(key, query_bytes, Linearizable | Stale, absolute_deadline)
    -> Result(bytes, watermark) | QueryError
shutdown(absolute_deadline) -> shutdown_report
```

The test FSM may carry a command ID inside its command bytes and persist the corresponding result; the generic Slot layer does not invent business dedup semantics. Applied is tied to the exact committed and applied proposal, not the latest observed group index. Linearizable queries use the target group's native ReadIndex and required apply visibility; stale queries return their explicit applied watermark. Leader observations are hints, never fences.

Any node may accept ingress. Resolve key to Slot/Group, check readiness and acquire bounded capacity; route locally or make at most one application forwarding attempt to the hinted leader. The target repeats manifest/key/Slot checks. A stale hint returns a classified outcome without an unbounded forwarding chain or automatic proposal retry. Application RPC and Raft transport retain their separate purposes; shared channel pools do not imply one physical connection.

Local calls keep one absolute deadline. Remote calls transmit remaining budget; they neither serialize a process-local Instant nor reset the original budget. Reject invalid/expired budgets before dispatch when possible. A definite pre-dispatch refusal is NotDispatched. Once dispatch may have happened, timeout/disconnect/cancellation must not claim NothingApplied; return Unknown unless the exact result is known. Pure query failure is not write Unknown, and a later result miss cannot prove a previous Unknown write did not commit.

## 5. Ownership, recovery and shutdown

A strong owner holds accepted asynchronous work and shutdown responsibility; handles must not accidentally keep the runtime alive. Caller cancellation or timeout releases its waiter, not already accepted execution or a storage completion callback. Preserve ownership through local proposals, forwarded work and RocksDB blocking writes. Define the acceptance/dispatch transition in the implementation and test it directly; no undocumented API name is assumed here.

Restore snapshots and replay logs behind the existing recovery gates. Capture proof state under the short FSM lock; perform asynchronous proof outside that lock with its budget, cancellation and panic handling. Activate only after proof and readiness hooks succeed. Pending-startup snapshots remain non-fatally rejected before the Raft core where C requires it. Purging requires sufficient validated recovery evidence, never merely a completed response.

For this foundation, startup/create/recovery proof errors retain C's local-node rollback, including cleanup of groups started by the failed operation. This is not a general assertion about every runtime Raft fatal path. Before production integration, demonstrate that a fault proven local to one Slot can quarantine that Slot without disrupting healthy Slots. Shared database failures and manifest/disk identity mismatch remain node-level failures. Do not advertise runtime fault isolation until evidence establishes its boundary.

Shutdown stops intake and new forwards, drains accepted owned work within a bound, then closes groups/storage and joins owners. Report unfinished/Unknown work and cleanup errors explicitly. Waiter completion alone is not clean shutdown. Recovery after interruption must distinguish durable committed state from unconfirmed caller outcomes.

## 6. Resource bounds

Enforce request count and byte limits at both node and Slot scope, including payloads waiting on dispatch or completion. Use bounded queues and bounded forwarding/storage/snapshot work. Limits are configured and validated rather than derived from transient heap measurements. Acquire/release capacity consistently across success, rejection, timeout, cancellation and shutdown; an accepted task retains the resources it still owns.

Fixed Slot count still entails per-group Raft tasks, heartbeats, log and snapshot costs. Shared RocksDB does not prove fairness, cross-group atomicity or group-commit speedup. Test hot/cold Slot progress, saturated limits and shutdown under load; record all limits in evidence. No capacity or single-hot-key speedup is claimed by this design.

## 7. Future production integration gates

A future ordered Admit command must atomically consult fixed successful dedup results, check deterministic admission inputs, allocate continuous business seq and update accepted head, pending and result. Raft index is not business seq. Preserve flush/stop, exact holder/revision, Unknown and fail-stop contracts. External eligibility reads happen before proposal with the existing accepted stale semantics; deterministic apply performs no TiKV/network/time/random I/O.

Full accepted head, pending, results, flush and matching persisted progress must migrate under a fence that stops old admission and collector work. Reading a possibly lagging TiKV head once is not initialization of an existing Channel. Do not silently narrow migration to head only or relax cross-Channel TiKV transaction semantics when grouping collector work by Slot.

Current command results are retained permanently. This delivery authorizes no dedup TTL/GC, highest-seq-only replacement, new rejection retention policy or other change to long-term dedup semantics. Explicit same-ID retry can recover the original result; Unknown is not permission for automatic retry. Admission runtime overload may still reject before the FSM is reachable.

## 8. Foundation acceptance

Acceptance must run three distinct OS processes with RF3 and multiple Slots. Evidence covers mapping/digest/disk mismatches, exact results through local and remote ingress, snapshots/log replay and full restart, leader loss/change, disconnect after commit with Unknown and safe explicit retry, cancellation ownership, shutdown and bounded-resource pressure. Inject proof failure and preserve C rollback behavior. Test local-Slot isolation before later production use.

Record exact source, toolchain/features, commands, data paths, process identities, limits, assertions and outcomes. Keep historical C/R evidence separate. A compile, in-process RF3 test or a recorded plan is not this acceptance. Execution status and any gaps belong in the [plan](../plans/2026-10-16-slot-multiraft-foundation.md).
