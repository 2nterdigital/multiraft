# Owned HA runtime and normative APIs

This integration implements the business-neutral library capabilities required by [Ech0 specification #164](https://github.com/2nterdigital/ech0-delivery/issues/164). The consumer capability inventory and acceptance boundaries are recorded in [the pre-implementation audit](https://github.com/2nterdigital/ech0-delivery/blob/codex/issue-164-ha-consolidation/docs/evidence/issue-164-ha-capability-audit.md).

Library baseline: `fe832257b0c744faf93f9c7a4ab1da63d37d6a46`. Existing proposals with effects, ReadIndex, control sampling/transfer/layout, Group observations and native snapshot maintenance are reused or upgraded. The library owns the listener, workers, observations, shared confirmation, maintenance and cancellation cleanup. Public consumers use a unique Node owner and weak request handles. They do not supplement the runtime with raw Raft, metrics or trigger operations.

The runtime composes the existing facade. Feature files isolate Group construction, membership, application operations and task/lifecycle ownership. Store remains below the runtime and supplies native persistence and opaque FSM resource-release observation. Application byte contents, business schema, service routing and balancing policy remain in the consumer.

Initial state: implementation in progress. Verification will include an independent simple FSM consumer, actual old-source disk/snapshot fixtures, deterministic concurrency/deadline/closure checks and real RF3 control/recovery. This document will be updated with delivered API paths and results. No consensus, election or replication algorithm or timing change is part of this integration.

## Consumer recovery validation and actual resource release

`StateMachineFactory::validate_recovered(FsmFactoryContext, &S)` is a default
accepting, business-neutral callback. `NodeOwner` invokes it for every new local
Group after native recovery reaches its persisted commit point, and before
publishing executable Group readiness. It also validates empty new Groups;
idempotently ensuring an already-ready Group does not repeat validation. The
callback holds the local FSM lock, must be bounded and synchronous, and confers
no leadership authority. Factories expressed as closures retain the default.
The library does not interpret application schemas or snapshot payloads.

Rejected validation preserves the consumer error chain. Validation panic,
native startup failure and expired startup budgets also prevent ready
publication. These failures fence the entire owned node, including failures of
dynamic `RuntimeHandle::create_group`; retained cleanup stops the listener,
drains/join tasks and waits for actual FSM destruction before successful rollback
returns the startup error. This owned-runtime policy does not change the legacy
standalone facade's Group-creation policy. Cancellation abandons only the
caller's wait. Node-owner Drop retains cleanup on the originating Tokio runtime.

Successful `NodeOwner::shutdown` and successful startup rollback are resource
reuse seams: the application FSM destructor has finished, rather than merely
native Raft shutdown acknowledging. Factories must return independently owned
FSM resources with bounded destructors. A cleanup timeout/error is not successful
resource release; retained rollback continues. Weak request handles cannot retain
the owner or FSM. Existing Store release witnesses continue to observe the real
application destructor without retaining it.

The external `tests/recovery_validation.rs` consumer acquires an exclusive
on-disk mock lease and only releases it in the FSM destructor. It validates,
reads/writes and restarts a copy of a fixed old-source native snapshot/log fixture;
refusal, panic, native corruption, pre-ready request fencing, elapsed validation,
Drop and canceled start/stop exercise lease, data-root and listener reuse. Exact
old-source file hashes, producer and original lock hash are retained under
`tests/fixtures/legacy-native-alpha30`. This proves library compatibility with
that fixture; Ech0 business schema and actual Store lease validation remain
consumer integration evidence.
