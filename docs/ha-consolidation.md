# Owned HA runtime and normative APIs

This integration implements the business-neutral library capabilities required by [Ech0 specification #164](https://github.com/2nterdigital/ech0-delivery/issues/164). The consumer capability inventory and acceptance boundaries are recorded in [the pre-implementation audit](https://github.com/2nterdigital/ech0-delivery/blob/codex/issue-164-ha-consolidation/docs/evidence/issue-164-ha-capability-audit.md).

Library baseline: `fe832257b0c744faf93f9c7a4ab1da63d37d6a46`. Existing proposals with effects, ReadIndex, control sampling/transfer/layout, Group observations and native snapshot maintenance are reused or upgraded. The library owns the listener, workers, observations, per-call native ReadIndex, maintenance and cancellation cleanup. Each linearizable call performs its own confirmation; shared rounds and their queue were removed following the consumer decision recorded in [the per-call read contract](per-call-read-api.md). Public consumers use a unique Node owner and weak request handles. They do not supplement the runtime with raw Raft, metrics or trigger operations.

The runtime composes the existing facade. Feature files isolate Group construction, membership, application operations and task/lifecycle ownership. Store remains below the runtime and supplies native persistence and opaque FSM resource-release observation. Application byte contents, business schema, service routing and balancing policy remain in the consumer.

The implementation slices are delivered through the public APIs described below and in [application RPC](application-rpc.md), [per-call reads](per-call-read-api.md) and [control](control-api.md). Independent consumers and Ech0 integration receipts cover old-source disk/snapshot fixtures, deadlines, cancellation, resource release and real RF3 control/recovery. Final whole-workspace laboratory results and two-axis review are recorded by the consumer integration; their completion is not inferred from these scoped tests. No consensus, election or replication algorithm or timing change is part of this integration.

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

## Owned native maintenance

`RuntimeHandle::request_compaction(group, absolute_deadline)` submits one local
native snapshot/compaction; `local_storage_status(group, absolute_deadline)`
observes the library's canonical progress and native/provider/log facts. Typed
`RuntimeError::MaintenanceRejected` preserves known native refusals. An expired
admission budget dispatches nothing. Capture/submission share the original caller
budget; `CompactionRejection::Deadline` means the budget expired before invoking
the native trigger. Once invoked, trigger and accepted native work stay owned if
the request waiter expires or is canceled. The caller then receives an unknown
submission outcome and can inspect storage status separately.

`CompletedObserved` requires actual builder return, a checksum-validated durable
provider, native snapshot coverage and the configured purge/retention condition.
The existing `completion_observed` predicate remains the sole classifier. A
previous checkpoint at the same cut cannot complete a still-running repeated
build. Observation has a bounded 30-second polling budget; unconfirmed shutdown,
panic, provider failure or observation expiry never turns submission into
completion. No automatic compaction, election timing or native protocol policy
was added. Native alpha.30 already defaults purge batching to 1; retention rules
are unchanged.

The existing Store supplies one capture permit per Node and the bounded FSM
capture hook. Known Unsupported/SizeLimit refusals never fall back to the legacy
unbounded serializer. One retained public status sampler per Node reports Busy
while actual provider/log work is unfinished, even when its original waiter was
canceled or expired. Per-Group operations and the sampler share the generic owned
task registry. Task futures capture independent state/permits rather than their
registry owner, preventing a registry/handle/owner Arc cycle. Registration and
close share a lock. Join polls handles retained in that registry, so canceling a
join abandons only its waiter instead of aborting a parent and detaching a
non-abortable filesystem child. Shutdown closes intake, stops native work and
joins maintenance and blocking children before actual FSM release. Bounded
synchronous FSM code and filesystem work cannot be forcibly preempted; the owned
cleanup remains retained when a shutdown wait expires.

Public consumer tests under `tests/owned_maintenance*.rs` recover the unchanged
old-source fixture, compact, append a tail and restart. They cover canonical
completion, repeated builds, node/group Busy, typed refusals, capture panic and
Unconfirmed, request deadlines/cancellation, the retained sampler, canceled stop
and real lease/listener/data-root reuse. They access no native Raft, metrics or
trigger handle. Bounded tracing events are used as deterministic scheduling seams;
new status tracing records only operation/phase/Node/Group identity.

## Single-transfer control and source observations

The weak runtime's normative sampling/transfer/layout APIs accept one opaque
invocation identity and inherited absolute deadline. Existing native qualification
and precheck algorithms are retained; structured source causes and full expected/
observed evidence cross the public API. One nonqueued Node control slot is shared
by all Group control calls. A queued trigger remains distinct from completion,
and independent target layout observation establishes no request causality.
See [the control API and source-log field contract](control-api.md) for interfaces,
cancellation stages, finite INFO/DEBUG output, and the downstream sink field list.
`tests/control_consumer.rs` verifies the public weak API over actual RF3 gRPC;
private control tests prove deterministic zero/one-trigger and stage facts.

## Snapshot peer recovery and input profiles

`ClusterConfig::new(node_id, actual_peers)` supplies production addresses and the
same defaults as the test helper; it performs no synthetic port arithmetic and
supports project Node ID 65,535. `for_test` delegates to that one defaults set.
`install_snapshot_timeout_ms` defaults to the native alpha.30 value200 and accepts
1..=30,000. Native image consumers can explicitly preserve5,000 without changing
heartbeat100 or election300..600. `non_durable_snapshot_log_retention: Option<u64>`
defaults toNone (the existing facade's outside-NativeDurable keep0); Some(1000)
preserves the old disabled HA native default. NativeDurable uses only canonical
`retain_log_entries` and rejects that mode-specific override. Its default remains
1024; consumer catalog keep1 is an explicit input, not a generic default change.

The user decision on2026-09-30 was to retain Ech0's original arbitrary u64 log
retention range through a business-neutral library extension. Both retention
inputs now support fullu64, including65,537 andu64::MAX; alpha.30 itself stores
this input asu64 and calculates purge using checked/saturating subtraction.
The existing completion predicate also uses checked subtraction, yielding an
explicit no_purge_needed outcome when the target is below retention. This
expands the former guard, without adding a second retention/completion policy.
64 MiB application bytes, the metadata envelope, one Node capture/send/receive
slot, election/heartbeat inputs and snapshot wire/storage formats remain.

Owned durable-local startup is distinct from `MultiRaft::wait_for_recovery`.
That existing strong native wait retains its cluster-commit-covers-local-tail
and applied-cluster-commit guarantee. Memory/Os owned startup retains it as well.
For a nonempty Data/All file root, owned startup captures only native forced
read_committed plus checksum-validated provider metadata BEFORE construction.
Only successful Raft::new validates that immutable construction basis: alpha.30
storage/helper.rs87–153 restores the persistent snapshot, rejects a missing
required purged basis and replays exactly the committed suffix before spawning
the runtime (raft/mod.rs465–530). FileLogStore.save_committed forces Data/All
hard state (log_file.rs635–650). Owned startup then confirms the constructor
basis through native applied metrics/running state, invokes the consumer
recovery validator under its FSM lock, checks native running state again, and
only then publishes executable readiness. It adds no app watermark/replay and
never trusts a caller-supplied applied pointer. The provenance is an immutable
owned construction target, not current Raft authority or a health cache.
It promises no quorum/leader readiness. Reads and writes keep their normal
native authority/ReadIndex checks; an isolated recovered RF3 Node can validate
its local data while an authoritative read correctly fails without live peers.

`RecoveryError` retains Group/stage and typed Deadline/Closed/native
Storage/Panicked facts. Display/tracing contain only bounded facts; the original
source chain is available separately through std::error::Error::source.
The source target `multiraft::recovery` records `recovery_phase` (`construct`,
`await`, or `unknown`) and `recovery_failure` (`deadline`, `closed`,
`native_storage`, `native_panicked`, or `unknown`) on native group-start and
recovery-wait errors. These finite labels are projected directly from the typed
`RecoveryError`; downstream sinks may explicitly preserve them for this exact
target. The native error chain is not formatted into logs. Missing required
log/snapshot basis remains a native recovery refusal: callers can inspect the
original `Error::source` chain separately, while the logged error stays a
bounded summary. These diagnostics add no recovery decision or application
watermark and never authorize retry or readiness.

Native cancel signals end snapshot stream waiters with Closed, while already
owned sends retain their slot/deadline. Receiver slots wait for the real native
transition even when the native API waiter has observed fatal shutdown.
Native provider install ordering, byte/wire bounds and voter membership stay.

Independent public consumers prove actual5 MiB install into a lagging RF3 peer,
restore refusal and original-directory retry, cancel/drop during real restore,
actual lease/port reuse, snapshot+tail cold recovery with all peers stopped and
unchanged RF3 membership. A valid uncommitted native normal frame carrying a
non-idempotent+9 is added only to the stopped consumer copy without changing its
forced commit record: the local recovery validator sees20, not29. The fixed
pre-migration snapshot10+tail5 fixture separately rejects missing checkpoint,
corrupt image and missing committed tail before validation, then recovers15
after repairing only the damaged file with the saved exact original bytes.
Failed attempt logs and data roots stay under `.tmp/issue-170/`; no failed disk
is regenerated or replaced to produce passing compatibility evidence.
