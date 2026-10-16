# Slot Channel fair admission design

**中文：** [中文版](./2026-10-16-slot-channel-fair-admission.zh-CN.md)
**Date:** 2026-10-16
**Status:** Adopted detailed fairness design; execution order overridden by the Admit-first plan; not implemented and not verified.

This specification retains the detailed fairness contract for T5 if needed, not current runtime behavior.
The [foundation](./2026-10-16-slot-multiraft-foundation.md) remains the implemented baseline.
The [old implementation plan](../plans/2026-10-16-slot-channel-fair-admission.md) retains detailed packages G0–G6 and later H1 with their current dependency overrides.
Historical 430-pass and A–F results retain their original scope; they do not validate this scheduler.
One cold request progressing in a different Slot is not evidence of Channel fairness.

The [Admit-first implementation plan](../plans/2026-10-17-slot-admit-first-implementation.md) T0–T7 order has highest execution priority and overrides the old G0–G6/H1 fairness-before-Admit order: M1 delivers real Admit, M2 connects claims/TiKV and HTTP/WS, delivery/history in isolated new namespaces, and T5 adds necessary fairness. Identity, budgets, Unknown/deadline/ownership, reserves and the complete lifecycle below remain binding; M1/M2 already require necessary hard bounds and read/control reserves. T6 production isolation, observer and snapshot-growth gates and T7 migration acceptance still precede production use. Full DRR, client NODELAY and #173 reruns are not M1/M2 prerequisites; retain existing necessary configuration.

## 1. Scope and fixed constraints

Keep fixed `slot_count`, hash bytes/version, manifest identity and Slot-to-Group mapping.
Keep one RocksDB database per node, using the default column family and group namespaces.
Do not introduce a database per Slot, production Admit, resharding or write retries here.
Slot fault containment and performance fairness are separate gates; neither proves the other.
Historical observer risk, production Admit/TiKV migration and capacity gates remain open.
Fairness covers admitted, eligible requests by bounded selections (estimated cost if DRR is selected), not wall-clock bounds.
A cold new-key SLO additionally requires protected ingress capacity and a declared load domain.

## 2. One destination admission seam

Current local ingress follows `route_write → execute_local`; remote ingress follows
RPC handler `→ accept_write → submit(routed=true) → execute_local`.
See [runtime](../../crates/multiraft-slot/src/runtime.rs), [routing/RPC](../../crates/multiraft-slot/src/rpc.rs)
and [budget](../../crates/multiraft-slot/src/limits.rs); source anchors are runtime 343/390/417/527,
RPC 82/250/283 and limits 66 in the foundation baseline.
A source-side wrapper alone cannot arbitrate simultaneous local and remote arrivals.
Both paths MUST converge on exactly one destination admission module before native dispatch.
The forwarded destination remains local-only: no recursive forwarding and no automatic write retry.
Source routing/hint work and destination execution have distinct ownership and accounting.

The business adapter registers a bounded, nonblocking, no-I/O classifier/validator.
It validates the entire canonical ChannelKey against the routing key and command identity,
including every identity dimension required by the schema, and computes trusted cost.
Client-declared keys, lengths or costs are not authoritative; reject mismatches before enqueue.
The generic module calls the adapter; it does not parse opaque business payloads.
The minimal test FSM MUST implement this validation too, including forged-key bypass tests.
Malformed or oversized commands cannot allocate unbounded parsing work or active-key entries.

The following is a proposed Interface, NOT existing Rust API or current error variants:

```text
register_admission(adapter: ClassifierValidator, limits: FairLimits)
validate(route_key, encoded_command) -> Validated { channel_key, cost, retained_bytes }
admit_destination(validated, original_deadline, observed_owner_generation)
  -> AdmissionReceipt { completion, stage_receipt }
stage_receipt -> NotSubmitted(TypedRefusal) | MayHaveSubmitted | ExactApplied(result)
```

Validation, reservation and ownership registration must either succeed together or unwind fully.
An admission receipt is local ownership evidence, never consensus execution authority.
G1 must make both ingress paths exercise this same Interface, including test adapters.

## 3. Bounded two-level scheduling

T5 starts with bounded Channel round-robin that meets hot/cold acceptance, with bounded ready-Slot rotation; select full DRR only when cost skew or other evidence makes it necessary. The cost/quantum/deficit rules below apply to the selected DRR variant; visit, window, active-key, blocked-skip and owner bounds apply to both policies.
A Channel FIFO determines scheduler selection order only. It promises neither arrival order
across entrances nor actual log order among multiple concurrently dispatched owned tasks.
If business ordering requires it, future Admit atomically allocates business sequence numbers;
Raft indices are not business sequence numbers.

Use `cost = fixed_command_cost + actual_encoded_command_bytes`, with checked arithmetic.
Schemas impose hard apply-work and payload limits; bytes alone do not measure arbitrary apply work.
Quantum is positive; finite `deficit_cap >= max_acceptable_cost` prevents impossible service.
On a visit, add quantum with saturation at the cap; select affordable FIFO heads and debit cost.
Bound messages, bytes and scheduler work per visit, then rotate even if backlog remains.
An unaffordable head rotates until later visits add credit; never loop on that head in one visit.
Empty Channels reset deficit; idle Channels cannot bank credit for a future burst.
Keep only a bounded active-key table and ready lists; never create a permanent task per Channel.
An entry is inactive/reclaimable only when Queued = Dispatching = InFlight = 0 and no
owned completion still references it; an empty queue with InFlight work is not a new key
and cannot claim new-key reserve. Reclaim/re-register under the same lock with generation
checks, so an old completion cannot mutate a rebuilt entry. Inactivity accrues no credit.
Resource-blocked Channels/Slots are skipped and reactivated by capacity or lifecycle events.
No busy-spin: arm a notification when all remaining work is blocked.
The dispatcher does not await committed/applied before selecting the next Channel or Slot.
An owned completion path retains dispatched work while the dispatcher continues.

Define independent dispatched count/byte windows per Channel, Slot and node.
A full Channel window must not prevent another eligible Channel from selection.
A full Slot window must not prevent another ready Slot from selection.
`per_channel_dispatched = 1` sacrifices pipelining; it is not an unmeasured production default.
This is an estimated-cost fairness policy. It does not inherit Linux/DRR proof guarantees,
and it cannot promise a hard service time under elections, storage stalls or unbounded failures.

## 4. Capacity and reservation transfer

Account three stages explicitly: source-forward retained, destination queued,
and destination dispatched retained (including completion/result buffers until released).
Destination accepted capacity is the single reservation `accepted = queued + dispatched`.
Here budget `dispatched` starts at the atomic Queued → Dispatching transfer and includes
Dispatching, InFlight and completion resources still retained by their owner; do not wait
for a native receipt to count them. It measures budget/execution retention, not proof that
a native command was submitted. Count native dispatch events separately; never infer stage
Unknown classification from this counter. Thus the equation has no transfer gap, and release
follows the owner's actual resource release.
Queue-to-dispatch atomically transfers that reservation and acquires an execution-window token;
it MUST NOT reacquire the same Slot accepted budget while holding its queued reservation.
Queue capacity and execution windows are independent controls with independent invariants.
Origin and remote destination each charge their actual retained resources.
For same-node convergence, transfer shared buffers without charging the same allocation twice;
charge every clone/copy and response reservation that actually remains retained.
Ingress decode buffers and forward tasks holding payloads belong in the ledger too.

Bound count, bytes and active keys at Channel, Slot and node levels.
For each level L, require `queued_L + dispatched_L <= accepted_limit_L` in count and bytes,
and independently `dispatched_L <= execution_window_L` and `active_keys_L <= key_limit_L`.
Channel active-key capacity is one; Slot/node key limits bound aggregate metadata.
Use conservative per-entry/task/allocator overhead bounds in addition to payload reservations.
For each allocation class, assign exactly one owner; total node bound is ingress + source retained
+ destination accepted + key/queue metadata + execution overhead + bounded result/cleanup storage.
RocksDB/native caches and transport pools require separate finite bounds; they are not free memory.
Overlapping counters describe the same reservation and must not be added as distinct allocations.

Reserve finite new-Channel entry headroom in count, bytes and active keys at every level,
or explicitly restrict the protected load domain so those resources remain available.
Prefer capacity for an eligible new key's first maximum-sized request, with a bounded key set/rate.
A non-idle existing key cannot label itself new; canonical authenticated identity governs eligibility.
Reject authenticated key churn outside the domain explicitly; arbitrary new keys cannot enter a full node.
Publish reserve consumption/rejection rules so existing traffic cannot consume protected headroom.
Compute memory and cold-admission feasibility from the configured limits before startup.
These reserves do not by themselves guarantee that a cold source can reach the destination.

## 5. Close the lower-layer capacity loop

The current Slot Budget is shared by reads and writes; native `max_inflight` derives from
`global_requests`, and an application RPC handler holds a permit throughout its wait.
G1 must limit write occupancy and reserve finite capacity for reads, result reconciliation,
hint probes and maintenance through Slot budgets, native admission AND application RPC.
An upper queue reserve alone is insufficient if writes exhaust the lower admission pools.
Use bounded predecode RPC ingress and bounded forward-retained/queue-holding permits.
Protect control/read classes from write waiters occupying every handler; also bound those classes.
Document whether separate pools, class caps or reserved permits implement each layer's policy.
Source saturation may reject a cold request before destination classification; record that refusal.

Startup configuration validation MUST check all of the following:

- Positive visit-work caps; when DRR is selected, positive quantum and a finite deficit cap sufficient for every accepted command cost.
  Visit byte/work caps must permit at least one maximum acceptable head to make progress.
- Schema byte/apply-work limits, checked cost arithmetic and conservative retained-byte bounds.
- Compatible Channel/Slot/node accepted limits, execution windows and active-key limits.
- Full-sized protected new-key headroom across count/bytes/keys at every required layer.
- Write caps plus read/control reserves fit actual Slot/native/RPC capacities without overcommit.
- No cyclic acquisition: a queued reservation transfers without reacquiring itself; cleanup can run.
- Bounded ingress, forwarding, completion, metrics and shutdown resources fit the memory ledger.

Production SLO values, quanta and window sizes are unknown parameters to freeze at G0.
Existing defaults are baseline facts, not measured defaults for this new design.

## 6. Lifecycle, outcomes and fences

The owned state machine is `New → Queued → Dispatching → InFlight → Terminal`.
Admission enqueues with the original deadline; queue time consumes it and never renews it.
Queued cancellation/deadline and dispatch race under one lock/atomic ownership decision.
Before destination native dispatch, an atomic proof of no submission permits precise
`NotSubmitted(TypedRefusal)` in the NEW stage-receipt contract; do not pretend it exists today.
If submission may have happened, classification is conservative and execution ownership persists.
Once the source application RPC is MayHaveDispatched, its timeout/disconnect is Unknown,
even when native submission has not been observed. Only an exact destination negative
(not-submitted) receipt or exact applied result permits a more precise outcome.
Queued-at-destination does not let a disconnected source independently prove non-submission.

Caller cancellation while InFlight removes the waiter, not execution permits or durable callbacks.
Completion or confirmed cleanup releases retained reservations and execution-window tokens.
All error/panic/closed paths must release each allocation once and report unresolved outcomes.
Leader hints are not authority; native `submit_owned` currently has no expected-term argument.
Fence and reject still-Queued requests under the queue lock on observed owner/leader changes.
Treat Dispatching conservatively; the first implementation must not invent an atomic term fence.
Do not copy or automatically resubmit a queue to another owner or term.
Native client-write ultimately judges eligibility at dispatch.
This does NOT provide atomic term-bound queue fencing or FIFO/fairness across terms.
A rapid unobserved loss and reelection may still process a request whose deadline remains valid.
A stronger term-bound contract requires a separately designed future native Interface.
Shutdown fences routing and queues, explicitly refuses Queued work and retains InFlight owners.
Inherit bounded cleanup and explicit unfinished/Unknown/result reporting from the foundation.
FSM apply remains deterministic and performs no external I/O.

## 7. Verification contract and frozen profile

G4 builds model tests and an RF3 harness; G5 evaluates the candidate after correctness gates.
Observe enqueue, selection, dispatch and terminal transitions explicitly; do not infer them from sleeps.
Include a no-hot-Slot cold baseline and local/remote simultaneous ingress to the same destination.
Test hot/cold Channels within one Slot and across Slots with the same or different leaders.
One Slot cannot have different simultaneous legitimate leaders as a test topology assumption.
Exercise sudden heat, hot-key rotation, new cold keys and authenticated key churn.
Exercise byte skew, maximum bounded apply work, capacity saturation and blocked-channel skipping.
Exercise RPC/native saturation and read/reconcile/hint/maintenance reserves at every layer.
Exercise queued expiry, cancellation/dispatch races, lost replies and post-dispatch Unknown.
Exercise stepdown/generation observation, shutdown and shared-DB flush/compaction pressure.
Assert forged-key rejection, bounded counters and no leaked/double-released reservations.
Use deterministic model assertions for FIFO selection and eventual eligible service; if DRR is selected, also assert deficit reset/cap.

For the primary T5/G5 comparison use bounded FIFO versus the selected bounded RR (DRR if needed), preferably in the same ELF
with feature off/on; keep classifier, queue/inflight/byte limits, new-key/control reserves
and observability identical. Record artifacts if one ELF is infeasible.
Use equivalent observation overhead and at least two alternating runs for each condition.
The current immediate-Busy/no-queue foundation may be an additional reference only; its
differences include more than scheduling, so total improvement cannot be attributed to the selected policy.
Do not reuse historical 430-pass/seven-case runs as this comparison or invent an existing CLI.
Freeze deadlines, offered load, hardware, durability, mapping and storage state protocol.
Four runs are a comparison minimum, not statistical proof or production certification.
Freeze profile fields before running: cold absolute P99 and maximum terminal-latency SLO,
minimum completion rate, maximum rejection and Unknown rates, relative cold latency inflation,
backlog bound and eligible fair-service gap bound (in selections/cost, plus observed elapsed time).
No business-unknown numeric threshold may be invented as a measured default.
Define cohorts, warmup/measurement windows, outstanding cutoff and exclusion rules in advance.
If the profile is unfrozen, unissued work unexplained or generator invalid, mark performance
`performance_not_evaluated`; correctness results remain independently reportable.

Use `planned = unissued + issued`; record why each unissued operation was not sent.
Partition issued operations into mutually exclusive terminal outcomes plus outstanding.
Accepted queued/inflight is a stage/cohort view of issued work, never another additive total.
Count client attempts separately from logical operations: Unknown terminates one attempt;
a request without a terminal outcome at measurement cutoff is outstanding. Later resolution
or an explicit same-ID retry cannot erase that Unknown from the frozen cohort or double-count
a terminal outcome. Keep a separate resolution ledger and verify idempotency by logical command ID.
Report by Channel (bounded samples), cold/hot cohort and Slot, with explicit denominators.
Separate successful-operation latency from all-terminal latency, refusals and Unknown outcomes.
Observe queue wait, predecessor backlog, RPC/storage wait, bytes, memory and RocksDB stalls.
Use low-cardinality aggregate metrics and bounded per-key sampling; never unbounded Channel labels.
The [old cold case](../../scripts/slot_acceptance.py) near line 232 checks progress only.
It cannot substitute for the above mixed-load, same-Slot and outcome-accounted evaluation.

## 8. Later hot isolation (H1, outside first implementation)

Consider isolation only when cold SLO/completion targets still fail and measurements locate
contention in a group or shared storage after fair admission and capacity tuning.
First consider whole-Slot leader placement/capacity within the legal fixed layout.
There is no current public migration-control Interface; implement and verify one before use.
Later versioned Channel overrides/dedicated groups require a separate complete migration design.
Move permanent dedup results, sequence/head/pending state, flush/stop state, persisted progress,
claims and unresolved Unknown outcomes, not merely recent payloads.
Freeze/fence old writers and collectors; checkpoint plus tail and reconciliation must converge
before an atomic routing-epoch cutover, followed by verification of the new owner.
Rollback must not revive two writers; after cutover reverse-migrate under a new epoch.
Never temporarily change the hash or silently reinterpret the existing manifest.
A dedicated group cannot raise a single Channel's ordered execution upper bound.
Shared-DB bottlenecks can cross Slots; future small storage groups or physical I/O isolation
need separate design and validation, while keeping this phase's one-DB-per-node constraint.
Old G6 real Admit/TiKV work is split into T1–T4 without waiting for G5 fairness evidence; historical observer, production Slot-local isolation and snapshot growth move to T6, full migration to T7, and remain preproduction gates. M2 uses isolated new namespaces only.

## 9. Reference scope

[Linux DRR](https://raw.githubusercontent.com/torvalds/linux/master/net/sched/sch_drr.c)
debits byte cost, adds quantum/rotates when insufficient and resets on empty; our bounded variant
adds work/windows/Slot scheduling and claims no inherited proof.
[RocksDB Write Stalls](https://github.com/facebook/rocksdb/wiki/Write-Stalls) explains DB-wide stalls;
[RocksDB Overview](https://github.com/facebook/rocksdb/wiki/RocksDB-Overview) notes shared resources across DBs.
See the repository [RocksDB log design](../rocksdb-raft-log.md) for the existing storage contract.
