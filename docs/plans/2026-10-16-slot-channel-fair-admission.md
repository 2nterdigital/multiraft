# Slot Channel fair-admission implementation plan

**中文：** [2026-10-16-slot-channel-fair-admission.zh-CN.md](./2026-10-16-slot-channel-fair-admission.zh-CN.md)

**Date:** 2026-10-16
**Status:** Retained detailed implementation package; execution order overridden by the Admit-first plan. Planned; not implemented; no new tests or load runs executed.

This refines the [completed foundation plan](./2026-10-16-slot-multiraft-foundation.md) using the [fair-admission specification](../specs/2026-10-16-slot-channel-fair-admission.md).
The A–F completion, 430 workspace passes, seven acceptance cases, original failures and open observer risk remain historical facts in the [ledger](../evidence/2026-10-16-slot-foundation.md).
Fixed Slots, persisted hash/layout and one RocksDB per node remain the baseline. No implementation, performance pass or production readiness is claimed here.

## Current execution priority and old-task mapping

The [Admit-first implementation plan](./2026-10-17-slot-admit-first-implementation.md) has highest priority for current execution: T0 freezes contract sources; T1 real business FSM and T2 Slot RF3 adapter form M1; T3 cross-Slot claims/TiKV handoff and T4 HTTP/WS, delivery/history in isolated new data namespaces form M2; T5 necessary bounded Channel fairness and hot/cold acceptance, T6 production fault isolation/observer/snapshot growth and T7 migration rehearsal/target acceptance form M3. M2 validates an isolated new namespace only; T6 remains a gate before production integration. This order does not authorize automatic deployment. Full DRR, client NODELAY and #173 reruns are not prerequisites for M1/M2; retain existing necessary configuration.

G0–G6/H1 remain detailed-package IDs; the old fairness-evidence-before-real-Admit order is overridden. G1–G5 scheduler dependencies below apply only when T5 selects this detailed package. Identity, budgets, Unknown, deadlines, ownership and all-layer read/control reserves remain binding; M1/M2 already need necessary hard bounds and the complete lifecycle.

| Old ID | Current assignment / override |
| --- | --- |
| G0 | T0 freezes business contract sources; scheduler profiles freeze at T5 |
| G1–G3 | Necessary identity/budget/lifecycle constraints accompany T1–T4; implement the full scheduler package at T5 if needed, using internal G1 → G2 → G3 dependencies |
| G4–G5 | T5 fairness models, hot/cold acceptance and comparable reports; do not block M1/M2 |
| G6 | Real Admit, adapter, claims/TiKV and business paths split into T1–T4; production isolation/observer/snapshot growth move to T6 and migration to T7; G5 no longer blocks real business integration work |
| H1 | Decide necessary hot isolation from evidence after T5 acceptance; any migration still obeys T6/T7 production gates |

## 1. Delivery contract and integration order

All local `route_write` and remote RPC `accept_write` paths must join one destination admission seam.
A business adapter supplies bounded, no-I/O key/command identity and cost validation; generic Slot code treats the command as opaque.
T5 starts with bounded Channel round-robin that meets hot/cold acceptance; adopt equal-weight bounded DRR only if necessary.
Channel FIFO means scheduler selection order, not global FIFO or equal throughput across all Channels on the node.
Rotate after bounded dispatch work without awaiting proposal completion. Bound active-key tables, queue count/bytes and dispatched windows at Channel, Slot and node scopes.
Cost is fixed per command plus encoded bytes; each visit bounds messages/bytes/work. If DRR is needed at T5, quantum is positive, deficit cap covers maximum legal cost and idle deficits reset.
Separate source-forward, destination-queued and destination-inflight capacity; atomically transfer the unique destination reservation from queue to inflight. Unknown retains owned capacity until actual completion. Destination `dispatched` budget starts at the atomic Queued→Dispatching transfer, covering Dispatching/InFlight and completion resources still retained by the owner without waiting for a native receipt. This is budget accounting, not proof of actual submission; record the native dispatch event separately.
Reserve new-cold active-key entries, count, bytes and lower-layer capacity within a declared protected load envelope; arbitrary key churn does not guarantee cold admission or unlimited progress.
Write limits must reserve Slot, native and application-RPC capacity for reads, reconciliation, hints and maintenance.
The current native maximum is shared by read/write, and application-RPC handlers retain permits while awaiting replies: both require explicit redesign and saturation proof.

When T5 selects this scheduler package, implement G1, then G2, then G3 at shared seams and enable that candidate only after all three integrated gates pass; this internal dependency does not block T1–T4.
G4's generator/accounting can proceed independently after G0 with disjoint ownership; candidate execution still waits for G1–G3 and G4 correctness gates.
Assign each implementation ticket exact files before editing. The anticipated ownership below is a starting split, not an existing API promise.

## 2. G0 — Freeze contracts and load profiles

- **Goal:** Freeze adapter identity/cost bounds, reservation stages, protected load envelope, fairness meaning and result classification against the specification.
- **Depends on:** Completed A–F evidence; no requirement to resolve the observer risk before documentation/model work.
- **Expected ownership:** This plan/spec pair, future profile definitions and report schema under `docs/`; no production code.
- **Done when:** Review records maximum keys/messages/bytes/inflight, visit bounds, read reserves, deadlines and profile denominators; business owners supply cold SLO and minimum-completion/maximum-rejection/Unknown thresholds without invented numbers.
- **State / rollback:** Planned, not implemented, no run. Version the frozen profile; amendments invalidate comparison and require a new profile version.

## 3. G1 — Destination validator, budgets and stage seam

- **Goal:** Converge local/remote admission, recompute key/Slot/digest, validate identity/cost before queue insertion, and expose precise queued/dispatched/terminal stages.
- **Depends on:** G0; RPC and native reserve behavior must be agreed together, not left as an implicit lower-layer assumption.
- **Expected ownership:** `crates/multiraft-slot/src/{runtime,rpc,limits,config,error}.rs`; coordinate native `crates/multiraft-net/src/runtime/{requests,proposal,read}.rs` and application-RPC admission modules in a separate, nonoverlapping ticket.
- **Done when:** Focused tests show ingress parity, invalid-input rejection, every budget scope bounded, exactly one destination reservation, atomic queued-to-inflight transfer and no release on caller Unknown; read/reconcile/hint/maintenance progress under write saturation is demonstrated at all layers.
- **State / rollback:** Planned, not implemented, no run. Keep baseline mode available; stop new intake and drain owned work before disabling candidate mode, never release dispatched ownership to roll back.

## 4. G2 — Bounded hierarchical fairness

- **Goal:** Implement node ready-Slot RR and necessary bounded Channel RR (DRR if needed) at T5 with bounded visits and independent completion ownership; make queue depth and fair-gap observation low-cardinality.
- **Depends on:** G1's reservation/stage contract; reuse its validated costs rather than parsing business commands in the scheduler.
- **Expected ownership:** New scheduler/budget modules in `crates/multiraft-slot/src/` with exact names decided in G0; G1's owner integrates `runtime.rs` after scheduler review, so workers never edit that file concurrently.
- **Done when:** Model/property tests prove legal-work progress, bounded work/state, selection FIFO, saturation behavior and protected new-cold headroom; if DRR is selected, also prove idle reset and bounded deficit. Same-Slot hot backlog cannot monopolize dispatch inside the frozen envelope.
- **State / rollback:** Planned, not implemented, no run. Candidate remains disabled until G3 passes; drain queued reservations with classified outcomes before reverting scheduling mode.

## 5. G3 — Deadline, cancellation, leadership and shutdown

- **Goal:** Charge queue residence to the original deadline; decide queued cancellation versus dispatch under one synchronization boundary; retain owned completion after native dispatch.
- **Depends on:** G1–G2; document both source-RPC and native dispatch boundaries and their result stages.
- **Expected ownership:** Slot runtime/RPC lifecycle integration after G2, plus owned request/completion modules in `multiraft-net`; serialize edits with G1 and keep tests in separately assigned files.
- **Done when:** Race tests cover timeout/cancel before and during dispatch, lost replies, observed stepdown/owner-generation changes, shutdown/drain and reservation release. Once source RPC may have dispatched, lack of an exact destination receipt yields Unknown; no automatic replay.
- **Leadership boundary:** Hints are not authority. There is no current atomic expected-term proposal interface; fence old queued work only on observed stepdown/owner generation, then let native admission decide final eligibility. Do not claim a strong cross-term queue fence or global FIFO.
- **State / rollback:** Planned, not implemented, no run. Quiesce intake, classify queued work and drain/report dispatched owners before rollback; unresolved work remains Unknown, never presumed absent.

## 6. G4 — Models and three-process mixed-load harness

- **Goal:** Add model/property checks and reproducible RF3 multi-source traffic while retaining all old acceptance receipts and the ability to run their scenarios.
- **Depends on:** G0 for independent generator work; G1–G3 plus model correctness before any candidate load run.
- **Expected ownership:** New test modules under `crates/multiraft-slot/tests/`, a new harness module/script under `scripts/`, and demo test controls/metrics in `crates/multiraft-slot-demo/src/{main,fsm}.rs`; assign each file once. Preserve `scripts/slot_acceptance.py` compatibility or use a separate entry point.
- **Done when:** Generator unit checks establish planned = issued + unissued and issued = mutually exclusive client-terminal outcomes + outstanding_at_measurement_cutoff; classify unissued by actual cause. Unknown is one client-attempt terminal outcome, distinct from outstanding work without a client terminal at cutoff. Later resolution/explicit retries retain attempt links in a separate resolution ledger and never erase the original cohort Unknown; verify idempotency by logical command ID. Unknown accounting composition makes the generator invalid and prohibits a performance pass.
- **State / rollback:** Planned, not implemented, no run. Keep harness isolated from production and old receipts; retain failed artifacts and stop only owned child processes on failure. No new CLI flags exist yet.

Freeze deterministic key fixtures using the persisted hash itself to deliberately construct same-Slot and distinct-Slot keys; never rely on random collisions.
Record actual observed leaders over time from node logs/status; desired placement alone does not establish same-leader or different-leader cases.
Every cold profile below uses identical cold traffic with multi-source local and remote ingress:

| Planned profile | Required observation |
| --- | --- |
| Cold-only baseline | Absolute cold latency, completion and resource reference |
| Same-Slot hot/cold | Fair selection/gaps and cold completion with a persistent hot backlog |
| Cross-Slot, same leader and different leaders | Node scheduling versus group effects, with observed leadership |
| Hot bursts/rotation, new cold arrivals and key churn | Protected headroom, active-key bounds and stated overload exclusions |
| Different encoded sizes and expensive apply | Cost accounting versus execution bottlenecks; bounded visits |
| Global/RPC/native saturation plus reads/reconciliation | Reserved nonwrite service at every layer, hint/maintenance progress |
| Queue timeout/cancel races, stepdown, lost reply and shutdown | Stage-correct results, no duplicate effects, retained ownership |
| Sustained shared-DB flush/compaction | RocksDB stalls/I/O and per-Slot backlog alongside cold service |

## 7. G5 — Comparable baseline/candidate report

- **Goal:** Evaluate frozen mixed profiles, with correctness gates independent from performance thresholds.
- **Depends on:** G0 profile sign-off, integrated G1–G3 and validated G4; report the historical observer risk separately and handle it before production integration.
- **Expected ownership:** New source-bound report/evidence artifacts and harness receipt schema; do not rewrite the foundation ledger into candidate evidence.
- **Done when:** Same binary (record ELF/build identity or platform equivalent), controllable bounded-FIFO baseline/selected bounded-RR candidate modes (DRR if needed), classifier, queue/inflight/byte limits, new-key/control reserves, instrumentation, hardware and durability; at least two runs of each in alternating order with fresh owned data.
- **Required correctness:** Zero duplicate effects by logical command ID, zero scheduler-selection FIFO violations per Channel, minimal-FSM effects consistent with committed order, recovered state and all resource bounds pass independently. This does not require concurrent ingress arrival order or owned-task dispatch order to equal actual log order; failures cannot be averaged into acceptable latency.
- **Required performance:** Pre-frozen absolute/relative cold SLO, minimum completion, maximum rejection/Unknown, fair-gap and backlog thresholds. Missing business numbers remain an open gate, not fabricated pass criteria.
- **State / rollback:** Planned, not implemented, no run. Any correctness failure or generator-invalid receipt blocks the candidate; baseline remains available after safe drain.

The immediate-Busy/no-queue foundation may be an additional behavioral reference; it changes more than scheduling policy and cannot isolate scheduler effects. Historical test receipts are not a new measured baseline.
Report successful latency separately from rejection/error/Unknown, outstanding_at_measurement_cutoff and unissued demand, preserving denominators and result stages. Reconcile issued attempts using G4’s mutually exclusive client-terminal/outstanding partition; retain original cohort Unknown outcomes alongside linked later resolutions/retry attempts.
Include RocksDB stall/flush/compaction/I/O, per-Slot backlog and bounded-cardinality fairness/resource metrics; never export unbounded Channel labels.
Bind each receipt to source/tree, binary and harness SHA, layout/hash, profile, limits, hardware/durability, process identities and owned cleanup evidence; preserve failures and fresh data.
The old different-Slot single-progress assertion does not establish same-Slot fairness, sustained cold SLO or shared-DB isolation.

## 8. G6 — Future Admit and production integration

- **Goal:** Design deterministic Admit and then integrate production admission, persistence and delivery with full authoritative migration.
- **Current dependency override:** Start real Admit/persistence/delivery under T1–T4 without fairness evidence; M2 uses isolated new namespaces only. Before production integration, T6 must still resolve observer notification risk without suppressing assertions, prove Slot-local quarantine/recovery and bounded snapshot growth; DB/identity failures remain node-wide, and T7 full-migration acceptance is required.
- **Expected ownership:** Future business Admit adapter/FSM and migration/collector integration modules; determine concrete repository/files in a separate production plan.
- **Done when:** Atomic dedup/seq/head/pending/result plus flush/stop, permanent successful-result replay, persisted progress/claims/Unknown and existing cross-Channel TiKV transaction semantics survive restart and fenced migration. Never initialize an existing Channel from a lagging TiKV head.
- **State / rollback:** Planned, not implemented, no run. Fence old admission/collector; checkpoint plus tail and validation precede cutover; after new writes, rollback requires state transfer and a new epoch.

RocksDB remains local consensus log/metadata, TiKV long-term message transaction storage; async delivery and TiKV begin as parallel post-admission paths. SENDACK remains committed + applied admission.
Observer investigation does not block isolated T0–T4 verification, but T6 must close it before production use; the old G6 production-wiring gate does not apply to M2 isolated new namespaces.

## 9. H1 — Later hot-isolation decision review

- **Goal / dependency:** Only after healthy same-hardware baseline and fair candidate still violate the frozen profile, attribute group-hot execution versus node-wide DB/I/O saturation before selecting isolation.
- **Expected ownership:** A future decision/spec/implementation plan; no routing or storage code in this delivery.
- **Options:** Move an entire Slot's leader/placement (no current public operation; explicitly implement and validate later), or versioned routing override/dedicated group. A single hot Channel retains its own ordering ceiling; no throughput improvement is guaranteed.
- **Migration gate:** Transfer full dedup/seq/head/pending/flush/stop, persisted progress/claims/Unknown; fence old writers/collectors; complete checkpoint plus tail and validation, then switch routing epoch. After new writes, rollback needs reverse migration under a new epoch, never just rehashing or dual writing.
- **Storage boundary:** A dedicated group does not cure shared-DB contention. A small set of storage groups or physical isolation needs a separate review; retain one DB per node now.
- **State / rollback:** Planned design review only, not implemented, no run. Reject unsupported isolation claims; preserve fixed layout until a separately validated migration exists.
