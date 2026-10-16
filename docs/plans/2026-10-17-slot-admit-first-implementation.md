# Slot Admit-first implementation handoff

**中文：** [中文计划](./2026-10-17-slot-admit-first-implementation.zh-CN.md)
**Date:** 2026-10-17
**Status:** Execution plan only; no implementation, build, new test, SSH or load run in this delivery.

## 1. Adopted direction and replacement order

Fixed Slot + Atomic Admit is the adopted long-term direction.
Build real ech0 admission on the existing Slot foundation, then complete asynchronous TiKV persistence and delivery.
Do not make further client/#173 optimization or a new Counter demonstration a prerequisite.
Do not rebuild the Slot foundation or implement two complete replacement systems.
This plan supersedes the execution order G0–G6 in [the previous plan](./2026-10-16-slot-channel-fair-admission.md).
In particular, full DRR and performance SLO certification no longer precede real Admit implementation.
The [fair-admission specification](../specs/2026-10-16-slot-channel-fair-admission.md) still defines fairness correctness.
Fixed persisted Slot/hash/layout, one RocksDB per node, ownership and hard resource bounds remain required.
The A–F foundation, 430 passed/12 ignored and seven acceptance cases remain [historical evidence](../evidence/2026-10-16-slot-foundation.md).
Preserve original failures and the unresolved observer risk; these receipts do not certify new business behavior or performance.
Ready for a milestone means its scoped gates passed; it does not authorize deployment.

| Milestone | Tasks | Required result | Explicit boundary |
| --- | --- | --- | --- |
| M1 | T0–T2 | Pure business FSM and real three-OS-process RF3 multi-Slot admission | No production TiKV or old-data cutover |
| M2 | T3–T4 | Real HTTP/WS, TiKV transactions/persisted proof, history/audience/delivery loop | New isolated business namespace; conservative whole-node fault behavior disclosed |
| M3 | T5–T7 | Necessary fairness/hot-load evidence, Slot fault isolation, observer closure, migration rehearsal and target acceptance | Readiness evidence; production cutover remains a separate action |

First handoff: deliver an M1 report, then continue directly to M2 when its dependencies pass.
Do not stop to reconfirm the already adopted architecture between these stages.
Do not treat DRR or unspecified performance SLOs as M1 blockers.
The implementation authorization in this handoff does not include production cutover or rewriting production namespaces.

## 2. Source freeze and repository ownership

| Repository | Starting reference | Worktree policy |
| --- | --- | --- |
| multiraft | `9e63b454848d18ceea4510b3f3ea0fb52af71b68`, `codex/slot-multiraft` | Continue `/Users/wei/Documents/WorktreesSpace/slot-multiraft` |
| ech0 | main `1606f85fc787604057f2ff7829df291c9ddc857b`; code baseline remains `8098a4f7` | Freeze the full actual SHA/tree and create an isolated worktree |
| ech0 earlier checkout observation | `claude/memory-docs-20261017`, `2f73a7d7` | Observed early in drafting; documentation subsequently merged, checkout now main |
| old multiraft checkout | `/Volumes/Space/2nterdigital/multiraft`, old `28e…` reference | Leave untouched |

The multiraft `9e63b45` reference is the implementation/documentation baseline before this plan: retain later documentation commits carrying this handoff, record actual HEAD and verify code equivalence; do not reset to it or delete the new plan.
Before editing, record each exact commit, tree, dirty status, manifest and lockfile hashes.
Read the actual branch/HEAD at execution time; main `1606f85fc787604057f2ff7829df291c9ddc857b` includes the subsequent documentation merge.
The verified diff from `8098a4f7` changes only docs/memory; Cargo.toml/Cargo.lock/crates/tools/vendor are unchanged, so source-code references at `8098a4f7` remain valid.
Never switch, reset or merge the shared ech0 main working directory.
Create new worktrees only under `/Users/wei/Documents/WorktreesSpace/`.
Suggested ech0 branch: `codex/admit-fsm`; the executor may choose another collision-free name.
Use only the active worktree `.tmp/` for temporary files; this document's staging area is `.tmp/slot-admit-first-plan/`.
Leave the existing untracked `scripts/__pycache__/` unchanged.
Normal cross-repository dependency: exact Git revision whose objects are confirmed obtainable by the receiving executor.
Record that the starting multiraft commit is currently local/unpublished; do not imply it can already be pulled remotely.
For M1, an owned local mirror/Git bundle or temporary Cargo patch under `.tmp/` may bridge unpublished objects.
Record source object identity and lock hash; do not commit absolute path dependencies or floating branch references.
A local successful build does not establish reproducible laboratory readiness.
Any remote publication is handled by the executing thread under its user's authorization; this planning delivery pushes nothing.
Prefer laboratory pull of committed fixed source, then laboratory build/run, following lease, mux and owned-cleanup manuals.

The §2 source/dependency preliminaries must resolve the dependency graph before T0 chooses the crate boundary:

- Keep old ech0 OpenRaft resolved/locked `0.9.25` (manifest semver `0.9.21`) without accidental upgrade, separate from multiraft `=0.10.0-alpha.30`.
- Keep the latter fork revision `ea46d0e571f7497f1f549e125254dfcb2ff45e2e` pinned.
- Isolate native types/protocols; do not inadvertently upgrade old raftd wire or disk formats.
- Check both SDK dependency sets and feature unification with the integrated lockfile.
- Generic multiraft must not depend on ech0 business, store or TiKV.
- Suggested new ech0 crate `crates/admission-log` is a proposal, not an existing API/path guarantee.
- A pure core may use existing neutral store models or a small extracted interface; avoid business↔new-crate cycles and broad store refactoring.

## 3. T0 — Freeze authority contracts, codecs and sources

| Item | Execution package |
| --- | --- |
| Depends on | §2 source/dependency preliminaries; existing Slot baseline |
| Modules/files | ech0 manifests/lock, relevant SEND/collector/read contracts; proposed admission crate; existing Slot public interfaces |
| Output | Versioned operation map, command/result/snapshot schemas, limits, trust inputs, backend configuration and exact file ownership |
| Acceptance | Every authority write/read has an owner; existing strong claims/transactions either preserved or explicitly resolved; dependency graph builds reproducibly |

T0 produces a cross-Slot Claim/Stop state-transition table, partial-success/Unknown/crash matrix and coordinator-ledger ownership, persistence and recovery design.
Prove preservation of the old observable contract or document an explicitly reviewed design change; unresolved coordination blocks T0 acceptance and the affected FSM/coordinator implementation.
This is an engineering design gate, not a request to reapprove fixed Slots; do not assume global atomicity or hidden 2PC.
The business adapter owns qualification, trusted time/message-ID input, request origin and the current short-stale audience capture contract.
Slot remains a generic command/query transport and persistence host.
Freeze the following operation mapping against actual source before implementation; names below describe roles, not invented callable APIs.

| Existing authority role | New role | Frozen invariant |
| --- | --- | --- |
| SEND head/dedup/pending admission | Admit command | Atomic fixed replay or seq/head/pending/result/flush initialization |
| Collector claim | Claim command(s) | Exact holder/revision, bounded request/input; no new lease-expiry takeover and cross-Slot partial outcome ledger |
| Confirm persistent messages/progress | CommitPersisted command | Only after confirmed TiKV transaction; exact claim proof |
| Release collector ownership | Release command | Conditional on exact holder/revision; no deletion of unique unknown pending |
| Stop affected admission | Stop command(s) | Per-Slot confirmed state; unrecorded outcomes explicitly reported |
| Flush bookkeeping / barrier | Flush commands and barrier query | Applied authoritative state and shutdown ordering |
| Head/pending/read-point lookup | Linear authority query | Readers use the selected backend; local watermark is not a linear barrier |
| Existing legacy cache namespaces | Existing cache API | Continue their current contracts |

Freeze canonical Channel encoding, command identity, fingerprint, message/result fields, versioning and verdict codes.
Source map at ech0 main: `crates/business/src/send/admission.rs` owns SendCommand/Outcome; resolve actual crate-prefixed paths at T0.
State families in `record.rs`: head {head, stopped}; command identity → intent/msgid/seq; pending → msgid/command/sender/content; flush Idle/InFlight(holder)/Stopped(holder).
Cache-entry metadata carries revision today; the new FSM must store an explicit equivalent version.
Identify specs/ADRs whose SEND authority wording must change; do not replace the generic cache contract wholesale.
Use explicit startup/campaign backend selection, retaining controlled legacy comparison.
No per-request Unknown fallback to the old writer, automatic write replay or writer dual-write.
Define limits for Channels, retained result metadata, pending payload bytes, command/query/effects, scans and inflight owners.
If permanent result retention fills capacity, reject new commands without losing prior successful results.
T0 records the new head/pending + group-version read set and every invalidation/version writer; prove existing qualification short-stale semantics are not widened.
If one shared view is required, integrate the corresponding replicated version/control and its writers in this scope; do not leave an implicit split view.
Document initialization proof and ownership epoch; no lagging TiKV-head import for previously admitted Channels.

## 4. T1 — Deterministic business FSM

| Item | Execution package |
| --- | --- |
| Depends on | T0 contracts, dependency and codec freeze |
| Modules/files | Proposed admission crate pure core, codec/snapshot modules and separately owned tests; neutral model seam if necessary |
| Output | Real Admit and complete collector/flush/stop authority state machine, versioned snapshot/digest |
| Acceptance | Deterministic replay, rejection/no-number-consumption, identity conflict, claims, bounded state and recovery tests |

Canonicalize Direct as low/high participants and Multi as the existing canonical group identity.
Command identity is Channel + sender + command ID; preserve the existing fingerprint fields.
Successful replay returns the original message ID, Channel seq and fixed result; recheck dedup inside apply to close ingress races.
Fix the proposal message-ID candidate before dispatch; do not reproduce the old random candidate per CAS attempt.
Authentication and structural validity precede replay; stored success precedes current qualification, stop and capacity decisions.
A stored result must never bypass authenticated identity binding.
New acceptance uses checked `head + 1` and atomically writes head, pending, permanent result and initial flush state.
Do not preallocate sequence ranges or use Raft log index as Channel seq.
Rejecting a command consumes no seq; changed intent under an existing identity changes no state.
Exact replay creates neither a new message nor another webhook.
Define all authority mutations, including Claim, CommitPersisted, Release, Stop and Flush; Admit alone is insufficient.
No network, clock, randomness or external I/O occurs inside apply.
Trusted time, message ID and payload are explicit input with documented validation/source.
No TTL/GC removes permanent successful results; bounded payload policy cannot silently delete dedup authority.
Read prior results before deciding a capacity refusal.
Clear a pending body only after confirmed persistence and an exact holder/revision plus frozen safety condition.
Snapshots include head, pending, successful results, flush/stop, persisted progress and ownership/claim state.
Version and digest the complete recovery basis; corrupt or incomplete state fails closed, never as an empty Channel.
Test overflow, conflict, replay after stop/capacity/qualification change and restart from snapshot plus tail.

## 5. T2 — Slot adapter and M1 real-process gate

| Item | Execution package |
| --- | --- |
| Depends on | T1; existing Slot factory/recovery interfaces |
| Modules/files | ech0 Slot adapter and business test host; existing `multiraft-fsm`/`multiraft-slot` public seams; new owned process harness |
| Output | Implementation of existing StateMachine/SlotStateMachine factory and recovery proof; real business commands/queries |
| Acceptance | Three OS processes, RF3, multiple Slots, canonical Channel seq/idempotency and restart/recovery evidence |

Use the existing abstraction rather than inventing a second Slot runtime.
Keep encoded command/query and per-apply effects at or below 128 KiB.
Paginate large scans with a declared total budget and stable semantics; no unbounded snapshot/query response.
Use linear reads for authority decisions; distinguish local progress/watermark inspection explicitly.
A fresh Channel requires proof of no accepted state plus the applicable ownership fence.
Existing Channels cannot start from an asynchronously persisted TiKV head.
Implement synchronous StateMachine apply/snapshot/restore, SlotStateMachine query and named Factory.validate_recovered.
NativeDurable freeze_bounded defaults to Unsupported: implement construction-time bounds, not an allocation followed by a size check.
Existing per-group full native snapshot cap is configurable 1–64 MiB; permanent dedup growth conflicts with that ceiling.
T0 budgets the worst-case legal complete state plus codec overhead and reserved Claim/Stop/confirmation/cleanup metadata against the snapshot cap.
After Admit stops for capacity, control operations must still complete safely; bounded fixtures below 64 MiB alone do not prove this.
T1/T2 test near-cap confirmation, shutdown, snapshot and recovery; M1/M2 reject new commands at capacity without TTL eviction.
Factory local proof is bounded/no-I/O under lock; external proof runs outside the lock with owned cancellation.
Apply/query receive group + payload, not route key: ingress must verify routekey equals payload Channel; FSM rehashes payload to group.
Same-Slot forged keys need destination validation; move the minimum validator seam into T2 if needed rather than waiting for T5.
Prebound business replies: oversize effects discovered after apply become Unknown. Pending lookup is paginated.
Expected Slot does not provide expected-term/atomic term fencing.
Owned proposals keep the original deadline; a lost receipt after possible dispatch is Unknown.
For Admit Unknown, a strong lookup of this candidate ID gives Accepted; another same-intent accepted candidate gives Replay; miss stays Unknown without resubmission.
An absent lookup is not proof that a proposal was never committed.
Same-ID explicit retry follows a frozen policy and returns fixed replay; no automatic write replay.
Purge logs/snapshots only when the complete recovery basis exists.
Hard count/byte/inflight limits start in T1/T2 even while sophisticated fair queuing remains later work.
Exercise concurrent same-Channel ingress on the same and different nodes.
Exercise different Channels in one Slot and across Slots; verify independent contiguous business seq.
Run leader loss, process restart, lagging replica recovery and lost-response identity cases using real processes.
Mocks/unit tests supplement this gate; they cannot replace the three-process RF3 gate.
M1 report lists what passed, unresolved failures and the deliberately absent production TiKV/old-data migration.
Continue to T3 after M1 correctness passes; full DRR and performance certification are not prerequisites.

## 6. T3 — Real TiKV handoff and persisted proof

| Item | Execution package |
| --- | --- |
| Depends on | T2; T0 cross-Slot claim contract resolution |
| Modules/files | ech0 collector/writer adapter, existing store transactions, authority claim/progress commands and owned fault tests |
| Output | Real TiKV transaction → confirmed persisted proof → conditional authoritative cleanup |
| Acceptance | Existing cross-Channel atomic transaction maintained; exact claims, crash/Unknown/stop/cleanup gates pass |

Preserve message rows and persisted heads across Channels in the same TiKV transaction.
Do not silently split a transaction by Slot.
Existing `collector.rs` batch claims are atomic; changing to per-Slot claims is a coordinator contract change.
Multi-Slot claim acquisition is not atomic and cannot be presented as a global CAS.
Freeze per-Slot acquisition, partial/Unknown ledger and exact holder/revision release before writing TiKV.
No TiKV write starts until every required claim is confirmed under the frozen contract.
If the public contract requires strong all-or-nothing claim semantics, explicitly resolve the design; do not introduce hidden 2PC.
Existing failed/Unknown TiKV semantics remain stop affected Channels + report.
No automatic TiKV retry, query/reconciliation, claim stealing or recovery of uncertain writes; stop all Channels in the failed/Unknown batch.
Map registry controls explicitly: Claim/FenceUncertain/Release/Stop/DeleteConfirmedPending are illustrative command names.
Preserve uncertain-idle revision fencing, exact holder validation, atomic head+flush stop and repeatable confirmed deletion then release.
Local registry phases/held claims require snapshot/restart evidence; do not assert automatic business recovery.
FSM state recovery does not authorize automatic recovery/reissue of Unknown TiKV work.
Crash after TiKV commit but before Raft confirmation must preserve pending/proof uncertainty safely.
Cover stale holder after handoff, partial claims, Unknown stop, lost confirmed-cleanup receipt and shutdown during commit.
Timeout cleanup cannot delete the only pending payload.
Record Stop separately for each Slot; if any Stop cannot be confirmed, fail-stop broader intake as needed.
Report `stop_unrecorded`; do not claim all Channels stopped without evidence.

## 7. T4 — Ingress, readers, delivery and M2 gate

| Item | Execution package |
| --- | --- |
| Depends on | T3; integrated backend and complete reader map |
| Modules/files | ech0 HTTP/WS SEND adapters, business delivery/audience/webhook, history/SYNC/join reads, runtime/app lifecycle |
| Output | Shared real ingress + asynchronous persistence/delivery + all authority readers in an isolated namespace |
| Acceptance | Real HTTP and WS with real TiKV independent oracle; wire/auth/origin/replay and shutdown behavior preserved |

Both HTTP and WS use the same explicitly selected backend and verdict mapping.
Preserve wire/result codes, exact origin, authentication and qualification staleness semantics.
Preserve `target.rs` fresh qualification capture with permitted short staleness; existing pending/result records contain no audience.
Replay clears audience/origin. Do not add a durable audience outbox or strict instantaneous revocation contract silently.
After Accepted, persistence and delivery may proceed in parallel.
SENDACK waits for committed + applied admission, not TiKV persistence or RECV acknowledgment.
Webhook runs only for first acceptance; replay does not emit another webhook.
Route every head/pending consumer, including history, SYNC and join/read-point operations, to the new authority.
Route history/SYNC accepted head + pending and group-join accepted head; coordinate group-version reads with the qualification source.
Other cache namespaces retain their authority; this does not require migrating the entire generic cache.
Do not let old head/pending cache reads determine decisions about new Slot state.
Shutdown fences ingress, drains/joins owned business tasks while persistence/delivery dependencies remain live, flushes the writer while Slot is live, then joins Slot/RPC/DB owners.
Report unresolved work/Unknown; never force a successful flush result to finish shutdown.
M2 uses a new isolated business namespace/test data and a real TiKV independent oracle.
No production old namespace, deployment or old-data rewrite belongs to this gate.
Conservative node-level failure/rollback is acceptable in the sandbox if recorded; it does not certify production Slot isolation.

## 8. T5–T7 — Fairness, reliability and migration readiness

| Task | Dependencies and owned modules | Output and acceptance |
| --- | --- | --- |
| T5 Necessary fairness | T4 real flow; Slot runtime/RPC/limits and separately owned load harness | Bounded RR and local/remote canonical-key parity; hot/cold, byte skew, churn and lower-layer reserves demonstrated |
| T6 Isolation/reliability | May start after T1 with a separate generic foundation owner | Slot fault quarantine/recovery and observer investigation; shared DB/manifest fatal remains node-wide |
| T7 Migration/evidence | T4 state contracts + T5/T6 gates | Full-state fenced migration rehearsal and frozen target acceptance; ready is not deployed |

T5 first enforces active-Channel count/bytes and bounded RR; it need not implement full DRR first.
Both local and remote destination ingress validate the same canonical key/cost/Slot identity.
Existing G1/G3 reserve, deadline and completion ownership obligations remain.
Prove Slot/native/application-RPC read/control reserves under saturation even without DRR.
Then run same-Slot and cross-Slot hot/cold profiles with observed same/different leaders, byte skew and key churn.
Report planned, issued, unissued, mutually exclusive terminal outcomes, original cohort Unknown and outstanding separately.
Missing numerical SLOs mean `performance_not_evaluated`, not a core correctness failure or invented performance pass.
Simple RR does not establish all byte fairness/SLO claims; add G2 DRR where byte fairness requires it.
G5 compares bounded FIFO with the selected bounded RR candidate, or DRR when required, using identical caps/reserves and observation.
Old no-queue behavior is a separate reference and cannot attribute differences to the scheduling algorithm.
T6 owns disjoint files and must not interleave edits with the business adapter owner.
M3 must implement/test a long-term recovery-capacity solution for permanent dedup (streamed/sharded/incremental snapshots or another explicit design).
Do not merely increase the cap, pretend streaming already exists or compress away unique authority state.
Investigate the unresolved observer assertion with separate evidence; do not suppress it to pass tests.
A fault scoped to one Slot may quarantine it while healthy Slots serve; shared DB/manifest faults remain node-wide.
T7 exports/imports dedup, head, pending, flush/stop, persisted progress, claims and Unknown with snapshot + tail reconciliation.
Fence old SEND, collector and read boundaries with an ownership epoch before cutover.
Never migrate by changing Slot count/hash directly, dual-running writers or falling back on an error.
Before new writes, abort is possible; after new writes, rollback requires reverse migration under a new epoch.
Actual production deployment/cutover is a separate authorized action after the rehearsal.
Freeze target throughput, cold tail, rejection/Unknown and resource thresholds before target runs.
Separate monitoring failures from product failures; retain original failures and historical receipts.

## 9. Work packages, commands and final evidence

T0 → T1 → T2 → T3 → T4 is the critical implementation path.
Assign exact files before each package; independent model/fault tests, harness and T6 work may run in parallel with disjoint ownership.
Use explorer for source reading, worker for implementation and reviewer after each completed package, following repository workflow.
One integration owner controls manifests/lockfiles and shared runtime/app integration; two threads must not edit those files concurrently.
No user chat creation is required for this workflow.
New crate/test/harness targets are to be created and named by T0; do not claim future flags/targets already exist.
Existing repository checks: `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets`, `cargo test --workspace`.
Use existing multiraft `./scripts/acceptance.sh` when affected; it must report `ACCEPTANCE OK`.
Choose relevant ech0 focused tests from the frozen source; report literal executed commands and scopes.
Do not require this planning thread to run cargo, SSH or load.
Bind reports to both exact commits/trees/locks, binary and harness hashes, codec/layout/hash, limits and hardware/durability.
Report commands, pass/fail/ignored counts, scoped failures, source bindings, process identities, lease and cleanup logs.
Retain failed artifacts, Unknown resolution links and independent oracle receipts; 430/seven old cases are not a new pass.

## 10. Copyable execution prompt

Implement T0–T4 of this latest plan, first report M1, then continue to M2 when its gates pass.
Plan: `/Users/wei/Documents/WorktreesSpace/slot-multiraft/docs/plans/2026-10-17-slot-admit-first-implementation.md`.
Continue multiraft worktree `/Users/wei/Documents/WorktreesSpace/slot-multiraft`, baseline `9e63b454848d18ceea4510b3f3ea0fb52af71b68`.
Retain subsequent documentation commits carrying this plan and record actual multiraft HEAD; do not reset to the pre-plan baseline.
Freeze ech0 main `1606f85fc787604057f2ff7829df291c9ddc857b` (code baseline remains `8098a4f7`), recheck actual branch/HEAD and create a new worktree under `/Users/wei/Documents/WorktreesSpace/`.
The shared ech0 checkout at `/Volumes/Space/2nterdigital/ech0-1delivery` was observed on main after the documentation merge; read its actual branch/HEAD and do not switch/reset/merge it.
Use a collision-free branch such as `codex/admit-fsm`, exact dependency revisions and one manifest/lock/runtime integration owner.
Keep old locked OpenRaft 0.9.25 (manifest semver 0.9.21) and new alpha.30/fork `ea46d0e571f7497f1f549e125254dfcb2ff45e2e` isolated; complete the §2 source/dependency preliminaries before T0.
Build real deterministic Admit + all authority mutations, real three-process RF3/multi-Slot admission, then real HTTP/WS + TiKV + delivery/read loop in a new isolated namespace.
Full DRR, client/#173 optimization and performance SLO certification are not prerequisites for M1/M2 correctness.
No NULL/Unknown fallback to legacy, automatic unknown-write recovery, writer dual-write, production old-data rewrite or online cutover.
Follow T0–T4 contracts and scoped gates; retain failures and report exact source/commands/counts/ignored/oracle/cleanup evidence.
