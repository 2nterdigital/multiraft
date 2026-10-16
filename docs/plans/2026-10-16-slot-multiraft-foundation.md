# Slot MultiRaft foundation implementation plan

**中文：** [2026-10-16-slot-multiraft-foundation.zh-CN.md](./2026-10-16-slot-multiraft-foundation.zh-CN.md)

**Date:** 2026-10-16
**Status:** Implementation authorized and in progress. The table below is a work plan, not a successful test receipt.

Implement the [adopted foundation design](../specs/2026-10-16-slot-multiraft-foundation.md). The [feasibility assessment](../specs/2026-10-16-slot-multiraft-feasibility.md) preserves historical source facts and explains the later ech0 integration gates. The user has confirmed the scope and start of implementation; these steps require verification evidence, not renewed approval of settled choices.

## 1. Fixed scope

Use C `28e668b676c803838eb64a04ba160fb10ad88316` as the working baseline, integrate necessary M `44baed14b3717b6b6f5f7327790cc97299889e19` improvements and port R `d6a5dbbb6d2f1c68e9e8bddbbfca18f562c1185c` RocksDB logs. Deliver a new Slot API, independent new data directories and a runnable three-process RF3 multiple-Slot test application. Retain old data without automatic API/WAL compatibility promises.

Freeze slot_count, hash version and one-to-one Slot/Group mapping in the persisted manifest. No online resharding, production Admit/TiKV integration, business data migration, dedup policy change or capacity certification is included. Snapshot/restart, leader change, post-commit disconnect/Unknown, owned cancellation, shutdown and resource limits are part of this delivery.

## 2. Execution steps and evidence

| Step | Current state | Deliverable | Verification gate |
| --- | --- | --- | --- |
| A — C + RocksDB | In progress | Focused R backend port with node-owned DB, group namespaces, durable callbacks and backend marker checks; preserve C ownership, exact results, validation/readiness and shutdown behavior | Backend tests; sync/callback/cancel behavior; missing-DB/backend mismatch; snapshot/replay/restart; no bypass of C proof gates |
| B — M integration review | Planned | Source-bounded inventory of M-only descriptor/shared restart and scripts/Jepsen portability; integrate relevant behavior and record deliberate exclusions | Exercise retained descriptor mismatch and shared restart behavior; check affected scripts, not only cargo compilation |
| C — Slot contracts | Planned | New `multiraft-slot` crate with canonical manifest/digest/disk binding, fixed hash/layout, limit configuration and test FSM seam | Known hash vectors, restart mapping, invalid versions/counts/mapping/replicas, node/disk mismatch and cross-node digest mismatch fail closed |
| D — Route and lifecycle | Planned | Key/Slot server validation, local and single-attempt remote path, exact result/query semantics, owned accepted work, deadlines and bounded close | Wrong-key/Slot/digest requests rejected before mutation; stale hint classified; post-dispatch Unknown; no retry loop; cancellation/drain and capacity release assertions |
| E — Runnable demo and RF3 | Planned | Minimal deterministic result-bearing FSM and reproducible three-OS-process RF3 multiple-Slot runner using RocksDB and new directories | Local/remote writes and reads; snapshot/purge/restart; leader change; committed response loss and same-ID retry; limits/hot-cold progress; shutdown and recovery proof failure |
| F — Independent review and evidence | Planned | Review integrated diff/contracts, repair verified issues, document exact-source command results and remaining gaps in both languages | Relevant tests/build and real-process acceptance pass for the actual source; reviewer findings resolved or explicitly recorded; no borrowed historical pass counts |

A and B precede the final integrated baseline check. C establishes the contracts consumed by D and E; implementations may proceed in parallel only with disjoint file ownership and coordinated interfaces. F reviews the completed integrated behavior. No step is marked complete until its actual evidence is attached.

## 3. Validation procedure

1. Freeze the tested source commit or an exact reproducible tree digest, OpenRaft pin, toolchain, features and limits. Keep temporary commands, logs and run artifacts under this worktree's `.tmp/`. Give every new run its own data directory; do not reuse or delete old data.
2. Run focused storage/runtime/Slot tests as their changes land. Exercise C's proof/readiness/cancellation/startup gates alongside the Rocks backend, rather than testing the new log independently and assuming composition works.
3. Run repository-required formatting, lint and workspace checks against the integrated result as applicable. Record commands exactly, exit status, pass/fail/ignored counts and meaningful limitations. An intentionally skipped environment-dependent check remains a gap, not a pass.
4. Run the applicable descriptor/shared-restart and portability checks for M changes. Retain a per-change inclusion/exclusion record and evidence that the selected behavior survived integration.
5. Run three distinct OS processes, RF3, multiple Slots and RocksDB through the demo/acceptance runner implemented in E. Record process identities, endpoints, manifest digest, node-bound directories, data assertions and cleanup. Test through public Slot ingress so lower-level Raft success cannot mask a broken facade.
6. Inject leader loss and response loss after commit. Verify the exact committed result can be recovered after explicit same-ID retry/restart without duplicate FSM effects. Verify pre-dispatch rejection is distinct from Unknown and that query misses do not resolve Unknown incorrectly.
7. Exercise snapshot restore/log replay, recovery proof rejection, cancellation after acceptance, saturated node/Slot byte/count limits, hot/cold progress and bounded shutdown. Record unfinished owned work and errors; do not call a run clean solely because client tasks exited.
8. Review the final integrated diff, fix verified findings, rerun affected checks and bind the evidence to that final source. Adjust API documentation to the actual implemented types, hash bytes and routing contract.

Do not invent names of future test binaries or acceptance flags. Step E must supply exact reproducible commands once its runner exists. Existing `scripts/acceptance.sh` evidence is useful for its covered behavior but is not automatically evidence of new Slot ingress or a RocksDB multi-Slot test.

## 4. Acceptance matrix

| Scenario | Required observation |
| --- | --- |
| Mapping and identity | Same key maps identically after restart; incompatible manifest/hash/node-bound disk is rejected; target recomputes key/Slot and checks digest |
| Exact apply and reads | Returned effects belong to the exact applied command; linearizable reads use native ReadIndex/apply visibility; stale reads carry watermark |
| Recovery | Committed test state and results survive full process restart and snapshot/log recovery; missing proof never yields ready |
| Response loss | Commit then disconnect may return Unknown; accepted work keeps its owner; explicit same-ID retry returns the fixed result with no extra effect |
| Leader changes | Any-node ingress makes bounded progress or classified refusal during/after change; hint does not become authority |
| Startup/create/recovery failure | C local-node rollback and cleanup remain intact; no blanket claim about all runtime fatal paths |
| Resources and close | Both count and bytes bounded at node and Slot; permits follow owned work; hot/cold progress measured; shutdown stops intake, drains and reports leftovers |
| Backend | RocksDB durable callbacks are not lost on waiter cancellation; marker/backend/DB errors fail closed; FSM state recovered from log plus snapshot |

Slot-local runtime fault isolation is a separate gate before production integration. Shared DB or manifest identity failures stay node-level. The first delivery may retain the proven C rollback boundary and must state that limitation.

## 5. Evidence ledger

| Item | Actual source / command / outcome |
| --- | --- |
| Integrated baseline A/B | Pending; no new successful result recorded |
| Slot unit/contract checks C/D | Pending; no new successful result recorded |
| Three-process RF3 multiple-Slot acceptance E | Pending; no new successful result recorded |
| Independent review and final verification F | Pending; no new successful result recorded |

Populate this ledger with actual commands and receipts after execution, including failures and unrun scenarios. The C historical `95c6e6c` record (366 passed, 12 ignored and one three-process acceptance) and R's backend commands are not passes of this worktree. A small 4/8-Slot acceptance does not certify production slot_count, resources, throughput or B4 latency.

## 6. Next-stage boundary

Production integration first requires proven Slot-local isolation and a deterministic Admit design preserving atomic dedup/seq/head/pending/result plus flush/stop. Fence old admission/collector work and migrate full authoritative state and persisted progress; never bootstrap an existing Channel from a lagging TiKV head. Permanent command-result retention and cross-Channel TiKV transaction semantics cannot be silently narrowed.

RocksDB remains local consensus log/metadata storage. TiKV remains long-term message transaction storage. Async delivery and async TiKV work start as parallel post-admission paths; SENDACK is committed + applied admission, not TiKV or RECV acknowledgement. These contracts constrain the next stage; this foundation does not implement it.
