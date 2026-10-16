# Slot foundation verification evidence

**中文：** [2026-10-16-slot-foundation.zh-CN.md](./2026-10-16-slot-foundation.zh-CN.md)

**Date:** 2026-10-16
**Status:** First verification version passed final integrated checks and local three-process all-case acceptance at 76ce0dd. The unlocated historical observer risk remains open; no production readiness, stability, capacity or latency certification.

## Source and reproduction

The final verified source is `76ce0dd6c686367f04de4cfc9772ed3942140c3c`, tree `6a7f7538c641dc3017ab9093283bc2b918f80503`. Its production implementation is unchanged from initial local implementation `e5546f1eb28908577745306cd61a57dd8e19b0dc`, tree `0ffa71d3165a946b2ac32bbf8cc5944edbe0d2c5`; `48af836` changes only routing test preconditions/read observations, and `8d2d003` changes only shutdown-fencing fixture observation/cleanup, and `76ce0dd` adds only observer-test failure diagnostics. This documentation follow-up does not alter executable source. The implementation starts at C `28e668b676c803838eb64a04ba160fb10ad88316`, selectively integrates M `44baed14b3717b6b6f5f7327790cc97299889e19`, and ports R `d6a5dbbb6d2f1c68e9e8bddbbfca18f562c1185c`. The OpenRaft fork remains pinned to `ea46d0e571f7497f1f549e125254dfcb2ff45e2e`; historical C/R passes are not integrated-source results.

Host toolchain: aarch64-apple-darwin, rustc 1.98.1 (`48a229cea`, LLVM 22.1.8), cargo 1.98.1 (`797e8a9bc`). Rust commands use workspace default features unless specified. Temporary dependencies, targets, logs, new node data and run receipts are retained below `.tmp/slot-foundation/` in `/Users/wei/Documents/WorktreesSpace/slot-multiraft`. They are local evidence, not tracked repository artifacts.

```sh
mkdir -p .tmp/slot-foundation/tmp
export TMPDIR="$PWD/.tmp/slot-foundation/tmp"
export CARGO_TARGET_DIR="$PWD/.tmp/slot-foundation/target"
# macOS source RocksDB/bindgen environment used for these runs:
export LIBCLANG_PATH=/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/lib
export DYLD_LIBRARY_PATH="$LIBCLANG_PATH${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
```

The [design](../specs/2026-10-16-slot-multiraft-foundation.md) records exact implemented APIs/hash/layout bytes and limits; [acceptance instructions](../slot-acceptance.md) describe the local three-process runner. Every run requires a fresh output directory; preserve earlier receipts and data.

## Focused implementation checks

These are incremental package checks made during implementation, before final integrated verification; do not add their counts together as distinct final-source tests. The logs record the individual binaries and assertions. Reproduction commands below identify the same test targets.

| Command | Recorded result | Local log |
| --- | --- | --- |
| `cargo test -p multiraft-store --test log_rocks` | 7 passed, 0 failed/ignored | `rocks-store-clang.log` |
| `cargo test -p multiraft-net --test owned_rocksdb_guards --test recovery_diagnostics --test rocksdb_async_recovery` | 3 + 6 + 1 passed, 0 failed/ignored | `rocks-net-final.log` |
| `cargo test -p multiraft-net --lib log_open_tests -- --test-threads=1` | 3 passed, 0 failed/ignored | `rocks-opener-cancel-serial.log` |
| `cargo test -p multiraft-net --test owned_proposal --test owned_runtime` | 5 + 8 passed, 0 failed/ignored | `ownproposal-tests-clang.log` |
| `cargo test -p multiraft-net --lib fingerprint` | 10 passed, 0 failed/ignored | `fingerprint-grpc-final.log` |
| `cargo test -p multiraft-net --test applied_watermark --test local_observation` | 4 + 2 passed, 0 failed/ignored | `watermark-tests-final.log` |
| `cargo test -p multiraft-slot` | 1 unit + 9 manifest + 12 runtime passed, 0 failed/ignored; 0 doctests | `slot-runtime-review-fixes.log` |
| `cargo test -p multiraft-slot --test manifest` | 9 passed, 0 failed/ignored | `manifest-tests-final.log` |
| `python3 scripts/test_slot_acceptance.py` | 15 helper tests passed again at final source | `harness-tests-76ce0dd.log` |

Storage checks cover durable writes/callbacks, namespace separation, backend markers, missing DB, async proof and cleanup ownership. The serial opener run follows an earlier scheduler-sensitive fixture failure retained in `rocks-opener-cancel-drop.log`; do not count that earlier run as successful. Owned proposals retain native admission after dropped/timed-out receipts. Manifest/runtime checks cover stable vectors, disk binding, invalid native modes, single application forwarding, nonhosted read-only probe failover, no write retry, post-dispatch Unknown, retained budgets, panic fencing and startup rollback. Watermark tests pair values and applied positions under one lock, including concurrent apply, never-applied and recovery-gated reads.

Native fingerprint checks cover exact legacy/bound compatibility, canonical metadata, every server path before group lookup/snapshot admission, and client response checks. The digest is a compatibility domain, not peer authentication or an all-node readiness certificate.

## M integration and Jepsen unit scope

`.tmp/slot-foundation/main-integration.md` records selection from M: version-1 `cluster.json`, descriptor-first admin lookup and shared restart, Java discovery and dynamic voter IDs. C's containment restrictions reject daisy/second standby modes. Bash `GROUPS` defaults and Jepsen relative-cwd restart behavior received explicit regressions. Shell syntax and `.tmp/slot-foundation/check-main-integration.py` passed; the executable fixture uses a fake process and does not prove real-cluster recovery.

`python3 .tmp/slot-foundation/run-jepsen-unit.py` passed **3 tests, 12 assertions, 0 failures/errors, exit 0**. Logs: `jepsen-lein-unit.log`, `lein-provenance.md`. The isolated official Leiningen 2.11.2 launcher/jar and existing Zulu JDK17 use temporary caches/profile/target directories; the jar SHA256 is `7d31ae23ae769e927438b0cd55d15a93e7dabab09fd4fc15877979161e108774`. This supersedes the earlier unavailable-Leiningen note. The final selective-integration independent review reported no remaining issues. The restart unit test mocks shell execution. No real Jepsen workload, SSH, laboratory or B4 run was performed.

## Preserved process receipts and failures

| Local receipt directory | Actual outcome |
| --- | --- |
| `acceptance-402af21c5ca44c42a6141398fb2c4cac` | Overall failed with Unavailable. Three recorded cases passed: real RF3 forwarding/exact results/dedup, gated Unknown/retained budget/cold-Slot progress, leader kill/quorum progress/same-data restart. Compaction observation ran too early; this is not all-case success. Cleanup recorded three exit-zero children. |
| `acceptance-3f4282a925194765b197b1d09cb99290` | Overall failed with Busy before completing a case; harness had not waited for retained permits to release. Cleanup recorded three exit-zero children. |
| `acceptance-1c96b849162244bea350ea3a6d8b9569` | Recovery-only receipt passed: durable snapshot/purge/suffix/all-node restart/dedup restore, and manifest mismatch/original directory reopen. Not a normal/all-case receipt. |
| `acceptance-e32ccb53f27348a3b2924a2b5d1caf8f` | Fingerprint-only RF3 receipt passed: A+A quorum commits, A+B cannot return Applied, B has no A value, restored A quorum resolves the ambiguous command by explicit same-ID retry. Not an all-case receipt. |

Each directory retains `receipt.json`, `requests.jsonl`, layout, child logs and data. The repaired harness polls durable snapshot/purge completion and budget release; a new raw HTTP RST scenario observes apply-gate entry before closing the client socket without reading a response. That caller receives no Unknown response. Its purpose is to prove execution ownership and dedup after real disconnect; the final all-case receipt below passed this scenario.

The original full workspace attempt in `workspace-tests.log` failed at `twenty_group_burst_reports_source_coverage_and_releases_all_resources` waiting for a leader within five seconds; two focused reproductions passed. Independent seam tests reproduced serial/whole-round probe blocking in the test startup helper. The fix retries each node independently under the same absolute five-second bound. `fixture-probe-red.log` and `fixture-probe-retry-red.log` preserve red results; final green seam checks passed 2 tests (`fixture-probe-retry-green.log`), election consumer 7 tests, and the twenty-group consumer 1 test. This establishes the helper defect and fix; it does not prove that defect caused the original full-workspace failure.

The subsequent e554 workspace run also failed; see the table below. The write succeeded; the later single-attempt linearizable read used a voter hint and native ReadIndex returned a classified NotLeader refusal. This is not evidence of a failed first write, and does not by itself establish a production defect or a startup-readiness cause. The bounded diagnosis and final verification are recorded below.

The original-binary focused run `slot-route-original-binary.log` failed instead at runtime.rs:337 on its first write; keep it separate from the workspace line-340 read failure. Six temporary diagnostic runs yielded four passes and two failures. One observed a successful write (term 1/index 2) then provider-1 hint 2/target 2 followed by read NotLeader(None); subsequent status showed node 2 Follower/None and node 3 Candidate/None, both term 1. The other refused the first write with hint/target 2. There is no observed target/hint disagreement or persistent leader refusal, and post-call status cannot establish the exact cause. The final test-only change establishes its startup success-path precondition with bounded read-only probes, keeps the write single-attempt, and permits explicit read-only NotLeader retries after the write under one absolute five-second budget. Every successful read value is asserted immediately. Production routing is unchanged; no root-cause fix is claimed. Independent fixture review reported no issues.

The 48af836 serial workspace failure is a separate test observation: an outer one-second wait for a local status Closed response timed out at owned_startup_source_uncertainty.rs:91. The later missing-lease error in startup_support.rs:65 followed panic-driven temporary-directory destruction. Source permits a local status probe to pass its accepting check, then wait behind cleanup's writer while startup holds an admission reader; the log does not prove that exact interleaving. This status probe is not a Raft RPC. The final test-only expired-deadline status probe observes only Closed as fencing and treats Deadline { phase: Admission, outcome_unknown: false } as not yet fenced; it does not enter the admission lock. The original one-second bound and held/Unknown/lease/reuse assertions remain. On observation failure it preserves the directory, releases the gate and performs an additional shared five-second bounded join before reporting the failure, retaining real cleanup errors. `fixture-fence-uncertainty-tests.log` passed 4 tests and Clippy passed; independent review reported no issues. A temporary forced-failure injection produced expected exit 101, startup Unknown/Released and shutdown Ok with no secondary panic (`fixture-fence-uncertainty-forced-cleanup.log`); its data remains in `.tmp/slot-foundation/tmp/.tmpqH2XAe`. The injection was removed and all 4 tests passed again. This is not a claimed production deadlock repair.

Six bounded observer diagnostic runs all passed without reproducing the wake: initial observations were Leader/term 1 with initialized effective/committed membership equal and a committed vote; wake was None. This does not prove data-only membership changes, a production notification leak or a root-cause repair. The follow-up only adds initial/wake and failure-branch post-observation three-node raw-metrics diagnostics; the original None assertion, three-node topology and budgets remain, with no ignore or retry; independent review reported no issues in this diagnostic-only change. The 8d2d003 observer failure remains an unlocated risk even if later full verification passes.

The preserved intermediate attempts are `workspace-tests-final.log` (e554, exit 101, Slot write passed then linearizable read NotLeader; 11 other runtime cases passed), `workspace-tests-verified.log` / `verification-48af836.json` (48af836, exit 101, startup uncertainty target 3 passed/1 failed on the fence-observation timeout), and `workspace-tests-8d2d003.log` / `verification-8d2d003.json` (8d2d003, exit 101, observer assertion; net lib 54 passed/1 failed). Their earlier format/Clippy successes are not substitutes for the final run. None of these failed logs was overwritten.

## Final integrated verification

All seven serial pipeline steps exited zero at source 76ce0dd. `verification-76ce0dd.json`, `root-final-verification-observer-evidence.log` and the named logs bind commands and exit status; `workspace-counts-76ce0dd.json` records parsed counts.

| Command | Actual result / log |
| --- | --- |
| `cargo fmt --all --check` | Passed; `fmt-76ce0dd.log` |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | Passed; `clippy-76ce0dd.log` |
| `cargo test --workspace --locked -- --test-threads=1` | 430 passed, 0 failed, 12 ignored over 92 successful result blocks; `workspace-tests-76ce0dd.log`. These are execution counts, not counts of unique test definitions. |
| `cargo build -p multiraft-demo -p multiraft-slot-demo --locked` | Passed; `build-76ce0dd.log` |
| `python3 scripts/test_slot_acceptance.py` | 15 passed; `harness-tests-76ce0dd.log` |
| `python3 .tmp/slot-foundation/run-final-slot-acceptance.py` | Passed; source-bound invocation of the public runner with `--case all`; 7 cases passed; `slot-all-76ce0dd.log` |
| `python3 .tmp/slot-foundation/run-final-legacy-acceptance.py` | Passed; invokes `./scripts/acceptance.sh`, ACCEPTANCE OK, exit 0; `legacy-76ce0dd.log`, `root-legacy-acceptance.log`, `root-legacy-binding.json` |

The 12 ignored entries include guarded child fixtures, manual microbenchmarks and dedicated laboratory recovery/ENOSPC checks. A child invoked by its parent is not an independent extra pass; this run did not execute the dedicated G1 laboratory cases. Jepsen unit scope remains the separate 3-test/12-assertion result above, not a real Jepsen workload.

The new Slot receipt is `.tmp/slot-foundation/root-final-acceptance-364b75700b804bd7be0e6a548a8c4772/receipt.json`, with `source-binding.json` and `requests.jsonl` beside it. It ran three actual RF3 processes, four Slots/Groups 100–103, RocksDB/NativeDurable and fresh node-bound data, from 10:02:54 to 10:03:04 UTC. Its seven passed cases cover:

1. Any-node forwarding, exact effects and same-ID/changed-intent dedup behavior.
2. Apply-gate Unknown, retained Slot budget, immediate Busy and cold-Slot progress without automatic resubmission.
3. Real public HTTP RST after gate entry, with no client response; retained work and exact explicit retry without a second increment.
4. Leader kill, surviving-quorum progress and same-data restart.
5. Completed durable snapshots/purge on every node/Slot, committed suffix, all-process restart and original results restored.
6. Changed manifest rejection and original-directory reopen.
7. Native fingerprint A+A quorum, A+B refusal to return Applied, isolated B with no A value, and explicit same-ID disambiguation after restoring A quorum.

The run uses per-Slot request limit 1, global 16, per-Slot bytes 16 MiB/global 128 MiB; command/query/effects caps are 128 KiB. These test limits differ from library request-count defaults. All 13 recorded process-start PIDs were absent after completion; all nine recorded loopback ports could be rebound for listening. The final cleanup recorded three exit-zero children. Data, logs and failed earlier runs remain retained.

`source-binding.json` verifies executable source paths against commit 76ce0dd and records default workspace features/debug profile, toolchains, exact commands, layout, PIDs/endpoints and cleanup. Binary SHA256: `20a87016c54ff003c6d36263c774440a87b9a9e05b239d912d3948dd4d609211`; harness SHA256: `6109063502d58abb11e19a85332ce024ed1f7bcee324ca990a6d18ea4534b825`; Cargo.lock SHA256: `17bed111dbc0713fe42dffa336b4b387561ca68963940c232b9b43d4fb1680ba`. The binding also includes receipt/request/layout hashes.

Legacy regression used `GROUPS=4 NODES=3 BASE_PORT=30341` and new data at `.tmp/slot-foundation/root-legacy-13a40c32d5c441dbbb52c5d99ef71b9a`; it passed all six checks plus `not_leader` (1 test) and `grpc_cluster` (2 tests) through 10:03:38 UTC. The temporary target symlink was removed exactly after the run. All three recorded PIDs were absent and all six ports could be rebound for listening. This validates the retained old demo/script path, independently of the new Slot receipt.

Package, fixture and documentation-draft reviews reported no issues in their reviewed changes. The historical failed attempts remain recorded above, including the unresolved intermittent observer observation. The baseline repository-wide trace-policy scan also remained failing on pre-existing content; `.tmp/slot-foundation/attribution-scan-limit.md` records the baseline comparison. It was not reported as a passed check or expanded into unrelated historical edits.

## Remaining production gates

The unlocated intermittent observer failure above is an explicit reliability item before production integration. A later passing full run does not certify stability or close that historical risk.

1. Inject a fault proven local to one Slot; demonstrate that healthy Slots keep serving and that quarantine/recovery cannot admit the damaged Slot. Shared DB or manifest faults remain node faults. Current startup/create/proof failures retain local-node rollback; runtime panic fencing is also node-wide.
2. Specify and test deterministic Admit with atomic permanent successful-result dedup, business seq, accepted head, pending, flush/stop and exact holder/revision semantics. Inject cancellation/Unknown and prove no duplicate seq. Apply must perform no external I/O.
3. Design a fenced migration: stop old admission/collector, drain, import full accepted/pending/results/flush and matching persisted progress, validate the imported state, then cut over with an interruption/rollback procedure. A single lagging TiKV head is insufficient.
4. Integrate TiKV and delivery as parallel post-admission paths while preserving cross-Channel transactional semantics and Unknown/fail-stop behavior. SENDACK remains committed/applied admission, not TiKV or RECV.
5. Before performance claims, fix terminal-outcome denominators and instrumentation, verify issuance/correctness, then run at least two alternating baseline/candidate comparisons with the same durability/load. Report offered/issued/completed/failed/unissued, successful latency histograms and resources. Fixed mapping has no online migration; more Slots do not accelerate one hot key by assumption.
