# Async recovery validation verification

Scope: MultiRaft issue #4 / Ech0 #245 T03, generic library recovery and native peer installation. Base revision: `ff52fc841b093d38e7961ed20e8b604ac186c626`. The implementation is the commit carrying this receipt. The readiness-publication correction starts from review revision `8999092938f3718f893b78ab55b74679bc7b7efb`. OpenRaft remains exactly `=0.10.0-alpha.30` at `ea46d0e571f7497f1f549e125254dfcb2ff45e2e`; workspace Cargo files are unchanged.

## Final source checks

- `cargo fmt --all --check`: passed.
- `cargo check --workspace --all-targets`: passed.
- `cargo clippy --workspace --all-targets`: passed.
- `RUST_TEST_THREADS=1 cargo test --workspace`: passed, 364 tests passed, zero failed, 12 declared ignored, 82 result blocks.
- `TMPDIR="$PWD/.tmp/issue-4-review" ACCEPTANCE_DATA="$PWD/.tmp/issue-4-review/acceptance-final-data" BASE_PORT=32950 ./scripts/acceptance.sh`: passed on final source, printed `ACCEPTANCE OK`. This run uses three actual OS processes, leader loss, retained state, exact restarted-voter floors and gRPC checks.

The ignored install/purge/catalog/orphan child functions are invoked by their enabled parent scenarios. Dedicated G1 ENOSPC/RF3 cases and optional manual microbenchmarks remain unrun. This receipt does not claim TiKV transaction verification, power-loss certification, capacity qualification or remote deployment. Remote functional validation time for this library task is zero.

All final checks use `CARGO_TARGET_DIR="$PWD/.tmp/issue-4-review/target"` and `TMPDIR="$PWD/.tmp/issue-4-review"`. The unmodified acceptance scripts use a local ignored `target` symlink to that cache. Rust/Cargo SHA-256 fingerprints match before and after the entire final sequence and match implementation revision `9198b06d0e2ea703a82adf52979ff863959f2501`; subsequent receipt-only amendments preserve that tested source. Logs, fingerprints, temporary test leases and acceptance data are local to this checkout; no remote laboratory run was used.

## Readiness-publication correction

The prior implementation passed its recorded 361-test workspace run and acceptance, but missed a rejecting readiness hook: native activation (and legacy catalog write) preceded that fallible hook. A rejection could therefore replace durable recovery authority despite returning install failure. Native and legacy installation now run the hook and recheck generation/close state before catalog publication, while reads, apply and capture remain gated. The native blocking publisher rechecks again when admitted, because its scheduling can be delayed. Startup also rechecks after the hook; a hook that closes its owner cannot publish readiness.

A new peer candidate during opted-in pending startup returns `WouldBlock` before restore, generation mutation or publication and preserves `RECOVERING`. Matching existing native active loading plus committed-suffix replay still proceeds. Default consumers remain unchanged. Module ownership and dependency direction are unchanged: FSM declares the consumer seam; store validation owns admission/generation/gating; native installation invokes catalog-owned durable publication and stage cleanup; net runtime invokes store validation before Group admission.

The additional `./scripts/check-no-ai-traces.sh` check reports three inherited matches in `CONTRIBUTING.md`, `CONTRIBUTING.zh-CN.md` and `docs/superpowers/plans/2026-08-28-node-rpc-transport-foundation.md`. All three files are byte-identical to review revision `8999092938f3718f893b78ab55b74679bc7b7efb`; this separate baseline failure is preserved in `no-traces-final.log` and does not supersede the required successful Rust/acceptance checks.

## Stable-interface evidence

- `async_validation`: nine tests cover candidate read/capture isolation, apply/install serialization, distinct generations, proof rejection/panic/deadline/caller cancellation/owner close, real temporary-file cleanup, unchanged prior provider, repeated failure/reopen and absent-proof admission bypass. Added stable cases cover native and legacy readiness-hook rejection preserving provider bytes and applied/membership, pending-startup candidate deferral followed by successful snapshot+suffix validation, and hook-initiated close during peer/startup validation with no authority/readiness publication.
- `async_recovery_validation`: four tests use the checked-in `legacy-native-alpha30` fixture and verify its file hashes/producer revision. Startup proof sees snapshot plus committed suffix value 15, not snapshot alone. Proof rejection, timeout and owner cancellation do not run local readiness hooks. A readiness-hook rejection runs only after external proof and the local factory check; it leaves authority unchanged, startup unsuccessful, and application/listener leases released. A separate process leaves a genuine public-catalog candidate without activation; legacy owned startup prunes it before suffix proof.
- Native catalog/staging: 22 tests plus seven provenance tests cover equal-generation ownership, bounded 16-slot partial/full staging, old active reads during paused candidate IO, cleanup debt and 64-entry startup budget. Original `owned_maintenance_sampler` passes unchanged.
- Original native post-purge recovery/crash tests pass at `staged`, `application_restored`, `activated` and `bridge_updated` install cuts. Exact missing/corrupt recovery-basis repair remains supported without deleting original bytes when active authority is absent.

All candidate tests use public FSM/store/owner/catalog interfaces. Controlled crash fixtures derive real snapshots from the checked-in producer or real native writes and never fabricate an acknowledged business SEND.

## Corrected findings and check provenance

Early checking found and fixed three compatibility defects: candidate polling now waits the new fail-closed unavailable result; staging no longer holds the publication mutex across candidate data IO; inherited install diagnostic phases were restored so real crash cuts remain reachable. Eager orphan deletion with missing active authority was corrected to preserve original recovery/diagnostic bytes. An early 200 ms callback-entry test missed its deadline under a concurrent cold run; the exact case and the final serial workspace run pass. One intermediate build crossed concurrent source edits and is not a verification receipt. Final source checks above supersede those runs. The first review-fix focused run used the native builder-refusal expectation for legacy mode; its assertion was corrected to verify failure when the returned legacy builder executes. The final nine-case focused run and full workspace run pass; this was a test expectation correction, not a production capture change.

Original verification records remain under `.tmp/issue-4/` at their original checkout. Correction logs and SHA-256 source fingerprints are retained here under `.tmp/issue-4-review/`: `store-focused-final.log`, `net-focused.log`, `fmt-final.log`, `check-final.log`, `clippy-final.log`, `workspace-tests-final.log`, `acceptance-final.log` and `frozen-source.json`. The completed spec is [async-recovery-validation.md](async-recovery-validation.md).

## Changed Rust line counts

Production files remain below 800 lines; the largest is 602. Test files remain below 1500 lines.

| File | Lines |
| --- | ---: |
| `crates/multiraft-fsm/src/lib.rs` | 94 |
| `crates/multiraft-net/src/fsm_factory.rs` | 85 |
| `crates/multiraft-net/src/multiraft/application.rs` | 308 |
| `crates/multiraft-net/src/multiraft/group_start.rs` | 480 |
| `crates/multiraft-net/src/multiraft/lifecycle.rs` | 124 |
| `crates/multiraft-net/src/multiraft/recovery.rs` | 172 |
| `crates/multiraft-net/src/multiraft/snapshot_runtime.rs` | 92 |
| `crates/multiraft-net/src/runtime/owner.rs` | 290 |
| `crates/multiraft-net/src/runtime/recovery.rs` | 86 |
| `crates/multiraft-net/src/runtime/startup/operation.rs` | 241 |
| `crates/multiraft-net/tests/async_recovery_validation.rs` | 416 |
| `crates/multiraft-net/tests/native_service_cancellation/mod.rs` | 144 |
| `crates/multiraft-store/src/lib.rs` | 44 |
| `crates/multiraft-store/src/sm_bridge.rs` | 493 |
| `crates/multiraft-store/src/sm_bridge/native.rs` | 404 |
| `crates/multiraft-store/src/sm_bridge/native/install.rs` | 151 |
| `crates/multiraft-store/src/sm_bridge/release.rs` | 92 |
| `crates/multiraft-store/src/sm_bridge/validation.rs` | 310 |
| `crates/multiraft-store/src/snapshot_catalog.rs` | 248 |
| `crates/multiraft-store/src/snapshot_catalog/native.rs` | 602 |
| `crates/multiraft-store/src/snapshot_catalog/native/lifecycle.rs` | 249 |
| `crates/multiraft-store/tests/async_validation.rs` | 796 |
| `crates/multiraft-store/tests/native_snapshot_catalog.rs` | 566 |
| `crates/multiraft-store/tests/native_snapshot_staging.rs` | 128 |
| `crates/multiraft-store/tests/startup_provenance.rs` | 136 |
