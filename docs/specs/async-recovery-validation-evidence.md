# Async recovery validation verification

Scope: MultiRaft issue #4 / Ech0 #245 T03, generic library recovery and native peer installation. Base revision: `ff52fc841b093d38e7961ed20e8b604ac186c626`. The readiness-publication correction starts from review revision `8999092938f3718f893b78ab55b74679bc7b7efb`; peer-ingress correction starts from `6aa210b28dc25181d0d9f52d7bc340a065b09e9f`. The final tested implementation revision is `95c6e6cad23dd0d8375d282907d61ada04dfcaaa`. OpenRaft remains exactly `=0.10.0-alpha.30` at `ea46d0e571f7497f1f549e125254dfcb2ff45e2e`; workspace Cargo files are unchanged.

## Final source checks

- `cargo fmt --all --check`: passed.
- `cargo check --workspace --all-targets`: passed.
- `cargo clippy --workspace --all-targets`: passed.
- `RUST_TEST_THREADS=1 cargo test --workspace`: passed, 366 tests passed, zero failed, 12 declared ignored, 82 result blocks.
- `TMPDIR="$PWD/.tmp/issue-4-review" ACCEPTANCE_DATA="$PWD/.tmp/issue-4-review/ingress-acceptance-final-data" BASE_PORT=33450 ./scripts/acceptance.sh`: passed on final source, printed `ACCEPTANCE OK`. This run uses three actual OS processes, leader loss, retained state, exact restarted-voter floors and gRPC checks.

The ignored install/purge/catalog/orphan child functions are invoked by their enabled parent scenarios. Dedicated G1 ENOSPC/RF3 cases and optional manual microbenchmarks remain unrun. This receipt does not claim TiKV transaction verification, power-loss certification, capacity qualification or remote deployment. Remote functional validation time for this library task is zero.

All final checks use `CARGO_TARGET_DIR="$PWD/.tmp/issue-4-review/target"` and `TMPDIR="$PWD/.tmp/issue-4-review"`. The unmodified acceptance scripts use a local ignored `target` symlink to that cache. Rust/Cargo SHA-256 fingerprints match before and after the entire final sequence and match implementation revision `95c6e6cad23dd0d8375d282907d61ada04dfcaaa`; the following receipt-only commit preserves that tested source. The first readiness-order run (364 passed) remains recorded in the earlier receipt and its original logs. Logs, fingerprints, temporary test leases and acceptance data are local to this checkout; no remote laboratory run was used.

## Readiness-publication correction

The prior implementation passed its recorded 361-test workspace run and acceptance, but missed a rejecting readiness hook: native activation (and legacy catalog write) preceded that fallible hook. A rejection could therefore replace durable recovery authority despite returning install failure. Native and legacy installation now run the hook and recheck generation/close state before catalog publication, while reads, apply and capture remain gated. The native blocking publisher rechecks again when admitted, because its scheduling can be delayed. Startup also rechecks after the hook; a hook that closes its owner cannot publish readiness.

A new peer candidate during opted-in pending startup is refused by owned gRPC/in-process ingress before native Core/SM dispatch. Matching existing native active loading plus committed-suffix replay still proceeds. Default consumers remain unchanged. Module ownership and dependency direction are unchanged: FSM declares the consumer seam; store validation owns admission/generation/gating; native installation invokes catalog-owned durable publication and stage cleanup; net runtime invokes store validation before Group admission.

The additional `./scripts/check-no-ai-traces.sh` check reports three inherited matches in `CONTRIBUTING.md`, `CONTRIBUTING.zh-CN.md` and `docs/superpowers/plans/2026-08-28-node-rpc-transport-foundation.md`. All three files are byte-identical to review revision `8999092938f3718f893b78ab55b74679bc7b7efb`; this separate baseline failure is preserved in `no-traces-final.log` and the final rerun `ingress-no-traces-final.log` and does not supersede the required successful Rust/acceptance checks.

## Peer-ingress correction

The first readiness-order fix still described its lower SM `WouldBlock` error as retryable. The exact-pinned OpenRaft worker treats every SM install error as fatal storage failure, so that could stop a real Group. Owned gRPC ingress now returns `Unavailable`; the in-process dispatcher returns a transport refusal mapped to `Unreachable`. Both gates run before Core/SM dispatch and return promptly without waiting on the startup transition or staging/restoring candidate bytes. The lower guard now reports `InvalidInput` for an ingress bypass and is explicitly documented as fatal through a raw native install API, not a retry protocol.

Two enabled real-peer regressions each exercise both transports. A native Group held in `RECOVERING` preserves applied/membership, authority and running state after repeated peer refusals, completes original snapshot+suffix proof, then accepts the retried candidate. An actual NodeOwner start handles repeated refusal during active proof, publishes a readable Group afterward, then gates and completes the peer install. The latter checks distinct generation, exact `(50, 9)` candidate metadata, unchanged applied/provider while peer proof waits, successful value 99 and real owner/application cleanup. Existing rejection/deadline/caller cancellation/owner-close tests continue to pass.

As a negative control, temporarily removing the ingress checks makes `recovering_native_group_refuses_peers_without_invoking_fatal_sm_guard` fail its native `running_state.is_ok()` assertion. `ingress-negative-control.log` preserves this expected failure; source was restored before final focused and workspace checks. This directly distinguishes the real native failure from a direct Store-only test.

## Stable-interface evidence

- `async_validation`: nine tests cover candidate read/capture isolation, apply/install serialization, distinct generations, proof rejection/panic/deadline/caller cancellation/owner close, real temporary-file cleanup, unchanged prior provider, repeated failure/reopen and absent-proof admission bypass. Added stable cases cover native and legacy readiness-hook rejection preserving provider bytes and applied/membership, direct Store startup bypass rejection followed by successful snapshot+suffix validation (not a native retry claim), and hook-initiated close during peer/startup validation with no authority/readiness publication.
- `async_recovery_validation`: six enabled tests use the checked-in `legacy-native-alpha30` fixture and verify its file hashes/producer revision. Startup proof sees snapshot plus committed suffix value 15, not snapshot alone. Proof rejection, timeout and owner cancellation do not run local readiness hooks. A readiness-hook rejection runs only after external proof and the local factory check; it leaves authority unchanged, startup unsuccessful, and application/listener leases released. A separate process leaves a genuine public-catalog candidate without activation; legacy owned startup prunes it before suffix proof.
- Native catalog/staging: 22 tests plus seven provenance tests cover equal-generation ownership, bounded 16-slot partial/full staging, old active reads during paused candidate IO, cleanup debt and 64-entry startup budget. Original `owned_maintenance_sampler` passes unchanged.
- Original native post-purge recovery/crash tests pass at `staged`, `application_restored`, `activated` and `bridge_updated` install cuts. Exact missing/corrupt recovery-basis repair remains supported without deleting original bytes when active authority is absent.

All candidate tests use public FSM/store/owner/catalog interfaces. Controlled crash fixtures derive real snapshots from the checked-in producer or real native writes and never fabricate an acknowledged business SEND.

## Corrected findings and check provenance

Early checking found and fixed three compatibility defects: candidate polling now waits the new fail-closed unavailable result; staging no longer holds the publication mutex across candidate data IO; inherited install diagnostic phases were restored so real crash cuts remain reachable. Eager orphan deletion with missing active authority was corrected to preserve original recovery/diagnostic bytes. An early 200 ms callback-entry test missed its deadline under a concurrent cold run; the exact case and the final serial workspace run pass. One intermediate build crossed concurrent source edits and is not a verification receipt. Final source checks above supersede those runs. The first review-fix focused run used the native builder-refusal expectation for legacy mode; its assertion was corrected to verify failure when the returned legacy builder executes. The final nine-case focused run and full workspace run pass; this was a test expectation correction, not a production capture change.

Original verification records remain under `.tmp/issue-4/` at their original checkout. Readiness-order correction logs remain under `.tmp/issue-4-review/` with their original names. Final peer-ingress logs there are `ingress-store-focused-final.log`, `ingress-net-focused-final.log`, `ingress-fmt-final.log`, `ingress-check-final.log`, `ingress-clippy-final.log`, `ingress-workspace-tests-final.log`, `ingress-acceptance-final.log`, `ingress-no-traces-final.log`, `ingress-negative-control.log` and `ingress-frozen-source.json`. The completed spec is [async-recovery-validation.md](async-recovery-validation.md).

## Changed Rust line counts

Production files remain below 800 lines; the largest is 602. Test files remain below 1500 lines.

| File | Lines |
| --- | ---: |
| `crates/multiraft-fsm/src/lib.rs` | 94 |
| `crates/multiraft-net/src/fsm_factory.rs` | 85 |
| `crates/multiraft-net/src/grpc/server.rs` | 175 |
| `crates/multiraft-net/src/multiraft/application.rs` | 308 |
| `crates/multiraft-net/src/multiraft/group_start.rs` | 480 |
| `crates/multiraft-net/src/multiraft/lifecycle.rs` | 124 |
| `crates/multiraft-net/src/multiraft/recovery.rs` | 172 |
| `crates/multiraft-net/src/multiraft/snapshot_runtime.rs` | 92 |
| `crates/multiraft-net/src/node.rs` | 247 |
| `crates/multiraft-net/src/router.rs` | 261 |
| `crates/multiraft-net/src/runtime/owner.rs` | 290 |
| `crates/multiraft-net/src/runtime/recovery.rs` | 86 |
| `crates/multiraft-net/src/runtime/startup/operation.rs` | 241 |
| `crates/multiraft-net/tests/async_recovery_validation.rs` | 761 |
| `crates/multiraft-net/tests/native_service_cancellation/mod.rs` | 144 |
| `crates/multiraft-store/src/lib.rs` | 44 |
| `crates/multiraft-store/src/sm_bridge.rs` | 493 |
| `crates/multiraft-store/src/sm_bridge/native.rs` | 404 |
| `crates/multiraft-store/src/sm_bridge/native/install.rs` | 152 |
| `crates/multiraft-store/src/sm_bridge/release.rs` | 92 |
| `crates/multiraft-store/src/sm_bridge/validation.rs` | 310 |
| `crates/multiraft-store/src/snapshot_catalog.rs` | 248 |
| `crates/multiraft-store/src/snapshot_catalog/native.rs` | 602 |
| `crates/multiraft-store/src/snapshot_catalog/native/lifecycle.rs` | 249 |
| `crates/multiraft-store/tests/async_validation.rs` | 796 |
| `crates/multiraft-store/tests/native_snapshot_catalog.rs` | 566 |
| `crates/multiraft-store/tests/native_snapshot_staging.rs` | 128 |
| `crates/multiraft-store/tests/startup_provenance.rs` | 136 |
