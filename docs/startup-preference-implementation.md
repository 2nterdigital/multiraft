# Owned startup preference capability (#194)

The opt-in `RuntimeHandle::start_groups_with_preference(StartupBatch)` registers every local Group before any initialization or recovery wait, then validates every recovered application image before publishing executable Group capabilities together. Existing `NodeOwner::start`, `create_group`, `create_group_with_recovery_timeout`, in-process ClusterGlue and native disk formats keep their existing contracts.

`StartupBatch` supplies `Vec<StartupGroup>` (a `GroupConfig` and optional `preferred_initializer`), explicit 100..=2000ms `grace`, nonzero per-Group `recovery_timeout` up to 30 seconds, and optional opaque `[u8;32]` `input_digest`. A missing preference uses ordinary native initialization after complete registration. The library echoes the digest, but `configuration_verified=false` explicitly means it has no cohort agreement authority. Ech0 uses a fixed ten seconds and the explicit first candidate of 500ms; the library neither recognizes Ech0 Group families nor evaluates its mapping digest.

Admission reserves the empty owner atomically. Both a retained batch and an admitted legacy startup waiting for construction serialization prevent another batch with `Busy`. After successful completion, another batch receives `OwnerNotEmpty`. Invalid/nonempty requests do not construct new Groups, initialize them or clean up unrelated work. Waiter cancellation leaves the reservation and task with the existing owner. Owner shutdown and Drop fence policy dispatch and join actual native/listener/FSM resources through the original lifecycle.

For each Group, `Cg` is captured at native registry insertion and `Dg=Cg+recovery_timeout`. Checks before/after subsequent constructors and every policy boundary preserve that anchor. `B` is the last native registration instant; nonpreferred grace ends at `min(B+grace,Dg)`. An expired deadline wins over fallback. Application validation remains after native waiting and outside this budget. A blocked synchronous consumer callback cannot be preempted; its lease stays owned until it actually returns. `Registered` is a truthful failure phase when a later constructor consumes an earlier Group's deadline, before preference dispatch.

Provider provenance is inspected before the constructor or migration can write native namespaces. File-log inspection uses the existing hard-state/bin/NDJSON/legacy parsers and validates every present format, including a format the normal loader would not select. Recognized empty native records can prove `Pristine`; any historical vote/log/purge/commit proves `Persisted`. Snapshot inspection reuses provider metadata/data checksum parsers. An active snapshot proves persisted state even with no applied log basis. Fully validated inactive generations remain inert and can participate in empty provenance; they are never activated or deleted. Unknown names, symlinks, malformed or incomplete unsupported namespaces fail closed. This opt-in rejection surface is deliberately stricter than legacy loading, including unproven staging remnants.

A pristine provider result alone does not grant initialization: the live native `is_initialized` check is required immediately before one initialize call. Received vote/log/membership/snapshot state exits grace or makes native initialize reject atomically. Persisted Groups skip preference and initialize entirely. `InitOk` is the native local IO-conditioned reply; it does not prove quorum, commitment, a campaign result or a desired leader layout. `NotAllowed`, `NotDispatched` and `Unknown` remain distinct. There are no added election inputs, transfers, retries, all-peer barriers or quorum publication gates.

`StartupReport` preserves Group/Node identity, provenance, monotonic registration/deadline, stage, fallback and initialization disposition. `StartupFailure` preserves a boxed partial report, original `RuntimeError` source chain, failing Group/stage, effect uncertainty, and independent cleanup result/error. Actual native effects can remain unknown outside initialization, and released resources never retract disk/peer effects. OpenRaft owns the conversion of IO errors to its `AnyError`; the library retains that native source without inventing missing original fields. `Released` is reported only after the existing stop/join path confirms real resource release. Cancellation itself returns no synthetic successful report. Bounded `multiraft::startup` records include opaque digest correlation at admission, initialize call/reply and terminal stages; campaign causality remains unknown.

## Public consumer evidence

All new consumers use public owners/handles/factories/provider fixtures and actual gRPC, FSM destructor leases and listener port reuse. They do not expose private production probes. Read-only readiness polling is bounded; dispatched proposals are never retried by these consumers.

| Design cases | Public evidence |
| --- | --- |
| F01–F03, F20 | Preferred arrival before/after grace, permanent preferred/controller absence; two communicating voters establish native authority and commit/read actual bytes. |
| F04, F12 | Partial constructor failure/panic and later validator refusal preserve Group/stage/source chain, publish no partial batch and reclaim leases/ports. |
| F05, F15 | Actual read-only native namespace causes initialization IO failure; a test-local source subscriber witnesses OpenRaft's received Initialize and closes its owner before the reply. Both retain `Unknown` and real cleanup. The subscriber observes existing native events rather than injecting protocol behavior. |
| F06–F07 | A committed leader stops; surviving voters recover authority and preserve data. Its original disk returns with persisted provenance and no initialization/reclaim, then catches up to the newer committed value. |
| F08, F14 | Three gRPC nodes race native initialization. A separate source-boundary consumer lets real peer state arrive between live eligibility and initialize, proving raw `NotAllowed` rather than folded success. |
| F09–F11 | Batch cancellation/duplicate, pending legacy admission, nonempty owner and invalid declarations/grace are checked before effects; unrelated Group remains writable. |
| F13 | Durable local startup can succeed without quorum while proposal fails; memory keeps the stronger native wait and expires. |
| F16–F17 | Canceled constructor/native-wait waiters retain ownership and deadline; shutdown during grace fences further dispatch and confirms actual resources release. |
| F18 | Existing independent `owner_resource_lifecycle` consumers prove canceled shutdown and actual slow destructors retained beyond both cleanup windows on the same owner lifecycle. |
| F19 | Fresh and persisted earlier Groups expire during a later bounded constructor; late validators remain outside the native budget and cannot expose earlier Groups prematurely. |
| F21 | Deliberately different local preferences remain bounded with `configuration_verified=false`; there is no invented runtime cohort consensus. |
| F22 | Existing native provider compatibility/recovery fixtures and unchanged single-Group consumers are retained. Ech0 schema, binary pins and offline native-plan preflight belong to #195/#196; these library tests do not prove mixed-binary cohort compatibility. |

New tests are `owned_startup_preference`, `owned_startup_lifecycle`, `owned_startup_source_uncertainty`, `owned_startup_native_rejection` and store `startup_provenance`. Compatibility checks include `owned_runtime`, `startup_wait_budget`, `owner_resource_lifecycle`, `owned_snapshot_basis`, `owned_snapshot_recovery`, `file_log_roundtrip`, `restart_recover`, `durable_committed` and `native_snapshot_catalog`. The native crash writer remains an intentionally ignored child-process entry invoked by its passing parent test.

Validation uses Rust/Cargo 1.98.1 and exact OpenRaft `0.10.0-alpha.30`. `cargo fmt --all -- --check`, affected `cargo check --all-targets` and strict affected `cargo clippy --all-targets -- -D warnings` pass. Rust 1.98 flags tonic's existing concrete `Status` signatures as `result_large_err`; narrow documented allowances are confined to those two transport functions and the generated protobuf inclusions. No crate-wide lint exemption or wire/API boxing was added. The new failure's report is boxed to keep its size below the lint threshold.

Commands run with `CARGO_TARGET_DIR` and `TMPDIR` under the active checkout's `.tmp/issue-194/`. Public network regressions run with `-- --test-threads=1` to give short explicit test budgets stable IO contention. This is correctness/public-interface evidence only: no authorized three-scenario formal comparison, performance improvement, optimal grace, controller simplification or automatic-balancing removal is claimed.

## Changed Rust line counts

The pre-existing 1114-line file-log module was split by behavior before new provenance code: parsing/migration/provenance in `log_file/recovery.rs`, storage operations in `log_file/storage.rs`. The remaining `log_file.rs` is 850 total lines, with 722 before its trailing test module; no changed production file exceeds 800 lines. New public tests remain below the 1500-line test limit.

| File | Lines |
| --- | ---: |
| crates/multiraft-core/src/lib.rs | 40 |
| crates/multiraft-core/src/startup.rs | 17 |
| crates/multiraft-net/src/api.rs | 149 |
| crates/multiraft-net/src/grpc/mod.rs | 24 |
| crates/multiraft-net/src/grpc/server.rs | 170 |
| crates/multiraft-net/src/lib.rs | 105 |
| crates/multiraft-net/src/multiraft.rs | 633 |
| crates/multiraft-net/src/multiraft/group_start.rs | 451 |
| crates/multiraft-net/src/multiraft/startup.rs | 68 |
| crates/multiraft-net/src/runtime.rs | 178 |
| crates/multiraft-net/src/runtime/owner.rs | 240 |
| crates/multiraft-net/src/runtime/requests.rs | 217 |
| crates/multiraft-net/src/runtime/startup.rs | 155 |
| crates/multiraft-net/src/runtime/startup/admission.rs | 214 |
| crates/multiraft-net/src/runtime/startup/operation.rs | 236 |
| crates/multiraft-net/tests/owned_startup_lifecycle.rs | 327 |
| crates/multiraft-net/tests/owned_startup_native_rejection.rs | 130 |
| crates/multiraft-net/tests/owned_startup_preference.rs | 705 |
| crates/multiraft-net/tests/owned_startup_source_uncertainty.rs | 164 |
| crates/multiraft-net/tests/startup_support/mod.rs | 247 |
| crates/multiraft-store/src/log_file.rs | 850 |
| crates/multiraft-store/src/log_file/recovery.rs | 180 |
| crates/multiraft-store/src/log_file/storage.rs | 146 |
| crates/multiraft-store/src/snapshot_catalog/native.rs | 484 |
| crates/multiraft-store/tests/startup_provenance.rs | 100 |
