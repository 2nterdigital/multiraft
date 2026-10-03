# Asynchronous recovery validation

Consumers can validate external recovery evidence without putting network waits in deterministic `apply`. The library has no dependency on a particular archive or database. OpenRaft remains pinned to the exact workspace revision.

## Consumer contract

Implement `StateMachine::requires_recovery_validation()` as `true` when the recovered application image requires external proof. `recovery_validation(context)` captures bounded, owned inputs synchronously under the FSM lock and returns an optional `ValidationFuture`. The library polls that future after releasing the lock. Capture must not do network IO or spawn detached work. The future owns its connections, requests and temporary resources; dropping it must cancel or release them. `None` means no external work is needed for this exact image and bypasses external validator admission, including an occupied consumer-supplied budget.

`ValidationContext` identifies the Group, application generation, applied `(index, term)` and `Startup` or `PeerInstall` origin. The consumer must freeze proof inputs from the supplied image, rather than querying mutable live FSM state later. `recovery_validated` is a fallible, local, synchronous readiness preparation hook after proof succeeds and the generation/owner checks pass; it must not wait on external IO. The library rechecks generation and owner state after the hook. A rejecting hook must finish before a new recovery provider is published. Its marker must be invalidated by consumer restore when appropriate. Business reads, apply and capture remain gated until durable publication and bridge updates finish, even if this consumer marker has already been prepared.

For example, an archive consumer can capture only its bounded archive watermark and proof digest. In its existing `StateMachine` implementation:

```rust,ignore
fn requires_recovery_validation(&self) -> bool { true }

fn recovery_validation(
    &self,
    context: multiraft_fsm::ValidationContext,
) -> Option<multiraft_fsm::ValidationFuture> {
    // Copy a small immutable proof request, not the complete archive.
    let request = self.archive_proof_request(context);
    let archive = self.archive.clone();
    Some(Box::pin(async move {
        // This request is cancel-safe and owns all remote resources.
        archive.verify(request).await
    }))
}

fn recovery_validated(
    &mut self,
    context: multiraft_fsm::ValidationContext,
) -> Result<(), Self::Error> {
    self.ready_generation = Some(context.generation);
    Ok(())
}
```

`StateMachineFactory::validation_timeout(context)` sets a positive relative deadline, default 30 seconds. Groups in a node share one external validator admission slot. Consumers can supply a shared `Arc<Semaphore>` through `StateMachineFactory::validation_budget()`; each call should return the same semaphore. The deadline includes waiting for that slot and executing the future. A direct `StateMachineStore` consumer configures `ValidationOptions { deadline, budget: Arc<Semaphore> }` before cloning/registering it. The byte ceiling and existing snapshot capture/send/receive admission remain in effect; validation does not enlarge the 64 MiB maximum.

## Startup and peer installation

Owned node startup calls `begin_recovery_validation`, loads an existing active provider and replays the committed suffix. Loading a matching existing active image does not activate a new peer candidate or run peer proof. `validate_recovery` then freezes the actual resulting generation, executes external proof, verifies generation and owner state, runs the existing local `StateMachineFactory::validate_recovered` check, rechecks generation and owner state after that synchronous callback, runs the fallible readiness hook, rechecks generation and owner state again, and opens the application gate before admitting the Group.

An opted-in low-level `MultiRaft` consumer first awaits `wait_for_recovery`, then calls `validate_recovered(group)` before business access. Proposal methods gate business dispatch until the application is ready, while native committed suffix replay remains admitted.

Direct native store consumers first run `SnapshotCatalog::startup_provenance(group, cap)` before constructing/loading recovery authority, to bound orphan cleanup and preserve corrupt namespace diagnostics. They must call `begin_recovery_validation` before recovery load/replay and `validate_recovery` after committed replay. Native replay is allowed during recovery; application reads and new capture are gated. `try_with_fsm` returns an error for an unvalidated generation. The legacy `with_fsm` convenience method expects readiness and panics if the caller violates that precondition.

A running native peer installation serializes against apply and another install. It stages durable bytes, restores the candidate while application reads and capture are gated, freezes its proof inputs, awaits proof without holding the FSM lock, and checks generation/close state before activation. It runs the fallible readiness hook and rechecks generation/close state while the candidate remains gated, then durably activates the provider, updates applied/membership and opens the application gate before returning success. Legacy installation uses the same hook-before-catalog-write order. Waiting application input cannot alter the candidate or reuse its proof.

During opted-in startup recovery, owned peer ingress refuses a new snapshot before native Core/SM dispatch: gRPC returns `Status::unavailable`, and the in-process router returns a transport `Unreachable` refusal. The peer can retry after startup validation succeeds. Refusal completes promptly and releases receive/send admission; it does not wait behind the startup proof, restore candidate bytes, change application generation/applied state or publish a provider. It covers both `RECOVERING` before proof capture and `VALIDATING` during proof. The original active snapshot and committed suffix can finish validation. Loading the matching existing native active image through the internal startup path remains admitted; it is not new provider publication. Default consumers do not enter this pending startup state.

The lower state-machine install guard is a fail-closed bypass check, not a native retry protocol. It returns `InvalidInput` for a new image during `RECOVERING` before mutation. Calling OpenRaft's raw install API bypasses the library ingress; the exact-pinned native worker converts every SM install error into fatal `StorageError`, including `WouldBlock`. Consumers using raw native handles must provide equivalent ingress isolation and must not interpret an SM error as retryable native deferral.

## Failure, cancellation and resource ownership

Refusal, timeout, panic, caller cancellation or owner close cannot publish an unverified candidate as ready. A failed transition fences application access until destruction/restart. External proof or readiness-hook rejection leaves the previously active durable provider, applied/membership and diagnostic facts available. The library does not attempt to roll back a mutated application through a second untrusted restore. Restart begins from the existing active provider and committed log.

Catalog admission allows at most 16 incomplete writers and owned stage handles combined in one catalog ownership domain, including duplicate handles to the same generation. Clones and independently reopened catalogs for the same root share ownership and this limit. Startup scans at most 64 entries in a Group’s native namespace. It validates the entire namespace and an existing active authority before pruning fully validated, inactive generations with no owner. If authority is absent, it preserves all generation bytes for native coverage checks and exact basis repair; the namespace budget still applies. Unknown entries, partial generations and `.stage-*` directories are preserved and cause fail-closed recovery; they are not silently deleted as abandoned work. A failed scan preserves diagnostic bytes and performs no pruning.

Inactive staged generations have an owned cleanup guard. Dropping the last owner attempts durable candidate removal; activation protects the active generation and other handles protect their generation. `NativeSnapshotStage::discard()` reports cleanup IO errors to direct callers. Drop logs errors and records cleanup debt, which fences new staging for that Group until a successful complete same-Group provenance scan with an existing valid active authority clears it. This prevents repeated cleanup failures from admitting unbounded candidates.

Validation permits live in the validation future and release on completion or cancellation. Native staging/activation blocking work owns its own permit even if its caller disappears. `close_native_intake` signals cancellation; `wait_native_quiescent` joins transitions and native blocking work. Owned runtime `begin_cleanup` immediately stops new admission and calls `cancel_pending_validation` for pending recovery or installation. Already admitted business requests on a ready generation can drain. Cleanup additionally joins the actual application destructor and releases the listener. Consumers must not bypass this lifecycle with `block_on` or detached tasks.

Existing FSMs default to `requires_recovery_validation == false`, no external future and an accepting readiness hook. Existing bounded snapshot, durable coverage, purge and `CompletedObserved` semantics remain unchanged. An external validation failure cannot manufacture completed installation or new purge coverage.

## Verification

`multiraft-store/tests/async_validation.rs` exercises default consumer admission bypass, proof success; read/capture/apply isolation; concurrent installs with distinct generations; rejection, deadline, caller abort and owner close; admission timeout; unchanged prior provider and repeated failure cleanup. Readiness-hook rejection covers both native and legacy catalogs, exact prior native authority/bytes, unchanged applied state and application fencing. The direct Store bypass rejection preserves generation, then validates the original snapshot plus committed suffix successfully; that direct Store check does not certify native deferral. The validator owns a real temporary file lease so cleanup is observed externally.

`multiraft-net/tests/async_recovery_validation.rs` uses the existing `legacy-native-alpha30` fixture, checks its manifest hashes and producer commit, and verifies that startup proof receives snapshot plus committed suffix exactly once. It covers proof-rejected, readiness-hook-rejected, timed-out and canceled starts and owned shutdown during dynamic Group validation. Two real-peer regressions cover both gRPC and in-process ingress. An owned NodeOwner startup receives repeated immediate refusals while proof is pending, then becomes readable and accepts the same peer candidate after startup; its peer proof gates read/applied/provider publication. A native Group explicitly held in `RECOVERING` stays running after repeated peer requests, completes snapshot+suffix validation, then installs the retried candidate. Removing ingress isolation makes that second case fail its native running-state assertion. Both cases observe actual application/listener cleanup.

These generic library tests do not certify any database transaction protocol; the consumer must separately verify its real external proof implementation.
