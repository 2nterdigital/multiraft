# Election source delivery: library #199

2026-10-01. Ech0 [#199](https://github.com/2nterdigital/ech0-delivery/issues/199)
is paired with native [PR #1](https://github.com/2nterdigital/openraft/pull/1),
revision `ea46d0e571f7497f1f549e125254dfcb2ff45e2e`, based on exact alpha30
`19be0c27e5141d8acea3468cdb8a90875f117c27`. Library base is
`455094dfc7e4fd666631454906e9cf328f704559`. See the self-contained
[interface/coverage specification](../specs/election-source-facts.md).

## Delivered boundary

Optional construction-before source observer; bounded typed run/boot/node/Group,
sequence and operation/campaign identities; public native state points;
source-native cause/timer/request/consumed-grant/quorum/leader facts; typed
outbound RPC boundaries. The ingress and retained ring each have configured
capacity, native copies are bounded to 1024 voter/granter items. No payload or
credentials, Debug inference, shadow eligibility, extra drainer/runtime, native
algorithm/parameter/storage/default policy change, or laboratory execution.

A source receiver retains no Raft/FSM/listener. Accepted point jobs survive caller
cancellation and are joined. Closure seals/final-drains ingress after actual
native/resource release. Public point private timer and uncorrelated network
consumption stay explicit unknown. Sequences mean ring admission order; native
source time remains separate. Capabilities are available hooks, never a claim of
complete window coverage.

## Evidence and repaired source loss

Initial shared-ring `try_lock` dropped source facts in a real public HA test and
20-Group Server capture. Library public transfer-source verification also timed
out, with a fixture dropping its data root before asynchronous cleanup. Those
failures were retained; no production-election cause was inferred from them.

The repair removes the ring lock entirely from native callbacks: existing Tokio
bounded MPSC `try_send` admits owned metadata; emit/read/status/close drains and
projects at most capacity packets under the ordinary ring owner. No new task or
dependency. Arc-backed records move consumer cloning outside the lock. Full/closed
ingress is explicit loss; terminal `native_pending` must be zero. Fixture assertion
failures now await owned shutdown before resuming the original failure, and keep
any root whose release could not be confirmed.

Deterministic regression holds the ring lock on another thread while a native
callback completes without drop; deferred source time is preserved. A separate
full-queue case checks per-receiver loss and final drain. Independent generic
leased-provider/gRPC tests cover actual initialized/transfer/automatic campaign
origins, lease rejection, consumed grants/quorum/leadership, opaque data,
point/cancel/repeated/lag/drop/close and real socket/FSM reuse.

Public concurrent preferred RF3 startup covers 20 Groups and one opaque committed
write per Group. Final bounded fixture observed Node1/2/3 native callbacks
259/241/235, drop0 and all20 Started on each node; terminal verifies pending0 and
native_received equals archived native facts. This is a fixture-specific
correctness result, not a stress, production-frequency or physical-environment
claim. The paired Ech0 consumer has its own runtime/output evidence.

## Validation

- `cargo fmt --all -- --check`, `cargo check --workspace --locked` and
  `cargo clippy --workspace --all-targets --locked -- -D warnings`: pass.
- `cargo test --workspace --locked`: 341 passed, 0 failed, 11 explicitly ignored,
  across79 suites including Rustdoc. After strengthening the terminal completeness
  assertions, all5 source consumers passed again; no production code changed.
- One existing `scripts/acceptance.sh` execution on an isolated owned port/data
  block: `ACCEPTANCE OK`. All20 demo Groups progressed; peer links remained2;
  actual leader process loss retained committed counters and continued progress;
  original-disk restart reached every leader-linearizable floor; three prescribed
  additional tests passed. Observer remained disabled on this default path.
- Initial wrapper post-cleanup bind without SO_REUSEADDR reported address in use;
  the error remains in the log. No business/fault run was repeated. Actual task
  PIDs ended and no listener remained; all8 owned ports subsequently supported
  SO_REUSEADDR bind+listen. Temporary target symlink was removed. No shared service
  was touched. Binary SHA256:
  `60d0ccd5bcc6f2c0c1187b65404ac5b2504a431260c35b860fbada5ed4ff6d37`.
- Cargo version stays exact alpha30. Lock changes only the five native packages
  from registry to the same Git source; no transitive version changes.

Raw task receipts are in `.tmp/issue-199/`: `pre-repair-workspace.log`,
`final-workspace.log`, `queue-regression.log`, `final-source.log`,
`final-clippy.log`, `final-pinned-check.log`, `acceptance/driver.log` and
`acceptance/receipt.json`. These diagnostics stay task-local; this document
records the reviewable outcome and limits.

## Changed Rust file lengths

| File | Lines |
| --- | ---: |
| `crates/multiraft-net/src/election_source.rs` | 224 |
| `crates/multiraft-net/src/election_source/buffer.rs` | 454 |
| `crates/multiraft-net/src/election_source/native.rs` | 255 |
| `crates/multiraft-net/src/election_source/network.rs` | 209 |
| `crates/multiraft-net/src/election_source/state.rs` | 80 |
| `crates/multiraft-net/src/lib.rs` | 119 |
| `crates/multiraft-net/src/multiraft.rs` | 641 |
| `crates/multiraft-net/src/multiraft/election_source.rs` | 16 |
| `crates/multiraft-net/src/multiraft/group_start.rs` | 455 |
| `crates/multiraft-net/src/multiraft/lifecycle.rs` | 115 |
| `crates/multiraft-net/src/multiraft/startup.rs` | 105 |
| `crates/multiraft-net/src/multiraft/transport_start.rs` | 120 |
| `crates/multiraft-net/src/runtime.rs` | 180 |
| `crates/multiraft-net/src/runtime/election_source.rs` | 55 |
| `crates/multiraft-net/src/runtime/owner.rs` | 289 |
| `crates/multiraft-net/tests/election_source_consumer.rs` | 591 |

Observation delivery is not a completed repair of historical frequent natural
campaigns. Root-cause capture, physical budgets, concrete candidate selection and
paired production acceptance remain the integration specification's later stages.
