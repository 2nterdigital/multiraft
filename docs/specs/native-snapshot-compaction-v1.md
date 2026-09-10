# Native snapshot compaction v1 (implementation candidate)

This contract is pending exact-source qualification. The sole implementation
plan and task ledger is ech0's establish-durable-raft-log-compaction change.

The existing OpenRaft public builder/install/startup APIs own native execution.
A normal builder request may defer with None for known capture/admission
refusal. A forced request always returns a builder: it supplies a sufficient
validated checkpoint or fails closed if safe capture cannot be supplied.
Neither path fabricates an empty snapshot. Application budget refusal is typed
and distinct from storage IO/corruption. Defaults preserve existing FSM
implementations; durable mode refuses unsupported bounded capture.

Capture preparation is a separate in-flight phase and is not native submission.
A local Group request is rejected (disabled, unsupported storage, unknown Group,
busy, capture limit), submitted, completion-observed, or unconfirmed. Native
trigger submission is never completion. A completion observation requires a
validated durable checkpoint and the actual native purge/retention facts;
no-purge-needed remains distinct. There is no raw log-cutoff operation.

One explicit operation per Group and one heavy build per node are admitted.
Waiter cancellation does not release native work's admission. Maintenance
bookkeeping is bounded, disposable and not replayed after restart. Status is a
sample of native metrics, the existing snapshot catalog and file-log owner,
not a second authoritative registry or a health/recoverability promise.

NativeDurable requires Data-or-stronger file logs, manual policy, retain count
0..65536 (default 1024), and application bytes 1..67108864 (default ceiling).
Application limits may be smaller. Native metadata/framing allowance is at
most 1048576 bytes. Disabled never uses the legacy 5000-log snapshot policy.
Standby HTTP/catalog/daisy recovery remains contained.

Immutable generations bind Group identity, full SnapshotMeta and checked
application bytes. Stage and sync before atomic active-manifest publication;
serialize activation and reject checkpoint regression. A build is usable only
after activation; an install succeeds only after restore and activation.
Data/All file logs persist the existing committed frontier before returning
from save_committed, after ordered log IO. Recovery does not rely on another
append or a live peer to rediscover the committed suffix.
Startup loads the validated active generation through get_current_snapshot;
OpenRaft installs and replays committed suffixes. Corrupt active state rejects
without falling back to an unproven older generation. Purged prefix P must be
covered by durable checkpoint S, with P <= S and a continuous required suffix.

G1 and G2, their exact source/lock identities, crash cuts, five repetitions per
positive recovery shape and direct target oracles are required before claiming
admission. Process-crash evidence is not power-loss certification.

## Module split plan

The touched facade starts at 1907 lines. Keep new maintenance/configuration
logic in cohesive helpers and extract existing snapshot runtime ownership from
multiraft.rs before adding integration wiring; do not cross 2000 lines.
The file-log owner starts at 1084 lines. Catalog native-format/publication and
bridge capture/install helpers live below their existing owning modules;
behavior tests remain in crate tests directories.


## Frozen G1 runner

`scripts/run_native_compaction_g1.py` accepts the laboratory root, exact pushed
backend SHA and fresh Teleport-label receipt. It rejects another host/login,
non-data-disk roots, a dirty checkout or a source mismatch. Each named positive
shape runs five times. Raw output, timing, commands, direct RF3 oracles, source
identity and checksums remain under the run root. All failures are retained.
The ENOSPC case injects one failed staged-data write into its own child process;
it never fills the shared laboratory disk. G1-only Rust tests are ignored by
default and require explicit exact-case invocation on the dedicated host.
