# Streaming snapshots for disk-backed state machines

**中文：** [2026-10-19-disk-fsm-streaming-snapshot.zh-CN.md](./2026-10-19-disk-fsm-streaming-snapshot.zh-CN.md)

Status: implemented for ech0 #190 (step 1 of the long-term capacity plan,
parent ech0 #184), on top of the
[disk-backed FSM mode](./2026-10-18-disk-backed-fsm-mode.md) (#186).

## Goal

A disk-backed Group (one FSM RocksDB per Slot) can compact its Raft log, and a
replica whose required log entries are gone catches up by snapshot. Snapshot
size is unbounded (no 64 MiB cap); sender and receiver memory stay bounded by
one transfer chunk, independent of snapshot size. Any failure fails closed:
the receiver keeps its old state and never exposes a half-installed one.

## Design

### What OpenRaft sees

`TypeConfig::SnapshotData` stays `Cursor<Vec<u8>>` (whole-image mode is
unchanged). For a disk-backed Group those bytes are a **manifest**: a magic
prefix plus JSON with the Group, the `SnapshotMeta` (cut and membership), the
checkpoint's file list (name and size) and the sender-local checkpoint path.
Disk snapshot ids start with `disk-v1-`; a payload is treated as a manifest
only with both the prefix and the magic. The manifest is a few kilobytes, so
it passes every whole-image cap untouched; the checkpoint files never pass
through those paths.

### Producing a snapshot (compaction)

Requires `SnapshotMode::NativeDurable` (manual compaction, as for whole-image
native snapshots) and an FSM implementing the `DiskState` snapshot hooks.
`DiskFsmDb` implements all of them; an FSM that does not keeps the old refusal
(`UnsupportedCapture`) and never purges.

1. Under the bridge's FSM lock, `DiskState::checkpoint` writes a RocksDB
   checkpoint (WAL sync, memtable flush, hard links) into
   `{fsm}.snapshots/tmp-*`. The lock guarantees the checkpoint's applied record
   equals the bridge's applied position; the hold is bounded by one flush.
2. Off the lock, the builder fsyncs and lists the files, renames the directory
   to `checkpoint-{index}-{nonce}`, writes the manifest last
   (`…manifest`, atomic replace) and removes older checkpoints. A newer
   published checkpoint wins over an older capture (same rule as native).
3. OpenRaft then purges the log up to the cut. The live FSM is durable up to
   the cut (the checkpoint flushed it), which the 10-18 spec required before
   any purge.

The workspace `{data_dir}/fsm-rocksdb/group-{id}.snapshots/` (sibling of the
FSM directory, same filesystem) is separate from the whole-image native
catalog, which stays empty in disk mode.

### Transfer (gRPC only)

The sender (`GrpcRouter::full_snapshot`) recognizes a manifest and:

1. Leases the checkpoint: hard links into `checkpoint-….send-{nonce}`, so a
   concurrent compaction that removes the published directory cannot pull the
   files away mid-transfer.
2. Sends `Begin{transfer, manifest}` and then every file in order as
   `/raft/snapshot_chunk` calls of at most 1 MiB; the last chunk of each file
   carries that file's SHA-256 (computed while reading).
3. Submits the manifest as the OpenRaft snapshot on `/raft/snapshot`.

Each call gets the native install timeout as its own deadline, so progress,
not total size, bounds the transfer. The node keeps its single owned outbound
snapshot slot; the owned job stops between chunks once OpenRaft dropped its
waiter. The in-process router refuses manifests ("disk snapshots require the
gRPC transport").

### Receiving and verification (transport ingress)

The receiver stages one transfer per Group in `staging-{nonce}`: chunks must
arrive in order, never exceed the manifest sizes, and each file's digest must
match before the next file starts. A new `Begin` replaces an unfinished
transfer. Before `/raft/snapshot` is dispatched to OpenRaft, the server checks:

- **digest**: every file arrived complete with a matching SHA-256;
- **layout**: `DiskState::inspect_checkpoint` opens the staged checkpoint
  read-only and requires this store's column families, schema, Group and
  application format markers;
- **applied index / epoch**: the checkpoint's own applied record equals the
  snapshot metadata's last log id (index **and** term) and membership; the
  manifest's metadata equals the RPC metadata.

Any failure here is a transport refusal (`FAILED_PRECONDITION`); OpenRaft
retries later. It never reaches `install_snapshot`, where a storage error would
stop the Group.

### Install (atomic swap)

`install_snapshot` requires the staged transfer verified at ingress for exactly
this manifest, refuses to regress the applied position, gates reads
(validation `begin`), then:

1. Writes the `installed` marker (the cut's log id), durable.
2. `DiskState::install_checkpoint`: `DiskFsmDb` closes the live database and
   swaps directories with two renames through the workspace
   (`live → retired-N`, `staging → live`), syncing both parents after each,
   then reopens. Any error renames the old directory back and reopens it.
3. Reloads the applied position from the swapped-in store and checks it
   against the metadata, runs the recovery-validation hooks, publishes a local
   checkpoint at the installed cut (the receiver's current snapshot), and
   reopens reads.

Crash recovery happens in `DiskFsmDb::open`, before RocksDB opens: live
missing and `retired-*` present means the crash hit between the renames, so
the retired (old) store is renamed back; live present means the swap finished
and `retired-*` is garbage. Workspace leftovers (`tmp-*`, `staging-*`,
leases, incomplete checkpoints) are removed when the bridge opens the
workspace.

A crash after the swap but before OpenRaft purged its log leaves the FSM ahead
of the local log tail. The startup basis check accepts that only when the
`installed` marker names exactly the FSM's applied position, publishes a
checkpoint there if none is newer, and lets OpenRaft purge up to it (the
installed state is committed). Every other "FSM ahead of log" case still fails
closed.

## Caps

Whole-image mode keeps every 64 MiB cap unchanged. Disk snapshots never route
bulk bytes through them:

| Cap site (ech0 long-term spec §4) | Disk mode |
|---|---|
| `multiraft-core` `ClusterConfig::max_snapshot_bytes` (1..=64 MiB) | Applies to the manifest only (KBs) |
| `sm_bridge/native.rs` `max_snapshot_bytes` / `snapshot_byte_limit` | Manifest only; checkpoint size is never compared |
| `snapshot_catalog/native.rs` `BYTE_LIMIT` | Catalog unused in disk mode |
| `router.rs` (in-process) | Refuses manifests; disk mode is gRPC-only |
| `api.rs`, `grpc/server.rs`, `grpc/router.rs`, `snapshot_send.rs` | Manifest message only; files go as ≤1 MiB `/raft/snapshot_chunk` calls |

## Risks and limits

- Mixed modes in one Group remain unsupported (a whole-image leader cannot feed
  a disk follower and vice versa).
- The recovery-validation hooks run after the swap, so a validation refusal
  stops the Group with the new store already durable (it is committed state).
- A verified staging directory whose install OpenRaft skipped (cut already
  committed locally) stays on disk until the next transfer or restart.
- The sender's lease, the receiver's staging and the published checkpoint can
  each hold one snapshot's worth of disk space (hard links share SST data
  with the live store until compaction rewrites it).
- `install_checkpoint` holds the FSM lock for close, two renames and reopen.

## Verification

- `multiraft-store` unit tests (`disk_snapshot`): manifest validation, swap
  rollback/commit and crash recovery, workspace cleanup and retention, chunk
  round trip across a concurrent republish, digest mismatch.
- `multiraft-store` `tests/disk_snapshot.rs`: build and restart serving the
  manifest; streamed install replacing state and surviving a restart with the
  FSM ahead of the log; refusals (corrupted chunk, partial transfer, forged
  metadata, unverified install, foreign layout) leaving the old state; injected
  swap failures keeping the old store.
- `multiraft-net` `tests/disk_snapshot_catchup.rs`: three gRPC nodes; a replica
  down during writes and compaction catches up by streamed checkpoint, restarts,
  and an injected install failure leaves its old state until a clean retry.
- `scripts/slot_acceptance.py --case disk-snapshot` (three OS processes): see
  [slot acceptance](../slot-acceptance.md) for the scenario and recorded numbers.

## Recorded runs

`scripts/slot_acceptance.py --case disk-snapshot`, release build of
`multiraft-slot-demo`, three local OS processes on one macOS (APFS) machine,
2026-10-19. RSS sampled with `ps -o rss` every 50 ms on all three processes
from the receiver's start until it caught up. "Before" is the sender's RSS just
before the receiver started (memory left by the write phase: memtables, the
shared Raft-log RocksDB with default 64 MiB memtables, allocator caches).

| Data written to the Slot | Snapshot (checkpoint bytes, files) | Catch-up | Receiver RSS peak | Sender RSS before → peak | Third node peak |
|---|---|---|---|---|---|
| 1 GiB | 1,073,819,485 B, 24 files | 9.3 s | 60.0 MiB | 177.4 → 181.7 MiB | 167.0 MiB |
| 2 GiB | 2,147,601,268 B, 40 files | 48.1 s | 60.7 MiB | 176.8 → 178.8 MiB | 153.5 MiB |

Later rounds of the same runs streamed 1.34 GB / 1.41 GB (1 GiB run) and
2.68 GB / 2.75 GB (2 GiB run) snapshots. Fault rounds: SIGKILL of the receiver
after 72 MB (1 GiB run) / 71 MB (2 GiB run) were staged, reopened at the old
applied index (259 / 514) and then caught up; an abort between the swap renames
(exit -6, live directory missing, one `retired-*` present) reopened at the old
applied index (324 / 642) after rollback and then caught up. Peaks do not grow
with snapshot size: streaming adds a few MiB on the sender (one chunk plus
gRPC buffers); the receiver starts fresh and stays near 60 MiB.
