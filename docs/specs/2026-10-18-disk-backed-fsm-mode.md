# Disk-backed state machine mode

**中文：** [2026-10-18-disk-backed-fsm-mode.zh-CN.md](./2026-10-18-disk-backed-fsm-mode.zh-CN.md)

Status: decisions frozen before implementation of ech0 #186 (step 1 of the
long-term capacity plan, parent ech0 #184). Streaming snapshots for this mode
(ech0 #190) are specified and implemented in
[2026-10-19-disk-fsm-streaming-snapshot.md](./2026-10-19-disk-fsm-streaming-snapshot.md);
the #190 notes below record the plan at freeze time.

## Goal

A state machine may keep its state in its own RocksDB database instead of
memory plus a whole-image `Vec<u8>` snapshot. Each applied entry commits its
business rows **and** the Raft applied position in one atomic `WriteBatch`.
On restart the bridge reads the applied position from that database; OpenRaft
then replays only the committed log suffix after it. No whole-image restore
is involved. The whole-image mode (`StateMachine::snapshot`/`restore`/
`freeze_bounded`, at most 64 MiB) is unchanged and remains the default for
small state machines.

## Frozen decision: database layout

**Each Slot (Consensus Group) on a node has its own FSM RocksDB instance,
separate from the node's shared Raft-log engine.**

| Data | Location | Engine |
|------|----------|--------|
| Raft log, vote, committed/purged (all Groups on the node) | `{data_dir}/raft-rocksdb/` | Existing shared engine (`LogStorageBackend::RocksDb`), unchanged |
| One Group's FSM state and applied position | `{data_dir}/fsm-rocksdb/group-{group_id}/` | One RocksDB instance per Group, owned by that Group's FSM |

Column families inside one Group's FSM instance:

| Column family | Owner | Contents |
|---------------|-------|----------|
| `multiraft_meta` | multiraft (reserved) | `schema` = `multiraft-disk-fsm-v1`; `group` = big-endian Group ID; `app_format` = application format tag; `applied` = the bridge's applied record |
| `default` and any other names | application | Business rows; the application declares the full set when opening |

The applied record is opaque to the application: a versioned bridge encoding
of the OpenRaft `LogId` and `StoredMembership` after that entry. The
application stores it verbatim in the same batch as the entry's rows.

Why not share the Raft-log engine, or use one FSM engine with a column family
per Slot:

- **Snapshots (#190).** A RocksDB checkpoint is per database. With one
  instance per Slot, a checkpoint is exactly one Slot's state and its applied
  record, consistent by construction, produced by hard links. A shared engine
  would contain every Slot (and, if shared with the log, every log), so the
  snapshot would have to be a key-range export, and install would have to
  splice ranges into a live shared database.
- **Install (#190).** Per-Slot instances allow "write to a temporary
  directory, verify, close, atomically replace the directory, reopen" without
  touching other Slots.
- **Write stalls.** RocksDB stalls writes for the whole database when one
  column family reaches a stall condition (memtable count, L0 file count,
  pending compaction bytes). If FSM data shared the log engine, one Slot's
  compaction debt would stall every Slot's Raft log appends and vote writes
  on that node. A shared FSM engine would stall every Slot's apply.
- **Ownership.** Dropping a Group's FSM closes its database; the existing
  FSM-release fence (shutdown waits for actual FSM destruction) already
  covers the resource.

Raft log atomicity does not extend to the FSM database, and does not need to:
OpenRaft treats the state machine's applied position as authoritative and
replays from it.

## API shape

In `multiraft-fsm` (no RocksDB dependency):

- `StateMachine::disk_state(&mut self) -> Option<&mut dyn DiskState>`. The
  default `None` keeps whole-image mode. A disk-backed FSM returns `Some`; the
  bridge records the mode once, at construction, for the FSM's lifetime.
- `DiskState::durable_applied(&self) -> io::Result<Option<Vec<u8>>>` returns
  the stored applied record (or `None` for a new database).
- `DiskState::apply_durable(&mut self, group, index, data: Option<&[u8]>,
  applied: &AppliedState) -> io::Result<ApplyOut>` is called exactly once
  per applied entry. `data` is `None` for blank, membership and internal
  entries. The implementation must make the entry's rows and
  `applied.record()` durable in **one** atomic write before returning. In
  disk mode the bridge never calls `apply`, `snapshot`, `restore` or the
  capture hooks.

In `multiraft-store`, `DiskFsmDb` is the RocksDB helper that implements the
layout above: it opens (or creates) the instance, checks the schema, Group
and application format markers (fail closed on any mismatch or unknown column
family), exposes the application column families for reads, and
`commit(batch, applied)` adds the applied record to the caller's batch and
writes it once. `DiskFsmDb::group_path(data_dir, group)` gives the canonical
location. The rocksdb crate is re-exported so consumers use the same pinned
version.

The bridge sends an entry's client response, and advances its in-memory
applied position and membership, only after `apply_durable` returns.

For #190 the plan is to add checkpoint/export and install hooks to
`DiskState`, with default implementations that refuse. Existing
implementations keep compiling, and streaming install can close, replace and
reopen the instance. ech0's composite FSM (#191) implements `DiskState` over
`DiskFsmDb` with its own column families.

## Relation to existing modes

- **`SnapshotMode` stays orthogonal.** Disk mode is declared by the FSM, not
  by cluster configuration. It is accepted with `Disabled` and
  `NativeDurable`, and rejected with `StandbyOffload` and with a Standby
  role. The Standby path hot-dumps whole images.
- **`NativeDurable` + disk mode.** The native snapshot catalog stays empty.
  Capture is refused as `Unsupported`, so manual compaction reports a refusal
  and the log is never purged. Whole-image build and install are refused
  explicitly. A catalog that already holds a whole-image snapshot fails group
  start (install is refused), because new formats start only in a new data
  space. #190 replaces the refusal with a streaming provider that reads
  this FSM instance.
- **`LogStorageBackend::RocksDb`.** This remains the separate, shared,
  synchronous-WAL log engine, and disk mode **requires** it (and a non-empty
  `data_dir`). Memory logs and FileLog at every sync level are rejected at group
  start. The pinned OpenRaft applies entries up to the *submitted* log IO, not
  the flushed IO (`engine_impl.rs`, `log_submitted.min(apply_accepted)`).
  RaftCore awaits `log_store.append` inline before it runs a later
  `SaveCommittedAndApply` (`core/raft_core.rs`). The RocksDB `append` returns
  only after its synchronous WAL write (`log_rocks/storage.rs`), so an entry is
  durable in the local log before its FSM write. FileLog's `append` can return
  before its coalesced group commit, which would let a crash leave the durable
  FSM ahead of the local log.

## Startup and failure rules

- FSM applied record present but the Group's Raft log pristine (no vote, no
  log, nothing purged): fail closed. The FSM directory does not belong to this
  log.
- Raft log persisted and FSM database empty: recoverable by replay from
  index 1 while nothing is purged. A purged log fails in OpenRaft's replay
  check (or in the refused install). Disk mode never purges until #190.
- FSM ahead of the local log tail: fail closed **before** OpenRaft runs.
  Otherwise OpenRaft purges the local log up to the applied position
  (irreversible), then finds no snapshot for the purged log and forces a
  snapshot build that this mode refuses, so the Group could never start again.
  With the RocksDB log, normal crashes cannot reach this state (see above), so
  it signals mismatched or damaged directories. Remedy: remove both the
  Group's FSM directory and that node's data, then rejoin. Log replay from
  the leader works, because this mode never purges. #190 can make the forced
  build succeed.
- Every write uses a synchronous WAL by default (`DiskFsmOptions::sync_writes`).
  Turning it off survives SIGKILL (the WAL is still written). After power
  loss the FSM may move back to an earlier applied position, which is safe
  only while the local log keeps every entry after the FSM's durable point.
  That is always true while disk mode cannot compact. #190 must flush the FSM
  before purging the log.

## Risks recorded

- **Cross-Slot write stalls.** Separate per-Slot instances contain a RocksDB
  write stall to one Slot's apply path. They still share disk bandwidth,
  the RocksDB default background thread pool and any shared block cache or
  write-buffer manager the application passes in, so sustained compaction
  debt in one Slot can slow other Slots' flushes and compactions and
  eventually stall them too. A stalled apply also delays that Slot's
  linearizable reads and leaves its in-flight proposals Unknown. The Raft-log
  engine is a separate database, so FSM stalls do not block other Slots'
  log appends, votes or heartbeats. However, a stall *in the shared log
  engine* (log compaction debt) still affects every Slot on the node.
  See [RocksDB Write Stalls](https://github.com/facebook/rocksdb/wiki/Write-Stalls).
- **Resources per Slot.** Each instance has its own WAL, memtables and file
  handles. Consumers should pass a shared `Cache`/`WriteBufferManager` through
  `DiskFsmOptions::db_options` and size `max_open_files` against the process
  file limit (macOS defaults are low).
- **Interrupted first creation.** A crash while RocksDB creates a new FSM
  instance (before `CURRENT`, or between column-family creations) leaves a
  directory that `open` rejects. No entry was applied yet. Remedy: delete that
  Group's FSM directory.
- **Mixed modes in one Group.** All replicas of a Group must run the same
  FSM mode in a new data space. A whole-image replica could compact and send
  a snapshot that a disk replica refuses (fatal on that follower). This is not
  checked at configuration time.
- **Apply latency.** Each entry's synchronous FSM write runs inside the
  bridge's async apply, under the FSM lock, on a Tokio worker. Many busy
  Slots can occupy workers and delay heartbeats. Batching or moving this
  write to a blocking thread is follow-up work.
- **No catch-up by snapshot.** Until #190, a follower whose required log
  entries are gone cannot catch up. Disk mode therefore never purges.
- **64 MiB caps.** The storage and network snapshot caps listed in ech0's
  long-term capacity spec §4 are not changed here. They apply only to
  whole-image snapshots, which disk mode never produces. Removing or
  bypassing them for disk mode is #190.

## Verification

- `multiraft-store` tests: `DiskFsmDb` markers and fail-closed opens; bridge
  recovery of applied position and membership without replaying persisted
  entries; refusal of whole-image build/install.
- `multiraft-net` test: three gRPC nodes with RocksDB logs and a disk FSM;
  restart and cold restart recover state and applied index from RocksDB.
- `multiraft-slot-demo --fsm-storage disk` with
  `scripts/slot_acceptance.py --case disk`: three OS processes, RF3,
  SIGKILL of the leader, then SIGKILL of all nodes and a cold restart; state
  and applied index agree on every node. These SIGKILLs hit an idle cluster;
  crashes during writes are covered by the ordering argument above, not by a
  test. The existing whole-image cases are unchanged.
