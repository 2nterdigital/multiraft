# RocksDB Raft log backend

This backend stores MultiRaft's local consensus log and metadata in RocksDB.
It does not select the application's FSM storage or define a business schema.
The [Raft DB change](https://github.com/2nterdigital/ech0-delivery/issues/267) is limited to this log driver and its runtime wiring;
new snapshot formats, rebuild protocols, and receipt handling remain deferred.

## Selection and layout

Set a nonempty `ClusterConfig::data_dir` and explicitly select
`LogStorageBackend::RocksDb`. For example, after constructing the config:

```rust
config.data_dir = node_data_directory;
config.log_storage_backend = LogStorageBackend::RocksDb;
config.file_log_sync_level = FileLogSyncLevel::Data;
```

An empty `data_dir` selects memory logs even when `RocksDb` is requested.
`ClusterConfig::new` and `for_test` retain the `FileLog` default for compatibility.
An Ech0 consumer choosing RocksDB must make that selection explicitly.
See [configuration](../crates/multiraft-core/src/config.rs).

Each node opens one shared engine at `{data_dir}/raft-rocksdb/` and records
its selection in `{data_dir}/raft-log-backend` (`rocksdb-v1` plus a newline).
Groups use stable numeric Group IDs as namespaces in the default column family;
there is no separate RocksDB engine or column family per group.
Application FSM databases are independent and are not included in this engine.
See [engine](../crates/multiraft-store/src/log_rocks.rs) and
[node startup](../crates/multiraft-net/src/multiraft/recovery.rs).

The engine has a versioned schema marker. Group keys have a `g` prefix followed
by the big-endian Group ID; entry keys additionally carry an entry tag and index.
Group metadata records `ever_written`, `vote`, `committed`, `purged`, `tail`,
and `bytes`, with a separate group schema marker. These are implementation
records, not an application schema or a cross-database transaction contract.

## Durability and log operations

Every database mutation uses an atomic RocksDB `WriteBatch` with synchronous
WAL writes. The Raft vote, committed/purged basis, retained entries, tail, and
encoded-entry byte count are updated through this storage interface.
Append, suffix truncation, and prefix purge update their entries and metadata
in the same batch. This atomicity does not extend to an application's FSM DB.

For RocksDB, `FileLogSyncLevel::Os` and `Data` both use synchronous WAL writes
with fdatasync; `All` selects fsync. `Os` does not weaken RocksDB writes to
page-cache-only durability. These choices concern local persistence, while
Raft quorum remains the native protocol's responsibility.

Per-group gates serialize operations on cloned handles. Async storage methods
run blocking database work with `spawn_blocking`; append's `IOFlushed` callback
belongs to that blocking operation and reports completion after the write result,
including when the awaiting caller is cancelled.
See [OpenRaft storage operations](../crates/multiraft-store/src/log_rocks/storage.rs).

Reads intersect the requested range with the retained interval. Missing entries
inside that interval fail rather than returning a silently shortened result.
Startup validates contiguous entries, matching key/log identities, monotonic
identities, byte counts, required metadata, and schema markers. Appends reject
gaps, identity regressions, and conflicting replacements until truncation.
Truncation cannot discard the committed or purged recovery basis; purge supports
native snapshot installation advancing the purged marker beyond the log tail.

File-specific coalescing and streaming controls are ignored by RocksDB:
`file_log_coalesce_us`, `file_log_stream_buf_bytes`, `file_log_stream_flush_ms`,
and `file_log_hold_overlap`. The backend does not promise FileLog's timed group
commit behavior. Native API/network batching settings retain their own meaning.

## Existing data and shutdown

Use a fresh node data directory for a new RocksDB deployment. Selecting RocksDB
is not an automatic FileLog migration. Startup scans legacy group directories
and snapshot catalog roots before selecting the backend. Persisted FileLog or
snapshot history requires a persisted RocksDB log basis for the corresponding
group; otherwise startup rejects the selection and requires explicit migration
or recovery. Malformed recovery evidence also fails instead of becoming pristine.

Once the backend marker exists, FileLog selection is rejected. A marked RocksDB
root whose database `CURRENT` file is missing is rejected before recreation.
Unknown backend or schema markers are errors. Do not remove old data or markers
to bypass these guards; this change supplies no supported conversion procedure.
Group admission also checks legacy/snapshot history against RocksDB provenance.

Engine clones and group handles keep the database alive. Node shutdown joins
Raft and retained maintenance work, waits for FSM resource release, and releases
the node's engine owner even if the stopped facade is retained by the caller.
Standalone storage callers must drop their engine and group clones before
reopening the same database. See [shutdown](../crates/multiraft-net/src/multiraft/lifecycle.rs).

## Snapshots, observation, and verification

The snapshot catalog remains file-backed. Native snapshots still use the existing
`Vec` image/provider path; application snapshot creation and FSM restore behavior
are unchanged. This driver adds no receipt, permission, or business-state schema,
no trusted-FSM recovery acceptance, and no new capacity certification.

Local storage status reports `RocksDb` and `retained_log_bytes_kind =
"encoded_entries"`. Its byte count sums retained encoded entry values only;
it excludes keys, metadata, SST/WAL files, and compaction overhead. Prefix purge
reduces this logical count without promising immediate physical disk reclamation.
FileLog status uses `physical_file`; memory status is unavailable.
See [status sampling](../crates/multiraft-net/src/multiraft/maintenance/status.rs).

Focused tests cover shared-group reopen, mutation atomicity, recovery corruption,
restart/compaction observation, backend guards, and shutdown resource release.
Run the relevant checks from the checkout; on macOS supply Xcode's libclang:

```sh
export LIBCLANG_PATH=/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/lib
export DYLD_FALLBACK_LIBRARY_PATH="$LIBCLANG_PATH"
cargo test -p multiraft-core
cargo test -p multiraft-store --test log_rocks
cargo test -p multiraft-net --test owned_peer_restart
cargo test -p multiraft-net --test owned_rocksdb_guards
```

These commands describe verification scope, not a claim of a completed full
workspace check, production throughput, or power-loss qualification.
