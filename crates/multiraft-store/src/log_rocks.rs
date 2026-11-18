//! Shared RocksDB Raft log engine, partitioned by stable group identity.
//!
//! Every mutation uses an atomic WriteBatch with synchronous WAL durability.
//! Retained bytes count encoded entry values only, not SST/WAL physical space.
use std::collections::HashMap;
use std::io;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use multiraft_core::StartupProvenance;
use multiraft_fsm::LatencyHistogram;
use openraft::alias::{LogIdOf, VoteOf};
use openraft::entry::RaftEntry;
use openraft::RaftTypeConfig;
use rocksdb::{
    BlockBasedOptions, Cache, Direction, IteratorMode, Options, WriteBatch, WriteBufferManager,
    WriteOptions, DB,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

mod adoption;
mod lifecycle;
mod storage;
pub use lifecycle::{ControlledGroupReceipt, ControlledGroupStage};

const SCHEMA_KEY: &[u8] = b"schema";
const SCHEMA: &[u8] = b"multiraft-rocks-log-v1";
const CONTROLLED_SCHEMA: &[u8] = b"multiraft-rocks-log-v2-controlled";
/// Node-shared Raft log block cache; a soft budget, not a process cap.
const RAFT_LOG_CACHE_BYTES: usize = 32 * 1024 * 1024;
/// Node-shared Raft log memtable budget (`allow_stall = false`).
const RAFT_LOG_WRITE_BUFFER_MANAGER_BYTES: usize = 64 * 1024 * 1024;

struct Engine {
    db: DB,
    gates: Mutex<HashMap<u64, Arc<Mutex<()>>>>,
    cache: Cache,
    write_buffer_manager: WriteBufferManager,
    db_id: String,
    io_latency: IoLatency,
}

/// Cumulative per-engine I/O stage latencies; diagnostics only.
#[derive(Default)]
struct IoLatency {
    /// `append` entry until just before `io_completed`: how long RaftCore waits.
    append_total: LatencyHistogram,
    /// `append` entry until its blocking closure starts.
    append_queue: LatencyHistogram,
    /// Waiting for the per-group gate in `append` and `blocking`.
    gate_wait: LatencyHistogram,
    /// Every synchronous `Engine::write`.
    sync_write: LatencyHistogram,
    /// Whole `save_committed`, scheduling included.
    save_committed: LatencyHistogram,
}

/// One node's shared log engine. Clones and group handles keep the DB alive.
#[derive(Clone)]
pub struct RocksLogDb<C: RaftTypeConfig> {
    engine: Arc<Engine>,
    config: PhantomData<C>,
}

/// One group's OpenRaft log interface in a shared engine.
#[derive(Clone)]
pub struct RocksLogStore<C: RaftTypeConfig> {
    owner: RocksLogDb<C>,
    group: u64,
    gate: Arc<Mutex<()>>,
    generation: Option<u64>,
}

#[derive(Serialize, Deserialize)]
#[serde(bound = "", deny_unknown_fields)]
struct Metadata<C: RaftTypeConfig>
where
    LogIdOf<C>: Serialize + DeserializeOwned,
    VoteOf<C>: Serialize + DeserializeOwned,
{
    #[serde(default, skip_serializing_if = "Option::is_none")]
    controlled_generation: Option<u64>,
    ever_written: bool,
    vote: Option<VoteOf<C>>,
    committed: Option<LogIdOf<C>>,
    purged: Option<LogIdOf<C>>,
    tail: Option<LogIdOf<C>>,
    bytes: u64,
}

/// shortcut: Lab-only experiment knobs read from the environment; replace with
/// real config (or drop) once the rate-limit / WAL-recycle experiment decides.
/// `MULTIRAFT_RAFT_LOG_RATE_LIMIT_MIB`: flush/compaction write rate cap.
/// `MULTIRAFT_RAFT_LOG_RECYCLE_WAL`: number of WAL files to recycle.
fn experimental_tuning(options: &mut Options) {
    let read = |name| {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
    };
    if let Some(mib) = read("MULTIRAFT_RAFT_LOG_RATE_LIMIT_MIB") {
        options.set_ratelimiter((mib * 1024 * 1024) as i64, 100_000, 10);
    }
    if let Some(files) = read("MULTIRAFT_RAFT_LOG_RECYCLE_WAL") {
        options.set_recycle_log_file_num(files as usize);
    }
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}
fn db_error(error: rocksdb::Error) -> io::Error {
    io::Error::other(error)
}
fn lock<T>(mutex: &Mutex<T>) -> io::Result<MutexGuard<'_, T>> {
    mutex
        .lock()
        .map_err(|_| io::Error::other("rocks log gate poisoned"))
}
fn encode<T: Serialize>(value: &T) -> io::Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|e| invalid(e.to_string()))
}
fn decode<T: DeserializeOwned>(value: &[u8]) -> io::Result<T> {
    serde_json::from_slice(value).map_err(|e| invalid(e.to_string()))
}
fn prefix(group: u64) -> Vec<u8> {
    let mut key = vec![b'g'];
    key.extend_from_slice(&group.to_be_bytes());
    key
}
fn key(group: u64, tag: u8) -> Vec<u8> {
    let mut key = prefix(group);
    key.push(tag);
    key
}
fn entry_key(group: u64, index: u64) -> Vec<u8> {
    let mut key = key(group, b'e');
    key.extend_from_slice(&index.to_be_bytes());
    key
}
fn entry_index(group: u64, raw: &[u8]) -> io::Result<u64> {
    if raw.len() != 18 || !raw.starts_with(&key(group, b'e')) {
        return Err(invalid("invalid group entry key"));
    }
    Ok(u64::from_be_bytes(
        raw[10..].try_into().map_err(|_| invalid("entry key"))?,
    ))
}

impl Engine {
    fn write(&self, batch: WriteBatch) -> io::Result<()> {
        let mut options = WriteOptions::default();
        options.set_sync(true);
        let started = Instant::now();
        let result = self.db.write_opt(batch, &options).map_err(db_error);
        self.io_latency.sync_write.record(started.elapsed());
        result
    }
    fn gate(&self, group: u64) -> io::Result<Arc<Mutex<()>>> {
        Ok(Arc::clone(
            lock(&self.gates)?
                .entry(group)
                .or_insert_with(|| Arc::new(Mutex::new(()))),
        ))
    }
}

impl<C> RocksLogDb<C>
where
    C: RaftTypeConfig,
    C::Entry: Clone + Serialize + DeserializeOwned,
    LogIdOf<C>: Serialize + DeserializeOwned,
    VoteOf<C>: Serialize + DeserializeOwned,
{
    /// Open or create the engine. Unknown or missing schema on a nonempty DB is an error.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::open_with_sync_all(path, false)
    }

    /// Select fsync (`true`) instead of fdatasync for synchronous WAL writes.
    pub fn open_with_sync_all(path: impl AsRef<Path>, sync_all: bool) -> io::Result<Self> {
        let cache = Cache::new_lru_cache(RAFT_LOG_CACHE_BYTES);
        let write_buffer_manager = WriteBufferManager::new_write_buffer_manager(
            RAFT_LOG_WRITE_BUFFER_MANAGER_BYTES,
            false,
        );
        let mut table = BlockBasedOptions::default();
        table.set_block_cache(&cache);
        let mut options = Options::default();
        options.set_block_based_table_factory(&table);
        options.set_write_buffer_manager(&write_buffer_manager);
        options.set_write_buffer_size(16 * 1024 * 1024);
        options.set_max_write_buffer_number(2);
        options.set_max_open_files(64);
        // Write compaction output back in 1 MiB steps: on ext4 (data=ordered)
        // a ~100 MiB burst of dirty SST pages otherwise lands in the journal
        // commit that every synchronous WAL append waits for.
        options.set_bytes_per_sync(1024 * 1024);
        experimental_tuning(&mut options);
        options.create_if_missing(true);
        options.set_use_fsync(sync_all);
        let db = DB::open(&options, path.as_ref()).map_err(db_error)?;
        match db.get(SCHEMA_KEY).map_err(db_error)? {
            Some(value) if value == SCHEMA || value == CONTROLLED_SCHEMA => {}
            Some(_) => return Err(invalid("unknown rocks log schema")),
            None => {
                if let Some(row) = db.iterator(IteratorMode::Start).next() {
                    row.map_err(db_error)?;
                    return Err(invalid("missing rocks log schema in nonempty engine"));
                }
                let mut batch = WriteBatch::default();
                batch.put(SCHEMA_KEY, SCHEMA);
                let mut write_options = WriteOptions::default();
                write_options.set_sync(true);
                db.write_opt(batch, &write_options).map_err(db_error)?;
            }
        }
        Ok(Self {
            engine: Arc::new(Engine {
                db,
                gates: Mutex::new(HashMap::new()),
                cache,
                write_buffer_manager,
                db_id: std::fs::canonicalize(path.as_ref())?.display().to_string(),
                io_latency: IoLatency::default(),
            }),
            config: PhantomData,
        })
    }

    /// Sample this shared engine once per database identity, across all group clones.
    pub fn storage_memory_stats(&self) -> io::Result<multiraft_fsm::StorageMemoryStats> {
        let engine = &self.engine;
        let mut stats = crate::storage_memory::collect(
            &engine.db,
            engine.db_id.clone(),
            &["default".to_owned()],
        );
        // Read back RocksDB's effective capacity rather than restating the constructor.
        let cache_capacity_bytes = match engine
            .db
            .property_int_value("rocksdb.block-cache-capacity")
        {
            Ok(Some(capacity)) => capacity,
            other => {
                stats.errors.push(format!(
                    "rocksdb.block-cache-capacity unavailable ({other:?}); reporting configured value"
                ));
                RAFT_LOG_CACHE_BYTES as u64
            }
        };
        stats.shared = Some(multiraft_fsm::SharedStorageMemoryStats {
            cache_id: format!("{}:cache", engine.db_id),
            cache_capacity_bytes,
            cache_usage_bytes: engine.cache.get_usage() as u64,
            cache_pinned_bytes: engine.cache.get_pinned_usage() as u64,
            write_buffer_manager_id: format!("{}:wbm", engine.db_id),
            write_buffer_manager_limit_bytes: engine.write_buffer_manager.get_buffer_size() as u64,
            write_buffer_manager_usage_bytes: engine.write_buffer_manager.get_usage() as u64,
        });
        let latency = &engine.io_latency;
        stats.io_latency = [
            ("log_append_total", &latency.append_total),
            ("log_append_queue", &latency.append_queue),
            ("log_gate_wait", &latency.gate_wait),
            ("log_sync_write", &latency.sync_write),
            ("log_save_committed", &latency.save_committed),
        ]
        .into_iter()
        .map(|(name, histogram)| (name, histogram.snapshot()))
        .collect();
        Ok(stats)
    }

    /// Open a stable group namespace, validating its persisted recovery basis.
    pub fn group(&self, group: u64) -> io::Result<RocksLogStore<C>> {
        let store = RocksLogStore {
            owner: self.clone(),
            group,
            generation: None,
            gate: self.engine.gate(group)?,
        };
        {
            let _guard = lock(&store.gate)?;
            store.check_generation()?;
            if store.metadata()?.is_none() {
                let metadata = Metadata::<C> {
                    controlled_generation: None,
                    ever_written: false,
                    vote: None,
                    committed: None,
                    purged: None,
                    tail: None,
                    bytes: 0,
                };
                let mut batch = WriteBatch::default();
                store.put_metadata(&mut batch, &metadata)?;
                self.engine.write(batch)?;
            }
            store.validate()?;
        }
        Ok(store)
    }

    /// Read provenance without creating the group's namespace.
    pub fn startup_provenance(&self, group: u64) -> io::Result<StartupProvenance> {
        let store = RocksLogStore {
            owner: self.clone(),
            group,
            generation: None,
            gate: self.engine.gate(group)?,
        };
        let _guard = lock(&store.gate)?;
        store.check_generation()?;
        match store.validate()? {
            Some(meta) if meta.ever_written => Ok(StartupProvenance::Persisted),
            _ => Ok(StartupProvenance::Pristine),
        }
    }

    /// Encoded retained entry-value bytes for this group, excluding keys and metadata.
    /// This logical count is independent of compaction, SST files, and WAL retention.
    pub fn retained_log_bytes(&self, group: u64) -> io::Result<u64> {
        let store = RocksLogStore {
            owner: self.clone(),
            group,
            generation: None,
            gate: self.engine.gate(group)?,
        };
        let _guard = lock(&store.gate)?;
        store.check_generation()?;
        Ok(store.metadata()?.map_or(0, |meta| meta.bytes))
    }
}

impl<C> RocksLogStore<C>
where
    C: RaftTypeConfig,
    C::Entry: Clone + Serialize + DeserializeOwned,
    LogIdOf<C>: Serialize + DeserializeOwned,
    VoteOf<C>: Serialize + DeserializeOwned,
{
    fn metadata(&self) -> io::Result<Option<Metadata<C>>> {
        let raw = self
            .owner
            .engine
            .db
            .get(key(self.group, b'm'))
            .map_err(db_error)?;
        if let Some(raw) = raw {
            // Option fields otherwise silently default to None on missing JSON keys.
            let object: serde_json::Value = decode(&raw)?;
            for field in [
                "ever_written",
                "vote",
                "committed",
                "purged",
                "tail",
                "bytes",
            ] {
                if object.get(field).is_none() {
                    return Err(invalid(format!("missing group metadata field {field}")));
                }
            }
            let meta: Metadata<C> = decode(&raw)?;
            if meta.controlled_generation != self.generation {
                return Err(invalid("group metadata generation mismatch"));
            }
            return Ok(Some(meta));
        }
        let group_prefix = prefix(self.group);
        if let Some(row) = self
            .owner
            .engine
            .db
            .iterator(IteratorMode::From(&group_prefix, Direction::Forward))
            .next()
        {
            let (raw, _) = row.map_err(db_error)?;
            if raw.starts_with(&group_prefix) {
                return Err(invalid("missing group metadata marker"));
            }
        }
        Ok(None)
    }
    fn required_metadata(&self) -> io::Result<Metadata<C>> {
        self.metadata()?
            .ok_or_else(|| invalid("group metadata disappeared"))
    }
    fn put_metadata(&self, batch: &mut WriteBatch, meta: &Metadata<C>) -> io::Result<()> {
        batch.put(key(self.group, b'm'), encode(meta)?);
        batch.put(key(self.group, b'r'), SCHEMA);
        Ok(())
    }
    fn entry(&self, index: u64) -> io::Result<Option<C::Entry>> {
        self.owner
            .engine
            .db
            .get(entry_key(self.group, index))
            .map_err(db_error)?
            .map(|raw| {
                let entry: C::Entry = decode(&raw)?;
                if entry.log_id().index() != index {
                    return Err(invalid("entry key/log id mismatch"));
                }
                Ok(entry)
            })
            .transpose()
    }
    fn validate(&self) -> io::Result<Option<Metadata<C>>> {
        let Some(meta) = self.metadata()? else {
            return Ok(None);
        };
        if self
            .owner
            .engine
            .db
            .get(key(self.group, b'r'))
            .map_err(db_error)?
            .as_deref()
            != Some(SCHEMA)
        {
            return Err(invalid("missing or unknown group schema marker"));
        }
        let start = prefix(self.group);
        let mut expected = meta
            .purged
            .as_ref()
            .map_or(Some(0), |id| id.index().checked_add(1));
        let mut tail = None;
        let mut bytes = 0u64;
        for row in self
            .owner
            .engine
            .db
            .iterator(IteratorMode::From(&start, Direction::Forward))
        {
            let (raw, value) = row.map_err(db_error)?;
            if !raw.starts_with(&start) {
                break;
            }
            if raw.as_ref() == key(self.group, b'm') || raw.as_ref() == key(self.group, b'r') {
                continue;
            }
            let index = entry_index(self.group, &raw)?;
            if Some(index) != expected {
                return Err(invalid("gap in retained group log"));
            }
            let entry: C::Entry = decode(&value)?;
            if entry.log_id().index() != index {
                return Err(invalid("entry key/log id mismatch"));
            }
            if let Some(previous) = tail.as_ref() {
                if entry.log_id() <= *previous {
                    return Err(invalid("nonmonotonic group log identities"));
                }
            }
            tail = Some(entry.log_id());
            expected = index.checked_add(1);
            bytes = bytes
                .checked_add(value.len() as u64)
                .ok_or_else(|| invalid("entry byte count overflow"))?;
        }
        if tail != meta.tail || bytes != meta.bytes {
            return Err(invalid(
                "group log metadata does not match retained entries",
            ));
        }
        if !meta.ever_written
            && (meta.vote.is_some()
                || meta.committed.is_some()
                || meta.purged.is_some()
                || tail.is_some())
        {
            return Err(invalid("pristine group marker contradicts persisted state"));
        }
        if let Some(committed) = meta.committed.as_ref() {
            self.check_recoverable(&meta, committed)?;
        }
        Ok(Some(meta))
    }
    fn check_recoverable(&self, meta: &Metadata<C>, id: &LogIdOf<C>) -> io::Result<()> {
        if let Some(purged) = meta.purged.as_ref() {
            if id.index() < purged.index() {
                return Ok(());
            }
            if id == purged {
                return Ok(());
            }
            if id.index() == purged.index() {
                return Err(invalid("conflicting purged identity"));
            }
        }
        if self
            .entry(id.index())?
            .is_some_and(|entry| entry.log_id() == *id)
        {
            return Ok(());
        }
        Err(invalid("committed/truncate identity is not recoverable"))
    }
    /// Take the group gate, recording the wait.
    fn lock_gate(&self) -> io::Result<MutexGuard<'_, ()>> {
        let started = Instant::now();
        let guard = lock(&self.gate);
        self.owner
            .engine
            .io_latency
            .gate_wait
            .record(started.elapsed());
        guard
    }
    async fn blocking<T, F>(&self, operation: F) -> io::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(Self) -> io::Result<T> + Send + 'static,
    {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            let _guard = store.lock_gate()?;
            store.check_generation()?;
            operation(store.clone())
        })
        .await
        .map_err(io::Error::other)?
    }
}

#[cfg(test)]
mod memory_tests;
