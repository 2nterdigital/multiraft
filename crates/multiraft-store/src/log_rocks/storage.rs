//! Group-scoped OpenRaft operations. Blocking IO and the group gate live together.
use std::fmt::Debug;
use std::io;
use std::ops::{Bound, RangeBounds};
use std::time::Instant;

use openraft::alias::{LogIdOf, VoteOf};
use openraft::entry::RaftEntry;
use openraft::storage::{IOFlushed, RaftLogStorage};
use openraft::{LogState, RaftLogReader, RaftTypeConfig};
use rocksdb::{Direction, IteratorMode, WriteBatch};
use serde::{de::DeserializeOwned, Serialize};

use super::lifecycle::generation_active;
use super::{
    db_error, decode_entry, encode_entry, entry_index, entry_key, invalid, key, GroupCache,
    Metadata, RocksLogStore,
};

/// Ranges of at most this many entries are read with point lookups. Replication
/// and apply reads are usually a few entries, where creating, seeking and
/// dropping a RocksDB iterator costs more than the lookups themselves.
const POINT_READ_MAX_ENTRIES: u64 = 8;

impl<C> RaftLogReader<C> for RocksLogStore<C>
where
    C: RaftTypeConfig,
    C::Entry: Clone + Serialize + DeserializeOwned,
    LogIdOf<C>: Serialize + DeserializeOwned,
    VoteOf<C>: Serialize + DeserializeOwned,
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
        &mut self,
        range: RB,
    ) -> io::Result<Vec<C::Entry>> {
        let lower = match range.start_bound() {
            Bound::Included(index) => Some(*index),
            Bound::Excluded(index) => index.checked_add(1),
            Bound::Unbounded => Some(0),
        };
        let upper = match range.end_bound() {
            Bound::Included(index) => Some(*index),
            Bound::Excluded(index) => index.checked_sub(1),
            Bound::Unbounded => Some(u64::MAX),
        };
        self.blocking(move |store, cache| {
            let meta = store.cached_metadata(cache)?;
            let (Some(lower), Some(upper), Some(tail)) = (lower, upper, meta.tail) else {
                return Ok(vec![]);
            };
            let first = match meta.purged {
                Some(id) => match id.index().checked_add(1) {
                    Some(index) => index,
                    None => return Ok(vec![]),
                },
                None => 0,
            };
            let lower = lower.max(first);
            let upper = upper.min(tail.index());
            if lower > upper {
                return Ok(vec![]);
            }
            if upper - lower < POINT_READ_MAX_ENTRIES {
                store.read_points(lower, upper)
            } else {
                store.read_iterated(lower, upper)
            }
        })
        .await
    }

    async fn read_vote(&mut self) -> io::Result<Option<VoteOf<C>>> {
        self.blocking(|store, cache| Ok(store.cached_metadata(cache)?.vote))
            .await
    }
}

impl<C> RaftLogStorage<C> for RocksLogStore<C>
where
    C: RaftTypeConfig,
    C::Entry: Clone + Serialize + DeserializeOwned,
    LogIdOf<C>: Serialize + DeserializeOwned,
    VoteOf<C>: Serialize + DeserializeOwned,
{
    type LogReader = Self;

    async fn get_log_state(&mut self) -> io::Result<LogState<C>> {
        self.blocking(|store, cache| {
            let meta = store.cached_metadata(cache)?;
            Ok(LogState {
                last_log_id: meta.tail.or_else(|| meta.purged.clone()),
                last_purged_log_id: meta.purged,
            })
        })
        .await
    }

    async fn save_vote(&mut self, vote: &VoteOf<C>) -> io::Result<()> {
        let vote = vote.clone();
        self.blocking(move |store, cache| {
            let mut meta = store.cached_metadata(cache)?;
            meta.vote = Some(vote);
            meta.ever_written = true;
            store.write_metadata(cache, WriteBatch::default(), meta)
        })
        .await
    }

    async fn save_committed(&mut self, committed: Option<LogIdOf<C>>) -> io::Result<()> {
        let started = Instant::now();
        let result = self
            .blocking(move |store, cache| {
                let mut meta = store.cached_metadata(cache)?;
                if let Some(old) = meta.committed.as_ref() {
                    if committed.as_ref().is_none_or(|new| new < old) {
                        return Err(invalid("committed log cannot regress"));
                    }
                }
                if let Some(id) = committed.as_ref() {
                    store.check_recoverable(&meta, id)?;
                }
                meta.ever_written |= committed.is_some();
                meta.committed = committed;
                store.write_metadata(cache, WriteBatch::default(), meta)
            })
            .await;
        let latency = &self.owner.engine.io_latency;
        latency.save_committed.record(started.elapsed());
        result
    }

    async fn read_committed(&mut self) -> io::Result<Option<LogIdOf<C>>> {
        self.blocking(|store, cache| Ok(store.cached_metadata(cache)?.committed))
            .await
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<C>) -> io::Result<()>
    where
        I: IntoIterator<Item = C::Entry>,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        // The callback belongs to the blocking operation: cancellation of the async
        // caller must not abandon notification after a durable write finishes.
        // Returning only after the write is also what keeps a disk-backed FSM
        // behind the durable log (docs/specs/2026-10-18-disk-backed-fsm-mode.md).
        let entered = Instant::now();
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            let latency = &store.owner.engine.io_latency;
            latency.append_queue.record(entered.elapsed());
            let result = (|| {
                let mut cache = store.lock_gate()?;
                store.check_generation_cached(&mut cache)?;
                store.append_entries(&mut cache, entries)
            })();
            latency.append_total.record(entered.elapsed());
            callback.io_completed(
                result
                    .as_ref()
                    .map(|_| ())
                    .map_err(|error| io::Error::new(error.kind(), error.to_string())),
            );
            result
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogIdOf<C>>) -> io::Result<()> {
        self.blocking(move |store, cache| {
            let mut meta = store.cached_metadata(cache)?;
            for protected in [&meta.purged, &meta.committed].into_iter().flatten() {
                if last_log_id
                    .as_ref()
                    .is_none_or(|last| last.index() < protected.index())
                {
                    return Err(invalid("truncate would remove committed/purged basis"));
                }
            }
            if let Some(last) = last_log_id.as_ref() {
                store.check_recoverable(&meta, last)?;
            }
            let start = match last_log_id.as_ref() {
                Some(last) => match last.index().checked_add(1) {
                    Some(index) => index,
                    None => return Ok(()),
                },
                None => 0,
            };
            let removed = store.range_bytes(start, u64::MAX)?;
            meta.bytes = meta
                .bytes
                .checked_sub(removed)
                .ok_or_else(|| invalid("retained byte count underflow"))?;
            meta.tail = last_log_id.filter(|id| {
                meta.purged
                    .as_ref()
                    .is_none_or(|purged| id.index() > purged.index())
            });
            let mut batch = WriteBatch::default();
            batch.delete_range(entry_key(store.group, start), key(store.group, b'f'));
            store.write_metadata(cache, batch, meta)
        })
        .await
    }

    async fn purge(&mut self, log_id: LogIdOf<C>) -> io::Result<()> {
        self.blocking(move |store, cache| {
            let mut meta = store.cached_metadata(cache)?;
            if let Some(previous) = meta.purged.as_ref() {
                if log_id.index() < previous.index() {
                    return Err(invalid("purge marker cannot regress"));
                }
                if log_id.index() == previous.index() {
                    return if log_id == *previous {
                        Ok(())
                    } else {
                        Err(invalid("conflicting purge identity"))
                    };
                }
                if log_id <= *previous {
                    return Err(invalid("purge identity cannot regress"));
                }
            }
            if let Some(entry) = store.entry(log_id.index())? {
                if entry.log_id() != log_id {
                    return Err(invalid("purge identity differs from retained entry"));
                }
            } else if meta
                .tail
                .as_ref()
                .is_some_and(|tail| tail.index() >= log_id.index())
            {
                return Err(invalid("purge boundary entry missing"));
            }
            // Snapshot installation may advance the purged marker beyond the log tail.
            let removed = store.range_bytes(0, log_id.index())?;
            meta.bytes = meta
                .bytes
                .checked_sub(removed)
                .ok_or_else(|| invalid("retained byte count underflow"))?;
            meta.tail = meta.tail.filter(|tail| tail.index() > log_id.index());
            meta.purged = Some(log_id.clone());
            meta.ever_written = true;
            let mut batch = WriteBatch::default();
            let end = log_id.index().checked_add(1).map_or_else(
                || key(store.group, b'f'),
                |index| entry_key(store.group, index),
            );
            batch.delete_range(entry_key(store.group, 0), end);
            store.write_metadata(cache, batch, meta)
        })
        .await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

impl<C> RocksLogStore<C>
where
    C: RaftTypeConfig,
    C::Entry: Clone + Serialize + DeserializeOwned,
    LogIdOf<C>: Serialize + DeserializeOwned,
    VoteOf<C>: Serialize + DeserializeOwned,
{
    /// Check the handle's generation against the receipt, loading it once.
    pub(super) fn check_generation_cached(&self, cache: &mut GroupCache<C>) -> io::Result<()> {
        if cache.receipt.is_none() {
            cache.receipt = Some(self.owner.engine.receipt(self.group)?);
        }
        generation_active(
            self.generation,
            cache.receipt.as_ref().and_then(Option::as_ref),
        )
    }

    /// Decoded metadata from the group cache, loading it once from RocksDB.
    fn cached_metadata(&self, cache: &mut GroupCache<C>) -> io::Result<Metadata<C>> {
        if let Some(meta) = cache.metadata.as_ref() {
            // The cache is per group; each handle still fences its own generation.
            if meta.controlled_generation != self.generation {
                return Err(invalid("group metadata generation mismatch"));
            }
            return Ok(meta.clone());
        }
        let meta = self.required_metadata()?;
        cache.metadata = Some(meta.clone());
        Ok(meta)
    }

    /// Write `batch` with `meta`, caching `meta` only once the write succeeds.
    fn write_metadata(
        &self,
        cache: &mut GroupCache<C>,
        mut batch: WriteBatch,
        meta: Metadata<C>,
    ) -> io::Result<()> {
        self.put_metadata(&mut batch, &meta)?;
        match self.owner.engine.write(batch) {
            Ok(()) => {
                cache.metadata = Some(meta);
                Ok(())
            }
            Err(error) => {
                *cache = GroupCache::unloaded();
                Err(error)
            }
        }
    }

    fn append_entries(&self, cache: &mut GroupCache<C>, entries: Vec<C::Entry>) -> io::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut meta = self.cached_metadata(cache)?;
        let mut batch = WriteBatch::default();
        let mut previous = None;
        for entry in entries {
            let id = entry.log_id();
            let index = id.index();
            if previous.as_ref().is_some_and(|old: &LogIdOf<C>| {
                old.index().checked_add(1) != Some(index) || id <= *old
            }) {
                return Err(invalid("append entries are not contiguous and monotonic"));
            }
            previous = Some(id.clone());
            if meta
                .purged
                .as_ref()
                .is_some_and(|purged| index <= purged.index())
            {
                return Err(invalid("append overlaps purged prefix"));
            }
            let encoded = encode_entry(&entry)?;
            let basis = meta.tail.as_ref().or(meta.purged.as_ref());
            let next = basis.map_or(Some(0), |last| last.index().checked_add(1));
            // Nothing is stored past the tail: entries and tail share one WriteBatch,
            // and truncate/purge delete_range in the batch that moves the tail.
            if next != Some(index) {
                if let Some(existing) = self
                    .owner
                    .engine
                    .db
                    .get(entry_key(self.group, index))
                    .map_err(db_error)?
                {
                    let existing_entry: C::Entry = decode_entry(&existing)?;
                    // Re-encode: a legacy JSON value of the same entry must still match.
                    if existing_entry.log_id() != id || encode_entry(&existing_entry)? != encoded {
                        return Err(invalid(
                            "append conflicts with existing entry; truncate first",
                        ));
                    }
                    continue;
                }
            }
            if next != Some(index) || basis.is_some_and(|last| id <= *last) {
                return Err(invalid("append leaves a gap or regresses log identity"));
            }
            meta.bytes = meta
                .bytes
                .checked_add(encoded.len() as u64)
                .ok_or_else(|| invalid("retained byte count overflow"))?;
            batch.put(entry_key(self.group, index), encoded);
            meta.tail = Some(id);
        }
        meta.ever_written = true;
        self.write_metadata(cache, batch, meta)
    }

    /// Read `lower..=upper`, all inside the retained log, one lookup per entry.
    /// On intact data, same results and errors as [`Self::read_iterated`]. On
    /// corrupt data (a missing entry followed by an undecodable one, or a
    /// malformed key in the entry range) both fail, possibly with other messages.
    pub(super) fn read_points(&self, lower: u64, upper: u64) -> io::Result<Vec<C::Entry>> {
        let mut entries = Vec::new();
        let mut previous = None;
        for index in lower..=upper {
            let Some(entry) = self.entry(index)? else {
                // The iterator reports a gap if any later entry in range exists.
                for later in index.saturating_add(1)..=upper {
                    if self.entry(later)?.is_some() {
                        return Err(invalid("gap in requested log range"));
                    }
                }
                return Err(invalid("missing entry in requested log range"));
            };
            if previous.as_ref().is_some_and(|id| entry.log_id() <= *id) {
                return Err(invalid("nonmonotonic log identities"));
            }
            previous = Some(entry.log_id());
            entries.push(entry);
        }
        Ok(entries)
    }

    /// Read `lower..=upper`, all inside the retained log, with one iterator.
    pub(super) fn read_iterated(&self, lower: u64, upper: u64) -> io::Result<Vec<C::Entry>> {
        let start = entry_key(self.group, lower);
        let end = key(self.group, b'f');
        let mut expected = Some(lower);
        let mut entries = Vec::new();
        let mut previous = None;
        for row in self
            .owner
            .engine
            .db
            .iterator(IteratorMode::From(&start, Direction::Forward))
        {
            let (raw, value) = row.map_err(db_error)?;
            if raw.as_ref() >= end.as_slice() {
                break;
            }
            let index = entry_index(self.group, &raw)?;
            if index > upper {
                break;
            }
            if Some(index) != expected {
                return Err(invalid("gap in requested log range"));
            }
            let entry: C::Entry = decode_entry(&value)?;
            if entry.log_id().index() != index {
                return Err(invalid("entry key/log id mismatch"));
            }
            if previous.as_ref().is_some_and(|id| entry.log_id() <= *id) {
                return Err(invalid("nonmonotonic log identities"));
            }
            previous = Some(entry.log_id());
            entries.push(entry);
            expected = index.checked_add(1);
        }
        if expected != upper.checked_add(1) {
            return Err(invalid("missing entry in requested log range"));
        }
        Ok(entries)
    }

    fn range_bytes(&self, first: u64, last: u64) -> io::Result<u64> {
        let start = entry_key(self.group, first);
        let end = key(self.group, b'f');
        let mut bytes = 0u64;
        for row in self
            .owner
            .engine
            .db
            .iterator(IteratorMode::From(&start, Direction::Forward))
        {
            let (raw, value) = row.map_err(db_error)?;
            if raw.as_ref() >= end.as_slice() {
                break;
            }
            let index = entry_index(self.group, &raw)?;
            if index > last {
                break;
            }
            bytes = bytes
                .checked_add(value.len() as u64)
                .ok_or_else(|| invalid("retained byte count overflow"))?;
        }
        Ok(bytes)
    }
}
