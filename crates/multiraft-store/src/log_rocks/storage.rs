//! Group-scoped OpenRaft operations. Blocking IO and the group gate live together.
use std::fmt::Debug;
use std::io;
use std::ops::{Bound, RangeBounds};

use openraft::alias::{LogIdOf, VoteOf};
use openraft::entry::RaftEntry;
use openraft::storage::{IOFlushed, RaftLogStorage};
use openraft::{LogState, RaftLogReader, RaftTypeConfig};
use rocksdb::{Direction, IteratorMode, WriteBatch};
use serde::{de::DeserializeOwned, Serialize};

use super::{db_error, decode, encode, entry_index, entry_key, invalid, key, RocksLogStore};

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
        self.blocking(move |store| {
            let meta = store.required_metadata()?;
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
            let start = entry_key(store.group, lower);
            let end = key(store.group, b'f');
            let mut expected = Some(lower);
            let mut entries = Vec::new();
            let mut previous = None;
            for row in store
                .owner
                .engine
                .db
                .iterator(IteratorMode::From(&start, Direction::Forward))
            {
                let (raw, value) = row.map_err(db_error)?;
                if raw.as_ref() >= end.as_slice() {
                    break;
                }
                let index = entry_index(store.group, &raw)?;
                if index > upper {
                    break;
                }
                if Some(index) != expected {
                    return Err(invalid("gap in requested log range"));
                }
                let entry: C::Entry = decode(&value)?;
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
        })
        .await
    }

    async fn read_vote(&mut self) -> io::Result<Option<VoteOf<C>>> {
        self.blocking(|store| Ok(store.required_metadata()?.vote))
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
        self.blocking(|store| {
            let meta = store.required_metadata()?;
            Ok(LogState {
                last_log_id: meta.tail.or_else(|| meta.purged.clone()),
                last_purged_log_id: meta.purged,
            })
        })
        .await
    }

    async fn save_vote(&mut self, vote: &VoteOf<C>) -> io::Result<()> {
        let vote = vote.clone();
        self.blocking(move |store| {
            let mut meta = store.required_metadata()?;
            meta.vote = Some(vote);
            meta.ever_written = true;
            let mut batch = WriteBatch::default();
            store.put_metadata(&mut batch, &meta)?;
            store.owner.engine.write(batch)
        })
        .await
    }

    async fn save_committed(&mut self, committed: Option<LogIdOf<C>>) -> io::Result<()> {
        self.blocking(move |store| {
            let mut meta = store.required_metadata()?;
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
            let mut batch = WriteBatch::default();
            store.put_metadata(&mut batch, &meta)?;
            store.owner.engine.write(batch)
        })
        .await
    }

    async fn read_committed(&mut self) -> io::Result<Option<LogIdOf<C>>> {
        self.blocking(|store| Ok(store.required_metadata()?.committed))
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
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            let result = (|| {
                let _guard = super::lock(&store.gate)?;
                store.append_entries(entries)
            })();
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
        self.blocking(move |store| {
            let mut meta = store.required_metadata()?;
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
            store.put_metadata(&mut batch, &meta)?;
            store.owner.engine.write(batch)
        })
        .await
    }

    async fn purge(&mut self, log_id: LogIdOf<C>) -> io::Result<()> {
        self.blocking(move |store| {
            let mut meta = store.required_metadata()?;
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
            store.put_metadata(&mut batch, &meta)?;
            store.owner.engine.write(batch)
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
    fn append_entries(&self, entries: Vec<C::Entry>) -> io::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut meta = self.required_metadata()?;
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
            let encoded = encode(&entry)?;
            if let Some(existing) = self
                .owner
                .engine
                .db
                .get(entry_key(self.group, index))
                .map_err(db_error)?
            {
                let existing_entry: C::Entry = decode(&existing)?;
                if existing_entry.log_id() != id || existing.as_slice() != encoded {
                    return Err(invalid(
                        "append conflicts with existing entry; truncate first",
                    ));
                }
                continue;
            }
            let basis = meta.tail.as_ref().or(meta.purged.as_ref());
            let next = basis.map_or(Some(0), |last| last.index().checked_add(1));
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
        self.put_metadata(&mut batch, &meta)?;
        self.owner.engine.write(batch)
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
