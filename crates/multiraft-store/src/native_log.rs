//! Backend dispatch for native runtime logs; each backend owns its IO contract.
use crate::{FileLogStoreOf, MemLogStore, RocksLogStore, TypeConfig};
use openraft::{
    alias::{LogIdOf, VoteOf},
    storage::{IOFlushed, RaftLogStorage},
    LogState, OptionalSend, RaftLogReader,
};
use std::{fmt::Debug, io, ops::RangeBounds};

/// Concrete native runtime log backend, including shared RocksDB group handles.
#[derive(Clone)]
pub enum NativeRaftLogStore {
    Memory(MemLogStore),
    File(FileLogStoreOf),
    Rocks(RocksLogStore<TypeConfig>),
}
impl RaftLogReader<TypeConfig> for NativeRaftLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> io::Result<Vec<<TypeConfig as openraft::RaftTypeConfig>::Entry>> {
        match self {
            Self::Memory(store) => store.try_get_log_entries(range).await,
            Self::File(store) => store.try_get_log_entries(range).await,
            Self::Rocks(store) => store.try_get_log_entries(range).await,
        }
    }
    async fn read_vote(&mut self) -> io::Result<Option<VoteOf<TypeConfig>>> {
        match self {
            Self::Memory(store) => store.read_vote().await,
            Self::File(store) => store.read_vote().await,
            Self::Rocks(store) => store.read_vote().await,
        }
    }
}
impl RaftLogStorage<TypeConfig> for NativeRaftLogStore {
    type LogReader = Self;
    async fn get_log_state(&mut self) -> io::Result<LogState<TypeConfig>> {
        match self {
            Self::Memory(store) => store.get_log_state().await,
            Self::File(store) => store.get_log_state().await,
            Self::Rocks(store) => store.get_log_state().await,
        }
    }
    async fn save_committed(&mut self, committed: Option<LogIdOf<TypeConfig>>) -> io::Result<()> {
        match self {
            Self::Memory(store) => store.save_committed(committed).await,
            Self::File(store) => store.save_committed(committed).await,
            Self::Rocks(store) => store.save_committed(committed).await,
        }
    }
    async fn read_committed(&mut self) -> io::Result<Option<LogIdOf<TypeConfig>>> {
        match self {
            Self::Memory(store) => store.read_committed().await,
            Self::File(store) => store.read_committed().await,
            Self::Rocks(store) => store.read_committed().await,
        }
    }
    async fn save_vote(&mut self, vote: &VoteOf<TypeConfig>) -> io::Result<()> {
        match self {
            Self::Memory(store) => store.save_vote(vote).await,
            Self::File(store) => store.save_vote(vote).await,
            Self::Rocks(store) => store.save_vote(vote).await,
        }
    }
    async fn append<I>(&mut self, entries: I, callback: IOFlushed<TypeConfig>) -> io::Result<()>
    where
        I: IntoIterator<Item = <TypeConfig as openraft::RaftTypeConfig>::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        match self {
            Self::Memory(store) => store.append(entries, callback).await,
            Self::File(store) => store.append(entries, callback).await,
            Self::Rocks(store) => store.append(entries, callback).await,
        }
    }
    async fn truncate_after(&mut self, last: Option<LogIdOf<TypeConfig>>) -> io::Result<()> {
        match self {
            Self::Memory(store) => store.truncate_after(last).await,
            Self::File(store) => store.truncate_after(last).await,
            Self::Rocks(store) => store.truncate_after(last).await,
        }
    }
    async fn purge(&mut self, id: LogIdOf<TypeConfig>) -> io::Result<()> {
        match self {
            Self::Memory(store) => store.purge(id).await,
            Self::File(store) => store.purge(id).await,
            Self::Rocks(store) => store.purge(id).await,
        }
    }
    async fn get_log_reader(&mut self) -> Self {
        self.clone()
    }
}
