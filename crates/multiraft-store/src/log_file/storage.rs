//! Native storage operations and flush ownership.
use std::fmt::Debug;
use std::io;
use std::ops::RangeBounds;
use std::sync::Arc;

use multiraft_core::FileLogSyncLevel;
use openraft::alias::LogIdOf;
use openraft::alias::VoteOf;
use openraft::storage::IOFlushed;
use openraft::storage::RaftLogStorage;
use openraft::LogState;
use openraft::RaftLogReader;
use openraft::RaftTypeConfig;
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::flush_pipeline;
use super::FileLogStore;

impl<C> RaftLogReader<C> for FileLogStore<C>
where
    C: RaftTypeConfig,
    C::Entry: Clone + Serialize + DeserializeOwned,
    LogIdOf<C>: Serialize + DeserializeOwned,
    VoteOf<C>: Serialize + DeserializeOwned,
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, io::Error> {
        let mut inner = self.inner.lock().await;
        inner.try_get_log_entries(range).await
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<C>>, io::Error> {
        let mut inner = self.inner.lock().await;
        inner.read_vote().await
    }
}

impl<C> RaftLogStorage<C> for FileLogStore<C>
where
    C: RaftTypeConfig,
    C::Entry: Clone + Serialize + DeserializeOwned,
    LogIdOf<C>: Serialize + DeserializeOwned,
    VoteOf<C>: Serialize + DeserializeOwned,
{
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<C>, io::Error> {
        let mut inner = self.inner.lock().await;
        inner.get_log_state().await
    }

    async fn save_committed(&mut self, committed: Option<LogIdOf<C>>) -> Result<(), io::Error> {
        let durable = !matches!(self.control.sync_level, FileLogSyncLevel::Os);
        let _io = if durable {
            Some(self.io_gate.lock().await)
        } else {
            None
        };
        let mut inner = self.inner.lock().await;
        if durable {
            inner.flush_pending()?;
        }
        inner.save_committed(committed).await
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<C>>, io::Error> {
        let mut inner = self.inner.lock().await;
        inner.read_committed().await
    }

    async fn save_vote(&mut self, vote: &VoteOf<C>) -> Result<(), io::Error> {
        let mut inner = self.inner.lock().await;
        inner.save_vote(vote).await
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<C>) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = C::Entry>,
    {
        let control = Arc::clone(&self.control);
        let inner = Arc::clone(&self.inner);
        let io_gate = Arc::clone(&self.io_gate);
        let mut guard = inner.lock().await;
        guard.append(entries, callback).await?;

        let pending_len = guard.pending_buf.len();
        let n_callbacks = guard.pending_callbacks.len();

        // Critical: when deferred flush is enabled, **never await durable IO on
        // this path**. openraft's command loop waits for `append()` to return;
        // awaiting fdatasync here caps outstanding log IO at ~1 and kills
        // group-commit. Caps still wake the flusher immediately.
        if control.defer_enabled() {
            let at_cap = control.hit_force_flush_cap(pending_len, n_callbacks);
            drop(guard);
            control.ensure_flusher(Arc::clone(&inner), Arc::clone(&io_gate));
            // `notify_one` stores a permit if the flusher has not parked yet
            // (`notify_waiters` would be lost). hold_overlap uses timer-only
            // wakes except at size/count caps.
            if at_cap || !control.stream.hold_overlap {
                control.stream_notify.notify_one();
                control.coalesce_notify.notify_one();
            }
            return Ok(());
        }

        drop(guard);
        flush_pipeline(&inner, &io_gate).await
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogIdOf<C>>) -> Result<(), io::Error> {
        let mut inner = self.inner.lock().await;
        inner.truncate_after(last_log_id).await
    }

    async fn purge(&mut self, log_id: LogIdOf<C>) -> Result<(), io::Error> {
        let diagnostic = {
            let mut inner = self.inner.lock().await;
            inner.purge(log_id).await?
        };
        tracing::info!(
            target: "multiraft::recovery",
            operation = "file_log_purge",
            phase = "complete",
            directory = %diagnostic.directory.display(),
            sync_level = ?diagnostic.sync_level,
            crash_durable = diagnostic.sync_level != FileLogSyncLevel::Os,
            previous_last_purged_index = ?diagnostic.previous_last_purged_index,
            requested_purge_index = diagnostic.requested_purge_index,
            removed_entries = diagnostic.removed_entries,
            remaining_entries = diagnostic.remaining_entries,
            first_retained_index = ?diagnostic.first_retained_index,
            last_retained_index = ?diagnostic.last_retained_index,
            "completed Raft log prefix purge"
        );
        Ok(())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}
