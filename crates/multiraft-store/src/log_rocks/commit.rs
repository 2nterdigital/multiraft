//! Node-level group-commit window for hot-path log writes (off by default).
//!
//! With a window, `write_metadata` writes without sync and its caller keeps the
//! group gate until one shared `flush_wal(true)` covers the write. Each group
//! therefore has at most one unsynced write, and its entries, metadata and
//! marker share that one WriteBatch: after power loss the group's last
//! unacknowledged write either fully exists or does not, which Raft treats like
//! a crash between write and acknowledgement. Groups share no keys, so a lost
//! write in one group next to a surviving later write in another is harmless.
//!
//! Release rule: a ticket is taken only after its `write_opt` returned, and a
//! flush releases only tickets up to the cut it read before starting. RocksDB
//! `FlushWAL(true)` syncs every WAL up to the current one, so the WAL record of
//! every ticket `<= cut` is covered. This needs RocksDB's default write path:
//! no manual WAL flush, pipelined, unordered or two-queue writes.
use std::io;
use std::sync::{Arc, Condvar, Mutex, OnceLock, PoisonError, Weak};
use std::time::{Duration, Instant};

use openraft::RaftTypeConfig;
use rocksdb::{WriteBatch, WriteOptions, DB};

use super::{db_error, lock, Engine};

/// How often an idle committer checks whether its engine was dropped.
const IDLE_CHECK: Duration = Duration::from_millis(100);
/// Largest accepted window: far above any useful value, and keeps
/// `pending_since + window` from overflowing `Instant`.
pub(super) const MAX_WINDOW: Duration = Duration::from_secs(1);

/// Commit state shared by windowed writers and the engine's committer thread.
pub(super) struct Commit {
    pub(super) window: Duration,
    pub(super) state: Mutex<CommitState>,
    /// The committer waits here; a writer `notify_one`s it on each new ticket.
    pub(super) pending: Condvar,
    /// Writers wait here; the committer `notify_all`s it after each flush outcome.
    pub(super) released: Condvar,
    /// Sticky engine failure, set under `state` and read lock-free by gate holders.
    fatal: OnceLock<String>,
    #[cfg(test)]
    pub(super) probe: Probe,
}

/// Tickets count `sync=false` writes that returned; `synced` is the highest released one.
#[derive(Default)]
pub(super) struct CommitState {
    pub(super) written: u64,
    pub(super) synced: u64,
    /// When the oldest ticket after the last cut was taken; the window runs from here.
    pub(super) pending_since: Option<Instant>,
}

impl Commit {
    pub(super) fn new(window: Duration) -> Self {
        Self {
            window,
            state: Mutex::default(),
            pending: Condvar::new(),
            released: Condvar::new(),
            fatal: OnceLock::new(),
            #[cfg(test)]
            probe: Probe::default(),
        }
    }

    fn fatal_error(&self) -> Option<io::Error> {
        self.fatal.get().map(|message| {
            io::Error::other(format!(
                "rocks log commit failed; restart the node: {message}"
            ))
        })
    }

    /// Make the engine fatal and wake every waiter. Under `state`, so a waiter
    /// cannot check the flag and then miss the wakeup; tolerates poisoning
    /// because it also runs while the committer unwinds.
    pub(super) fn fail(&self, message: String) {
        let _state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        // The first failure is the cause; a later exit message adds nothing.
        let _ = self.fatal.set(message);
        self.released.notify_all();
    }

    fn flush(&self, db: &DB) -> Result<(), String> {
        #[cfg(test)]
        self.probe.before_flush(self)?;
        db.flush_wal(true).map_err(|error| error.to_string())
    }
}

impl<C: RaftTypeConfig> Engine<C> {
    /// Refuse any gate holder or write once the committer failed: RocksDB may
    /// still serve unsynced data that callers were told failed.
    pub(super) fn check_fatal(&self) -> io::Result<()> {
        match self.commit.as_deref().and_then(Commit::fatal_error) {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// A hot-path write: synchronous without a window, else released by a flush.
    pub(super) fn write_hot(&self, batch: WriteBatch) -> io::Result<()> {
        let Some(commit) = self.commit.as_deref() else {
            return self.write(batch);
        };
        let started = Instant::now();
        let result = self.write_windowed(commit, batch);
        self.io_latency.sync_write.record(started.elapsed());
        result
    }

    fn write_windowed(&self, commit: &Commit, batch: WriteBatch) -> io::Result<()> {
        self.check_fatal()?;
        let mut options = WriteOptions::default();
        options.set_sync(false);
        self.db.write_opt(batch, &options).map_err(db_error)?;
        #[cfg(test)]
        commit.probe.wrote();
        // Only now may a cut include this write; see the release rule above.
        let mut state = lock(&commit.state)?;
        state.written += 1;
        let ticket = state.written;
        state.pending_since.get_or_insert_with(Instant::now);
        commit.pending.notify_one();
        while state.synced < ticket && commit.fatal.get().is_none() {
            state = commit
                .released
                .wait(state)
                .map_err(|_| io::Error::other("rocks log commit state poisoned"))?;
        }
        if state.synced >= ticket {
            return Ok(());
        }
        Err(commit
            .fatal_error()
            .unwrap_or_else(|| io::Error::other("rocks log commit failed")))
    }
}

/// Start the engine's committer. It holds only a `Weak` engine between flushes
/// so dropping the last handle still closes the DB on the dropping thread.
pub(super) fn spawn<C: RaftTypeConfig>(engine: &Arc<Engine<C>>) -> io::Result<()> {
    let Some(commit) = engine.commit.clone() else {
        return Ok(());
    };
    let engine = Arc::downgrade(engine);
    std::thread::Builder::new()
        .name("rocks-log-commit".to_owned())
        .spawn(move || {
            // Any exit, a panic included, must release waiters with an error.
            let _exit = ExitGuard(&commit);
            run(&engine, &commit);
        })?;
    Ok(())
}

struct ExitGuard<'a>(&'a Commit);

impl Drop for ExitGuard<'_> {
    fn drop(&mut self) {
        self.0.fail("committer thread exited".to_owned());
    }
}

fn run<C: RaftTypeConfig>(engine: &Weak<Engine<C>>, commit: &Commit) {
    loop {
        let Ok(mut state) = lock(&commit.state) else {
            return;
        };
        while state.written == state.synced {
            if engine.strong_count() == 0 {
                return;
            }
            state = match commit.pending.wait_timeout(state, IDLE_CHECK) {
                Ok((state, _)) => state,
                Err(_) => return,
            };
        }
        // The window runs from the oldest unreleased ticket, so writes that
        // arrived during the previous flush wait only what is left of it.
        let deadline = state.pending_since.unwrap_or_else(Instant::now) + commit.window;
        drop(state);
        std::thread::sleep(deadline.saturating_duration_since(Instant::now()));
        // RELEASE RULE: read the cut BEFORE flush_wal starts. Tickets taken
        // after this read wait for a later flush.
        let (cut, released) = match lock(&commit.state) {
            Ok(mut state) => {
                state.pending_since = None;
                (state.written, state.written - state.synced)
            }
            Err(_) => return,
        };
        let Some(engine) = engine.upgrade() else {
            return;
        };
        let started = Instant::now();
        let result = commit.flush(&engine.db);
        engine.io_latency.commit_flush.record(started.elapsed());
        if result.is_ok() {
            // Tickets per flush in 1 ms units; see `IoLatency::commit_batch`.
            engine
                .io_latency
                .commit_batch
                .record_ns(released.saturating_mul(1_000_000));
        }
        // The strong ref drops before any writer is released.
        drop(engine);
        match result {
            Ok(()) => {
                let Ok(mut state) = lock(&commit.state) else {
                    return;
                };
                state.synced = cut;
                commit.released.notify_all();
            }
            Err(message) => {
                commit.fail(format!("WAL flush failed: {message}"));
                return;
            }
        }
    }
}

/// Test hooks: a counter of returned `write_opt`s and a hook run before each flush.
#[cfg(test)]
#[derive(Default)]
pub(super) struct Probe {
    pub(super) writes_done: std::sync::atomic::AtomicU64,
    /// Delay between `write_opt` returning and `writes_done` counting it.
    pub(super) write_delay: Mutex<Duration>,
    #[allow(clippy::type_complexity)]
    pub(super) before_flush: Mutex<Option<Box<dyn Fn(&Commit) -> Result<(), String> + Send>>>,
}

#[cfg(test)]
impl Probe {
    fn wrote(&self) {
        let delay = *self.write_delay.lock().unwrap();
        if !delay.is_zero() {
            std::thread::sleep(delay);
        }
        self.writes_done
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn before_flush(&self, commit: &Commit) -> Result<(), String> {
        match self.before_flush.lock().unwrap().as_ref() {
            Some(hook) => hook(commit),
            None => Ok(()),
        }
    }
}
