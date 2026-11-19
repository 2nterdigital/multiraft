//! Process-wide RocksDB background threads.
//!
//! Every RocksDB instance of the process (the shared Raft log and each Group's
//! FSM store) runs its flushes in the default Env's high-priority pool. The
//! default `max_background_jobs = 2` sizes that pool to one thread, so a Raft
//! log flush stretched by its rate limiter (0.1-0.7 s on the ech0 Lab) held up
//! the forced memtable flush of an FSM checkpoint, taken under the Group's FSM
//! lock: lease reads and apply stopped and Slot admission filled up.
use std::sync::Once;

/// Flush threads of the default Env, shared by every RocksDB in the process.
const FLUSH_THREADS: i32 = 4;

/// Size the default Env's flush pool before the first database opens. Opening
/// a database only ever grows the pool (`IncBackgroundThreadsIfNeeded`), so it
/// stays at least [`FLUSH_THREADS`] afterwards.
///
/// shortcut: sets the pool to exactly [`FLUSH_THREADS`], which would shrink a
/// larger pool configured before the first open (nothing in multiraft or ech0
/// does that); read it first if an embedder ever does.
pub(crate) fn ensure_flush_threads() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| match rocksdb::Env::new() {
        Ok(mut env) => env.set_high_priority_background_threads(FLUSH_THREADS),
        Err(error) => tracing::warn!(%error, "cannot size the RocksDB flush pool"),
    });
}
