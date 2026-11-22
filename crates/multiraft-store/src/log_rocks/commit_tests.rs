//! Commit window: the release rule, shared flushes, and fatal failure handling.
use super::commit::{Commit, MAX_WINDOW};
use super::*;
use multiraft_core::typ::{Entry, LogId};
use multiraft_core::TypeConfig;
use openraft::alias::LeaderIdOf;
use openraft::storage::{RaftLogStorage, RaftLogStorageExt};
use openraft::vote::{RaftLeaderIdExt, RaftVote};
use openraft::RaftLogReader;
use std::sync::atomic::Ordering;
use std::sync::mpsc;

const GROUPS: u64 = 8;
const ROUNDS: u64 = 5;
/// Bound on any wait in these tests, so a regression fails instead of hanging.
const DEADLINE: Duration = Duration::from_secs(30);

fn id(term: u64, index: u64) -> LogId {
    LogId::new(LeaderIdOf::<TypeConfig>::new_committed(term, 1), index)
}
fn open(path: &Path, window: Duration) -> RocksLogDb<TypeConfig> {
    RocksLogDb::open_with_commit_window(path, false, 0, window).unwrap()
}
fn commit(db: &RocksLogDb<TypeConfig>) -> &Commit {
    db.engine.commit.as_deref().unwrap()
}
fn set_flush_hook(
    db: &RocksLogDb<TypeConfig>,
    hook: impl Fn(&Commit) -> Result<(), String> + Send + 'static,
) {
    *commit(db).probe.before_flush.lock().unwrap() = Some(Box::new(hook));
}
fn histogram(db: &RocksLogDb<TypeConfig>, name: &str) -> multiraft_fsm::LatencySnapshot {
    db.storage_memory_stats().unwrap().io_latency[name].clone()
}
/// Fail the test after releasing every waiter, so the runtime or thread
/// cleanup that follows cannot hang on them either.
fn release_and_panic(db: &RocksLogDb<TypeConfig>, what: &str) -> ! {
    commit(db).fail(format!("test deadline: {what}"));
    panic!("{what} did not finish within {DEADLINE:?}");
}
async fn bounded<T>(
    db: &RocksLogDb<TypeConfig>,
    what: &str,
    future: impl std::future::Future<Output = T>,
) -> T {
    match tokio::time::timeout(DEADLINE, future).await {
        Ok(value) => value,
        Err(_) => release_and_panic(db, what),
    }
}
/// `write_hot` on another thread, bounded by [`DEADLINE`].
fn write_within(db: &RocksLogDb<TypeConfig>, batch: WriteBatch) -> io::Result<()> {
    let engine = Arc::clone(&db.engine);
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || sender.send(engine.write_hot(batch)));
    receiver
        .recv_timeout(DEADLINE)
        .unwrap_or_else(|_| release_and_panic(db, "windowed write"))
}
fn put(key: &[u8]) -> WriteBatch {
    let mut batch = WriteBatch::default();
    batch.put(key, b"v");
    batch
}

/// The release rule against a flush that covers only writes returned before it
/// starts. At each flush start the hook records how many `write_opt`s had
/// returned, and checks the previous flush's cut (now `synced`) against the
/// count recorded at that flush's start. Taking the ticket before `write_opt`,
/// or reading the cut after the flush, releases writes past that count.
#[test]
fn a_flush_releases_only_writes_returned_before_it_started() {
    const THREADS: u8 = 16;
    const WRITES: u8 = 100;
    let root = tempfile::tempdir().unwrap();
    let db = open(root.path(), Duration::from_micros(200));
    // Widen the gap a wrong ticket order would leave between write_opt and the count.
    *commit(&db).probe.write_delay.lock().unwrap() = Duration::from_micros(50);
    let observed = Arc::new(Mutex::new((None::<u64>, Vec::<(u64, u64)>::new(), 0u64)));
    let record = Arc::clone(&observed);
    set_flush_hook(&db, move |commit| {
        let done = commit.probe.writes_done.load(Ordering::SeqCst);
        let previous_cut = lock(&commit.state).unwrap().synced;
        let mut observed = record.lock().unwrap();
        let (last_done, violations, flushes) = &mut *observed;
        if let Some(last_done) = *last_done {
            if previous_cut > last_done {
                violations.push((previous_cut, last_done));
            }
        }
        *last_done = Some(done);
        *flushes += 1;
        drop(observed);
        std::thread::sleep(Duration::from_millis(1));
        Ok(())
    });
    let (sender, receiver) = mpsc::channel();
    for thread in 0..THREADS {
        let engine = Arc::clone(&db.engine);
        let sender = sender.clone();
        std::thread::spawn(move || {
            let result = (0..WRITES).try_for_each(|round| {
                // Spread arrivals so tickets also land during flushes.
                let jitter = (u64::from(thread) * 7 + u64::from(round) * 13) % 20;
                std::thread::sleep(Duration::from_micros(jitter * 100));
                engine.write_hot(put(&[b'x', thread, round]))
            });
            sender.send(result)
        });
    }
    let deadline = Instant::now() + DEADLINE;
    for _ in 0..THREADS {
        match receiver.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
            Ok(result) => result.unwrap(),
            Err(_) => release_and_panic(&db, "release-rule writers"),
        }
    }
    let synced = lock(&commit(&db).state).unwrap().synced;
    let (last_done, violations, flushes) = observed.lock().unwrap().clone();
    let writes = u64::from(THREADS) * u64::from(WRITES);
    let first: Vec<_> = violations.iter().take(5).collect();
    assert!(
        violations.is_empty(),
        "{} of {flushes} flushes released past the returned writes (cut, returned): {first:?}",
        violations.len()
    );
    assert!(synced <= last_done.unwrap(), "{synced} > {last_done:?}");
    assert_eq!(synced, writes);
    assert!(flushes > 1 && flushes < writes, "{flushes} flushes");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_appends_share_flushes_and_survive_reopen() {
    let root = tempfile::tempdir().unwrap();
    {
        let db = open(root.path(), Duration::from_millis(2));
        let stores: Vec<_> = (0..GROUPS).map(|group| db.group(group).unwrap()).collect();
        let tasks: Vec<_> = stores
            .into_iter()
            .map(|mut store| {
                tokio::spawn(async move {
                    for index in 0..ROUNDS {
                        let entry = Entry::new_blank(id(1, index));
                        store.blocking_append(vec![entry]).await.unwrap();
                    }
                })
            })
            .collect();
        for task in tasks {
            bounded(&db, "windowed appends", task).await.unwrap();
        }
        let writes = GROUPS * ROUNDS;
        let flushes = histogram(&db, "log_commit_flush").count;
        assert!(flushes >= 1 && flushes < writes, "{flushes} flushes");
        // Every windowed write is released by exactly one flush.
        let batch = histogram(&db, "log_commit_batch");
        assert_eq!((batch.count, batch.sum_ns / 1_000_000), (flushes, writes));
        let state = lock(&commit(&db).state).unwrap();
        assert_eq!((state.written, state.synced), (writes, writes));
    }
    let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
    for group in 0..GROUPS {
        let mut store = db.group(group).unwrap();
        let entries = store.try_get_log_entries(0..ROUNDS).await.unwrap();
        assert_eq!(entries.len() as u64, ROUNDS, "group {group}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_flush_fails_waiters_and_every_later_operation() {
    let root = tempfile::tempdir().unwrap();
    let db = open(root.path(), Duration::from_millis(5));
    let mut stores: Vec<_> = (0..4).map(|group| db.group(group).unwrap()).collect();
    set_flush_hook(&db, |_| {
        std::thread::sleep(Duration::from_millis(20));
        Err("injected EIO".to_owned())
    });
    let tasks: Vec<_> = stores
        .iter()
        .cloned()
        .map(|mut store| {
            tokio::spawn(async move {
                store
                    .blocking_append(vec![Entry::new_blank(id(1, 0))])
                    .await
            })
        })
        .collect();
    for task in tasks {
        let error = bounded(&db, "failed-flush waiters", task)
            .await
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("restart the node"), "{error}");
    }
    // RocksDB itself is healthy; only the engine flag refuses these.
    assert!(db.engine.db.get(SCHEMA_KEY).unwrap().is_some());
    let store = &mut stores[0];
    assert!(store.read_vote().await.is_err());
    assert!(store.try_get_log_entries(0..1).await.is_err());
    assert!(store.get_log_state().await.is_err());
    let vote =
        VoteOf::<TypeConfig>::from_leader_id(LeaderIdOf::<TypeConfig>::new_committed(2, 1), true);
    assert!(bounded(&db, "save_vote", store.save_vote(&vote))
        .await
        .is_err());
    assert!(db.lifecycle_receipt(0).is_err());
    assert!(db.retained_log_bytes(1).is_err());
    assert!(db.group(9).is_err());
    assert!(db.engine.write(put(b"refused")).is_err());
    assert!(db.engine.db.get(b"refused").unwrap().is_none());
}

#[test]
fn a_committer_panic_releases_waiters_with_an_error() {
    let root = tempfile::tempdir().unwrap();
    let db = open(root.path(), Duration::from_millis(1));
    let store = db.group(0).unwrap();
    set_flush_hook(&db, |_| panic!("injected committer panic"));
    let (sender, receiver) = mpsc::channel();
    for thread in 0..3u8 {
        let engine = Arc::clone(&db.engine);
        let sender = sender.clone();
        std::thread::spawn(move || sender.send(engine.write_hot(put(&[b'p', thread]))));
    }
    for _ in 0..3 {
        let result = receiver
            .recv_timeout(DEADLINE)
            .expect("waiter hung after committer panic");
        assert!(result.is_err());
    }
    assert!(store.lock_gate().is_err());
    assert!(write_within(&db, put(b"later")).is_err());
}

#[test]
fn an_overdue_ticket_is_flushed_without_a_fresh_window() {
    let root = tempfile::tempdir().unwrap();
    let window = MAX_WINDOW;
    let db = open(root.path(), window);
    let commit = commit(&db);
    let started = Instant::now();
    let mut state = lock(&commit.state).unwrap();
    // A ticket whose window already expired: the committer must not sleep another window.
    state.written = 1;
    state.pending_since = Some(started - window);
    commit.pending.notify_one();
    while state.synced < 1 {
        let (next, timeout) = commit.released.wait_timeout(state, window / 2).unwrap();
        state = next;
        assert!(!timeout.timed_out(), "committer slept a fresh window");
    }
    assert!(started.elapsed() < window / 2);
}

/// A write wakes an idle committer instead of waiting out its idle check. The
/// minimum over a few writes absorbs slow flushes; without the wakeup every
/// write waits most of `IDLE_CHECK` (100 ms).
#[test]
fn a_write_wakes_an_idle_committer() {
    let root = tempfile::tempdir().unwrap();
    let db = open(root.path(), Duration::from_millis(1));
    let fastest = (0..5u8)
        .map(|round| {
            // Let the committer go back to its idle wait.
            std::thread::sleep(Duration::from_millis(5));
            let started = Instant::now();
            write_within(&db, put(&[b'w', round])).unwrap();
            started.elapsed()
        })
        .min()
        .unwrap();
    assert!(
        fastest < Duration::from_millis(50),
        "fastest write {fastest:?}"
    );
}

#[test]
fn a_window_above_the_limit_is_rejected() {
    let root = tempfile::tempdir().unwrap();
    let too_long = MAX_WINDOW + Duration::from_nanos(1);
    let error = RocksLogDb::<TypeConfig>::open_with_commit_window(root.path(), false, 0, too_long)
        .err()
        .expect("window above the limit opened");
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("1s limit"), "{error}");
    assert!(
        RocksLogDb::<TypeConfig>::open_with_commit_window(root.path(), false, 0, MAX_WINDOW)
            .is_ok()
    );
}

#[test]
fn a_zero_window_keeps_synchronous_writes() {
    let root = tempfile::tempdir().unwrap();
    let db = open(root.path(), Duration::ZERO);
    assert!(db.engine.commit.is_none());
    let stats = db.storage_memory_stats().unwrap();
    assert!(!stats.io_latency.contains_key("log_commit_flush"));
    assert!(!stats.io_latency.contains_key("log_commit_batch"));
}
