//! `DiskFsmDb::checkpoint` flushes the memtable under the Group's FSM lock. The
//! flush must not queue behind another database's slow flush in the process's
//! shared RocksDB flush pool, and the checkpoint must inspect, stay immutable
//! and install with exactly the committed entries, recycled WAL files included.
use std::collections::BTreeMap;
use std::path::Path;
use std::time::{Duration, Instant};

use multiraft_fsm::{AppliedState, GroupId};
use multiraft_store::rocksdb::{FlushOptions, IteratorMode, Options, WriteBatch, DB};
use multiraft_store::{DiskFsmDb, DiskFsmOptions, DISK_FSM_META_CF};

const GROUP: GroupId = 7;

fn scratch() -> tempfile::TempDir {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/fsm-checkpoint/store-tests");
    std::fs::create_dir_all(&root).unwrap();
    tempfile::tempdir_in(root).unwrap()
}

/// `recycle`: reuse WAL files, as the ech0 Slot stores do.
fn options(recycle: bool) -> DiskFsmOptions {
    let mut options = DiskFsmOptions::new(b"checkpoint-v1".to_vec());
    options.column_families = vec!["rows".into()];
    options.sync_writes = false;
    if recycle {
        options.db_options.set_recycle_log_file_num(4);
    }
    options
}

/// Commit entries `from..=to`, each one row, then sync them as one batch.
fn commit(db: &DiskFsmDb, from: u64, to: u64) {
    for index in from..=to {
        let mut batch = WriteBatch::default();
        batch.put_cf(db.cf("rows").unwrap(), index.to_be_bytes(), [1_u8; 256]);
        db.commit(batch, &applied(index)).unwrap();
    }
    db.sync_wal().unwrap();
}

/// Entry `index`'s applied state; its record names the index.
fn applied(index: u64) -> AppliedState {
    AppliedState::new(index, 1, index.to_be_bytes().to_vec())
}

fn files(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let name = entry.file_name().into_string().unwrap();
            (name, std::fs::read(entry.path()).unwrap())
        })
        .collect()
}

#[test]
fn checkpoint_does_not_wait_behind_another_databases_slow_flush() {
    let root = scratch();
    // Another database in the process, configured like the shared Raft log: a
    // rate-limited flush of 6 MiB at 2 MiB/s holds one flush thread ~3 s. With
    // RocksDB's default pool that is the only one.
    let mut slow_options = Options::default();
    slow_options.create_if_missing(true);
    slow_options.set_write_buffer_size(64 << 20);
    slow_options.set_ratelimiter(2 << 20, 100_000, 10);
    let slow = DB::open(&slow_options, root.path().join("slow")).unwrap();
    let value = vec![7_u8; 4096];
    for index in 0..1536_u32 {
        slow.put(index.to_be_bytes(), &value).unwrap();
    }
    let fsm = DiskFsmDb::open(root.path().join("fsm"), GROUP, options(false)).unwrap();
    commit(&fsm, 1, 100);

    let started = Instant::now();
    let mut no_wait = FlushOptions::default();
    no_wait.set_wait(false);
    slow.flush_opt(&no_wait).unwrap();
    std::thread::sleep(Duration::from_millis(100));
    let checkpoint_started = Instant::now();
    fsm.checkpoint(&root.path().join("checkpoint")).unwrap();
    let checkpoint = checkpoint_started.elapsed();
    slow.flush().unwrap();
    let flush = started.elapsed();

    assert!(
        flush >= Duration::from_secs(2),
        "the other flush must be slow enough to tell the cases apart: {flush:?}"
    );
    assert!(
        checkpoint < Duration::from_millis(200),
        "FSM checkpoint waited {checkpoint:?} behind another database's {flush:?} flush"
    );
}

/// Checkpoint committed entries, check the checkpoint stays byte-identical
/// while the live store keeps writing and rotating (and recycling) WAL files,
/// and installs into another store with every entry.
fn checkpoint_round_trip(recycle: bool) {
    let root = scratch();
    let live = DiskFsmDb::open(root.path().join("live"), GROUP, options(recycle)).unwrap();
    commit(&live, 1, 50);
    let dir = root.path().join("checkpoint");
    live.checkpoint(&dir).unwrap();

    let before = files(&dir);
    let latest = Some(applied(50).record().to_vec());
    assert_eq!(live.inspect_checkpoint(&dir).unwrap(), latest);

    for round in 0..3 {
        commit(&live, 51 + round * 10, 60 + round * 10);
        live.db().flush().unwrap();
    }
    assert_eq!(files(&dir), before, "a published checkpoint never changes");

    let mut other = DiskFsmDb::open(root.path().join("other"), GROUP, options(recycle)).unwrap();
    other.install_checkpoint(&dir).unwrap();
    assert_eq!(other.applied_record().unwrap(), latest);
    let rows = other.cf("rows").unwrap();
    let row = |index: u64| other.db().get_cf(rows, index.to_be_bytes()).unwrap();
    assert!((1..=50).all(|index| row(index).is_some()));
    assert!(row(51).is_none());
    drop(other);

    // The replayed WAL is durable in the installed store across a reopen.
    let reopened = DiskFsmDb::open(root.path().join("other"), GROUP, options(recycle)).unwrap();
    assert_eq!(reopened.applied_record().unwrap(), latest);
}

#[test]
fn checkpoint_inspects_stays_immutable_and_installs() {
    checkpoint_round_trip(false);
}

#[test]
fn checkpoint_with_recycled_wal_files_inspects_stays_immutable_and_installs() {
    checkpoint_round_trip(true);
}

/// Flush every column family, so the current WAL becomes obsolete.
fn flush_all(db: &DiskFsmDb) {
    for name in ["default", DISK_FSM_META_CF, "rows"] {
        db.db().flush_cf(db.db().cf_handle(name).unwrap()).unwrap();
    }
}

/// Every row key of the `rows` family, in order.
fn row_keys(db: &DiskFsmDb) -> Vec<u64> {
    db.db()
        .iterator_cf(db.cf("rows").unwrap(), IteratorMode::Start)
        .map(|row| u64::from_be_bytes(row.unwrap().0[..].try_into().unwrap()))
        .collect()
}

/// WAL files recycled after full flushes keep the records of their previous
/// use as a stale tail. A checkpoint taken after such a rotation must inspect,
/// install and reopen to exactly the committed rows: the puts of rows later
/// deleted must not come back.
#[test]
fn checkpoint_after_recycled_wal_rotation_holds_exactly_the_committed_rows() {
    let root = scratch();
    let live = DiskFsmDb::open(root.path().join("live"), GROUP, options(true)).unwrap();
    // WAL A: ~4 MiB of puts. Flushing moves to WAL B and frees A for reuse.
    for index in 1..=2000_u64 {
        let mut batch = WriteBatch::default();
        batch.put_cf(live.cf("rows").unwrap(), index.to_be_bytes(), [1_u8; 2048]);
        live.commit(batch, &applied(index)).unwrap();
    }
    live.sync_wal().unwrap();
    flush_all(&live);
    // WAL B: delete them all. Flushing reuses A, whose tail still holds the puts.
    let mut batch = WriteBatch::default();
    for index in 1..=2000_u64 {
        batch.delete_cf(live.cf("rows").unwrap(), index.to_be_bytes());
    }
    live.commit(batch, &applied(2001)).unwrap();
    live.sync_wal().unwrap();
    flush_all(&live);
    commit(&live, 3001, 3010);

    let dir = root.path().join("checkpoint");
    live.checkpoint(&dir).unwrap();

    let latest = Some(applied(3010).record().to_vec());
    let expected: Vec<u64> = (3001..=3010).collect();
    assert_eq!(live.inspect_checkpoint(&dir).unwrap(), latest);
    let mut other = DiskFsmDb::open(root.path().join("other"), GROUP, options(true)).unwrap();
    other.install_checkpoint(&dir).unwrap();
    assert_eq!(other.applied_record().unwrap(), latest);
    assert_eq!(row_keys(&other), expected);
    drop(other);
    let reopened = DiskFsmDb::open(root.path().join("other"), GROUP, options(true)).unwrap();
    assert_eq!(reopened.applied_record().unwrap(), latest);
    assert_eq!(row_keys(&reopened), expected);
}
