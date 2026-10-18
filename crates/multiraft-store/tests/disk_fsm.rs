//! Disk-backed FSM mode through the public store seams: the per-Group RocksDB
//! helper and the bridge's applied-position recovery without whole-image restore.
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::stream;
use multiraft_core::typ::{Entry, LogId};
use multiraft_core::{Request, TypeConfig};
use multiraft_fsm::{AppliedState, ApplyOut, CaptureRefusal, DiskState, GroupId, StateMachine};
use multiraft_store::rocksdb::WriteBatch;
use multiraft_store::{
    DiskFsmDb, DiskFsmOptions, NativeCaptureError, NativeSmOptions, SnapshotCatalog,
    StateMachineStore, DISK_FSM_META_CF,
};
use openraft::alias::{LeaderIdOf, SnapshotMetaOf};
use openraft::storage::RaftStateMachine;
use openraft::vote::RaftLeaderIdExt;
use openraft::{EntryPayload, Membership, RaftSnapshotBuilder};
use tokio::sync::Semaphore;

const GROUP: GroupId = 7;

fn scratch() -> tempfile::TempDir {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/issue-186/store-tests");
    std::fs::create_dir_all(&root).unwrap();
    tempfile::tempdir_in(root).unwrap()
}

fn options() -> DiskFsmOptions {
    let mut options = DiskFsmOptions::new(b"disk-counter-v1".to_vec());
    options.column_families = vec!["calls".into()];
    options
}

/// Counter whose value and apply count live only in RocksDB.
struct DiskCounter {
    db: DiskFsmDb,
}

impl DiskCounter {
    fn open(path: &Path) -> Self {
        Self {
            db: DiskFsmDb::open(path, GROUP, options()).unwrap(),
        }
    }
    fn read(&self, cf: &str, key: &[u8]) -> i64 {
        let cf = self.db.cf(cf).unwrap();
        self.db
            .db()
            .get_cf(cf, key)
            .unwrap()
            .map_or(0, |bytes| i64::from_be_bytes(bytes.try_into().unwrap()))
    }
    fn value(&self) -> i64 {
        self.read("default", b"value")
    }
    fn calls(&self) -> i64 {
        self.read("calls", b"apply")
    }
}

impl StateMachine for DiskCounter {
    type Error = io::Error;
    fn apply(&mut self, _: GroupId, _: u64, _: &[u8]) -> io::Result<ApplyOut> {
        unreachable!("disk mode never calls whole-image apply")
    }
    fn snapshot(&self, _: GroupId) -> io::Result<Vec<u8>> {
        unreachable!("disk mode never calls whole-image snapshot")
    }
    fn restore(&mut self, _: GroupId, _: &[u8]) -> io::Result<()> {
        unreachable!("disk mode never calls whole-image restore")
    }
    fn disk_state(&mut self) -> Option<&mut dyn DiskState> {
        Some(self)
    }
}

impl DiskState for DiskCounter {
    fn durable_applied(&self) -> io::Result<Option<Vec<u8>>> {
        self.db.applied_record()
    }
    fn apply_durable(
        &mut self,
        group: GroupId,
        _index: u64,
        data: Option<&[u8]>,
        applied: &AppliedState,
    ) -> io::Result<ApplyOut> {
        assert_eq!(group, GROUP);
        let mut batch = WriteBatch::default();
        let mut effects = Vec::new();
        if let Some(data) = data {
            let delta = i64::from_be_bytes(data.try_into().map_err(io::Error::other)?);
            let value = self.value() + delta;
            batch.put_cf(self.db.cf("default")?, b"value", value.to_be_bytes());
            batch.put_cf(
                self.db.cf("calls")?,
                b"apply",
                (self.calls() + 1).to_be_bytes(),
            );
            effects = value.to_be_bytes().to_vec();
        }
        self.db.commit(batch, applied)?;
        Ok(ApplyOut { effects })
    }
}

fn log_id(index: u64) -> LogId {
    LogId::new(LeaderIdOf::<TypeConfig>::new_committed(1, 1), index)
}

fn add(index: u64, delta: i64) -> Entry {
    Entry {
        log_id: log_id(index),
        payload: EntryPayload::Normal(Request::new(delta.to_be_bytes().to_vec())),
    }
}

fn blank(index: u64) -> Entry {
    Entry {
        log_id: log_id(index),
        payload: EntryPayload::Blank,
    }
}

fn membership(index: u64) -> Entry {
    let nodes: BTreeMap<u64, openraft::impls::BasicNode> = [1, 2, 3]
        .into_iter()
        .map(|id| (id, openraft::impls::BasicNode::default()))
        .collect();
    Entry {
        log_id: log_id(index),
        payload: EntryPayload::Membership(
            Membership::new(vec![BTreeSet::from([1, 2, 3])], nodes).unwrap(),
        ),
    }
}

async fn apply(store: &mut StateMachineStore<DiskCounter>, entries: Vec<Entry>) -> io::Result<()> {
    store
        .apply(stream::iter(
            entries.into_iter().map(|entry| Ok((entry, None))),
        ))
        .await
}

fn db_path(root: &Path) -> PathBuf {
    DiskFsmDb::group_path(root, GROUP)
}

#[test]
fn helper_creates_markers_and_fails_closed_on_foreign_databases() {
    let root = scratch();
    let path = db_path(root.path());
    assert_eq!(path, root.path().join("fsm-rocksdb").join("group-7"));
    {
        let db = DiskFsmDb::open(&path, GROUP, options()).unwrap();
        assert_eq!(db.applied_record().unwrap(), None);
        assert!(db.cf(DISK_FSM_META_CF).is_err(), "meta CF is reserved");
        db.commit(
            WriteBatch::default(),
            &AppliedState::new(3, 1, b"record".to_vec()),
        )
        .unwrap();
    }
    let reopened = DiskFsmDb::open(&path, GROUP, options()).unwrap();
    assert_eq!(
        reopened.applied_record().unwrap().as_deref(),
        Some(&b"record"[..])
    );
    drop(reopened);

    let other_group = DiskFsmDb::open(&path, GROUP + 1, options()).err().unwrap();
    assert_eq!(
        other_group.kind(),
        io::ErrorKind::InvalidData,
        "{other_group}"
    );
    let mut other_format = options();
    other_format.app_format = b"other".to_vec();
    let error = DiskFsmDb::open(&path, GROUP, other_format).err().unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
    let fewer_cfs = DiskFsmDb::open(
        &path,
        GROUP,
        DiskFsmOptions::new(b"disk-counter-v1".to_vec()),
    );
    assert!(
        fewer_cfs.is_err(),
        "column family set is part of the format"
    );
    let mut reserved = options();
    reserved.column_families.push(DISK_FSM_META_CF.into());
    assert!(DiskFsmDb::open(root.path().join("reserved"), GROUP, reserved).is_err());

    let stray = root.path().join("stray");
    std::fs::create_dir_all(&stray).unwrap();
    std::fs::write(stray.join("file"), b"x").unwrap();
    let error = DiskFsmDb::open(&stray, GROUP, options()).err().unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
}

#[tokio::test]
async fn restart_recovers_applied_position_and_membership_without_replay() {
    let root = scratch();
    let path = db_path(root.path());
    let mut store = StateMachineStore::new(GROUP, DiskCounter::open(&path));
    assert!(store.is_disk_backed());
    assert_eq!(store.applied_state().await.unwrap().0, None);
    apply(
        &mut store,
        vec![blank(0), membership(1), add(2, 5), add(3, 4)],
    )
    .await
    .unwrap();
    let (applied, members) = store.applied_state().await.unwrap();
    assert_eq!(applied, Some(log_id(3)));
    assert_eq!(members.log_id(), &Some(log_id(1)));
    drop(store);

    let mut restarted = StateMachineStore::new(GROUP, DiskCounter::open(&path));
    let (applied, recovered) = restarted.applied_state().await.unwrap();
    assert_eq!(applied, Some(log_id(3)), "applied index comes from RocksDB");
    assert_eq!(recovered, members, "membership comes from RocksDB");
    assert_eq!(restarted.last_applied().await, Some((3, 1)));
    let (value, calls) = restarted.with_fsm(|fsm| (fsm.value(), fsm.calls())).await;
    assert_eq!((value, calls), (9, 2), "no entry was replayed");

    // OpenRaft replays only the suffix; a replayed persisted entry is a gap.
    let gap = apply(&mut restarted, vec![add(3, 4)]).await.err().unwrap();
    assert_eq!(gap.kind(), io::ErrorKind::InvalidData, "{gap}");
    apply(&mut restarted, vec![add(4, 1)]).await.unwrap();
    let (value, calls) = restarted.with_fsm(|fsm| (fsm.value(), fsm.calls())).await;
    assert_eq!((value, calls), (10, 3));
}

#[tokio::test]
async fn whole_image_snapshot_paths_are_refused() {
    let root = scratch();
    let mut store = StateMachineStore::new(GROUP, DiskCounter::open(&db_path(root.path())));
    apply(&mut store, vec![blank(0), add(1, 2)]).await.unwrap();
    assert!(store.try_create_snapshot_builder(false).await.is_none());
    let error = store
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), io::ErrorKind::Unsupported);
    let meta = SnapshotMetaOf::<TypeConfig> {
        last_log_id: Some(log_id(9)),
        last_membership: Default::default(),
        snapshot_id: "whole-image".into(),
    };
    let error = store
        .install_snapshot(&meta, io::Cursor::new(Vec::new()))
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), io::ErrorKind::Unsupported);
    assert!(store.get_current_snapshot().await.unwrap().is_none());
    assert_eq!(store.last_applied().await, Some((1, 1)));
}

#[tokio::test]
async fn native_durable_compaction_refuses_disk_capture() {
    let root = scratch();
    let store = StateMachineStore::with_native_options(
        GROUP,
        DiskCounter::open(&db_path(root.path())),
        NativeSmOptions {
            catalog: Arc::new(SnapshotCatalog::new(root.path().join("snapshots"), 1)),
            max_snapshot_bytes: 1024,
            build_budget: Arc::new(Semaphore::new(1)),
        },
    )
    .unwrap();
    assert!(matches!(
        store.reserve_native_compaction().await,
        Err(NativeCaptureError::Refused(CaptureRefusal::Unsupported))
    ));
    let mut store = store;
    assert!(store.get_current_snapshot().await.unwrap().is_none());
    let error = store
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .err()
        .unwrap();
    assert_eq!(
        error.kind(),
        io::ErrorKind::Unsupported,
        "forced build refuses"
    );
}

#[tokio::test]
async fn leftover_whole_image_catalog_snapshot_fails_closed() {
    let root = scratch();
    let catalog = Arc::new(SnapshotCatalog::new(root.path().join("snapshots"), 1));
    let meta = SnapshotMetaOf::<TypeConfig> {
        last_log_id: Some(log_id(1)),
        last_membership: Default::default(),
        snapshot_id: "whole-image".into(),
    };
    catalog
        .publish_native(GROUP, &meta, b"image", 1024)
        .unwrap();
    let mut store = StateMachineStore::with_native_options(
        GROUP,
        DiskCounter::open(&db_path(root.path())),
        NativeSmOptions {
            catalog,
            max_snapshot_bytes: 1024,
            build_budget: Arc::new(Semaphore::new(1)),
        },
    )
    .unwrap();
    // The FSM is already ahead, so OpenRaft would skip installing the image.
    apply(&mut store, vec![blank(0), add(1, 2), add(2, 3)])
        .await
        .unwrap();
    let mut log = multiraft_store::MemLogStore::default();
    let error = store.check_disk_log_basis(&mut log).await.err().unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
}

#[tokio::test]
async fn disk_state_without_a_log_basis_fails_closed() {
    let root = scratch();
    let path = db_path(root.path());
    let mut store = StateMachineStore::new(GROUP, DiskCounter::open(&path));
    let mut empty_log = multiraft_store::MemLogStore::default();
    store.check_disk_log_basis(&mut empty_log).await.unwrap();
    apply(&mut store, vec![blank(0), add(1, 2)]).await.unwrap();
    drop(store);
    let store = StateMachineStore::new(GROUP, DiskCounter::open(&path));
    let error = store
        .check_disk_log_basis(&mut empty_log)
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
}
