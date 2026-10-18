//! Disk snapshots through the store seams: checkpoint capture and publication,
//! chunked transfer into staging, ingress verification, crash-safe install, and
//! fail-closed refusals that leave the receiver on its old state.
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::stream;
use multiraft_core::typ::{Entry, LogId};
use multiraft_core::{Request, TypeConfig};
use multiraft_fsm::{AppliedState, ApplyOut, DiskState, GroupId, StateMachine};
use multiraft_store::disk_snapshot::{inject_install_fault, ChunkSource, InstallFault};
use multiraft_store::rocksdb::WriteBatch;
use multiraft_store::{
    is_disk_snapshot, DiskChunk, DiskFsmDb, DiskFsmOptions, NativeSmOptions, SnapshotCatalog,
    StateMachineStore,
};
use openraft::alias::{LeaderIdOf, SnapshotMetaOf};
use openraft::storage::{IOFlushed, RaftLogStorage, RaftStateMachine};
use openraft::vote::RaftLeaderIdExt;
use openraft::{EntryPayload, Membership, RaftSnapshotBuilder};
use tokio::sync::Semaphore;

const GROUP: GroupId = 7;
/// Install faults are process-global; tests that install take this lock.
static INSTALLS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn scratch() -> tempfile::TempDir {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/issue-190/store-tests");
    std::fs::create_dir_all(&root).unwrap();
    tempfile::tempdir_in(root).unwrap()
}

fn options(format: &[u8]) -> DiskFsmOptions {
    let mut options = DiskFsmOptions::new(format.to_vec());
    options.column_families = vec!["blobs".into()];
    options
}

/// Sum plus one incompressible blob row per entry, all in RocksDB.
struct DiskBlobs {
    db: DiskFsmDb,
}

impl DiskBlobs {
    fn open(path: &Path) -> Self {
        Self::open_as(path, b"disk-blobs-v1")
    }
    fn open_as(path: &Path, format: &[u8]) -> Self {
        Self {
            db: DiskFsmDb::open(path, GROUP, options(format)).unwrap(),
        }
    }
    fn sum(&self) -> u64 {
        self.db
            .db()
            .get_cf(self.db.cf("default").unwrap(), b"sum")
            .unwrap()
            .map_or(0, |bytes| u64::from_be_bytes(bytes.try_into().unwrap()))
    }
    fn blobs(&self) -> usize {
        self.db
            .db()
            .iterator_cf(
                self.db.cf("blobs").unwrap(),
                multiraft_store::rocksdb::IteratorMode::Start,
            )
            .count()
    }
}

impl StateMachine for DiskBlobs {
    type Error = io::Error;
    fn apply(&mut self, _: GroupId, _: u64, _: &[u8]) -> io::Result<ApplyOut> {
        unreachable!()
    }
    fn snapshot(&self, _: GroupId) -> io::Result<Vec<u8>> {
        unreachable!()
    }
    fn restore(&mut self, _: GroupId, _: &[u8]) -> io::Result<()> {
        unreachable!()
    }
    fn disk_state(&mut self) -> Option<&mut dyn DiskState> {
        Some(self)
    }
}

impl DiskState for DiskBlobs {
    fn durable_applied(&self) -> io::Result<Option<Vec<u8>>> {
        self.db.applied_record()
    }
    fn apply_durable(
        &mut self,
        _: GroupId,
        index: u64,
        data: Option<&[u8]>,
        applied: &AppliedState,
    ) -> io::Result<ApplyOut> {
        let mut batch = WriteBatch::default();
        if let Some(data) = data {
            let delta = u64::from_be_bytes(data[..8].try_into().unwrap());
            batch.put_cf(
                self.db.cf("default")?,
                b"sum",
                (self.sum() + delta).to_be_bytes(),
            );
            batch.put_cf(self.db.cf("blobs")?, index.to_be_bytes(), &data[8..]);
        }
        self.db.commit(batch, applied)?;
        Ok(ApplyOut::default())
    }
    fn snapshot_workspace(&self) -> io::Result<PathBuf> {
        self.db.snapshot_workspace()
    }
    fn checkpoint(&self, dir: &Path) -> io::Result<()> {
        self.db.checkpoint(dir)
    }
    fn inspect_checkpoint(&self, dir: &Path) -> io::Result<Option<Vec<u8>>> {
        self.db.inspect_checkpoint(dir)
    }
    fn install_checkpoint(&mut self, dir: &Path) -> io::Result<()> {
        self.db.install_checkpoint(dir)
    }
}

fn log_id(index: u64) -> LogId {
    LogId::new(LeaderIdOf::<TypeConfig>::new_committed(1, 1), index)
}

fn add(index: u64, delta: u64) -> Entry {
    let mut data = delta.to_be_bytes().to_vec();
    let mut state = index.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    for _ in 0..64 * 1024 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        data.push(state as u8);
    }
    Entry {
        log_id: log_id(index),
        payload: EntryPayload::Normal(Request::new(data)),
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

fn store(root: &Path, fsm: DiskBlobs) -> StateMachineStore<DiskBlobs> {
    StateMachineStore::with_native_options(
        GROUP,
        fsm,
        NativeSmOptions {
            catalog: Arc::new(SnapshotCatalog::new(root.join("snapshots"), 1)),
            max_snapshot_bytes: 1024 * 1024,
            build_budget: Arc::new(Semaphore::new(1)),
        },
    )
    .unwrap()
}

fn path(root: &Path) -> PathBuf {
    DiskFsmDb::group_path(root, GROUP)
}

async fn apply(store: &mut StateMachineStore<DiskBlobs>, entries: Vec<Entry>) {
    store
        .apply(stream::iter(
            entries.into_iter().map(|entry| Ok((entry, None))),
        ))
        .await
        .unwrap();
}

/// A leader-side store with `n` blob entries after a membership entry, and its built snapshot.
async fn leader(
    root: &Path,
    n: u64,
) -> (
    StateMachineStore<DiskBlobs>,
    SnapshotMetaOf<TypeConfig>,
    Vec<u8>,
) {
    let mut leader = store(root, DiskBlobs::open(&path(root)));
    let mut entries = vec![membership(0)];
    entries.extend((1..=n).map(|index| add(index, index)));
    apply(&mut leader, entries).await;
    let snapshot = leader
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();
    (leader, snapshot.meta, snapshot.snapshot.into_inner())
}

/// Stream every chunk of `manifest` into `receiver`, optionally corrupting one data chunk.
async fn transfer(
    receiver: &StateMachineStore<DiskBlobs>,
    manifest: Vec<u8>,
    corrupt: Option<usize>,
) -> io::Result<()> {
    let mut source = ChunkSource::open(manifest, GROUP)?;
    let mut data_chunks = 0;
    while let Some(mut chunk) = source.next_chunk()? {
        if let DiskChunk::Data { bytes, .. } = &mut chunk {
            if corrupt == Some(data_chunks) {
                bytes[0] ^= 0xff;
            }
            data_chunks += 1;
        }
        receiver.receive_disk_chunk(chunk).await?;
    }
    Ok(())
}

#[tokio::test]
async fn build_publishes_a_manifest_and_restart_serves_it() {
    let root = scratch();
    let (leader, meta, data) = leader(root.path(), 40).await;
    assert!(is_disk_snapshot(&meta, &data));
    assert!(
        data.len() < 64 * 1024,
        "the snapshot payload is only a manifest"
    );
    assert_eq!(meta.last_log_id, Some(log_id(40)));
    let info = leader.native_snapshot_info().await.unwrap().unwrap();
    assert_eq!(info.meta, meta);
    assert!(
        info.size > 40 * 64 * 1024,
        "checkpoint holds the blobs: {}",
        info.size
    );
    drop(leader);

    let mut restarted = store(root.path(), DiskBlobs::open(&path(root.path())));
    let current = restarted.get_current_snapshot().await.unwrap().unwrap();
    assert_eq!(current.meta, meta);
    assert_eq!(current.snapshot.into_inner(), data);
}

#[tokio::test]
async fn streamed_install_replaces_state_and_survives_restart() {
    let _serial = INSTALLS.lock().await;
    let root = scratch();
    let (leader, meta, data) = leader(&root.path().join("a"), 40).await;
    let expected = leader.with_fsm(|fsm| (fsm.sum(), fsm.blobs())).await;

    let follower_root = root.path().join("b");
    let mut follower = store(&follower_root, DiskBlobs::open(&path(&follower_root)));
    apply(&mut follower, vec![membership(0), add(1, 1)]).await;
    transfer(&follower, data.clone(), None).await.unwrap();
    follower.verify_disk_snapshot(&meta, &data).await.unwrap();
    follower
        .install_snapshot(&meta, io::Cursor::new(data.clone()))
        .await
        .unwrap();
    assert_eq!(
        follower.with_fsm(|fsm| (fsm.sum(), fsm.blobs())).await,
        expected
    );
    assert_eq!(follower.last_applied().await, Some((40, 1)));
    let local = follower.get_current_snapshot().await.unwrap().unwrap();
    assert_eq!(
        local.meta.last_log_id, meta.last_log_id,
        "receiver publishes its own checkpoint"
    );
    drop(follower);

    // Restart before OpenRaft purged the log: the FSM is ahead of the log tail,
    // which is accepted only because the install marker names this position.
    let mut restarted = store(&follower_root, DiskBlobs::open(&path(&follower_root)));
    assert_eq!(restarted.applied_state().await.unwrap().0, meta.last_log_id);
    let mut log = multiraft_store::MemLogStore::default();
    log.save_vote(&multiraft_core::typ::Vote::new_committed(1, 1))
        .await
        .unwrap();
    log.append([membership(0), add(1, 1)], IOFlushed::noop())
        .await
        .unwrap();
    restarted.check_disk_log_basis(&mut log).await.unwrap();
    assert_eq!(
        restarted.with_fsm(|fsm| (fsm.sum(), fsm.blobs())).await,
        expected
    );
}

#[tokio::test]
async fn refused_transfers_leave_the_old_state() {
    let _serial = INSTALLS.lock().await;
    let root = scratch();
    let (_leader, meta, data) = leader(&root.path().join("a"), 40).await;
    let follower_root = root.path().join("b");
    let mut follower = store(&follower_root, DiskBlobs::open(&path(&follower_root)));
    apply(&mut follower, vec![membership(0), add(1, 1)]).await;
    let old = follower.with_fsm(|fsm| (fsm.sum(), fsm.blobs())).await;

    // A corrupted chunk fails its file digest; nothing is left to verify.
    let error = transfer(&follower, data.clone(), Some(1))
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
    assert!(follower.verify_disk_snapshot(&meta, &data).await.is_err());

    // A partial transfer is incomplete.
    let mut source = ChunkSource::open(data.clone(), GROUP).unwrap();
    for _ in 0..3 {
        follower
            .receive_disk_chunk(source.next_chunk().unwrap().unwrap())
            .await
            .unwrap();
    }
    assert!(follower.verify_disk_snapshot(&meta, &data).await.is_err());

    // Metadata that disagrees with the checkpoint's applied record is refused.
    transfer(&follower, data.clone(), None).await.unwrap();
    let mut forged = meta.clone();
    forged.last_log_id = Some(log_id(41));
    assert!(follower.verify_disk_snapshot(&forged, &data).await.is_err());

    // Install without ingress verification is refused before any swap.
    let error = follower
        .install_snapshot(&meta, io::Cursor::new(data.clone()))
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
    assert_eq!(follower.with_fsm(|fsm| (fsm.sum(), fsm.blobs())).await, old);
    assert_eq!(follower.last_applied().await, Some((1, 1)));

    // A checkpoint of another application format fails the layout check.
    let other_root = root.path().join("c");
    let mut other = store(
        &other_root,
        DiskBlobs::open_as(&path(&other_root), b"other-format"),
    );
    apply(&mut other, vec![membership(0)]).await;
    transfer(&other, data.clone(), None).await.unwrap();
    let error = other
        .verify_disk_snapshot(&meta, &data)
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
    drop(follower);
    let staged = std::fs::read_dir(DiskFsmDb::workspace_path(&path(&follower_root)).unwrap())
        .unwrap()
        .filter(|entry| {
            entry
                .as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("staging-")
        })
        .count();
    assert!(staged <= 1, "at most the one unfinished transfer is staged");
}

#[tokio::test]
async fn failed_swaps_keep_the_old_store_live_and_durable() {
    let _serial = INSTALLS.lock().await;
    let root = scratch();
    let (_leader, meta, data) = leader(&root.path().join("a"), 40).await;
    let follower_root = root.path().join("b");
    for fault in [
        InstallFault::ErrorBeforeSwap,
        InstallFault::ErrorAfterRetire,
    ] {
        let mut follower = store(&follower_root, DiskBlobs::open(&path(&follower_root)));
        if follower.applied_state().await.unwrap().0.is_none() {
            apply(&mut follower, vec![membership(0), add(1, 1)]).await;
        }
        let old = follower.with_fsm(|fsm| (fsm.sum(), fsm.blobs())).await;
        transfer(&follower, data.clone(), None).await.unwrap();
        follower.verify_disk_snapshot(&meta, &data).await.unwrap();
        inject_install_fault(Some(fault));
        let result = follower
            .install_snapshot(&meta, io::Cursor::new(data.clone()))
            .await;
        inject_install_fault(None);
        assert!(result.is_err(), "{fault:?}");
        // The failed install fences this generation (the Group stops); the
        // store on disk is still the old one.
        assert!(follower.application_ready().is_err());
        drop(follower);
        let reopened = DiskBlobs::open(&path(&follower_root));
        assert_eq!(
            (reopened.sum(), reopened.blobs()),
            old,
            "{fault:?} after reopen"
        );
    }
}
