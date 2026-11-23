//! Disk-backed FSM mode over three gRPC nodes with RocksDB logs: peer restart
//! and cold restart recover state and applied index from each node's FSM
//! RocksDB, replaying only the unapplied suffix. A node whose FSM rolled back
//! to its published checkpoint (unsynced batch lost) replays the gap from its
//! Raft log; a rollback below the purged log is refused at startup.
use multiraft_core::{ClusterConfig, LogStorageBackend, SnapshotMode};
use multiraft_fsm::{AppliedState, ApplyOut, DiskState, GroupId, StateMachine};
use multiraft_net::{GroupConfig, NodeOwner, RuntimeConfig, RuntimeHandle};
use multiraft_store::rocksdb::WriteBatch;
use multiraft_store::{DiskFsmDb, DiskFsmOptions};
use std::{
    convert::Infallible,
    io,
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::time::Instant;

const GROUP: GroupId = 7;

struct DiskCounter {
    db: DiskFsmDb,
    /// Applied index found in RocksDB when this FSM was opened.
    opened: Option<u64>,
    /// Payload entries applied by this process (shared across restarts).
    payload_applies: Arc<AtomicUsize>,
}

impl DiskCounter {
    fn row(&self, key: &[u8]) -> u64 {
        let cf = self.db.cf("default").unwrap();
        self.db
            .db()
            .get_cf(cf, key)
            .unwrap()
            .map_or(0, |bytes| u64::from_be_bytes(bytes.try_into().unwrap()))
    }
    /// (sum, payload entries, FSM-stored applied index)
    fn facts(&self) -> (u64, u64, u64) {
        (self.row(b"sum"), self.row(b"entries"), self.row(b"applied"))
    }
}

impl StateMachine for DiskCounter {
    type Error = io::Error;
    fn apply(&mut self, _: GroupId, _: u64, _: &[u8]) -> io::Result<ApplyOut> {
        Err(io::Error::other("whole-image apply in disk mode"))
    }
    fn snapshot(&self, _: GroupId) -> io::Result<Vec<u8>> {
        Err(io::Error::other("whole-image snapshot in disk mode"))
    }
    fn restore(&mut self, _: GroupId, _: &[u8]) -> io::Result<()> {
        Err(io::Error::other("whole-image restore in disk mode"))
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
        _: GroupId,
        index: u64,
        data: Option<&[u8]>,
        applied: &AppliedState,
    ) -> io::Result<ApplyOut> {
        let cf = self.db.cf("default")?;
        let mut batch = WriteBatch::default();
        batch.put_cf(cf, b"applied", index.to_be_bytes());
        let mut effects = Vec::new();
        if let Some(data) = data {
            let delta = u64::from_be_bytes(data.try_into().map_err(io::Error::other)?);
            let sum = self.row(b"sum") + delta;
            batch.put_cf(cf, b"sum", sum.to_be_bytes());
            batch.put_cf(cf, b"entries", (self.row(b"entries") + 1).to_be_bytes());
            effects = sum.to_be_bytes().to_vec();
            self.payload_applies.fetch_add(1, Ordering::SeqCst);
        }
        self.db.commit(batch, applied)?;
        Ok(ApplyOut { effects })
    }
    fn defers_sync(&self) -> bool {
        self.db.defers_sync()
    }
    fn sync_applied(&mut self) -> io::Result<()> {
        self.db.sync_wal()
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

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}

#[derive(Clone)]
struct Node {
    config: ClusterConfig,
    payload_applies: Arc<AtomicUsize>,
    /// false: batch mode, one WAL sync per apply batch (ADR 0022).
    sync_writes: bool,
}

async fn start(node: &Node) -> Result<NodeOwner<DiskCounter>, multiraft_net::RuntimeError> {
    let data_dir: PathBuf = node.config.data_dir.clone();
    let payload_applies = node.payload_applies.clone();
    let mut options = DiskFsmOptions::new(b"counter-v1".to_vec());
    options.sync_writes = node.sync_writes;
    NodeOwner::start(
        RuntimeConfig::new(
            node.config.clone(),
            vec![GroupConfig {
                group_id: GROUP,
                voters: vec![1, 2, 3],
            }],
        ),
        move |context: multiraft_net::FsmFactoryContext| {
            let path = DiskFsmDb::group_path(&data_dir, context.group_id());
            let db = DiskFsmDb::open(path, context.group_id(), options.clone())?;
            let opened = db
                .db()
                .get_cf(db.cf("default")?, b"applied")?
                .map(|bytes| u64::from_be_bytes(bytes.try_into().unwrap()));
            Ok(DiskCounter {
                db,
                opened,
                payload_applies: payload_applies.clone(),
            })
        },
        deadline(),
    )
    .await
}

async fn write(handles: &[RuntimeHandle<DiskCounter>], delta: u64) -> u64 {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            for handle in handles {
                match handle
                    .propose(
                        GROUP,
                        delta.to_be_bytes().to_vec(),
                        Instant::now() + Duration::from_secs(2),
                    )
                    .await
                {
                    Ok(receipt) => return u64::from_be_bytes(receipt.effects.try_into().unwrap()),
                    // A possibly committed write must not be resubmitted.
                    Err(
                        error @ (multiraft_net::RuntimeError::Deadline {
                            outcome_unknown: true,
                            ..
                        }
                        | multiraft_net::RuntimeError::Interrupted {
                            outcome_unknown: true,
                            ..
                        }),
                    ) => panic!("ambiguous write outcome: {error}"),
                    Err(_) => {}
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("a majority accepts the write")
}

/// Local view: (sum, payload entries, FSM applied row, bridge applied index, opened).
async fn local(handle: &RuntimeHandle<DiskCounter>) -> (u64, u64, u64, u64, Option<u64>) {
    let view = handle
        .try_read_applied_with_watermark(GROUP, deadline(), |fsm| {
            Ok::<_, Infallible>((fsm.facts(), fsm.opened))
        })
        .await
        .unwrap();
    let ((sum, entries, applied_row), opened) = view.value;
    (
        sum,
        entries,
        applied_row,
        view.applied.unwrap().index,
        opened,
    )
}

/// Wait until every node reports the same applied index and the expected state.
async fn converged(
    handles: &[RuntimeHandle<DiskCounter>],
    sum: u64,
    entries: u64,
) -> Vec<(u64, u64, u64, u64, Option<u64>)> {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let mut views = Vec::new();
            for handle in handles {
                views.push(local(handle).await);
            }
            let same_index = views.iter().all(|v| v.3 == views[0].3);
            if same_index && views.iter().all(|v| v.0 == sum && v.1 == entries) {
                return views;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("replicas converge")
}

fn nodes(root: &std::path::Path) -> Vec<Node> {
    let listeners: Vec<_> = (0..3)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let peers: Vec<(u64, SocketAddr)> = listeners
        .iter()
        .enumerate()
        .map(|(i, listener)| ((i + 1) as u64, listener.local_addr().unwrap()))
        .collect();
    drop(listeners);
    peers
        .iter()
        .map(|&(node, _)| {
            let mut config = ClusterConfig::for_test(node, &[1, 2, 3]);
            config.peers = peers.clone();
            config.data_dir = root.join(format!("node-{node}"));
            config.log_storage_backend = LogStorageBackend::RocksDb;
            config.snapshot_mode = SnapshotMode::NativeDurable;
            Node {
                config,
                payload_applies: Arc::new(AtomicUsize::new(0)),
                sync_writes: true,
            }
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_and_cold_restart_recover_state_and_applied_index_from_rocksdb() {
    let root = tempfile::tempdir().unwrap();
    let nodes = nodes(root.path());
    let (a, b, c) = tokio::join!(start(&nodes[0]), start(&nodes[1]), start(&nodes[2]));
    let owners = vec![a.unwrap(), b.unwrap(), c.unwrap()];
    let handles: Vec<_> = owners.iter().map(|o| o.handle()).collect();
    let mut sum = 0;
    for delta in 1..=5 {
        sum += delta;
        assert_eq!(write(&handles, delta).await, sum);
    }
    let first = converged(&handles, sum, 5).await;
    for view in &first {
        assert_eq!(
            view.2, view.3,
            "FSM applied row matches the bridge applied index"
        );
        assert_eq!(view.4, None, "fresh FSM databases");
    }

    // Peer restart: node 3 recovers from its own RocksDB, then catches up by log.
    let stopped_at = local(&handles[2]).await.3;
    let mut owners = owners;
    let third = owners.pop().unwrap();
    third.shutdown(deadline()).await.unwrap();
    let applies_before = nodes[2].payload_applies.load(Ordering::SeqCst);
    sum += 10;
    assert_eq!(write(&handles[..2], 10).await, sum);
    let restarted = start(&nodes[2]).await.unwrap();
    let view = local(&restarted.handle()).await;
    assert_eq!(
        view.4,
        Some(stopped_at),
        "opened at the applied index it had committed"
    );
    owners.push(restarted);
    let handles: Vec<_> = owners.iter().map(|o| o.handle()).collect();
    converged(&handles, sum, 6).await;
    assert_eq!(
        nodes[2].payload_applies.load(Ordering::SeqCst) - applies_before,
        1,
        "only the missed write is applied after the peer restart"
    );

    // Cold restart: every node stops, then all start from their own databases.
    let mut before = Vec::new();
    for handle in &handles {
        before.push(local(handle).await);
    }
    drop(handles);
    for owner in owners {
        owner.shutdown(deadline()).await.unwrap();
    }
    let counts: Vec<_> = nodes
        .iter()
        .map(|n| n.payload_applies.load(Ordering::SeqCst))
        .collect();
    let (a, b, c) = tokio::join!(start(&nodes[0]), start(&nodes[1]), start(&nodes[2]));
    let owners = vec![a.unwrap(), b.unwrap(), c.unwrap()];
    let handles: Vec<_> = owners.iter().map(|o| o.handle()).collect();
    for (i, handle) in handles.iter().enumerate() {
        let view = local(handle).await;
        assert_eq!(
            view.4,
            Some(before[i].3),
            "node {} reopened at its applied index",
            i + 1
        );
        assert!(view.3 >= before[i].3, "applied index never moves back");
        assert_eq!(
            (view.0, view.1),
            (sum, 6),
            "state recovered without whole-image restore"
        );
    }
    sum += 100;
    assert_eq!(write(&handles, 100).await, sum);
    let after = converged(&handles, sum, 7).await;
    for (i, view) in after.iter().enumerate() {
        assert_eq!(
            view.2, view.3,
            "FSM applied row matches the bridge applied index"
        );
        assert_eq!(
            nodes[i].payload_applies.load(Ordering::SeqCst) - counts[i],
            1,
            "node {} replays no persisted payload after cold restart",
            i + 1
        );
    }
    drop(handles);
    for owner in owners {
        owner.shutdown(deadline()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disk_mode_requires_a_durable_voter_log() {
    let root = tempfile::tempdir().unwrap();
    let mut memory = nodes(root.path()).remove(0);
    // An empty data_dir selects memory logs; the FSM still has its own path.
    let fsm_root = memory.config.data_dir.clone();
    memory.config.data_dir = PathBuf::new();
    memory.config.snapshot_mode = SnapshotMode::Disabled;
    let payload_applies = memory.payload_applies.clone();
    let result = NodeOwner::start(
        RuntimeConfig::new(
            memory.config.clone(),
            vec![GroupConfig {
                group_id: GROUP,
                voters: vec![1],
            }],
        ),
        move |context: multiraft_net::FsmFactoryContext| {
            Ok(DiskCounter {
                db: DiskFsmDb::open(
                    DiskFsmDb::group_path(&fsm_root, context.group_id()),
                    context.group_id(),
                    DiskFsmOptions::new(b"counter-v1".to_vec()),
                )?,
                opened: None,
                payload_applies: payload_applies.clone(),
            })
        },
        deadline(),
    )
    .await;
    let error = format!("{:?}", result.err().unwrap());
    assert!(error.contains("requires the RocksDB Raft log"), "{error}");

    let mut file_log = nodes(root.path()).remove(0);
    file_log.config.log_storage_backend = LogStorageBackend::FileLog;
    file_log.config.file_log_sync_level = multiraft_core::FileLogSyncLevel::Data;
    file_log.config.snapshot_mode = SnapshotMode::Disabled;
    let error = format!("{:?}", start(&file_log).await.err().unwrap());
    assert!(error.contains("requires the RocksDB Raft log"), "{error}");

    let mut standby = nodes(root.path()).remove(0);
    standby.config.snapshot_mode = SnapshotMode::StandbyOffload;
    let error = format!("{:?}", start(&standby).await.err().unwrap());
    assert!(error.contains("without StandbyOffload"), "{error}");
}

/// Compact on every given node and wait until each published a checkpoint at
/// the submitted applied index and purged its log up to it. Returns that index.
async fn compact(handles: &[RuntimeHandle<DiskCounter>]) -> u64 {
    let mut index = None;
    for handle in handles {
        let submission = handle.request_compaction(GROUP, deadline()).await.unwrap();
        let target = submission.target.expect("applied position").index;
        assert!(index.is_none_or(|index| index == target), "converged nodes");
        index = Some(target);
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let status = handle
                    .local_storage_status(GROUP, deadline())
                    .await
                    .unwrap();
                let snapshot = status
                    .durable_snapshot
                    .as_ref()
                    .and_then(|s| s.last_log_id.map(|id| id.index));
                if snapshot == Some(target) && status.purged.is_some_and(|id| id.index >= target) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("compaction publishes a checkpoint and purges the log");
    }
    index.unwrap()
}

/// The node's single published checkpoint directory (`checkpoint-{index}-{nonce}`).
fn published_checkpoint(node: &Node) -> PathBuf {
    let workspace =
        DiskFsmDb::workspace_path(&DiskFsmDb::group_path(&node.config.data_dir, GROUP)).unwrap();
    let mut dirs: Vec<_> = std::fs::read_dir(workspace)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            let name = path.file_name().unwrap().to_string_lossy();
            name.starts_with("checkpoint-") && !name.contains('.') && path.is_dir()
        })
        .collect();
    assert_eq!(dirs.len(), 1, "one published checkpoint: {dirs:?}");
    dirs.pop().unwrap()
}

/// Copy a (flat) RocksDB checkpoint directory.
fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        assert!(entry.file_type().unwrap().is_file(), "{:?}", entry.path());
        std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
    }
}

/// Simulate power loss rolling the stopped node's FSM back: its live FSM
/// directory becomes a copy of `image`; the Raft log and workspace are untouched.
fn roll_back_fsm(node: &Node, image: &Path) {
    let live = DiskFsmDb::group_path(&node.config.data_dir, GROUP);
    std::fs::remove_dir_all(&live).unwrap();
    copy_dir(image, &live);
}

fn rollback_nodes(root: &Path, sync_writes: bool) -> Vec<Node> {
    let mut nodes = nodes(root);
    for node in &mut nodes {
        // The log is purged exactly up to each published checkpoint.
        node.config.retain_log_entries = 0;
        node.sync_writes = sync_writes;
    }
    nodes
}

async fn start_all(nodes: &[Node]) -> Vec<NodeOwner<DiskCounter>> {
    let (a, b, c) = tokio::join!(start(&nodes[0]), start(&nodes[1]), start(&nodes[2]));
    vec![a.unwrap(), b.unwrap(), c.unwrap()]
}

/// Write `count` deltas of 1; returns the new sum.
async fn write_ones(handles: &[RuntimeHandle<DiskCounter>], mut sum: u64, count: u64) -> u64 {
    for _ in 0..count {
        sum += 1;
        assert_eq!(write(handles, 1).await, sum);
    }
    sum
}

async fn rollback_to_checkpoint_replays_the_log_suffix(sync_writes: bool) {
    let root = tempfile::tempdir().unwrap();
    let nodes = rollback_nodes(root.path(), sync_writes);
    let mut owners = start_all(&nodes).await;
    let handles: Vec<_> = owners.iter().map(|o| o.handle()).collect();
    let sum = write_ones(&handles, 0, 5).await;
    converged(&handles, sum, 5).await;
    let checkpoint = compact(&handles).await;
    let sum = write_ones(&handles, sum, 4).await;
    let before = converged(&handles, sum, 9).await;
    let stopped_at = before[2].3;
    assert!(stopped_at > checkpoint);

    owners.pop().unwrap().shutdown(deadline()).await.unwrap();
    roll_back_fsm(&nodes[2], &published_checkpoint(&nodes[2]));
    let applies_before = nodes[2].payload_applies.load(Ordering::SeqCst);
    owners.push(start(&nodes[2]).await.expect("rolled-back FSM starts"));
    let handles: Vec<_> = owners.iter().map(|o| o.handle()).collect();
    let view = local(&handles[2]).await;
    assert_eq!(view.4, Some(checkpoint), "FSM reopened at the checkpoint");
    let after = converged(&handles, sum, 9).await;
    assert_eq!(after[2].3, stopped_at, "caught up to the committed index");
    assert_eq!(after[2].2, after[2].3, "FSM applied row matches the bridge");
    assert_eq!(
        nodes[2].payload_applies.load(Ordering::SeqCst) - applies_before,
        4,
        "exactly the writes after the checkpoint are replayed"
    );
    assert!(
        !DiskFsmDb::workspace_path(&DiskFsmDb::group_path(&nodes[2].config.data_dir, GROUP))
            .unwrap()
            .join("installed")
            .exists(),
        "replayed from the log, not by snapshot install"
    );
    drop(handles);
    for owner in owners {
        owner.shutdown(deadline()).await.unwrap();
    }
}

async fn rollback_below_the_purged_log_is_refused(sync_writes: bool) {
    let root = tempfile::tempdir().unwrap();
    let nodes = rollback_nodes(root.path(), sync_writes);
    let mut owners = start_all(&nodes).await;
    let handles: Vec<_> = owners.iter().map(|o| o.handle()).collect();
    let sum = write_ones(&handles, 0, 5).await;
    converged(&handles, sum, 5).await;
    let old = compact(&handles).await;
    let old_image = root.path().join("old-image");
    copy_dir(&published_checkpoint(&nodes[2]), &old_image);
    let sum = write_ones(&handles, sum, 4).await;
    converged(&handles, sum, 9).await;
    let purged = compact(&handles).await;
    assert!(purged > old);

    owners.pop().unwrap().shutdown(deadline()).await.unwrap();
    roll_back_fsm(&nodes[2], &old_image);
    let error = format!(
        "{:?}",
        start(&nodes[2])
            .await
            .err()
            .expect("an FSM behind the purged log must not start")
    );
    // No basis check compares the FSM with the purged log; the refusal comes
    // from OpenRaft re-installing the local published checkpoint at construction,
    // which the disk bridge rejects as an unverified (non-transport) snapshot.
    assert!(error.contains("stage: Construct"), "{error}");
    // OpenRaft 0.10.1 signatures carry no snapshot id: name the checkpoint by its cut.
    assert!(
        error.contains(&format!("index: {purged} }}), last_membership_log_id")),
        "{error}"
    );
    assert!(
        error.contains("disk snapshot was not verified at transport ingress"),
        "{error}"
    );
    let fsm = DiskFsmDb::open(
        DiskFsmDb::group_path(&nodes[2].config.data_dir, GROUP),
        GROUP,
        DiskFsmOptions::new(b"counter-v1".to_vec()),
    )
    .unwrap();
    let counter = DiskCounter {
        db: fsm,
        opened: None,
        payload_applies: Arc::default(),
    };
    assert_eq!(
        counter.facts(),
        (5, 5, old),
        "the refused FSM keeps its rolled-back state"
    );
    drop(counter);
    drop(handles);
    for owner in owners {
        owner.shutdown(deadline()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rollback_to_checkpoint_replays_the_log_suffix_batch_sync() {
    rollback_to_checkpoint_replays_the_log_suffix(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rollback_to_checkpoint_replays_the_log_suffix_per_entry_sync() {
    rollback_to_checkpoint_replays_the_log_suffix(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rollback_below_the_purged_log_is_refused_batch_sync() {
    rollback_below_the_purged_log_is_refused(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rollback_below_the_purged_log_is_refused_per_entry_sync() {
    rollback_below_the_purged_log_is_refused(true).await;
}
