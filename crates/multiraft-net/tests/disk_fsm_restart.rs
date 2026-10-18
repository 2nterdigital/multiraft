//! Disk-backed FSM mode over three gRPC nodes with RocksDB logs: peer restart
//! and cold restart recover state and applied index from each node's FSM
//! RocksDB, replaying only the unapplied suffix.
use multiraft_core::{ClusterConfig, LogStorageBackend, SnapshotMode};
use multiraft_fsm::{AppliedState, ApplyOut, DiskState, GroupId, StateMachine};
use multiraft_net::{GroupConfig, NodeOwner, RuntimeConfig, RuntimeHandle};
use multiraft_store::rocksdb::WriteBatch;
use multiraft_store::{DiskFsmDb, DiskFsmOptions};
use std::{
    convert::Infallible,
    io,
    net::{SocketAddr, TcpListener},
    path::PathBuf,
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
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}

#[derive(Clone)]
struct Node {
    config: ClusterConfig,
    payload_applies: Arc<AtomicUsize>,
}

async fn start(node: &Node) -> Result<NodeOwner<DiskCounter>, multiraft_net::RuntimeError> {
    let data_dir: PathBuf = node.config.data_dir.clone();
    let payload_applies = node.payload_applies.clone();
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
            let db = DiskFsmDb::open(
                path,
                context.group_id(),
                DiskFsmOptions::new(b"counter-v1".to_vec()),
            )?;
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
    assert!(result.is_err(), "memory logs cannot back a disk FSM");

    let mut file_log = nodes(root.path()).remove(0);
    file_log.config.log_storage_backend = LogStorageBackend::FileLog;
    file_log.config.file_log_sync_level = multiraft_core::FileLogSyncLevel::Data;
    file_log.config.snapshot_mode = SnapshotMode::Disabled;
    let result = start(&file_log).await;
    assert!(result.is_err(), "FileLog may apply before its group commit");

    let mut standby = nodes(root.path()).remove(0);
    standby.config.snapshot_mode = SnapshotMode::StandbyOffload;
    let result = start(&standby).await;
    assert!(result.is_err(), "StandbyOffload hot-dumps whole images");
}
