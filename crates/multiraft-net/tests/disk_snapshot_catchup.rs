//! Disk snapshots over three gRPC nodes (RocksDB logs, NativeDurable): a
//! replica that was down while the others compacted their logs catches up by
//! a streamed checkpoint, survives a restart afterwards, and an injected
//! install failure leaves it on its old state until a clean retry.
use multiraft_core::{ClusterConfig, LogStorageBackend, SnapshotMode};
use multiraft_fsm::{AppliedState, ApplyOut, DiskState, GroupId, StateMachine};
use multiraft_net::{GroupConfig, NodeOwner, RuntimeConfig, RuntimeHandle};
use multiraft_store::disk_snapshot::{inject_install_fault, InstallFault};
use multiraft_store::rocksdb::{IteratorMode, WriteBatch};
use multiraft_store::{DiskFsmDb, DiskFsmOptions};
use std::{
    convert::Infallible,
    io,
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::time::Instant;

const GROUP: GroupId = 7;
const BLOB: usize = 64 * 1024;

struct DiskBlobs {
    db: DiskFsmDb,
}

fn options() -> DiskFsmOptions {
    let mut options = DiskFsmOptions::new(b"blobs-v1".to_vec());
    options.column_families = vec!["blobs".into()];
    options
}

impl DiskBlobs {
    fn open(path: &Path) -> io::Result<Self> {
        Ok(Self {
            db: DiskFsmDb::open(path, GROUP, options())?,
        })
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
            .iterator_cf(self.db.cf("blobs").unwrap(), IteratorMode::Start)
            .count()
    }
}

impl StateMachine for DiskBlobs {
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
        let mut effects = Vec::new();
        if let Some(data) = data {
            let delta = u64::from_be_bytes(data[..8].try_into().map_err(io::Error::other)?);
            let sum = self.sum() + delta;
            batch.put_cf(self.db.cf("default")?, b"sum", sum.to_be_bytes());
            if data.len() > 8 {
                batch.put_cf(self.db.cf("blobs")?, index.to_be_bytes(), &data[8..]);
            }
            effects = sum.to_be_bytes().to_vec();
        }
        self.db.commit(batch, applied)?;
        Ok(ApplyOut { effects })
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

async fn start(
    config: &ClusterConfig,
) -> Result<NodeOwner<DiskBlobs>, multiraft_net::RuntimeError> {
    let data_dir = config.data_dir.clone();
    NodeOwner::start(
        RuntimeConfig::new(
            config.clone(),
            vec![GroupConfig {
                group_id: GROUP,
                voters: vec![1, 2, 3],
            }],
        ),
        move |context: multiraft_net::FsmFactoryContext| {
            Ok(DiskBlobs::open(&DiskFsmDb::group_path(
                &data_dir,
                context.group_id(),
            ))?)
        },
        deadline(),
    )
    .await
}

fn payload(seed: u64, delta: u64) -> Vec<u8> {
    let mut data = delta.to_be_bytes().to_vec();
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    data.extend((0..BLOB).map(|_| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state as u8
    }));
    data
}

async fn write(handles: &[RuntimeHandle<DiskBlobs>], data: Vec<u8>) {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            for handle in handles {
                match handle
                    .propose(GROUP, data.clone(), Instant::now() + Duration::from_secs(2))
                    .await
                {
                    Ok(_) => return,
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

/// (sum, blob rows, applied index)
async fn local(handle: &RuntimeHandle<DiskBlobs>) -> Option<(u64, usize, u64)> {
    let view = handle
        .try_read_applied_with_watermark(GROUP, deadline(), |fsm| {
            Ok::<_, Infallible>((fsm.sum(), fsm.blobs()))
        })
        .await
        .ok()?;
    Some((view.value.0, view.value.1, view.applied?.index))
}

async fn converged(handles: &[RuntimeHandle<DiskBlobs>], sum: u64, blobs: usize) -> u64 {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let mut views = Vec::new();
            for handle in handles {
                views.push(local(handle).await);
            }
            if views.iter().all(|view| {
                view.is_some_and(|v| (v.0, v.1, v.2) == (sum, blobs, views[0].unwrap().2))
            }) {
                return views[0].unwrap().2;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("replicas converge")
}

/// Compact on every given node and wait until each purged its log up to the target.
async fn compact(handles: &[RuntimeHandle<DiskBlobs>]) {
    for handle in handles {
        let submission = handle.request_compaction(GROUP, deadline()).await.unwrap();
        let target = submission.target.expect("applied position").index;
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
                if snapshot >= Some(target) && status.purged.is_some_and(|id| id.index >= target) {
                    assert!(status
                        .durable_snapshot
                        .unwrap()
                        .snapshot_id
                        .starts_with("disk-v1-"));
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("compaction purges the log");
    }
}

fn configs(root: &Path) -> Vec<ClusterConfig> {
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
            config.retain_log_entries = 0;
            config.install_snapshot_timeout_ms = 10_000;
            config
        })
        .collect()
}

fn installed_marker(config: &ClusterConfig) -> Option<String> {
    let path = DiskFsmDb::workspace_path(&DiskFsmDb::group_path(&config.data_dir, GROUP))
        .unwrap()
        .join("installed");
    std::fs::read_to_string(path).ok()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lagging_replica_catches_up_by_streamed_checkpoint_and_fails_closed() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let root = tempfile::tempdir().unwrap();
    let configs = configs(root.path());
    let (a, b, c) = tokio::join!(start(&configs[0]), start(&configs[1]), start(&configs[2]));
    let mut owners = vec![a.unwrap(), b.unwrap(), c.unwrap()];
    let handles: Vec<_> = owners.iter().map(|o| o.handle()).collect();
    let mut sum = 0;
    for delta in 1..=3_u64 {
        sum += delta;
        write(&handles, delta.to_be_bytes().to_vec()).await;
    }
    converged(&handles, sum, 0).await;

    // Node 3 is down while blobs are written (far more than one chunk) and the
    // other two compact, so its next entries exist only in their snapshots.
    owners.pop().unwrap().shutdown(deadline()).await.unwrap();
    let mut blobs = 0;
    for seed in 0..48 {
        sum += 7;
        blobs += 1;
        write(&handles[..2], payload(seed, 7)).await;
    }
    compact(&handles[..2]).await;
    let restarted = start(&configs[2]).await.unwrap();
    owners.push(restarted);
    let handles: Vec<_> = owners.iter().map(|o| o.handle()).collect();
    let index = converged(&handles, sum, blobs).await;
    let status = handles[2]
        .local_storage_status(GROUP, deadline())
        .await
        .unwrap();
    let local_snapshot = status
        .durable_snapshot
        .expect("receiver published a checkpoint");
    assert!(
        local_snapshot.bytes > (blobs * BLOB) as u64,
        "{}",
        local_snapshot.bytes
    );
    assert!(
        installed_marker(&configs[2]).is_some(),
        "caught up by snapshot install"
    );

    // A restart right after the catch-up recovers from the installed store.
    owners.pop().unwrap().shutdown(deadline()).await.unwrap();
    owners.push(start(&configs[2]).await.unwrap());
    let handles: Vec<_> = owners.iter().map(|o| o.handle()).collect();
    assert!(converged(&handles, sum, blobs).await >= index);

    // Injected failure during the next install: the replica keeps its old state.
    owners.pop().unwrap().shutdown(deadline()).await.unwrap();
    let old = (sum, blobs);
    let marker_before = installed_marker(&configs[2]);
    for seed in 100..116 {
        sum += 3;
        blobs += 1;
        write(&handles[..2], payload(seed, 3)).await;
    }
    compact(&handles[..2]).await;
    inject_install_fault(Some(InstallFault::ErrorBeforeSwap));
    let failing = start(&configs[2]).await.unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        while installed_marker(&configs[2]) == marker_before {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("an install was attempted");
    tokio::time::sleep(Duration::from_millis(200)).await;
    let _ = failing.shutdown(deadline()).await;
    inject_install_fault(None);
    {
        let fsm = DiskBlobs::open(&DiskFsmDb::group_path(&configs[2].data_dir, GROUP)).unwrap();
        assert_eq!(
            (fsm.sum(), fsm.blobs()),
            old,
            "failed install left the old state"
        );
    }
    owners.push(start(&configs[2]).await.unwrap());
    let handles: Vec<_> = owners.iter().map(|o| o.handle()).collect();
    converged(&handles, sum, blobs).await;
    drop(handles);
    for owner in owners {
        owner.shutdown(deadline()).await.unwrap();
    }
}
