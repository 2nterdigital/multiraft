#![allow(dead_code)] // Shared by independent public consumer cases.
use multiraft_core::{ClusterConfig, FileLogSyncLevel, SnapshotMode};
use multiraft_fsm::{ApplyOut, CaptureError, CaptureRefusal, GroupId, StateMachine};
use multiraft_net::{
    CompactionProgress, FsmFactoryContext, GroupConfig, LocalStorageStatus, NodeOwner,
    RuntimeConfig, RuntimeError, RuntimeHandle, StateMachineFactory,
};
use std::fs::{self, OpenOptions};
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::Instant;

pub const GROUP: GroupId = 7;
pub const IMAGE_BYTES: usize = 5 * 1024 * 1024;
pub fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(8)
}
#[derive(Clone, Default)]
pub struct Gate {
    pub entered: Arc<Notify>,
    state: Arc<(Mutex<bool>, Condvar)>,
}
impl Gate {
    pub fn wait(&self) {
        self.entered.notify_one();
        let (lock, changed) = &*self.state;
        let (open, timeout) = changed
            .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(3), |open| !*open)
            .unwrap();
        assert!(
            *open && !timeout.timed_out(),
            "bounded restore gate must be released"
        );
    }
    pub fn release(&self) {
        *self.state.0.lock().unwrap() = true;
        self.state.1.notify_all();
    }
    pub async fn entered(&self) {
        tokio::time::timeout(Duration::from_secs(8), self.entered.notified())
            .await
            .unwrap();
    }
}
#[derive(Default)]
pub struct Stats {
    pub value: AtomicI64,
    pub released: AtomicUsize,
    pub restored: AtomicUsize,
    pub rejected: AtomicUsize,
    pub reject_restore: AtomicBool,
    pub rejection: Notify,
    pub validations: Mutex<Vec<i64>>,
}
#[derive(Clone)]
pub struct Factory {
    pub root: PathBuf,
    pub stats: Arc<Stats>,
    pub minimum: i64,
    pub exact: Option<i64>,
    pub gate: Option<Gate>,
}
impl StateMachineFactory<Image> for Factory {
    fn create(&self, _: FsmFactoryContext) -> anyhow::Result<Image> {
        let lease = self.root.join("consumer.lease");
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lease)?;
        self.stats.value.store(0, Ordering::SeqCst);
        Ok(Image {
            value: 0,
            factory: self.clone(),
            lease,
        })
    }
    fn validate_recovered(&self, _: FsmFactoryContext, fsm: &Image) -> anyhow::Result<()> {
        assert!(fsm.value >= self.minimum);
        if let Some(exact) = self.exact {
            assert_eq!(fsm.value, exact);
        }
        self.stats.validations.lock().unwrap().push(fsm.value);
        Ok(())
    }
}
pub struct Image {
    pub value: i64,
    factory: Factory,
    lease: PathBuf,
}
impl Drop for Image {
    fn drop(&mut self) {
        fs::remove_file(&self.lease).unwrap();
        self.factory.stats.released.fetch_add(1, Ordering::SeqCst);
    }
}
impl StateMachine for Image {
    type Error = std::io::Error;
    fn apply(&mut self, _: GroupId, _: u64, data: &[u8]) -> Result<ApplyOut, Self::Error> {
        let delta = i64::from_le_bytes(
            data.try_into()
                .map_err(|_| std::io::Error::other("invalid counter command"))?,
        );
        self.value += delta;
        self.factory.stats.value.store(self.value, Ordering::SeqCst);
        Ok(ApplyOut {
            effects: self.value.to_le_bytes().to_vec(),
        })
    }
    fn snapshot(&self, _: GroupId) -> Result<Vec<u8>, Self::Error> {
        Err(std::io::Error::other(
            "legacy unbounded serializer must never be used",
        ))
    }
    fn freeze_bounded(&self, _: GroupId, cap: usize) -> Result<Vec<u8>, CaptureError<Self::Error>> {
        if cap < IMAGE_BYTES {
            return Err(CaptureError::Refused(CaptureRefusal::SizeLimit));
        }
        let mut image = vec![7; IMAGE_BYTES];
        image[..8].copy_from_slice(&self.value.to_le_bytes());
        Ok(image)
    }
    fn restore(&mut self, _: GroupId, image: &[u8]) -> Result<(), Self::Error> {
        if image.len() != IMAGE_BYTES || image[8..].iter().any(|&byte| byte != 7) {
            return Err(std::io::Error::other("invalid bounded image"));
        }
        self.factory.stats.restored.fetch_add(1, Ordering::SeqCst);
        if self.factory.stats.reject_restore.load(Ordering::SeqCst) {
            self.factory.stats.rejected.fetch_add(1, Ordering::SeqCst);
            self.factory.stats.rejection.notify_one();
            return Err(std::io::Error::other("controlled consumer image rejection"));
        }
        if let Some(gate) = &self.factory.gate {
            gate.wait();
        }
        self.value = i64::from_le_bytes(image[..8].try_into().unwrap());
        self.factory.stats.value.store(self.value, Ordering::SeqCst);
        Ok(())
    }
}
pub struct Cluster {
    pub root: PathBuf,
    pub peers: Vec<(u64, SocketAddr)>,
    pub owners: Vec<Option<NodeOwner<Image>>>,
    pub factories: Vec<Factory>,
}
impl Cluster {
    pub fn new(case: &str) -> Self {
        let scratch =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/issue-170/consumers");
        fs::create_dir_all(&scratch).unwrap();
        // Retain real attempts and failed disks for diagnosis; no test regenerates
        // or replaces a failed directory to make recovery pass.
        let root = tempfile::Builder::new()
            .prefix(case)
            .tempdir_in(scratch)
            .unwrap()
            .keep();
        let listeners: Vec<_> = (0..3)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let peers = listeners
            .iter()
            .enumerate()
            .map(|(i, listener)| ((i + 1) as u64, listener.local_addr().unwrap()))
            .collect();
        drop(listeners);
        let factories = (1..=3)
            .map(|id| {
                let node = root.join(format!("node-{id}"));
                fs::create_dir_all(&node).unwrap();
                Factory {
                    root: node,
                    stats: Arc::new(Stats::default()),
                    minimum: 0,
                    exact: None,
                    gate: None,
                }
            })
            .collect();
        Self {
            root,
            peers,
            owners: vec![None, None, None],
            factories,
        }
    }
    pub fn config(&self, index: usize) -> RuntimeConfig {
        let mut cluster = ClusterConfig::for_test((index + 1) as u64, &[1, 2, 3]);
        cluster.peers = self.peers.clone();
        cluster.data_dir = self.factories[index].root.clone();
        cluster.file_log_sync_level = FileLogSyncLevel::Data;
        cluster.snapshot_mode = SnapshotMode::NativeDurable;
        cluster.max_snapshot_bytes = 8 * 1024 * 1024;
        cluster.install_snapshot_timeout_ms = 5000;
        cluster.snapshot_keep = 1;
        cluster.retain_log_entries = 0;
        RuntimeConfig::new(
            cluster,
            vec![GroupConfig {
                group_id: GROUP,
                voters: vec![1, 2, 3],
            }],
        )
    }
    pub async fn start(&mut self, index: usize) {
        assert!(self.owners[index].is_none());
        self.owners[index] = Some(
            NodeOwner::start(
                self.config(index),
                self.factories[index].clone(),
                deadline(),
            )
            .await
            .unwrap(),
        );
    }
    pub async fn start_all(&mut self) {
        let (one, two, three) = tokio::join!(
            NodeOwner::start(self.config(0), self.factories[0].clone(), deadline()),
            NodeOwner::start(self.config(1), self.factories[1].clone(), deadline()),
            NodeOwner::start(self.config(2), self.factories[2].clone(), deadline()),
        );
        self.owners = vec![Some(one.unwrap()), Some(two.unwrap()), Some(three.unwrap())];
    }
    pub fn handle(&self, index: usize) -> RuntimeHandle<Image> {
        self.owners[index].as_ref().unwrap().handle()
    }
    pub async fn leader(&self) -> usize {
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                for index in 0..3 {
                    if let Some(owner) = &self.owners[index] {
                        if owner
                            .handle()
                            .read_linearizable(GROUP, deadline(), |fsm| fsm.value)
                            .await
                            .is_ok()
                        {
                            return index;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap()
    }
    pub async fn add(&self, delta: i64) {
        let leader = self.leader().await;
        self.handle(leader)
            .propose(GROUP, delta.to_le_bytes().to_vec(), deadline())
            .await
            .unwrap();
    }
    pub async fn wait_value(&self, index: usize, expected: i64) {
        tokio::time::timeout(Duration::from_secs(8), async {
            while self.factories[index].stats.value.load(Ordering::SeqCst) != expected {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
    pub async fn wait_all(&self, expected: i64) {
        for index in 0..3 {
            if self.owners[index].is_some() {
                self.wait_value(index, expected).await;
            }
        }
    }
    pub async fn status(&self, index: usize) -> LocalStorageStatus {
        loop {
            match self
                .handle(index)
                .local_storage_status(GROUP, deadline())
                .await
            {
                Ok(status) => return status,
                Err(RuntimeError::MaintenanceRejected(
                    multiraft_net::CompactionRejection::Busy,
                )) => tokio::task::yield_now().await,
                error => panic!("unexpected storage observation: {error:?}"),
            }
        }
    }
    pub async fn compact_live(&self) {
        for index in 0..3 {
            if self.owners[index].is_none() {
                continue;
            }
            self.handle(index)
                .request_compaction(GROUP, deadline())
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(8), async {
                while self.status(index).await.progress != CompactionProgress::CompletedObserved {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
        }
    }
    pub async fn stop(&mut self, index: usize, allow_native_failure: bool) {
        if let Some(owner) = self.owners[index].take() {
            let result = owner.shutdown(deadline()).await;
            if !allow_native_failure {
                result.unwrap();
            }
            assert!(!self.factories[index].root.join("consumer.lease").exists());
            TcpListener::bind(self.peers[index].1).unwrap();
        }
    }
    pub async fn stop_all(&mut self) {
        for index in 0..3 {
            self.stop(index, false).await;
        }
    }
    pub async fn dropped_reclaimed(&self, index: usize, releases: usize) {
        tokio::time::timeout(Duration::from_secs(8), async {
            while self.factories[index].stats.released.load(Ordering::SeqCst) != releases
                || TcpListener::bind(self.peers[index].1).is_err()
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(!self.factories[index].root.join("consumer.lease").exists());
    }
}

/// Mutate only the persisted, fixed alpha.30 record vector after all peers stop.
/// Reuse an actual normal frame; TypeConfig's committed leader(term,node)/index
/// prefix is3 little-endian u64s, with the index at bodyoffset16. No native handle
/// or alternate replay is used. The forced commit record is deliberately unchanged.
pub fn append_uncommitted_tail(root: &std::path::Path) {
    let path = root.join("group-7/log.bin");
    let bytes = fs::read(&path).unwrap();
    let mut position = 0;
    let mut last_index = 0;
    let mut normal = None;
    while position < bytes.len() {
        let size = u32::from_le_bytes(bytes[position..position + 4].try_into().unwrap()) as usize;
        let end = position + 4 + size;
        let frame = &bytes[position..end];
        assert!(size >= 28);
        last_index = u64::from_le_bytes(frame[20..28].try_into().unwrap());
        // A normal opaque eight-byte delta has a44-byte native entry body.
        if size == 44 {
            normal = Some(frame.to_vec());
        }
        position = end;
    }
    let mut extra = normal.expect("actual committed normal tail frame");
    extra[20..28].copy_from_slice(&last_index.checked_add(1).unwrap().to_le_bytes());
    let end = extra.len();
    extra[end - 8..].copy_from_slice(&9_i64.to_le_bytes());
    fs::write(root.join("original-before-uncommitted-log.bin"), &bytes).unwrap();
    let mut with_extra = bytes;
    with_extra.extend_from_slice(&extra);
    fs::write(path, with_extra).unwrap();
}
