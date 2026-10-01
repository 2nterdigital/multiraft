#![allow(dead_code)] // Shared by independently scoped consumer executables.
use std::fs::{self, OpenOptions};
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use multiraft_core::{ClusterConfig, FileLogSyncLevel, MultiRaftError, SnapshotMode};
use multiraft_fsm::{ApplyOut, CaptureError, CaptureRefusal, CounterFsm, GroupId, StateMachine};
use multiraft_net::{
    CompactionProgress, CompactionRejection, FsmFactoryContext, GroupConfig, LocalStorageStatus,
    NodeOwner, RuntimeConfig, RuntimeError, RuntimeHandle, StateMachineFactory,
};
use sha2::{Digest, Sha256};
use tokio::sync::Notify;
use tokio::time::Instant;

pub const GROUP: GroupId = 7;
pub fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
pub fn address() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}
pub fn old_disk() -> tempfile::TempDir {
    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/legacy-native-alpha30");
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(fixture.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(
        manifest["source"],
        "fe832257b0c744faf93f9c7a4ab1da63d37d6a46"
    );
    let scratch = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/issue-169/consumers");
    fs::create_dir_all(&scratch).unwrap();
    let root = tempfile::Builder::new()
        .prefix("maintenance-")
        .tempdir_in(scratch)
        .unwrap();
    for file in manifest["files"].as_array().unwrap() {
        let relative = file["path"].as_str().unwrap();
        let bytes = fs::read(fixture.join("disk").join(relative)).unwrap();
        assert_eq!(format!("{:x}", Sha256::digest(&bytes)), file["sha256"]);
        let output = root.path().join(relative);
        fs::create_dir_all(output.parent().unwrap()).unwrap();
        fs::write(output, bytes).unwrap();
    }
    root
}
#[derive(Clone, Default)]
pub struct Gate {
    pub entered: Arc<Notify>,
    state: Arc<(Mutex<bool>, Condvar)>,
}
impl Gate {
    pub fn wait(&self) {
        self.entered.notify_one();
        let (state, changed) = &*self.state;
        let open = state.lock().unwrap();
        let (open, timeout) = changed
            .wait_timeout_while(open, Duration::from_secs(3), |open| !*open)
            .unwrap();
        assert!(
            *open && !timeout.timed_out(),
            "consumer gate release must be bounded"
        );
    }
    pub fn release(&self) {
        *self.state.0.lock().unwrap() = true;
        self.state.1.notify_all();
    }
    pub async fn entered(&self) {
        tokio::time::timeout(Duration::from_secs(2), self.entered.notified())
            .await
            .unwrap();
    }
}
#[derive(Clone)]
pub struct Factory {
    pub root: PathBuf,
    pub released: Arc<AtomicUsize>,
    pub captures: Arc<AtomicUsize>,
    pub legacy_serializations: Arc<AtomicUsize>,
    pub gate: Option<Gate>,
    pub unsupported: bool,
    pub panic_capture: bool,
    pub expected: i64,
}
impl Factory {
    pub fn new(root: &Path) -> Self {
        Self {
            root: root.to_owned(),
            released: Arc::new(AtomicUsize::new(0)),
            captures: Arc::new(AtomicUsize::new(0)),
            legacy_serializations: Arc::new(AtomicUsize::new(0)),
            gate: None,
            unsupported: false,
            panic_capture: false,
            expected: 15,
        }
    }
    pub fn config(&self, address: SocketAddr, groups: &[GroupId]) -> RuntimeConfig {
        let mut cluster = ClusterConfig::for_test(1, &[1]);
        cluster.peers = vec![(1, address)];
        cluster.data_dir = self.root.clone();
        cluster.file_log_sync_level = FileLogSyncLevel::Data;
        cluster.snapshot_mode = SnapshotMode::NativeDurable;
        cluster.retain_log_entries = 2;
        RuntimeConfig::new(
            cluster,
            groups
                .iter()
                .map(|&group_id| GroupConfig {
                    group_id,
                    voters: vec![1],
                })
                .collect(),
        )
    }
    pub async fn start(&self, address: SocketAddr, groups: &[GroupId]) -> NodeOwner<Consumer> {
        NodeOwner::start(self.config(address, groups), self.clone(), deadline())
            .await
            .unwrap()
    }
}
pub struct Consumer {
    counter: CounterFsm,
    factory: Factory,
    lease: PathBuf,
}
impl Drop for Consumer {
    fn drop(&mut self) {
        fs::remove_file(&self.lease).unwrap();
        self.factory.released.fetch_add(1, Ordering::SeqCst);
    }
}
impl StateMachine for Consumer {
    type Error = <CounterFsm as StateMachine>::Error;
    fn apply(&mut self, group: GroupId, index: u64, bytes: &[u8]) -> Result<ApplyOut, Self::Error> {
        self.counter.apply(group, index, bytes)
    }
    fn snapshot(&self, group: GroupId) -> Result<Vec<u8>, Self::Error> {
        self.factory
            .legacy_serializations
            .fetch_add(1, Ordering::SeqCst);
        self.counter.snapshot(group)
    }
    fn restore(&mut self, group: GroupId, bytes: &[u8]) -> Result<(), Self::Error> {
        self.counter.restore(group, bytes)
    }
    fn freeze_bounded(
        &self,
        group: GroupId,
        cap: usize,
    ) -> Result<Vec<u8>, CaptureError<Self::Error>> {
        self.factory.captures.fetch_add(1, Ordering::SeqCst);
        if self.factory.unsupported {
            return Err(CaptureError::Refused(CaptureRefusal::Unsupported));
        }
        if let Some(gate) = &self.factory.gate {
            gate.wait();
        }
        assert!(
            !self.factory.panic_capture,
            "controlled consumer capture panic"
        );
        self.counter.freeze_bounded(group, cap)
    }
}
impl StateMachineFactory<Consumer> for Factory {
    fn create(&self, context: FsmFactoryContext) -> anyhow::Result<Consumer> {
        let lease = self
            .root
            .join(format!("consumer-{}.lease", context.group_id()));
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lease)?;
        Ok(Consumer {
            counter: CounterFsm::new(),
            factory: self.clone(),
            lease,
        })
    }
    fn validate_recovered(&self, context: FsmFactoryContext, fsm: &Consumer) -> anyhow::Result<()> {
        assert_eq!(
            fsm.counter.value(context.group_id()),
            if context.group_id() == GROUP {
                self.expected
            } else {
                0
            }
        );
        Ok(())
    }
}
pub async fn read(handle: &RuntimeHandle<Consumer>, group: GroupId) -> i64 {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match handle
                .read_linearizable(group, deadline(), |fsm| fsm.counter.value(group))
                .await
            {
                Ok(value) => return value,
                Err(RuntimeError::Source(MultiRaftError::NotLeader { .. })) => {
                    tokio::time::sleep(Duration::from_millis(5)).await
                }
                error => panic!("unexpected read: {error:?}"),
            }
        }
    })
    .await
    .unwrap()
}
pub async fn status(handle: &RuntimeHandle<Consumer>, group: GroupId) -> LocalStorageStatus {
    loop {
        match handle.local_storage_status(group, deadline()).await {
            Ok(status) => return status,
            Err(RuntimeError::MaintenanceRejected(CompactionRejection::Busy)) => {
                tokio::task::yield_now().await
            }
            error => panic!("unexpected status: {error:?}"),
        }
    }
}
pub async fn completed(handle: &RuntimeHandle<Consumer>, group: GroupId) -> LocalStorageStatus {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let observation = status(handle, group).await;
            if observation.progress == CompactionProgress::CompletedObserved {
                return observation;
            }
            assert_ne!(observation.progress, CompactionProgress::Unconfirmed);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap()
}
pub fn reusable(factory: &Factory, address: SocketAddr, count: usize) {
    assert_eq!(factory.released.load(Ordering::SeqCst), count);
    assert!(!factory.root.join("consumer-7.lease").exists());
    assert!(!factory.root.join("consumer-8.lease").exists());
    TcpListener::bind(address).unwrap();
}
pub async fn eventually_reusable(factory: &Factory, address: SocketAddr, count: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if factory.released.load(Ordering::SeqCst) == count
                && TcpListener::bind(address).is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    reusable(factory, address, count);
}
