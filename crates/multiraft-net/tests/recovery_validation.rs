//! External consumer: only owned public APIs, consumer data and real lease release.
use std::fs::{self, OpenOptions};
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use multiraft_core::{ClusterConfig, FileLogSyncLevel, MultiRaftError, SnapshotMode};
use multiraft_fsm::{ApplyOut, CounterFsm, GroupId, StateMachine};
use multiraft_net::{
    FsmFactoryContext, GroupConfig, NodeOwner, RuntimeConfig, RuntimeError, RuntimeHandle,
    StateMachineFactory,
};
use sha2::{Digest, Sha256};
use tokio::sync::Notify;
use tokio::time::Instant;

const GROUP: GroupId = 7;
const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/legacy-native-alpha30"
);
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
fn address() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}
fn verify_and_copy_fixture() -> tempfile::TempDir {
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(Path::new(FIXTURE).join("manifest.json")).unwrap())
            .unwrap();
    assert_eq!(
        manifest["source"],
        "fe832257b0c744faf93f9c7a4ab1da63d37d6a46"
    );
    assert_eq!(manifest["expected_value"], 15);
    let scratch = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/issue-167/consumers");
    fs::create_dir_all(&scratch).unwrap();
    let destination = tempfile::Builder::new()
        .prefix("legacy-consumer-")
        .tempdir_in(scratch)
        .unwrap();
    for file in manifest["files"].as_array().unwrap() {
        let relative = file["path"].as_str().unwrap();
        let bytes = fs::read(Path::new(FIXTURE).join("disk").join(relative)).unwrap();
        assert_eq!(bytes.len() as u64, file["bytes"].as_u64().unwrap());
        assert_eq!(format!("{:x}", Sha256::digest(&bytes)), file["sha256"]);
        let target = destination.path().join(relative);
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::write(target, bytes).unwrap();
    }
    destination
}

struct LeasedCounter {
    counter: CounterFsm,
    lease: PathBuf,
    released: Arc<AtomicUsize>,
    restores: usize,
}
impl Drop for LeasedCounter {
    fn drop(&mut self) {
        // The consumer lease survives all native shutdown acknowledgements until
        // the actual FSM destructor runs. Factory retry uses exclusive creation.
        fs::remove_file(&self.lease).unwrap();
        self.released.fetch_add(1, Ordering::SeqCst);
    }
}
impl StateMachine for LeasedCounter {
    type Error = <CounterFsm as StateMachine>::Error;
    fn apply(&mut self, group: GroupId, index: u64, bytes: &[u8]) -> Result<ApplyOut, Self::Error> {
        self.counter.apply(group, index, bytes)
    }
    fn snapshot(&self, group: GroupId) -> Result<Vec<u8>, Self::Error> {
        self.counter.snapshot(group)
    }
    fn restore(&mut self, group: GroupId, bytes: &[u8]) -> Result<(), Self::Error> {
        self.restores += 1;
        self.counter.restore(group, bytes)
    }
}

#[derive(Clone, Default)]
struct Gate {
    entered: Arc<Notify>,
    state: Arc<(Mutex<bool>, Condvar)>,
}
impl Gate {
    fn wait(&self) {
        self.entered.notify_one();
        let (lock, changed) = &*self.state;
        let open = lock.lock().unwrap();
        let (open, result) = changed
            .wait_timeout_while(open, Duration::from_secs(3), |open| !*open)
            .unwrap();
        assert!(
            *open && !result.timed_out(),
            "consumer gate must be released"
        );
    }
    fn release(&self) {
        *self.state.0.lock().unwrap() = true;
        self.state.1.notify_all();
    }
}
#[derive(Clone)]
struct ConsumerFactory {
    root: PathBuf,
    released: Arc<AtomicUsize>,
    validations: Arc<AtomicUsize>,
    expected: i64,
    refuse: bool,
    panic: bool,
    gate: Option<Gate>,
}
impl ConsumerFactory {
    fn new(root: &Path) -> Self {
        Self {
            root: root.to_owned(),
            released: Arc::new(AtomicUsize::new(0)),
            validations: Arc::new(AtomicUsize::new(0)),
            expected: 15,
            refuse: false,
            panic: false,
            gate: None,
        }
    }
    fn config(&self, address: SocketAddr, declared: bool) -> RuntimeConfig {
        let mut cluster = ClusterConfig::for_test(1, &[1]);
        cluster.peers = vec![(1, address)];
        cluster.data_dir = self.root.clone();
        cluster.file_log_sync_level = FileLogSyncLevel::Data;
        cluster.snapshot_mode = SnapshotMode::NativeDurable;
        cluster.retain_log_entries = 2;
        RuntimeConfig::new(
            cluster,
            if declared {
                vec![GroupConfig {
                    group_id: GROUP,
                    voters: vec![1],
                }]
            } else {
                vec![]
            },
        )
    }
    async fn start(&self, address: SocketAddr) -> Result<NodeOwner<LeasedCounter>, RuntimeError> {
        NodeOwner::start(self.config(address, true), self.clone(), deadline()).await
    }
}
impl StateMachineFactory<LeasedCounter> for ConsumerFactory {
    fn create(&self, context: FsmFactoryContext) -> anyhow::Result<LeasedCounter> {
        assert_eq!((context.node_id(), context.group_id()), (1, GROUP));
        let lease = self.root.join("consumer.lease");
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lease)?;
        Ok(LeasedCounter {
            counter: CounterFsm::new(),
            lease,
            released: self.released.clone(),
            restores: 0,
        })
    }
    fn validate_recovered(
        &self,
        context: FsmFactoryContext,
        fsm: &LeasedCounter,
    ) -> anyhow::Result<()> {
        assert_eq!((context.node_id(), context.group_id()), (1, GROUP));
        assert!(fsm.restores > 0, "old native snapshot was restored");
        assert_eq!(
            fsm.counter.value(GROUP),
            self.expected,
            "old committed tail replayed"
        );
        self.validations.fetch_add(1, Ordering::SeqCst);
        if let Some(gate) = &self.gate {
            gate.wait();
        }
        assert!(!self.panic, "controlled recovery callback panic");
        anyhow::ensure!(!self.refuse, "consumer refuses recovered image");
        Ok(())
    }
}
async fn read(handle: &RuntimeHandle<LeasedCounter>) -> i64 {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match handle
                .read_linearizable(GROUP, deadline(), |fsm| fsm.counter.value(GROUP))
                .await
            {
                Ok(value) => return value,
                Err(RuntimeError::Source(MultiRaftError::NotLeader { .. })) => {
                    tokio::time::sleep(Duration::from_millis(5)).await
                }
                error => panic!("unexpected authority result: {error:?}"),
            }
        }
    })
    .await
    .unwrap()
}
fn assert_reusable(factory: &ConsumerFactory, address: SocketAddr, expected_drops: usize) {
    assert_eq!(factory.released.load(Ordering::SeqCst), expected_drops);
    assert!(!factory.root.join("consumer.lease").exists());
    TcpListener::bind(address).unwrap();
    let lease = factory.root.join("consumer.lease");
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&lease)
        .unwrap();
    fs::remove_file(lease).unwrap();
}
async fn eventually_reusable(factory: &ConsumerFactory, address: SocketAddr, drops: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if factory.released.load(Ordering::SeqCst) == drops
                && TcpListener::bind(address).is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_reusable(factory, address, drops);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_snapshot_and_tail_recover_validate_read_write_and_restart() {
    let disk = verify_and_copy_fixture();
    let mut factory = ConsumerFactory::new(disk.path());
    let address = address();
    let owner = factory.start(address).await.unwrap();
    let handle = owner.handle();
    assert_eq!(factory.validations.load(Ordering::SeqCst), 1);
    assert_eq!(read(&handle).await, 15);
    handle
        .create_group(
            GroupConfig {
                group_id: GROUP,
                voters: vec![1],
            },
            deadline(),
        )
        .await
        .unwrap();
    assert_eq!(factory.validations.load(Ordering::SeqCst), 1);
    handle
        .propose(GROUP, CounterFsm::encode_add(2, 12), deadline())
        .await
        .unwrap();
    assert_eq!(read(&handle).await, 17);
    owner.shutdown(deadline()).await.unwrap();
    assert_reusable(&factory, address, 1);
    factory.expected = 17;
    let owner = factory.start(address).await.unwrap();
    assert_eq!(read(&owner.handle()).await, 17);
    owner.shutdown(deadline()).await.unwrap();
    assert_reusable(&factory, address, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejection_releases_old_disk_lease_and_port_before_retry() {
    let disk = verify_and_copy_fixture();
    let mut factory = ConsumerFactory::new(disk.path());
    factory.refuse = true;
    let address = address();
    let error = match factory.start(address).await {
        Err(error) => error,
        Ok(_) => panic!("rejected image cannot publish an owner"),
    };
    match error {
        RuntimeError::Source(MultiRaftError::Other(source)) => {
            assert!(source.to_string().contains("validate recovered FSM"));
            assert!(format!("{source:#}").contains("consumer refuses recovered image"));
        }
        error => panic!("unexpected rejection: {error:?}"),
    }
    assert_reusable(&factory, address, 1);
    factory.refuse = false;
    let owner = factory.start(address).await.unwrap();
    assert_eq!(read(&owner.handle()).await, 15);
    owner.shutdown(deadline()).await.unwrap();
    assert_reusable(&factory, address, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn validation_panic_still_joins_native_and_actual_fsm_owners() {
    let disk = verify_and_copy_fixture();
    let mut factory = ConsumerFactory::new(disk.path());
    factory.panic = true;
    let address = address();
    assert!(matches!(
        factory.start(address).await,
        Err(RuntimeError::Source(_))
    ));
    assert_reusable(&factory, address, 1);
    factory.panic = false;
    factory
        .start(address)
        .await
        .unwrap()
        .shutdown(deadline())
        .await
        .unwrap();
    assert_reusable(&factory, address, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dynamic_group_rejection_fences_existing_handles_and_waits_for_release() {
    let disk = verify_and_copy_fixture();
    let mut factory = ConsumerFactory::new(disk.path());
    factory.refuse = true;
    let address = address();
    let owner = NodeOwner::start(factory.config(address, false), factory.clone(), deadline())
        .await
        .unwrap();
    let handle = owner.handle();
    assert!(handle
        .create_group(
            GroupConfig {
                group_id: GROUP,
                voters: vec![1]
            },
            deadline()
        )
        .await
        .is_err());
    assert!(matches!(
        handle.propose(GROUP, vec![], deadline()).await,
        Err(RuntimeError::Closed)
    ));
    assert_reusable(&factory, address, 1);
    owner.shutdown(deadline()).await.unwrap();
    factory.refuse = false;
    factory
        .start(address)
        .await
        .unwrap()
        .shutdown(deadline())
        .await
        .unwrap();
    assert_reusable(&factory, address, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_owner_releases_legacy_disk_and_lease_despite_retained_handle() {
    let disk = verify_and_copy_fixture();
    let factory = ConsumerFactory::new(disk.path());
    let address = address();
    let owner = factory.start(address).await.unwrap();
    let handle = owner.handle();
    drop(owner);
    assert!(matches!(
        handle.propose(GROUP, vec![], deadline()).await,
        Err(RuntimeError::Closed)
    ));
    eventually_reusable(&factory, address, 1).await;
    factory
        .start(address)
        .await
        .unwrap()
        .shutdown(deadline())
        .await
        .unwrap();
    assert_reusable(&factory, address, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_validation_start_keeps_rollback_owner_until_actual_release() {
    let disk = verify_and_copy_fixture();
    let mut factory = ConsumerFactory::new(disk.path());
    let gate = Gate::default();
    factory.gate = Some(gate.clone());
    let address = address();
    let start_factory = factory.clone();
    let start = tokio::spawn(async move { start_factory.start(address).await });
    tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .unwrap();
    start.abort();
    assert!(start.await.err().unwrap().is_cancelled());
    assert_eq!(factory.released.load(Ordering::SeqCst), 0);
    assert!(OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(factory.root.join("consumer.lease"))
        .is_err());
    gate.release();
    eventually_reusable(&factory, address, 1).await;
    factory.gate = None;
    factory
        .start(address)
        .await
        .unwrap()
        .shutdown(deadline())
        .await
        .unwrap();
    assert_reusable(&factory, address, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_shutdown_wait_keeps_legacy_lease_until_read_and_fsm_release() {
    let disk = verify_and_copy_fixture();
    let factory = ConsumerFactory::new(disk.path());
    let address = address();
    let owner = factory.start(address).await.unwrap();
    let handle = owner.handle();
    assert_eq!(read(&handle).await, 15);
    let gate = Gate::default();
    let query_gate = gate.clone();
    let query_handle = handle.clone();
    let read = tokio::spawn(async move {
        query_handle
            .read_linearizable(GROUP, deadline(), move |_| query_gate.wait())
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .unwrap();
    let stop = tokio::spawn(owner.shutdown(deadline()));
    loop {
        if matches!(
            handle.propose(GROUP, vec![], Instant::now()).await,
            Err(RuntimeError::Closed)
        ) {
            break;
        }
        tokio::task::yield_now().await;
    }
    stop.abort();
    assert!(stop.await.unwrap_err().is_cancelled());
    assert_eq!(factory.released.load(Ordering::SeqCst), 0);
    gate.release();
    let _ = read.await.unwrap();
    eventually_reusable(&factory, address, 1).await;
    factory
        .start(address)
        .await
        .unwrap()
        .shutdown(deadline())
        .await
        .unwrap();
    assert_reusable(&factory, address, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_recovery_failure_skips_validation_releases_lease_and_can_retry() {
    let disk = verify_and_copy_fixture();
    let factory = ConsumerFactory::new(disk.path());
    let state = disk.path().join("group-7/hard_state.json");
    let original = fs::read(&state).unwrap();
    fs::write(&state, b"invalid persisted hard state").unwrap();
    let address = address();
    assert!(matches!(
        factory.start(address).await,
        Err(RuntimeError::Source(_))
    ));
    assert_eq!(factory.validations.load(Ordering::SeqCst), 0);
    assert_reusable(&factory, address, 1);
    fs::write(&state, original).unwrap();
    let owner = factory.start(address).await.unwrap();
    assert_eq!(read(&owner.handle()).await, 15);
    owner.shutdown(deadline()).await.unwrap();
    assert_reusable(&factory, address, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_requests_are_fenced_until_consumer_validation_finishes() {
    let disk = verify_and_copy_fixture();
    let mut factory = ConsumerFactory::new(disk.path());
    let gate = Gate::default();
    factory.gate = Some(gate.clone());
    let address = address();
    let owner = NodeOwner::start(factory.config(address, false), factory.clone(), deadline())
        .await
        .unwrap();
    let handle = owner.handle();
    let creation_handle = handle.clone();
    let creating = tokio::spawn(async move {
        creation_handle
            .create_group(
                GroupConfig {
                    group_id: GROUP,
                    voters: vec![1],
                },
                deadline(),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .unwrap();
    assert!(matches!(
        handle.propose(GROUP, vec![], deadline()).await,
        Err(RuntimeError::Source(MultiRaftError::UnknownGroup(GROUP)))
    ));
    assert!(matches!(
        handle.read_linearizable(GROUP, deadline(), |_| ()).await,
        Err(RuntimeError::Source(MultiRaftError::UnknownGroup(GROUP)))
    ));
    gate.release();
    creating.await.unwrap().unwrap();
    assert_eq!(read(&handle).await, 15);
    owner.shutdown(deadline()).await.unwrap();
    assert_reusable(&factory, address, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn validation_finishing_after_deadline_cannot_publish_and_still_releases_fsm() {
    let disk = verify_and_copy_fixture();
    let mut factory = ConsumerFactory::new(disk.path());
    let gate = Gate::default();
    factory.gate = Some(gate.clone());
    let address = address();
    let inputs = factory.config(address, true);
    let expires = Instant::now() + Duration::from_millis(200);
    let starting = tokio::spawn(NodeOwner::start(inputs, factory.clone(), expires));
    tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .unwrap();
    tokio::time::sleep_until(expires + Duration::from_millis(5)).await;
    gate.release();
    assert!(matches!(
        starting.await.unwrap(),
        Err(RuntimeError::Deadline { .. })
    ));
    assert_reusable(&factory, address, 1);
    factory.gate = None;
    factory
        .start(address)
        .await
        .unwrap()
        .shutdown(deadline())
        .await
        .unwrap();
    assert_reusable(&factory, address, 2);
}
