//! Neutral public consumer proof of stage budgets and actual resource rollback.
use multiraft_core::{ClusterConfig, FileLogSyncLevel, MultiRaftError};
use multiraft_fsm::{ApplyOut, GroupId, StateMachine};
use multiraft_net::{
    FsmFactoryContext, GroupConfig, NodeOwner, RuntimeConfig, RuntimeError, RuntimePhase,
    StateMachineFactory,
};
use std::fs::{self, OpenOptions};
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::{timeout, Instant};
const WAIT: Duration = Duration::from_millis(100);
#[derive(Clone, Default)]
struct Gate {
    entered: Arc<Notify>,
    state: Arc<(Mutex<bool>, Condvar)>,
}
impl Gate {
    fn block(&self) {
        self.entered.notify_one();
        let (lock, changed) = &*self.state;
        let (open, elapsed) = changed
            .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(3), |open| !*open)
            .unwrap();
        assert!(
            *open && !elapsed.timed_out(),
            "bounded consumer must release its callback"
        );
    }
    fn release(&self) {
        *self.state.0.lock().unwrap() = true;
        self.state.1.notify_all();
    }
}
struct Release(Vec<Gate>);
impl Drop for Release {
    fn drop(&mut self) {
        for gate in &self.0 {
            gate.release();
        }
    }
}
struct Consumer {
    lease: PathBuf,
}
impl Drop for Consumer {
    fn drop(&mut self) {
        fs::remove_file(&self.lease).unwrap();
    }
}
impl StateMachine for Consumer {
    type Error = std::io::Error;
    fn apply(&mut self, _: GroupId, _: u64, _: &[u8]) -> Result<ApplyOut, Self::Error> {
        Ok(ApplyOut::default())
    }
    fn snapshot(&self, _: GroupId) -> Result<Vec<u8>, Self::Error> {
        Ok(vec![])
    }
    fn restore(&mut self, _: GroupId, _: &[u8]) -> Result<(), Self::Error> {
        Ok(())
    }
}
#[derive(Clone)]
struct Factory {
    lease: PathBuf,
    construct: Option<Gate>,
    validate: Option<Gate>,
    reject: bool,
}
impl StateMachineFactory<Consumer> for Factory {
    fn create(&self, context: FsmFactoryContext) -> anyhow::Result<Consumer> {
        assert_eq!((context.node_id(), context.group_id()), (1, 9));
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&self.lease)?;
        let fsm = Consumer {
            lease: self.lease.clone(),
        };
        if let Some(gate) = &self.construct {
            gate.block();
        }
        Ok(fsm)
    }
    fn validate_recovered(&self, context: FsmFactoryContext, fsm: &Consumer) -> anyhow::Result<()> {
        assert_eq!((context.node_id(), context.group_id()), (1, 9));
        assert!(fsm.lease.exists());
        if let Some(gate) = &self.validate {
            gate.block();
        }
        anyhow::ensure!(!self.reject, "consumer rejects original local image");
        Ok(())
    }
}
fn address() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}
fn group() -> GroupConfig {
    GroupConfig {
        group_id: 9,
        voters: vec![1],
    }
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
async fn start(root: &std::path::Path, addr: SocketAddr, factory: Factory) -> NodeOwner<Consumer> {
    let mut cluster = ClusterConfig::for_test(1, &[1]);
    cluster.peers = vec![(1, addr)];
    cluster.data_dir = root.to_owned();
    cluster.file_log_sync_level = FileLogSyncLevel::Data;
    NodeOwner::start(RuntimeConfig::new(cluster, vec![]), factory, deadline())
        .await
        .unwrap()
}
fn reusable(lease: &std::path::Path, addr: SocketAddr) {
    assert!(!lease.exists());
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(lease)
        .unwrap();
    drop(file);
    fs::remove_file(lease).unwrap();
    TcpListener::bind(addr).unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_wait_budget_excludes_constructor_and_consumer_validation() {
    let root = tempfile::tempdir().unwrap();
    let addr = address();
    let lease = root.path().join("lease");
    let construct = Gate::default();
    let validate = Gate::default();
    let _release = Release(vec![construct.clone(), validate.clone()]);
    let owner = start(
        root.path(),
        addr,
        Factory {
            lease: lease.clone(),
            construct: Some(construct.clone()),
            validate: Some(validate.clone()),
            reject: false,
        },
    )
    .await;
    let handle = owner.handle();
    let creating_handle = handle.clone();
    // The same WAIT value bounds only native waiting, not the legacy
    // constructor or validation callbacks that the consumer also owns.
    let mut creating = tokio::spawn(async move {
        creating_handle
            .create_group_with_recovery_timeout(group(), WAIT)
            .await
    });
    timeout(Duration::from_secs(2), construct.entered.notified())
        .await
        .unwrap();
    assert!(timeout(WAIT * 2, &mut creating).await.is_err());
    construct.release();
    tokio::select! {
        () = validate.entered.notified() => {},
        result = &mut creating => panic!("legacy constructor was wrongly charged to native wait: {result:?}"),
        () = tokio::time::sleep(Duration::from_secs(2)) => panic!("validator never entered"),
    }
    assert!(matches!(
        handle.local_group_status(9, deadline()).await,
        Err(RuntimeError::Source(MultiRaftError::UnknownGroup(9)))
    ));
    assert!(timeout(WAIT * 2, &mut creating).await.is_err());
    validate.release();
    creating.await.unwrap().unwrap();
    handle.local_group_status(9, deadline()).await.unwrap();
    owner.shutdown(deadline()).await.unwrap();
    reusable(&lease, addr);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn absolute_startup_deadline_still_includes_construction_and_rolls_back() {
    let root = tempfile::tempdir().unwrap();
    let addr = address();
    let lease = root.path().join("lease");
    let construct = Gate::default();
    let _release = Release(vec![construct.clone()]);
    let owner = start(
        root.path(),
        addr,
        Factory {
            lease: lease.clone(),
            construct: Some(construct.clone()),
            validate: None,
            reject: false,
        },
    )
    .await;
    let handle = owner.handle();
    let creating_handle = handle.clone();
    let mut creating = tokio::spawn(async move {
        creating_handle
            .create_group(group(), Instant::now() + WAIT)
            .await
    });
    timeout(Duration::from_secs(2), construct.entered.notified())
        .await
        .unwrap();
    // The request budget expires, but rollback cannot report actual release
    // while the constructor owns the application lease.
    assert!(timeout(WAIT * 2, &mut creating).await.is_err());
    assert!(lease.exists());
    assert!(matches!(
        handle.local_group_status(9, deadline()).await,
        Err(RuntimeError::Closed)
    ));
    construct.release();
    assert!(matches!(
        creating.await.unwrap(),
        Err(RuntimeError::Deadline {
            phase: RuntimePhase::GroupStart,
            outcome_unknown: true,
        })
    ));
    reusable(&lease, addr);
    owner.shutdown(deadline()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn canceled_stage_waiter_and_owner_drop_retain_constructor_until_release() {
    let root = tempfile::tempdir().unwrap();
    let addr = address();
    let lease = root.path().join("lease");
    let construct = Gate::default();
    let _release = Release(vec![construct.clone()]);
    let owner = start(
        root.path(),
        addr,
        Factory {
            lease: lease.clone(),
            construct: Some(construct.clone()),
            validate: None,
            reject: false,
        },
    )
    .await;
    let handle = owner.handle();
    let creating_handle = handle.clone();
    let creating = tokio::spawn(async move {
        creating_handle
            .create_group_with_recovery_timeout(group(), WAIT)
            .await
    });
    timeout(Duration::from_secs(2), construct.entered.notified())
        .await
        .unwrap();
    creating.abort();
    assert!(creating.await.unwrap_err().is_cancelled());
    drop(owner);
    assert!(lease.exists());
    assert!(OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&lease)
        .is_err());
    assert!(matches!(
        handle.local_group_status(9, deadline()).await,
        Err(RuntimeError::Closed)
    ));
    construct.release();
    timeout(Duration::from_secs(2), async {
        while lease.exists() || TcpListener::bind(addr).is_err() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    reusable(&lease, addr);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stage_validator_refusal_keeps_original_cause_and_actual_rollback() {
    let root = tempfile::tempdir().unwrap();
    let addr = address();
    let lease = root.path().join("lease");
    let owner = start(
        root.path(),
        addr,
        Factory {
            lease: lease.clone(),
            construct: None,
            validate: None,
            reject: true,
        },
    )
    .await;
    let handle = owner.handle();
    let error = handle
        .create_group_with_recovery_timeout(group(), WAIT)
        .await
        .unwrap_err();
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
    let mut original = false;
    while let Some(cause) = source {
        original |= cause
            .to_string()
            .contains("consumer rejects original local image");
        source = cause.source();
    }
    assert!(original, "original validator cause must survive: {error:?}");
    assert!(matches!(
        handle.local_group_status(9, deadline()).await,
        Err(RuntimeError::Closed)
    ));
    reusable(&lease, addr);
    owner.shutdown(deadline()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stage_timeout_still_bounds_native_cluster_tail_wait_and_rolls_back() {
    let root = tempfile::tempdir().unwrap();
    let addr = address();
    let lease = root.path().join("lease");
    let factory = Factory {
        lease: lease.clone(),
        construct: None,
        validate: None,
        reject: false,
    };
    let mut cluster = ClusterConfig::for_test(1, &[1, 2, 3]);
    cluster.peers = vec![(1, addr), (2, address()), (3, address())];
    cluster.data_dir = root.path().to_owned();
    cluster.file_log_sync_level = FileLogSyncLevel::Os;
    let owner = NodeOwner::start(RuntimeConfig::new(cluster, vec![]), factory, deadline())
        .await
        .unwrap();
    let handle = owner.handle();
    let started = Instant::now();
    let error = timeout(
        Duration::from_secs(2),
        handle.create_group_with_recovery_timeout(
            GroupConfig {
                group_id: 9,
                voters: vec![1, 2, 3],
            },
            WAIT,
        ),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(started.elapsed() >= WAIT);
    let RuntimeError::Source(MultiRaftError::Recovery(recovery)) = error else {
        panic!("native wait must retain its exact typed deadline: {error:?}");
    };
    assert_eq!(recovery.group_id, 9);
    assert_eq!(recovery.stage, multiraft_core::RecoveryStage::Await);
    assert_eq!(recovery.failure, multiraft_core::RecoveryFailure::Deadline);
    assert!(matches!(
        handle.local_group_status(9, deadline()).await,
        Err(RuntimeError::Closed)
    ));
    reusable(&lease, addr);
    owner.shutdown(deadline()).await.unwrap();
}
