//! Public shutdown observes actual application destruction across canceled waits.
use multiraft_core::{ClusterConfig, FileLogSyncLevel};
use multiraft_fsm::{ApplyOut, GroupId, StateMachine};
use multiraft_net::MultiRaft;
use std::fs::{self, OpenOptions};
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Clone, Default)]
struct Gate {
    entered: Arc<Notify>,
    open: Arc<(Mutex<bool>, Condvar)>,
}
impl Gate {
    fn block(&self) {
        self.entered.notify_one();
        let (lock, changed) = &*self.open;
        let (open, timeout) = changed
            .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(5), |open| !*open)
            .unwrap();
        assert!(
            *open && !timeout.timed_out(),
            "test must release the actual destructor"
        );
    }
    fn release(&self) {
        *self.open.0.lock().unwrap() = true;
        self.open.1.notify_all();
    }
}
struct LeasedFsm {
    lease: PathBuf,
    drop_gate: Gate,
}
impl StateMachine for LeasedFsm {
    type Error = std::io::Error;
    fn apply(
        &mut self,
        _group: GroupId,
        _index: u64,
        _data: &[u8],
    ) -> Result<ApplyOut, Self::Error> {
        Ok(ApplyOut::default())
    }
    fn snapshot(&self, _group: GroupId) -> Result<Vec<u8>, Self::Error> {
        Ok(vec![])
    }
    fn restore(&mut self, _group: GroupId, _data: &[u8]) -> Result<(), Self::Error> {
        Ok(())
    }
}
impl Drop for LeasedFsm {
    fn drop(&mut self) {
        self.drop_gate.block();
        fs::remove_file(&self.lease).unwrap();
    }
}
fn address() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_shutdown_retry_waits_for_actual_fsm_destructor_and_lease_release() {
    let root = tempfile::tempdir().unwrap();
    let lease = root.path().join("consumer.lease");
    let gate = Gate::default();
    let addr = address();
    let mut config = ClusterConfig::for_test(1, &[1]);
    config.peers = vec![(1, addr)];
    config.data_dir = root.path().to_owned();
    config.file_log_sync_level = FileLogSyncLevel::Data;
    let factory_lease = lease.clone();
    let factory_gate = gate.clone();
    let node = Arc::new(
        MultiRaft::start_grpc_with_factory(config, move |_| {
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&factory_lease)?;
            Ok(LeasedFsm {
                lease: factory_lease.clone(),
                drop_gate: factory_gate.clone(),
            })
        })
        .await
        .unwrap(),
    );
    node.create_group(9, &[1]).await.unwrap();
    // A previously admitted public local read keeps the FSM alive after its
    // Group is cleared, so its eventual Drop runs in the read task, not the
    // shutdown waiter whose cancellation we exercise.
    let read_gate = Gate::default();
    let query_gate = read_gate.clone();
    let query_node = node.clone();
    let read = tokio::spawn(async move { query_node.with_fsm(9, |_| query_gate.block()).await });
    tokio::time::timeout(Duration::from_secs(2), read_gate.entered.notified())
        .await
        .unwrap();
    let first_node = node.clone();
    let first = tokio::spawn(async move { first_node.shutdown().await });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if matches!(
                node.observe_group(9),
                Err(multiraft_core::MultiRaftError::UnknownGroup(9))
            ) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    read_gate.release();
    tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .unwrap();
    assert!(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lease)
            .is_err(),
        "real consumer lease remains held while Drop is blocked"
    );
    let second_node = node.clone();
    let mut second = tokio::spawn(async move { second_node.shutdown().await });
    let early = tokio::time::timeout(Duration::from_millis(40), &mut second).await;
    gate.release();
    assert!(
        early.is_err(),
        "a retry must not report shutdown success while the actual FSM destructor is blocked"
    );
    tokio::time::timeout(Duration::from_secs(2), second)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    read.await.unwrap();
    assert!(!lease.exists());
    let recreated = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&lease)
        .unwrap();
    drop(recreated);
    fs::remove_file(lease).unwrap();
    TcpListener::bind(addr).expect("successful shutdown releases its actual gRPC listener");
}
