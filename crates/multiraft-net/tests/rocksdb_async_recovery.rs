//! RocksDB recovery keeps the native snapshot/suffix proof and owner rollback gates.
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use multiraft_core::{ClusterConfig, SnapshotMode};
use multiraft_fsm::{
    ApplyOut, CounterFsm, GroupId, StateMachine, ValidationContext, ValidationFuture,
    ValidationKind,
};
use multiraft_net::{
    CompactionProgress, FsmFactoryContext, GroupConfig, NodeOwner, RuntimeConfig, SharedFabric,
    StateMachineFactory,
};
use tokio::sync::Semaphore;
use tokio::time::Instant;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(8)
}
fn config(root: &Path) -> ClusterConfig {
    let mut config = ClusterConfig::for_test(1, &[1]);
    config.peers = vec![(
        1,
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap(),
    )];
    config.data_dir = root.to_path_buf();
    config.log_storage_backend = multiraft_core::LogStorageBackend::RocksDb;
    // RocksDB remains durable at the default Os setting.
    config.snapshot_mode = SnapshotMode::NativeDurable;
    config.retain_log_entries = 0;
    config
}
struct Proof {
    entered: Semaphore,
    release: Semaphore,
    inputs: Mutex<Vec<(ValidationContext, i64)>>,
    ready: AtomicUsize,
    dropped: AtomicUsize,
    reject: bool,
}
struct Consumer {
    counter: CounterFsm,
    proof: Arc<Proof>,
}
impl Drop for Consumer {
    fn drop(&mut self) {
        self.proof.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
impl StateMachine for Consumer {
    type Error = <CounterFsm as StateMachine>::Error;
    fn apply(&mut self, group: GroupId, index: u64, bytes: &[u8]) -> Result<ApplyOut, Self::Error> {
        self.counter.apply(group, index, bytes)
    }
    fn snapshot(&self, group: GroupId) -> Result<Vec<u8>, Self::Error> {
        self.counter.snapshot(group)
    }
    fn restore(&mut self, group: GroupId, bytes: &[u8]) -> Result<(), Self::Error> {
        self.counter.restore(group, bytes)
    }
    fn requires_recovery_validation(&self) -> bool {
        true
    }
    fn recovery_validation(&self, context: ValidationContext) -> Option<ValidationFuture> {
        let proof = self.proof.clone();
        proof
            .inputs
            .lock()
            .unwrap()
            .push((context, self.counter.value(context.group_id)));
        Some(Box::pin(async move {
            proof.entered.add_permits(1);
            proof.release.acquire().await.unwrap().forget();
            if proof.reject {
                Err(io::Error::other("local recovery proof rejected"))
            } else {
                Ok(())
            }
        }))
    }
    fn recovery_validated(&mut self, _: ValidationContext) -> Result<(), Self::Error> {
        self.proof.ready.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
#[derive(Clone)]
struct Factory(Arc<Proof>);
impl StateMachineFactory<Consumer> for Factory {
    fn create(&self, _: FsmFactoryContext) -> anyhow::Result<Consumer> {
        Ok(Consumer {
            counter: CounterFsm::default(),
            proof: self.0.clone(),
        })
    }
    fn validate_recovered(&self, context: FsmFactoryContext, fsm: &Consumer) -> anyhow::Result<()> {
        anyhow::ensure!(
            fsm.counter.value(context.group_id()) == 15,
            "snapshot plus committed suffix must recover before proof"
        );
        anyhow::ensure!(
            self.0.ready.load(Ordering::SeqCst) == 0,
            "ready follows proof"
        );
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_and_committed_suffix_wait_for_async_proof_and_rejection_rolls_back() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let fabric = SharedFabric::new();
    let node = fabric.start_node(config.clone()).await.unwrap();
    node.create_group(7, &[1]).await.unwrap();
    multiraft_net::wait_for_leader(std::slice::from_ref(&node), 7, Duration::from_secs(5))
        .await
        .unwrap();
    node.propose(7, CounterFsm::encode_add(10, 1))
        .await
        .unwrap();
    node.request_compaction(7).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let status = node.local_storage_status(7).await.unwrap();
            if status.progress == CompactionProgress::CompletedObserved {
                assert!(status.durable_snapshot.is_some());
                assert_eq!(status.retained_log_bytes_kind, "encoded_entries");
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    node.propose(7, CounterFsm::encode_add(5, 2)).await.unwrap();
    node.shutdown().await.unwrap();

    for reject in [true, false] {
        let proof = Arc::new(Proof {
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            inputs: Mutex::new(Vec::new()),
            ready: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
            reject,
        });
        let runtime = RuntimeConfig::new(
            config.clone(),
            vec![GroupConfig {
                group_id: 7,
                voters: vec![1],
            }],
        );
        let starting = tokio::spawn(NodeOwner::start(
            runtime,
            Factory(proof.clone()),
            deadline(),
        ));
        tokio::time::timeout(Duration::from_secs(5), proof.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        assert!(
            !starting.is_finished(),
            "owner cannot publish while proof is pending"
        );
        let inputs = proof.inputs.lock().unwrap().clone();
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].0.kind, ValidationKind::Startup);
        assert_eq!(inputs[0].1, 15);
        assert_eq!(proof.ready.load(Ordering::SeqCst), 0);
        proof.release.add_permits(1);
        let result = starting.await.unwrap();
        if reject {
            assert!(result.is_err(), "rejected proof must roll back the owner");
            assert_eq!(proof.ready.load(Ordering::SeqCst), 0);
        } else {
            let owner = result.unwrap();
            assert_eq!(proof.ready.load(Ordering::SeqCst), 1);
            owner.shutdown(deadline()).await.unwrap();
        }
        assert_eq!(proof.dropped.load(Ordering::SeqCst), 1);
        // Successful immediate reopen proves both rejected and accepted owners
        // released the shared RocksDB, even while the original facade is alive.
        drop(
            multiraft_store::RocksLogDb::<multiraft_core::TypeConfig>::open(
                root.path().join("raft-rocksdb"),
            )
            .unwrap(),
        );
    }
}
