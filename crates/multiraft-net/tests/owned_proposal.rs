//! Owned writes retain native admission independently of their receipt observer.
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use multiraft_core::{ClusterConfig, MultiRaftError};
use multiraft_fsm::{ApplyOut, GroupId, StateMachine};
use multiraft_net::{
    GroupConfig, NodeOwner, RuntimeConfig, RuntimeError, RuntimeHandle, RuntimePhase,
    RuntimeTransport, SharedFabric,
};
use tokio::sync::Notify;
use tokio::time::Instant;

#[derive(Default)]
struct Gate {
    open: Mutex<bool>,
    release: Condvar,
    entered: Notify,
}
impl Gate {
    fn wait(&self) {
        self.entered.notify_one();
        let mut open = self.open.lock().unwrap();
        while !*open {
            open = self.release.wait(open).unwrap();
        }
    }
    fn open(&self) {
        *self.open.lock().unwrap() = true;
        self.release.notify_all();
    }
}
// Ensure an assertion failure never strands a synchronous FSM on the test runtime.
struct ReleaseOnDrop(Arc<Gate>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.open();
    }
}
struct BytesFsm {
    gate: Arc<Gate>,
    writes: Vec<Vec<u8>>,
    released: Arc<AtomicUsize>,
}
impl StateMachine for BytesFsm {
    type Error = io::Error;
    fn apply(&mut self, _: GroupId, _: u64, data: &[u8]) -> io::Result<ApplyOut> {
        if data == b"gated" {
            self.gate.wait();
        }
        self.writes.push(data.to_vec());
        Ok(ApplyOut {
            effects: data.to_vec(),
        })
    }
    fn snapshot(&self, _: GroupId) -> io::Result<Vec<u8>> {
        Ok(Vec::new())
    }
    fn restore(&mut self, _: GroupId, _: &[u8]) -> io::Result<()> {
        Ok(())
    }
}
impl Drop for BytesFsm {
    fn drop(&mut self) {
        self.released.fetch_add(1, Ordering::SeqCst);
    }
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
async fn start(gate: Arc<Gate>, released: Arc<AtomicUsize>) -> NodeOwner<BytesFsm> {
    let mut config = RuntimeConfig::new(
        ClusterConfig::for_test(1, &[1]),
        vec![GroupConfig {
            group_id: 7,
            voters: vec![1],
        }],
    );
    config.transport = RuntimeTransport::InProcess(SharedFabric::new());
    config.max_inflight = 1;
    let owner = NodeOwner::start(
        config,
        move |_| {
            Ok(BytesFsm {
                gate: gate.clone(),
                writes: Vec::new(),
                released: released.clone(),
            })
        },
        deadline(),
    )
    .await
    .unwrap();
    let handle = owner.handle();
    tokio::time::timeout_at(deadline(), async {
        loop {
            match handle.read_linearizable(7, deadline(), |_| ()).await {
                Ok(()) => break,
                Err(RuntimeError::Source(MultiRaftError::NotLeader { .. })) => {
                    tokio::task::yield_now().await
                }
                error => panic!("unexpected readiness: {error:?}"),
            }
        }
    })
    .await
    .unwrap();
    owner
}
async fn next_write(handle: &RuntimeHandle<BytesFsm>) {
    tokio::time::timeout_at(deadline(), async {
        loop {
            match handle.propose(7, b"next".to_vec(), deadline()).await {
                Err(RuntimeError::Busy) => tokio::task::yield_now().await,
                Ok(applied) => {
                    assert_eq!(applied.effects, b"next");
                    break;
                }
                error => panic!("unexpected write: {error:?}"),
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_and_timed_out_receipts_keep_admission_until_native_completion() {
    for timeout in [false, true] {
        let gate = Arc::new(Gate::default());
        let _release = ReleaseOnDrop(gate.clone());
        let owner = start(gate.clone(), Arc::new(AtomicUsize::new(0))).await;
        let handle = owner.handle();
        let receipt = handle
            .submit_owned(7, b"gated".to_vec(), deadline())
            .await
            .unwrap();
        tokio::time::timeout_at(deadline(), gate.entered.notified())
            .await
            .unwrap();
        if timeout {
            assert!(
                tokio::time::timeout(Duration::from_millis(10), receipt.completion())
                    .await
                    .is_err()
            );
        } else {
            drop(receipt);
        }
        assert!(matches!(
            handle.propose(7, vec![1], deadline()).await,
            Err(RuntimeError::Busy)
        ));
        assert!(matches!(
            handle.submit_owned(7, vec![1], deadline()).await,
            Err(RuntimeError::Busy)
        ));
        gate.open();
        next_write(&handle).await;
        let applied = handle
            .submit_owned(7, vec![0, 255, 9], deadline())
            .await
            .unwrap()
            .completion()
            .await
            .unwrap();
        assert_eq!(applied.effects, vec![0, 255, 9]);
        let original = handle.propose(7, vec![9, 0], deadline()).await.unwrap();
        assert!(original.index > applied.index);
        assert_eq!(original.effects, vec![9, 0]);
        owner.shutdown(deadline()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expired_or_unpolled_admission_never_writes_and_readiness_is_enforced() {
    let owner = start(Arc::new(Gate::default()), Arc::new(AtomicUsize::new(0))).await;
    let handle = owner.handle();
    assert!(matches!(
        handle.submit_owned(7, vec![1], Instant::now()).await,
        Err(RuntimeError::Deadline {
            phase: RuntimePhase::Admission,
            outcome_unknown: false,
        })
    ));
    drop(handle.submit_owned(7, vec![2], deadline()));
    assert!(matches!(
        handle.submit_owned(8, vec![3], deadline()).await,
        Err(RuntimeError::Source(MultiRaftError::UnknownGroup(8)))
    ));
    assert!(handle
        .read_linearizable(7, deadline(), |fsm| fsm.writes.is_empty())
        .await
        .unwrap());
    owner.shutdown(deadline()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_owner_interrupts_owned_wait_and_retains_native_cleanup() {
    let gate = Arc::new(Gate::default());
    let _release = ReleaseOnDrop(gate.clone());
    let released = Arc::new(AtomicUsize::new(0));
    let owner = start(gate.clone(), released.clone()).await;
    let handle = owner.handle();
    let receipt = handle
        .submit_owned(7, b"gated".to_vec(), deadline())
        .await
        .unwrap();
    tokio::time::timeout_at(deadline(), gate.entered.notified())
        .await
        .unwrap();
    drop(owner);
    assert!(matches!(
        tokio::time::timeout_at(deadline(), receipt.completion())
            .await
            .unwrap(),
        Err(RuntimeError::Interrupted {
            phase: RuntimePhase::Propose,
            outcome_unknown: true,
        })
    ));
    assert!(matches!(
        handle.submit_owned(7, vec![1], deadline()).await,
        Err(RuntimeError::Closed)
    ));
    assert_eq!(released.load(Ordering::SeqCst), 0);
    gate.open();
    tokio::time::timeout_at(deadline(), async {
        while released.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graceful_shutdown_drains_an_owned_proposal_before_native_teardown() {
    let gate = Arc::new(Gate::default());
    let _release = ReleaseOnDrop(gate.clone());
    let released = Arc::new(AtomicUsize::new(0));
    let owner = start(gate.clone(), released.clone()).await;
    let handle = owner.handle();
    let receipt = handle
        .submit_owned(7, b"gated".to_vec(), deadline())
        .await
        .unwrap();
    tokio::time::timeout_at(deadline(), gate.entered.notified())
        .await
        .unwrap();
    let shutdown = tokio::spawn(owner.shutdown(deadline()));
    tokio::time::timeout_at(deadline(), async {
        loop {
            if matches!(
                handle.submit_owned(7, vec![1], deadline()).await,
                Err(RuntimeError::Closed)
            ) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!shutdown.is_finished());
    assert_eq!(released.load(Ordering::SeqCst), 0);
    gate.open();
    assert_eq!(receipt.completion().await.unwrap().effects, b"gated");
    shutdown.await.unwrap().unwrap();
    assert_eq!(released.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn deadline_is_rechecked_before_the_owned_task_first_dispatches() {
    let owner = start(Arc::new(Gate::default()), Arc::new(AtomicUsize::new(0))).await;
    let handle = owner.handle();
    tokio::time::pause();
    // On this single-thread runtime no spawned proposal can run until this task
    // yields. Advance the clock after registration, before native dispatch.
    let receipt = handle
        .submit_owned(7, vec![4], Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(2)).await;
    assert!(matches!(
        receipt.completion().await,
        Err(RuntimeError::Deadline {
            phase: RuntimePhase::Admission,
            outcome_unknown: false,
        })
    ));
    tokio::time::resume();
    assert!(handle
        .read_linearizable(7, deadline(), |fsm| fsm.writes.is_empty())
        .await
        .unwrap());
    owner.shutdown(deadline()).await.unwrap();
}
