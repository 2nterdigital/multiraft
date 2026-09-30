//! An independent byte FSM consumer: no raw Raft, metrics, trigger or Ech0 types.
use std::io;
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use multiraft_core::{ClusterConfig, MultiRaftError};
use multiraft_fsm::{ApplyOut, GroupId, StateMachine};
use multiraft_net::{
    FsmFactoryContext, GroupConfig, NodeOwner, RuntimeConfig, RuntimeError, RuntimeHandle,
    RuntimePhase, RuntimeTransport, SharedFabric,
};
use tokio::sync::Notify;
use tokio::time::Instant;

struct BytesFsm {
    value: Vec<u8>,
    released: Arc<AtomicUsize>,
}
impl BytesFsm {
    fn new(released: Arc<AtomicUsize>) -> Self {
        Self {
            value: Vec::new(),
            released,
        }
    }
}
impl Drop for BytesFsm {
    fn drop(&mut self) {
        self.released.fetch_add(1, Ordering::SeqCst);
    }
}
impl StateMachine for BytesFsm {
    type Error = io::Error;
    fn apply(&mut self, _: GroupId, _: u64, data: &[u8]) -> Result<ApplyOut, Self::Error> {
        self.value = data.to_vec();
        Ok(ApplyOut {
            effects: self.value.clone(),
        })
    }
    fn snapshot(&self, _: GroupId) -> Result<Vec<u8>, Self::Error> {
        Ok(self.value.clone())
    }
    fn restore(&mut self, _: GroupId, data: &[u8]) -> Result<(), Self::Error> {
        self.value = data.to_vec();
        Ok(())
    }
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
fn address() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}
fn config(address: SocketAddr, groups: &[GroupId]) -> RuntimeConfig {
    let mut cluster = ClusterConfig::for_test(1, &[1]);
    cluster.peers = vec![(1, address)];
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
async fn leader(handle: &RuntimeHandle<BytesFsm>, group: GroupId) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match handle.read_linearizable(group, deadline(), |_| ()).await {
                Ok(()) => return,
                Err(RuntimeError::Source(MultiRaftError::NotLeader { .. })) => {
                    tokio::time::sleep(Duration::from_millis(5)).await
                }
                error => panic!("unexpected readiness result: {error:?}"),
            }
        }
    })
    .await
    .expect("single-node authority forms");
}
async fn reclaimed(address: SocketAddr, released: &AtomicUsize, expected: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if released.load(Ordering::SeqCst) == expected {
                if let Ok(listener) = TcpListener::bind(address) {
                    drop(listener);
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("listener and actual FSM resources reclaimed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unchanged_bytes_exact_effects_readindex_and_synchronous_resource_reuse() {
    let address = address();
    let released = Arc::new(AtomicUsize::new(0));
    let factory_releases = released.clone();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let factory_calls = calls.clone();
    let owner = NodeOwner::start(
        config(address, &[7]),
        move |context: FsmFactoryContext| {
            factory_calls
                .lock()
                .unwrap()
                .push((context.node_id(), context.group_id()));
            Ok(BytesFsm::new(factory_releases.clone()))
        },
        deadline(),
    )
    .await
    .unwrap();
    let handle = owner.handle();
    leader(&handle, 7).await;
    let command = vec![0, 255, 1, 0, 17];
    let receipt = handle
        .propose(7, command.clone(), deadline())
        .await
        .unwrap();
    assert_eq!(receipt.effects, command);
    assert!(receipt.index > 0);
    assert_eq!(
        handle
            .read_linearizable(7, deadline(), |fsm| fsm.value.clone())
            .await
            .unwrap(),
        command
    );
    assert_eq!(*calls.lock().unwrap(), vec![(1, 7)]);
    assert!(matches!(
        handle.propose(9, vec![1], deadline()).await,
        Err(RuntimeError::Source(MultiRaftError::UnknownGroup(9)))
    ));
    owner.shutdown(deadline()).await.unwrap();
    // Successful shutdown is the reuse seam: no eventual waiting here.
    assert_eq!(released.load(Ordering::SeqCst), 1);
    let listener = TcpListener::bind(address).unwrap();
    assert!(matches!(
        handle.read_linearizable(7, deadline(), |_| ()).await,
        Err(RuntimeError::Closed)
    ));
    drop(listener);
    let factory_releases = released.clone();
    let restarted = NodeOwner::start(
        config(address, &[7]),
        move |_| Ok(BytesFsm::new(factory_releases.clone())),
        deadline(),
    )
    .await
    .unwrap();
    restarted.shutdown(deadline()).await.unwrap();
    assert_eq!(released.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_start_rolls_back_already_created_groups_and_listener() {
    let address = address();
    let released = Arc::new(AtomicUsize::new(0));
    let factory_releases = released.clone();
    let result = NodeOwner::start(
        config(address, &[7, 8]),
        move |context: FsmFactoryContext| {
            if context.group_id() == 8 {
                anyhow::bail!("application factory refused Group 8");
            }
            Ok(BytesFsm::new(factory_releases.clone()))
        },
        deadline(),
    )
    .await;
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("factory failure must prevent publication"),
    };
    assert!(error.to_string().contains("create FSM"));
    assert_eq!(released.load(Ordering::SeqCst), 1);
    TcpListener::bind(address).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retained_weak_handle_cannot_keep_dropped_node_alive() {
    let address = address();
    let released = Arc::new(AtomicUsize::new(0));
    let factory_releases = released.clone();
    let owner = NodeOwner::start(
        config(address, &[7]),
        move |_| Ok(BytesFsm::new(factory_releases.clone())),
        deadline(),
    )
    .await
    .unwrap();
    let handle = owner.handle();
    let second_handle = handle.clone();
    drop(owner);
    assert!(matches!(
        second_handle.propose(7, vec![1], deadline()).await,
        Err(RuntimeError::Closed)
    ));
    reclaimed(address, &released, 1).await;
    assert!(matches!(
        handle.read_linearizable(7, deadline(), |_| ()).await,
        Err(RuntimeError::Closed)
    ));
}

fn release(gate: &Arc<(Mutex<bool>, Condvar)>) {
    *gate.0.lock().unwrap() = true;
    gate.1.notify_all();
}
fn wait(gate: &Arc<(Mutex<bool>, Condvar)>) {
    let mut open = gate.0.lock().unwrap();
    while !*open {
        open = gate.1.wait(open).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_start_waiter_does_not_abandon_native_group_construction() {
    let address = address();
    let released = Arc::new(AtomicUsize::new(0));
    let factory_releases = released.clone();
    let entered = Arc::new(Notify::new());
    let factory_entered = entered.clone();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let factory_gate = gate.clone();
    let start = tokio::spawn(NodeOwner::start(
        config(address, &[7]),
        move |_| {
            // A bounded externally controlled application factory is the publication seam.
            factory_entered.notify_one();
            wait(&factory_gate);
            Ok(BytesFsm::new(factory_releases.clone()))
        },
        deadline(),
    ));
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    start.abort();
    match start.await {
        Err(error) => assert!(error.is_cancelled()),
        Ok(_) => panic!("start waiter must be canceled"),
    }
    release(&gate);
    reclaimed(address, &released, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_shutdown_waiter_continues_drain_stop_and_join() {
    let address = address();
    let released = Arc::new(AtomicUsize::new(0));
    let factory_releases = released.clone();
    let mut inputs = config(address, &[7]);
    inputs.max_inflight = 1;
    let owner = NodeOwner::start(
        inputs,
        move |_| Ok(BytesFsm::new(factory_releases.clone())),
        deadline(),
    )
    .await
    .unwrap();
    let handle = owner.handle();
    leader(&handle, 7).await;
    let entered = Arc::new(Notify::new());
    let query_entered = entered.clone();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let query_gate = gate.clone();
    let reading_handle = handle.clone();
    let read = tokio::spawn(async move {
        reading_handle
            .read_linearizable(7, deadline(), move |_| {
                query_entered.notify_one();
                wait(&query_gate);
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    assert!(matches!(
        handle.propose(7, vec![1], deadline()).await,
        Err(RuntimeError::Busy)
    ));
    let shutdown = tokio::spawn(owner.shutdown(deadline()));
    // Wait for externally visible fencing before canceling the stop waiter.
    loop {
        if matches!(
            handle.propose(7, vec![], Instant::now()).await,
            Err(RuntimeError::Closed)
        ) {
            break;
        }
        tokio::task::yield_now().await;
    }
    shutdown.abort();
    assert!(shutdown.await.unwrap_err().is_cancelled());
    release(&gate);
    let _ = read.await.unwrap();
    reclaimed(address, &released, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_group_creation_and_expired_admission_use_same_runtime() {
    let address = address();
    let released = Arc::new(AtomicUsize::new(0));
    let factory_releases = released.clone();
    let mut inputs = config(address, &[]);
    inputs.transport = RuntimeTransport::InProcess(SharedFabric::new());
    let owner = NodeOwner::start(
        inputs,
        move |_| Ok(BytesFsm::new(factory_releases.clone())),
        deadline(),
    )
    .await
    .unwrap();
    let handle = owner.handle();
    handle
        .create_group(
            GroupConfig {
                group_id: 7,
                voters: vec![1],
            },
            deadline(),
        )
        .await
        .unwrap();
    leader(&handle, 7).await;
    assert!(matches!(
        handle.propose(7, vec![1], Instant::now()).await,
        Err(RuntimeError::Deadline {
            phase: RuntimePhase::Admission,
            outcome_unknown: false
        })
    ));
    assert_eq!(
        handle
            .read_linearizable(7, deadline(), |fsm| fsm.value.clone())
            .await
            .unwrap(),
        Vec::<u8>::new()
    );
    owner.shutdown(deadline()).await.unwrap();
    assert_eq!(released.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owner_drop_outside_entered_runtime_still_dispatches_owned_cleanup() {
    let address = address();
    let released = Arc::new(AtomicUsize::new(0));
    let factory_releases = released.clone();
    let owner = NodeOwner::start(
        config(address, &[7]),
        move |_| Ok(BytesFsm::new(factory_releases.clone())),
        deadline(),
    )
    .await
    .unwrap();
    let handle = owner.handle();
    // This thread has no entered Tokio runtime. Cleanup belongs to the runtime
    // that created the node, so Drop does not rely on the dropping thread.
    std::thread::spawn(move || drop(owner)).join().unwrap();
    assert!(matches!(
        handle.propose(7, vec![1], deadline()).await,
        Err(RuntimeError::Closed)
    ));
    reclaimed(address, &released, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn panicked_factory_cannot_bypass_cleanup_of_previous_groups() {
    let address = address();
    let released = Arc::new(AtomicUsize::new(0));
    let factory_releases = released.clone();
    let result = NodeOwner::start(
        config(address, &[7, 8]),
        move |context: FsmFactoryContext| {
            if context.group_id() == 8 {
                panic!("controlled application factory panic");
            }
            Ok(BytesFsm::new(factory_releases.clone()))
        },
        deadline(),
    )
    .await;
    assert!(matches!(result, Err(RuntimeError::Source(_))));
    assert_eq!(released.load(Ordering::SeqCst), 1);
    TcpListener::bind(address).unwrap();
}
