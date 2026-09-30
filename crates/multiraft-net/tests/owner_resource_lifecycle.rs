//! Public shutdown observes actual application destruction across canceled waits.
use multiraft_core::{ClusterConfig, FileLogSyncLevel};
use multiraft_fsm::{ApplyOut, GroupId, StateMachine};
use multiraft_net::{GroupConfig, MultiRaft, NodeOwner, RuntimeConfig, RuntimeError, RuntimePhase};
use std::fs::{self, OpenOptions};
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tokio::sync::Notify;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;
use tracing_subscriber::Layer;

// Observe the real operational transition, never a scheduling/yield count.
struct ShutdownStarted(Arc<Notify>);
#[derive(Default)]
struct ReleaseBoundary {
    shutdown: bool,
    stopping: bool,
    local_node: bool,
}
impl Visit for ReleaseBoundary {
    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "operation" => self.shutdown = value == "node_shutdown",
            "phase" => self.stopping = value == "start",
            _ => {}
        }
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "node_id" {
            self.local_node = value == 11;
        }
    }
    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
}
impl<S: Subscriber> Layer<S> for ShutdownStarted {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        if event.metadata().target() != "multiraft::recovery" {
            return;
        }
        let mut boundary = ReleaseBoundary::default();
        event.record(&mut boundary);
        if boundary.shutdown && boundary.stopping && boundary.local_node {
            self.0.notify_one();
        }
    }
}

#[derive(Default)]
struct GateState {
    entered: bool,
    released: bool,
}
#[derive(Clone, Default)]
struct Gate {
    entered: Arc<Notify>,
    open: Arc<(Mutex<GateState>, Condvar)>,
}
impl Gate {
    fn block(&self) {
        let (lock, changed) = &*self.open;
        let mut state = lock.lock().unwrap();
        state.entered = true;
        changed.notify_all();
        self.entered.notify_one();
        let (state, timeout) = changed
            .wait_timeout_while(state, Duration::from_secs(5), |state| !state.released)
            .unwrap();
        assert!(
            state.released && !timeout.timed_out(),
            "test must release the actual destructor"
        );
    }
    fn wait_for_entrance(&self) -> bool {
        let (lock, changed) = &*self.open;
        let (state, _) = changed
            .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(5), |state| {
                !state.entered
            })
            .unwrap();
        state.entered
    }
    fn release(&self) {
        self.open.0.lock().unwrap().released = true;
        self.open.1.notify_all();
    }
}
struct LeasedFsm {
    lease: PathBuf,
    drop_gate: Gate,
    apply_gate: Option<Gate>,
}
impl StateMachine for LeasedFsm {
    type Error = std::io::Error;
    fn apply(
        &mut self,
        _group: GroupId,
        _index: u64,
        _data: &[u8],
    ) -> Result<ApplyOut, Self::Error> {
        if let Some(gate) = &self.apply_gate {
            gate.block();
        }
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
                apply_gate: None,
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

// Native workers are created on a separate runtime through the public handle.
// Cleanup belongs to the original runtime, whose clock advances while native
// apply still owns the FSM. Actual Drop is released by an independent consumer
// thread, whichever executor drops the final Arc. No private state is read.
#[tokio::test]
async fn owned_cleanup_retains_actual_destructor_beyond_both_native_shutdown_windows() {
    let native_stopping = Arc::new(Notify::new());
    // One bounded public source observer for this test binary, filtered by its
    // distinct Node identity. Other native/runtime threads keep the same source
    // boundary observable; no thread-local dispatcher lifetime is assumed.
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry().with(ShutdownStarted(native_stopping.clone())),
    )
    .unwrap();
    let root = tempfile::tempdir().unwrap();
    let lease = root.path().join("late-consumer.lease");
    let gate = Gate::default();
    let addr = address();
    let mut cluster = ClusterConfig::for_test(11, &[11]);
    cluster.peers = vec![(11, addr)];
    cluster.data_dir = root.path().to_owned();
    let factory_lease = lease.clone();
    let factory_gate = gate.clone();
    let apply_gate = Gate::default();
    let factory_apply_gate = apply_gate.clone();
    let owner = NodeOwner::start(
        RuntimeConfig::new(cluster, vec![]),
        move |_| {
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&factory_lease)?;
            Ok(LeasedFsm {
                lease: factory_lease.clone(),
                drop_gate: factory_gate.clone(),
                apply_gate: Some(factory_apply_gate.clone()),
            })
        },
        tokio::time::Instant::now() + Duration::from_secs(5),
    )
    .await
    .unwrap();
    let handle = owner.handle();
    let (runtime_sender, runtime_receiver) = std::sync::mpsc::channel();
    let (stop_worker, stopped) = tokio::sync::oneshot::channel();
    let worker = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        runtime_sender.send(runtime.handle().clone()).unwrap();
        runtime.block_on(async {
            let _ = stopped.await;
        });
    });
    let native_runtime = runtime_receiver.recv().unwrap();
    let creation_handle = handle.clone();
    native_runtime
        .spawn(async move {
            creation_handle
                .create_group(
                    GroupConfig {
                        group_id: 9,
                        voters: vec![11],
                    },
                    tokio::time::Instant::now() + Duration::from_secs(5),
                )
                .await
        })
        .await
        .unwrap()
        .unwrap();
    let writing_handle = handle.clone();
    let write = native_runtime.spawn(async move {
        writing_handle
            .propose(
                9,
                vec![1],
                tokio::time::Instant::now() + Duration::from_secs(180),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), apply_gate.entered.notified())
        .await
        .unwrap();
    let mut stopping =
        tokio::spawn(owner.shutdown(tokio::time::Instant::now() + Duration::from_secs(180)));
    tokio::task::yield_now().await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(31)).await;
    // Interrupted admission never retracts the dispatched native apply. Its
    // independent worker keeps the real FSM throughout native stopping.
    tokio::time::resume();
    assert!(matches!(
        write.await.unwrap(),
        Err(RuntimeError::Interrupted { .. })
    ));
    // The existing operational source event proves fallback native stop began.
    // Keep native apply blocked throughout its second complete30s window.
    tokio::time::timeout(Duration::from_secs(2), native_stopping.notified())
        .await
        .unwrap();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(31)).await;
    let early = tokio::time::timeout(Duration::from_secs(1), &mut stopping).await;
    let falsely_completed = early.is_ok();
    let lease_held = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&lease)
        .is_err();
    let weak_closed = matches!(
        handle
            .local_group_status(9, tokio::time::Instant::now())
            .await,
        Err(RuntimeError::Closed)
    );
    // The final FSM Arc may drop on the native worker OR the original cleanup
    // driver. An independent consumer thread observes the actual destructor and
    // keeps the unchanged5s gate bounded without relying on executor affinity.
    let drop_gate = gate.clone();
    let drop_lease = lease.clone();
    let public_completion = stopping.abort_handle();
    let destructor = std::thread::spawn(move || {
        let entered = drop_gate.wait_for_entrance();
        let held = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&drop_lease)
            .is_err();
        let pending = !public_completion.is_finished();
        drop_gate.release();
        (entered, held, pending)
    });
    tokio::time::resume();
    apply_gate.release();
    let result = match early {
        Ok(result) => result.unwrap(),
        Err(_) => tokio::time::timeout(Duration::from_secs(2), stopping)
            .await
            .unwrap()
            .unwrap(),
    };
    let (actual_drop_entered, lease_held_during_drop, pending_during_drop) =
        destructor.join().unwrap();
    let _ = stop_worker.send(());
    worker.join().unwrap();
    assert!(
        !falsely_completed,
        "owned cleanup must not complete after its second window while actual Drop is blocked"
    );
    assert!(
        lease_held,
        "actual consumer lease remains exclusive beyond both windows"
    );
    assert!(
        actual_drop_entered,
        "the actual application destructor must run"
    );
    assert!(
        lease_held_during_drop,
        "actual Drop must still hold the consumer lease"
    );
    assert!(
        pending_during_drop,
        "public shutdown cannot finish during actual Drop"
    );
    assert!(
        weak_closed,
        "weak requests remain fenced throughout retained rollback"
    );
    assert!(
        matches!(result, Err(RuntimeError::ShutdownFailed(source))
        if matches!(source.as_ref(), RuntimeError::Deadline { phase: RuntimePhase::Shutdown, outcome_unknown: true })),
        "late release must preserve the original unconfirmed cleanup deadline"
    );
    assert!(!lease.exists());
    let recreated = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&lease)
        .unwrap();
    drop(recreated);
    fs::remove_file(lease).unwrap();
    TcpListener::bind(addr).expect("actual final join releases the listener");
}
