//! External consumer gates existing bounded tracing events, never native handles.
mod owned_maintenance_support;
use multiraft_net::{CompactionProgress, CompactionRejection, RuntimeError, RuntimePhase};
use owned_maintenance_support::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tracing_subscriber::prelude::*;

struct ObserveWork {
    build_enabled: Arc<AtomicBool>,
    build_gate: Gate,
    sample_stage: Arc<AtomicUsize>,
    sample_gates: [Gate; 3],
}
impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for ObserveWork {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        struct Phase {
            building: bool,
            sampling: bool,
        }
        impl tracing::field::Visit for Phase {
            fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                if field.name() == "phase" {
                    self.building = value == "data_synced";
                    self.sampling = value == "sample_start";
                }
            }
        }
        let mut phase = Phase {
            building: false,
            sampling: false,
        };
        event.record(&mut phase);
        if event.metadata().target() == "multiraft::native_catalog"
            && phase.building
            && self.build_enabled.swap(false, Ordering::SeqCst)
        {
            self.build_gate.wait();
        }
        if event.metadata().target() == "multiraft::maintenance" && phase.sampling {
            let stage = self.sample_stage.swap(0, Ordering::SeqCst);
            if stage > 0 {
                self.sample_gates[stage - 1].wait();
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_builder_and_node_sampler_keep_real_work_owned_across_cancellation_deadline_stop()
{
    let build_gate = Gate::default();
    let sample_gates: [Gate; 3] = std::array::from_fn(|_| Gate::default());
    let build_enabled = Arc::new(AtomicBool::new(false));
    let sample_stage = Arc::new(AtomicUsize::new(0));
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(ObserveWork {
        build_enabled: build_enabled.clone(),
        build_gate: build_gate.clone(),
        sample_stage: sample_stage.clone(),
        sample_gates: sample_gates.clone(),
    }))
    .unwrap();
    let disk = old_disk();
    let factory = Factory::new(disk.path());
    let address = address();
    let owner = factory.start(address, &[GROUP, 8]).await;
    let handle = owner.handle();
    assert_eq!(read(&handle, GROUP).await, 15);
    handle.request_compaction(GROUP, deadline()).await.unwrap();
    completed(&handle, GROUP).await;

    // A validated provider/native/purge sample from an earlier same-cut build
    // cannot certify this still-running builder. Consumers use only its canonical
    // progress; they do not supplement/recompute the completion classifier.
    build_enabled.store(true, Ordering::SeqCst);
    handle.request_compaction(GROUP, deadline()).await.unwrap();
    build_gate.entered().await;
    let prior_cut = status(&handle, GROUP).await;
    assert!(prior_cut.durable_snapshot.is_some());
    assert!(prior_cut.native_snapshot.is_some());
    assert_eq!(prior_cut.progress, CompactionProgress::Submitted);
    for group in [GROUP, 8] {
        assert!(matches!(
            handle.request_compaction(group, deadline()).await,
            Err(RuntimeError::MaintenanceRejected(CompactionRejection::Busy))
        ));
    }
    build_gate.release();
    completed(&handle, GROUP).await;

    // First sampler: cancel only the request waiter; the Node-wide slot stays
    // Busy for another Group until the original provider/log sampler returns.
    sample_stage.store(1, Ordering::SeqCst);
    let sampler_handle = handle.clone();
    let waiter =
        tokio::spawn(async move { sampler_handle.local_storage_status(GROUP, deadline()).await });
    sample_gates[0].entered().await;
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    assert!(matches!(
        handle.local_storage_status(8, deadline()).await,
        Err(RuntimeError::MaintenanceRejected(CompactionRejection::Busy))
    ));
    sample_gates[0].release();
    assert_eq!(
        status(&handle, GROUP).await.progress,
        CompactionProgress::CompletedObserved
    );

    // Second sampler: expiration has the same resource ownership boundary.
    sample_stage.store(2, Ordering::SeqCst);
    let sampler_handle = handle.clone();
    let expires = Instant::now() + Duration::from_millis(200);
    let waiter =
        tokio::spawn(async move { sampler_handle.local_storage_status(GROUP, expires).await });
    sample_gates[1].entered().await;
    assert!(matches!(
        waiter.await.unwrap(),
        Err(RuntimeError::Deadline {
            phase: RuntimePhase::StorageStatus,
            outcome_unknown: false
        })
    ));
    assert!(matches!(
        handle.local_storage_status(8, deadline()).await,
        Err(RuntimeError::MaintenanceRejected(CompactionRejection::Busy))
    ));
    sample_gates[1].release();
    status(&handle, GROUP).await;

    // Third sampler: cancel stop while actual maintenance still holds the FSM.
    // A detached/aborted child would allow early lease reuse and fail this seam.
    sample_stage.store(3, Ordering::SeqCst);
    let sampler_handle = handle.clone();
    let waiter =
        tokio::spawn(async move { sampler_handle.local_storage_status(GROUP, deadline()).await });
    sample_gates[2].entered().await;
    let stopping = tokio::spawn(owner.shutdown(deadline()));
    loop {
        if matches!(
            handle.request_compaction(8, Instant::now()).await,
            Err(RuntimeError::Closed)
        ) {
            break;
        }
        tokio::task::yield_now().await;
    }
    stopping.abort();
    assert!(stopping.await.unwrap_err().is_cancelled());
    assert!(factory.root.join("consumer-7.lease").exists());
    sample_gates[2].release();
    let _ = waiter.await.unwrap();
    eventually_reusable(&factory, address, 2).await;
    let restarted = factory.start(address, &[GROUP, 8]).await;
    assert_eq!(read(&restarted.handle(), GROUP).await, 15);
    restarted.shutdown(deadline()).await.unwrap();
    reusable(&factory, address, 4);
    assert_eq!(factory.legacy_serializations.load(Ordering::SeqCst), 0);
}
