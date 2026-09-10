//! An older checkpoint must not prematurely finish a new in-flight build at the same cut.
use multiraft_core::{ClusterConfig, FileLogSyncLevel, SnapshotMode};
use multiraft_fsm::CounterFsm;
use multiraft_net::{CompactionProgress, MultiRaft};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc, Condvar, Mutex,
    },
    time::Duration,
};
use tracing_subscriber::prelude::*;

struct BlockPublish {
    enabled: Arc<AtomicBool>,
    entered: mpsc::Sender<()>,
    gate: Arc<(Mutex<bool>, Condvar)>,
}
impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for BlockPublish {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().target() != "multiraft::native_catalog" {
            return;
        }
        struct Phase(bool);
        impl tracing::field::Visit for Phase {
            fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                if field.name() == "phase" && value == "data_synced" {
                    self.0 = true;
                }
            }
        }
        let mut phase = Phase(false);
        event.record(&mut phase);
        if phase.0 && self.enabled.swap(false, Ordering::SeqCst) {
            self.entered.send(()).unwrap();
            let mut released = self.gate.0.lock().unwrap();
            while !*released {
                released = self.gate.1.wait(released).unwrap();
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn existing_checkpoint_does_not_complete_a_still_running_repeated_build() {
    let (entered, receiver) = mpsc::channel();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let enabled = Arc::new(AtomicBool::new(false));
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(BlockPublish {
        enabled: enabled.clone(),
        entered,
        gate: gate.clone(),
    }))
    .unwrap();
    let root = tempfile::tempdir().unwrap();
    let mut config = ClusterConfig::for_test(1, &[1]);
    config.data_dir = root.path().into();
    config.file_log_sync_level = FileLogSyncLevel::Data;
    config.snapshot_mode = SnapshotMode::NativeDurable;
    config.retain_log_entries = 0;
    let node = MultiRaft::start(config).await.unwrap();
    node.create_group(7, &[1]).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !node.is_leader(7) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    node.propose(7, CounterFsm::encode_add(3, 1)).await.unwrap();
    node.request_compaction(7).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if node.local_storage_status(7).await.unwrap().progress
                == CompactionProgress::CompletedObserved
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    enabled.store(true, Ordering::SeqCst);
    node.request_compaction(7).await.unwrap();
    tokio::task::spawn_blocking(move || receiver.recv_timeout(Duration::from_secs(5)).unwrap())
        .await
        .unwrap();
    let premature = tokio::time::timeout(Duration::from_millis(250), async {
        loop {
            let status = node.local_storage_status(7).await.unwrap();
            if status.progress != CompactionProgress::Submitted {
                break status.progress;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    *gate.0.lock().unwrap() = true;
    gate.1.notify_all();
    node.shutdown().await.unwrap();
    assert!(
        premature.is_err(),
        "old checkpoint prematurely completed new work: {premature:?}"
    );
}
