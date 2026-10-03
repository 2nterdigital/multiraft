//! Partial staging keeps admission owned while existing active readers continue.
use multiraft_core::{StartupProvenance, TypeConfig};
use multiraft_store::SnapshotCatalog;
use openraft::alias::SnapshotMetaOf;
use std::sync::{mpsc, Arc, Barrier, Mutex};
use std::time::Duration;
use tracing_subscriber::prelude::*;

fn meta(id: &str) -> SnapshotMetaOf<TypeConfig> {
    SnapshotMetaOf::<TypeConfig> {
        last_log_id: None,
        last_membership: Default::default(),
        snapshot_id: id.to_owned(),
    }
}

struct PauseWrite {
    entered: Mutex<Option<mpsc::Sender<()>>>,
    resume: Arc<Barrier>,
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for PauseWrite {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().target() != "multiraft::native_catalog" {
            return;
        }
        struct Phase(bool);
        impl tracing::field::Visit for Phase {
            fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                if field.name() == "phase" {
                    self.0 = value == "data_synced";
                }
            }
        }
        let mut phase = Phase(false);
        event.record(&mut phase);
        if phase.0 {
            if let Some(sender) = self.entered.lock().unwrap().take() {
                sender.send(()).unwrap();
                self.resume.wait();
            }
        }
    }
}

#[test]
fn paused_partial_stage_keeps_permit_allows_active_reads_and_observes_cleanup_debt() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = SnapshotCatalog::new(dir.path(), 1);
    catalog
        .publish_native(42, &meta("active"), b"old", 1024)
        .unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let resume = Arc::new(Barrier::new(2));
    let writer_catalog = catalog.clone();
    let writer_resume = resume.clone();
    let writer = std::thread::spawn(move || {
        let subscriber = tracing_subscriber::registry().with(PauseWrite {
            entered: Mutex::new(Some(entered_tx)),
            resume: writer_resume,
        });
        tracing::subscriber::with_default(subscriber, || {
            writer_catalog.stage_native(42, &meta("paused"), b"candidate", 1024)
        })
    });
    entered_rx.recv_timeout(Duration::from_secs(3)).unwrap();

    // The bounded channel timeout also makes a publication-lock regression
    // fail without leaving a blocked worker behind.
    let (read_tx, read_rx) = mpsc::channel();
    let reader_catalog = catalog.clone();
    let reader = std::thread::spawn(move || {
        read_tx
            .send(reader_catalog.describe_native(42, 1024))
            .unwrap();
    });
    let observed = read_rx.recv_timeout(Duration::from_secs(1));
    if observed.is_err() {
        resume.wait();
        drop(writer.join().unwrap());
        reader.join().unwrap();
        panic!("old active reader waited for partial staging IO");
    }
    assert_eq!(observed.unwrap().unwrap().unwrap().meta, meta("active"));
    reader.join().unwrap();
    let mut candidates: Vec<_> = (0..15)
        .map(|index| {
            catalog
                .stage_native(42, &meta(&format!("candidate-{index}")), b"candidate", 1024)
                .unwrap()
        })
        .collect();
    let error = catalog
        .stage_native(42, &meta("excess"), b"candidate", 1024)
        .err()
        .unwrap();
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);

    // A concurrent cleanup failure must stay attached to the flag captured by
    // the paused writer. Successful candidate construction cannot erase it.
    let manifest_path = dir.path().join("42/native-v1/active.json");
    let manifest = std::fs::read(&manifest_path).unwrap();
    std::fs::write(&manifest_path, b"broken").unwrap();
    assert!(candidates.pop().unwrap().discard().is_err());
    std::fs::write(&manifest_path, manifest).unwrap();
    drop(candidates);
    resume.wait();
    assert!(writer.join().unwrap().is_err());
    assert!(catalog
        .stage_native(42, &meta("after-failure"), b"candidate", 1024)
        .is_err());
    assert_eq!(
        catalog.startup_provenance(42, 1024).unwrap(),
        StartupProvenance::Persisted
    );
    let recovered = catalog
        .stage_native(42, &meta("recovered"), b"candidate", 1024)
        .unwrap();
    recovered.discard().unwrap();
    assert_eq!(catalog.load_native(42, 1024).unwrap().unwrap().data, b"old");
    assert_eq!(
        std::fs::read_dir(dir.path().join("42/native-v1"))
            .unwrap()
            .count(),
        2
    );
}
