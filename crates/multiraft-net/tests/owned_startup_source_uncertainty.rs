//! A public source subscriber witnesses a native-received initialization request.
//! No injected transport/storage/protocol seam or production probe is involved.
mod startup_support;
use multiraft_net::{InitializeDisposition, StartupCleanup, StartupPhase};
use startup_support::*;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::time::timeout;
use tracing::{
    field::{Field, Visit},
    Event, Subscriber,
};
use tracing_subscriber::{
    layer::{Context, SubscriberExt},
    Layer,
};
struct SourceBoundary {
    gate: Gate,
    first: Arc<AtomicBool>,
    address: String,
}
#[derive(Default)]
struct Message(String);
impl Visit for Message {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}
impl<S: Subscriber> Layer<S> for SourceBoundary {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        if !event.metadata().target().starts_with("openraft::") {
            return;
        }
        let mut message = Message::default();
        event.record(&mut message);
        if message.0.contains("received RaftMsg::Initialize:")
            && message.0.contains(&self.address)
            && !self.first.swap(true, Ordering::AcqRel)
        {
            self.gate.block();
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn native_received_request_losing_its_reply_keeps_unknown_until_true_resource_release() {
    let gate = Gate::default();
    let _release = Release(vec![gate.clone()]);
    let witnessed = Arc::new(AtomicBool::new(false));
    let root = tempfile::tempdir().unwrap();
    let peers = vec![(1, addr())];
    let subscriber = tracing_subscriber::registry().with(SourceBoundary {
        gate: gate.clone(),
        first: witnessed.clone(),
        address: peers[0].1.to_string(),
    });
    tracing::subscriber::set_global_default(subscriber).unwrap();
    let owner = start(root.path(), 1, &peers, Factory::new(root.path()), true).await;
    let h = owner.handle();
    let worker = h.clone();
    let mut startup = tokio::spawn(async move {
        worker
            .start_groups_with_preference(batch(&[7], &[1], Some(1), Duration::from_secs(2)))
            .await
    });
    timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .unwrap();
    assert!(witnessed.load(Ordering::Acquire));
    let mut shutdown = tokio::spawn(async move { owner.shutdown(deadline()).await });
    // The native channel has actually delivered Initialize; its reply has not
    // been produced. Fence cancels only the waiting future, never this request.
    timeout(Duration::from_secs(1), async {
        loop {
            if matches!(
                h.local_group_status(7, deadline()).await,
                Err(multiraft_net::RuntimeError::Closed)
            ) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(timeout(Duration::from_millis(30), &mut startup)
        .await
        .is_err());
    assert!(timeout(Duration::from_millis(30), &mut shutdown)
        .await
        .is_err());
    assert!(root.path().join("consumer-7.lease").exists());
    gate.release();
    let failure = startup.await.unwrap().unwrap_err();
    assert_eq!(failure.phase, StartupPhase::Initialize);
    assert_eq!(
        failure.report.groups[0].initialization,
        InitializeDisposition::Unknown
    );
    assert!(failure.outcome_unknown);
    assert_eq!(failure.cleanup, StartupCleanup::Released);
    shutdown.await.unwrap().unwrap();
    reusable(root.path(), &peers);
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_native_initialization_io_failure_keeps_unknown_and_original_io_chain() {
    use std::os::unix::fs::PermissionsExt;
    struct Restore(std::path::PathBuf);
    impl Drop for Restore {
        fn drop(&mut self) {
            let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o700));
        }
    }
    let root = tempfile::tempdir().unwrap();
    let native = root.path().join("group-7");
    std::fs::create_dir(&native).unwrap();
    std::fs::set_permissions(&native, std::fs::Permissions::from_mode(0o500)).unwrap();
    let _restore = Restore(native.clone());
    let peers = vec![(1, addr())];
    let owner = start(root.path(), 1, &peers, Factory::new(root.path()), true).await;
    let failure = owner
        .handle()
        .start_groups_with_preference(batch(&[7], &[1], Some(1), Duration::from_secs(2)))
        .await
        .unwrap_err();
    assert_eq!(failure.phase, StartupPhase::Initialize);
    assert_eq!(
        failure.report.groups[0].initialization,
        InitializeDisposition::Unknown
    );
    assert!(failure.outcome_unknown);
    assert_eq!(failure.cleanup, StartupCleanup::Released);
    let multiraft_net::RuntimeError::Source(multiraft_core::MultiRaftError::Other(native_error)) =
        &failure.source
    else {
        panic!("native source missing");
    };
    let native = native_error
        .downcast_ref::<multiraft_core::typ::RaftError<multiraft_core::typ::InitializeError>>()
        .unwrap();
    assert!(matches!(
        native,
        openraft::errors::RaftError::Fatal(openraft::errors::Fatal::StorageError(_))
    ));
    let mut original = false;
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&failure);
    while let Some(error) = source {
        // OpenRaft owns the IO conversion to AnyError; preserve that native
        // source and chain rather than inventing a missing std::io::Error.
        original |= error.to_string().contains("Permission denied");
        source = error.source();
    }
    assert!(original, "native IO source must survive {failure:?}");
    reusable(root.path(), &peers);
    owner.shutdown(deadline()).await.unwrap();
}
