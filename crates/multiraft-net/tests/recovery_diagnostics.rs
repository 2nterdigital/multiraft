use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use multiraft_core::typ::{Entry, LogId};
use multiraft_core::{ClusterConfig, FileLogSyncLevel, TypeConfig};
use multiraft_fsm::CounterFsm;
use multiraft_net::MultiRaft;
use multiraft_store::FileLogStoreOf;
use openraft::alias::LeaderIdOf;
use openraft::entry::RaftEntry;
use openraft::storage::{RaftLogStorage, RaftLogStorageExt};
use openraft::vote::RaftLeaderIdExt;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;
use tracing_subscriber::Layer;

static DIAGNOSTIC_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Clone, Default)]
struct EventCapture {
    events: Arc<Mutex<Vec<BTreeMap<String, String>>>>,
}

impl EventCapture {
    fn matching(&self, operation: &str) -> Vec<BTreeMap<String, String>> {
        self.events
            .lock()
            .expect("event capture lock")
            .iter()
            .filter(|event| event.get("operation").map(String::as_str) == Some(operation))
            .cloned()
            .collect()
    }
}

#[derive(Default)]
struct FieldVisitor {
    fields: BTreeMap<String, String>,
}

impl Visit for FieldVisitor {
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields
            .insert(field.name().to_owned(), value.to_string());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields
            .insert(field.name().to_owned(), value.to_string());
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields
            .insert(field.name().to_owned(), value.to_owned());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.fields
            .insert(field.name().to_owned(), format!("{value:?}"));
    }
}

impl<S> Layer<S> for EventCapture
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);
        visitor
            .fields
            .insert("_target".to_owned(), event.metadata().target().to_owned());
        self.events
            .lock()
            .expect("event capture lock")
            .push(visitor.fields);
    }
}

fn blank(index: u64) -> Entry {
    let leader_id = LeaderIdOf::<TypeConfig>::new_committed(1, 1);
    Entry::new_blank(LogId::new(leader_id, index))
}

async fn seed_purged_committed_log(group_path: &std::path::Path) {
    let mut store = FileLogStoreOf::open_with_options(group_path, 0, FileLogSyncLevel::Data)
        .expect("open recovery fixture log");
    let committed = blank(2).log_id();
    store
        .blocking_append(vec![blank(1), blank(2)])
        .await
        .expect("append recovery fixture entries");
    store
        .save_committed(Some(committed))
        .await
        .expect("save recovery fixture commit");
    store
        .purge(blank(1).log_id())
        .await
        .expect("purge recovery fixture prefix");
}

fn assert_group_start_failure(
    capture: &EventCapture,
    expected_directory: &str,
    expected_cause: &str,
) {
    let events = capture.matching("group_start");
    let starts: Vec<_> = events
        .iter()
        .filter(|event| event.get("phase").map(String::as_str) == Some("start"))
        .collect();
    let errors: Vec<_> = events
        .iter()
        .filter(|event| event.get("phase").map(String::as_str) == Some("error"))
        .collect();
    let completes: Vec<_> = events
        .iter()
        .filter(|event| event.get("phase").map(String::as_str) == Some("complete"))
        .collect();

    assert_eq!(starts.len(), 1, "expected one group start: {events:?}");
    assert_eq!(
        errors.len(),
        1,
        "every group start needs one error: {events:?}"
    );
    assert!(
        completes.is_empty(),
        "failed group was published: {events:?}"
    );

    for event in [starts[0], errors[0]] {
        assert_eq!(
            event.get("_target").map(String::as_str),
            Some("multiraft::recovery"),
            "wrong diagnostics target: {event:?}"
        );
        assert_eq!(event.get("node_id").map(String::as_str), Some("1"));
        assert_eq!(event.get("group_id").map(String::as_str), Some("9"));
        assert_eq!(
            event.get("directory").map(String::as_str),
            Some(expected_directory)
        );
    }
    assert!(
        errors[0]
            .get("error")
            .is_some_and(|error| error.contains(expected_cause)),
        "group error omitted root cause {expected_cause:?}: {events:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn file_group_lifecycle_reports_start_recovery_and_shutdown() {
    let _diagnostic_test_guard = DIAGNOSTIC_TEST_LOCK.lock().await;
    let data_root = tempfile::tempdir().expect("temporary data root");
    let capture = EventCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    let _subscriber_guard = tracing::subscriber::set_default(subscriber);

    let mut config = ClusterConfig::for_test(1, &[1]);
    config.data_dir = data_root.path().to_path_buf();
    config.file_log_sync_level = FileLogSyncLevel::Data;
    let group_path = data_root.path().join("group-9");
    let expected_directory = group_path.display().to_string();
    let node = MultiRaft::start(config).await.expect("start node");
    node.create_group(9, &[1]).await.expect("create group");

    let leader_deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !node.is_leader(9) {
        assert!(
            std::time::Instant::now() < leader_deadline,
            "single voter did not become leader"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let proposal = node
        .propose(9, CounterFsm::encode_add(1, 1))
        .await
        .expect("propose");
    let expected_applied_index = format!("Some({})", proposal.index);
    let expected_applied_term = format!("Some({})", proposal.term);
    node.wait_for_recovery(9, Duration::from_secs(2))
        .await
        .expect("wait for recovery");
    node.shutdown().await.expect("shutdown");

    let group_events = capture.matching("group_start");
    assert!(
        group_events.iter().any(|event| {
            event.get("_target").map(String::as_str) == Some("multiraft::recovery")
                && event.get("phase").map(String::as_str) == Some("start")
                && event.get("node_id").map(String::as_str) == Some("1")
                && event.get("group_id").map(String::as_str) == Some("9")
                && event.get("storage").map(String::as_str) == Some("file")
                && event.get("directory").map(String::as_str) == Some(expected_directory.as_str())
                && event.get("snapshot_policy").map(String::as_str) == Some("never")
        }),
        "group start diagnostic omitted recovery inputs: {group_events:?}"
    );
    assert!(
        group_events.iter().any(|event| {
            event.get("_target").map(String::as_str) == Some("multiraft::recovery")
                && event.get("phase").map(String::as_str) == Some("complete")
                && event.get("node_id").map(String::as_str) == Some("1")
                && event.get("group_id").map(String::as_str) == Some("9")
                && event.get("directory").map(String::as_str) == Some(expected_directory.as_str())
        }),
        "group publication diagnostic missing: {group_events:?}"
    );

    let recovery_events = capture.matching("recovery_wait");
    assert!(
        recovery_events.iter().any(|event| {
            event.get("_target").map(String::as_str) == Some("multiraft::recovery")
                && event.get("phase").map(String::as_str) == Some("start")
                && event.get("timeout_ms").map(String::as_str) == Some("2000")
        }) && recovery_events.iter().any(|event| {
            event.get("_target").map(String::as_str) == Some("multiraft::recovery")
                && event.get("phase").map(String::as_str) == Some("complete")
                && event.get("applied_index").map(String::as_str)
                    == Some(expected_applied_index.as_str())
                && event.get("applied_term").map(String::as_str)
                    == Some(expected_applied_term.as_str())
        }),
        "recovery wait diagnostics incomplete: {recovery_events:?}"
    );

    let shutdown_events = capture.matching("node_shutdown");
    assert!(
        shutdown_events.iter().any(|event| {
            event.get("_target").map(String::as_str) == Some("multiraft::recovery")
                && event.get("phase").map(String::as_str) == Some("start")
                && event.get("group_count").map(String::as_str) == Some("1")
        }) && shutdown_events.iter().any(|event| {
            event.get("_target").map(String::as_str) == Some("multiraft::recovery")
                && event.get("phase").map(String::as_str) == Some("complete")
                && event.get("remaining_groups").map(String::as_str) == Some("0")
        }),
        "shutdown diagnostics incomplete: {shutdown_events:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn file_group_open_failure_reports_node_group_path_and_error() {
    let _diagnostic_test_guard = DIAGNOSTIC_TEST_LOCK.lock().await;
    let data_root = tempfile::tempdir().expect("temporary data root");
    let group_path = data_root.path().join("group-9");
    std::fs::write(&group_path, b"not a directory").expect("preseed invalid group path");
    let expected_directory = group_path.display().to_string();
    let expected_cause = std::fs::create_dir_all(&group_path)
        .expect_err("fixture path is a file")
        .to_string();

    let capture = EventCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    let _subscriber_guard = tracing::subscriber::set_default(subscriber);

    let mut config = ClusterConfig::for_test(1, &[1]);
    config.data_dir = data_root.path().to_path_buf();
    let node = MultiRaft::start(config).await.expect("start node shell");
    let error = node
        .create_group(9, &[1])
        .await
        .expect_err("invalid FileLog path must reject group creation");
    let returned_error = error.to_string();
    node.shutdown().await.expect("shutdown unpublished node");

    let group_events = capture.matching("group_start");
    assert!(
        group_events.iter().any(|event| {
            event.get("_target").map(String::as_str) == Some("multiraft::recovery")
                && event.get("phase").map(String::as_str) == Some("error")
                && event.get("node_id").map(String::as_str) == Some("1")
                && event.get("group_id").map(String::as_str) == Some("9")
                && event.get("directory").map(String::as_str) == Some(expected_directory.as_str())
                && event.get("error").map(String::as_str) == Some(expected_cause.as_str())
        }),
        "group open failure diagnostic omitted context: {group_events:?}; returned {error:?}"
    );
    assert!(
        returned_error.contains(&format!(
            "open file log for node 1, group 9 at {expected_directory}"
        )) && returned_error.contains(&expected_cause),
        "returned error lost operation context or source: {returned_error}"
    );
    assert!(
        !group_events
            .iter()
            .any(|event| event.get("phase").map(String::as_str) == Some("complete")),
        "failed group must not be published: {group_events:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn purged_log_without_persisted_fsm_reports_recovery_failure() {
    let _diagnostic_test_guard = DIAGNOSTIC_TEST_LOCK.lock().await;
    let data_root = tempfile::tempdir().expect("temporary data root");
    let group_path = data_root.path().join("group-9");
    seed_purged_committed_log(&group_path).await;

    let capture = EventCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    let _subscriber_guard = tracing::subscriber::set_default(subscriber);

    let mut config = ClusterConfig::for_test(1, &[1]);
    config.data_dir = data_root.path().to_path_buf();
    config.file_log_sync_level = FileLogSyncLevel::Data;
    let node = MultiRaft::start(config).await.expect("start node shell");
    let error = node
        .create_group(9, &[1])
        .await
        .expect_err("purged committed prefix with empty FSM must reject restart");
    node.shutdown().await.expect("shutdown unpublished node");

    let expected_directory = group_path.display().to_string();
    let expected_cause = "Cannot re-apply logs: need logs from index 0, but purged up to";
    assert_group_start_failure(&capture, &expected_directory, expected_cause);
    assert!(
        error.to_string().contains(expected_cause),
        "returned recovery error omitted storage root cause: {error:?}"
    );

    let open_events = capture.matching("file_log_open");
    assert!(
        open_events.iter().any(|event| {
            event.get("_target").map(String::as_str) == Some("multiraft::recovery")
                && event.get("directory").map(String::as_str) == Some(expected_directory.as_str())
                && event.get("persisted_last_purged_index").map(String::as_str) == Some("Some(1)")
                && event.get("persisted_committed_index").map(String::as_str) == Some("Some(2)")
                && event.get("retained_first_index").map(String::as_str) == Some("Some(2)")
        }),
        "restart did not observe the intended persisted fixture: {open_events:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn invalid_openraft_config_closes_group_start_lifecycle() {
    let _diagnostic_test_guard = DIAGNOSTIC_TEST_LOCK.lock().await;
    let data_root = tempfile::tempdir().expect("temporary data root");
    let group_path = data_root.path().join("group-9");
    let expected_directory = group_path.display().to_string();
    let expected_cause = "election timeout: min(100) must be < max(100)";
    let capture = EventCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    let _subscriber_guard = tracing::subscriber::set_default(subscriber);

    let mut config = ClusterConfig::for_test(1, &[1]);
    config.data_dir = data_root.path().to_path_buf();
    config.election_timeout_min_ms = 100;
    config.election_timeout_max_ms = 100;
    let node = MultiRaft::start(config).await.expect("start node shell");
    let error = node
        .create_group(9, &[1])
        .await
        .expect_err("invalid OpenRaft config must reject group creation");
    node.shutdown().await.expect("shutdown unpublished node");

    assert!(
        error.to_string().contains(expected_cause),
        "returned config error omitted root cause: {error:?}"
    );
    assert_group_start_failure(&capture, &expected_directory, expected_cause);
}

#[tokio::test(flavor = "current_thread")]
async fn failing_fsm_factory_closes_group_start_lifecycle() {
    let _diagnostic_test_guard = DIAGNOSTIC_TEST_LOCK.lock().await;
    let data_root = tempfile::tempdir().expect("temporary data root");
    let group_path = data_root.path().join("group-9");
    let expected_directory = group_path.display().to_string();
    let expected_cause = "diagnostic factory sentinel";
    let capture = EventCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    let _subscriber_guard = tracing::subscriber::set_default(subscriber);

    let mut config = ClusterConfig::for_test(1, &[1]);
    config.data_dir = data_root.path().to_path_buf();
    let node = MultiRaft::<CounterFsm>::start_with_factory(config, move |_| {
        Err::<CounterFsm, anyhow::Error>(anyhow::anyhow!(expected_cause))
    })
    .await
    .expect("start node shell");
    let error = node
        .create_group(9, &[1])
        .await
        .expect_err("factory failure must reject group creation");
    node.shutdown().await.expect("shutdown unpublished node");

    let returned_display = error.to_string();
    let returned_debug = format!("{error:?}");
    assert!(
        returned_display.contains("create FSM for node 1, group 9")
            && returned_debug.contains(expected_cause),
        "returned factory error omitted context or source chain: {returned_debug}"
    );
    assert_group_start_failure(&capture, &expected_directory, expected_cause);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_wait_does_not_block_on_diagnostic_fsm_read() {
    let _diagnostic_test_guard = DIAGNOSTIC_TEST_LOCK.lock().await;
    let node = Arc::new(
        MultiRaft::start(ClusterConfig::for_test(1, &[1]))
            .await
            .expect("start node"),
    );
    node.create_group(9, &[1]).await.expect("create group");

    let leader_deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !node.is_leader(9) {
        assert!(
            std::time::Instant::now() < leader_deadline,
            "single voter did not become leader"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    node.propose(9, CounterFsm::encode_add(1, 1))
        .await
        .expect("propose");
    node.wait_for_recovery(9, Duration::from_secs(2))
        .await
        .expect("initial recovery wait");

    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let holder_node = Arc::clone(&node);
    let holder_release = Arc::clone(&release);
    let holder = tokio::spawn(async move {
        holder_node
            .with_fsm(9, move |_| {
                let _ = entered_tx.send(());
                let (released, released_cv) = &*holder_release;
                let mut is_released = released.lock().expect("release lock");
                while !*is_released {
                    is_released = released_cv.wait(is_released).expect("release wait");
                }
            })
            .await
    });
    entered_rx.await.expect("FSM lock holder entered");

    let recovery = tokio::time::timeout(
        Duration::from_millis(500),
        node.wait_for_recovery(9, Duration::from_millis(200)),
    )
    .await;
    {
        let (released, released_cv) = &*release;
        *released.lock().expect("release lock") = true;
        released_cv.notify_one();
    }
    holder.await.expect("FSM lock holder task");

    assert!(
        matches!(recovery, Ok(Ok(()))),
        "diagnostic logging must not add an FSM-lock await after recovery: {recovery:?}"
    );
    node.shutdown().await.expect("shutdown");
}
