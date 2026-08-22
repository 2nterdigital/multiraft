use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::{Arc, Mutex};

use multiraft_core::typ::{Entry, LogId};
use multiraft_core::{FileLogSyncLevel, TypeConfig};
use multiraft_fsm::{CounterFsm, StateMachine};
use multiraft_store::{FileLogStoreOf, StateMachineStore};
use openraft::alias::LeaderIdOf;
use openraft::alias::{SnapshotMetaOf, StoredMembershipOf};
use openraft::entry::RaftEntry;
use openraft::storage::{RaftLogStorage, RaftLogStorageExt, RaftStateMachine};
use openraft::type_config::TypeConfigExt;
use openraft::vote::RaftLeaderIdExt;
use openraft::RaftSnapshotBuilder;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;
use tracing_subscriber::Layer;

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

#[test]
fn durable_recovery_events_report_persisted_boundaries() {
    let data_root = tempfile::tempdir().expect("temporary data root");
    let expected_directory = data_root.path().display().to_string();
    let os_data_root = tempfile::tempdir().expect("temporary OS-sync data root");
    let expected_os_directory = os_data_root.path().display().to_string();
    let capture = EventCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());

    tracing::subscriber::with_default(subscriber, || {
        TypeConfig::run(async {
            let mut store =
                FileLogStoreOf::open_with_options(data_root.path(), 0, FileLogSyncLevel::Data)
                    .expect("open new log");
            store
                .blocking_append(vec![blank(1), blank(2)])
                .await
                .expect("append entries");
            store
                .save_committed(Some(blank(2).log_id()))
                .await
                .expect("save committed boundary");
            store
                .purge(LogId::new(LeaderIdOf::<TypeConfig>::new_committed(1, 1), 1))
                .await
                .expect("purge prefix");
            drop(store);

            let _reopened =
                FileLogStoreOf::open_with_options(data_root.path(), 0, FileLogSyncLevel::Data)
                    .expect("reopen log");

            let mut os_store =
                FileLogStoreOf::open_with_options(os_data_root.path(), 0, FileLogSyncLevel::Os)
                    .expect("open OS-sync log");
            os_store
                .blocking_append(vec![blank(1)])
                .await
                .expect("append OS-sync entry");
            os_store
                .purge(blank(1).log_id())
                .await
                .expect("purge OS-sync prefix");

            let mut seeded_fsm = CounterFsm::new();
            seeded_fsm
                .apply(44, 7, &CounterFsm::encode_add(7, 99))
                .expect("seed snapshot state");
            let seeded_snapshot = seeded_fsm.snapshot(44).expect("encode seeded snapshot");
            assert_eq!(seeded_snapshot, br#"[7,[99]]"#);

            let last_log_id = LogId::new(LeaderIdOf::<TypeConfig>::new_committed(3, 2), 7);
            let seed_meta = SnapshotMetaOf::<TypeConfig> {
                last_log_id: Some(last_log_id),
                last_membership: StoredMembershipOf::<TypeConfig>::default(),
                snapshot_id: "seed-3-2-7".to_owned(),
            };
            let mut state_machine = StateMachineStore::new(44, CounterFsm::new());
            RaftStateMachine::install_snapshot(
                &mut state_machine,
                &seed_meta,
                Cursor::new(seeded_snapshot.clone()),
            )
            .await
            .expect("install seeded snapshot");
            let built = RaftSnapshotBuilder::build_snapshot(&mut state_machine)
                .await
                .expect("build snapshot");
            assert_eq!(built.snapshot.into_inner(), seeded_snapshot);
        });
    });

    let open_events = capture.matching("file_log_open");
    assert!(
        open_events.len() >= 2,
        "expected initial open and reopen diagnostics, got {open_events:?}"
    );
    let reopened = open_events
        .iter()
        .find(|event| {
            event.get("directory").map(String::as_str) == Some(expected_directory.as_str())
                && event.get("persisted_last_purged_index").map(String::as_str) == Some("Some(1)")
        })
        .expect("reopen event");
    assert!(
        reopened.get("_target").map(String::as_str) == Some("multiraft::recovery")
            && reopened.get("directory").map(String::as_str) == Some(expected_directory.as_str())
            && reopened
                .get("persisted_last_purged_index")
                .map(String::as_str)
                == Some("Some(1)")
            && reopened
                .get("persisted_committed_index")
                .map(String::as_str)
                == Some("Some(2)")
            && reopened.get("retained_entries").map(String::as_str) == Some("1")
            && reopened.get("retained_first_index").map(String::as_str) == Some("Some(2)")
            && reopened.get("sync_level").map(String::as_str) == Some("Data"),
        "reopen diagnostic omitted durable state: {reopened:?}"
    );

    let purge_events = capture.matching("file_log_purge");
    assert!(
        purge_events.iter().any(|event| {
            event.get("_target").map(String::as_str) == Some("multiraft::recovery")
                && event.get("phase").map(String::as_str) == Some("complete")
                && event.get("directory").map(String::as_str) == Some(expected_directory.as_str())
                && event.get("requested_purge_index").map(String::as_str) == Some("1")
                && event.get("removed_entries").map(String::as_str) == Some("1")
                && event.get("remaining_entries").map(String::as_str) == Some("1")
                && event.get("first_retained_index").map(String::as_str) == Some("Some(2)")
                && event.get("sync_level").map(String::as_str) == Some("Data")
                && event.get("crash_durable").map(String::as_str) == Some("true")
        }),
        "purge diagnostic omitted physical boundary: {purge_events:?}"
    );
    assert!(
        purge_events.iter().any(|event| {
            event.get("_target").map(String::as_str) == Some("multiraft::recovery")
                && event.get("directory").map(String::as_str)
                    == Some(expected_os_directory.as_str())
                && event.get("sync_level").map(String::as_str) == Some("Os")
                && event.get("crash_durable").map(String::as_str) == Some("false")
        }),
        "OS-sync purge must not claim power-loss durability: {purge_events:?}"
    );

    let snapshot_events = capture.matching("native_snapshot_build");
    assert!(
        snapshot_events.iter().any(|event| {
            event.get("_target").map(String::as_str) == Some("multiraft::recovery")
                && event.get("phase").map(String::as_str) == Some("complete")
                && event.get("group_id").map(String::as_str) == Some("44")
                && event.get("last_log_index").map(String::as_str) == Some("Some(7)")
                && event.get("last_log_term").map(String::as_str) == Some("Some(3)")
                && event.get("snapshot_bytes").map(String::as_str) == Some("8")
                && event.get("durability").map(String::as_str) == Some("memory_only")
        }),
        "snapshot diagnostic omitted identity or durability: {snapshot_events:?}"
    );
}
