//! Direct fresh-FSM oracle after real native prefix removal and suffix replay.
use multiraft_core::{FileLogSyncLevel, Request};
use multiraft_fsm::CounterFsm;
use multiraft_store::{
    FileLogStoreOf, NativeSmOptions, Raft, SnapshotCatalog, StateMachineStore, StubNetworkFactory,
};
use openraft::async_runtime::WatchReceiver;
use openraft::storage::{RaftLogReader, RaftLogStorage};
use openraft::{BasicNode, Config, SnapshotPolicy};
use std::{collections::BTreeMap, path::Path, sync::Arc, time::Duration};

async fn open(root: &Path) -> (Raft<CounterFsm>, StateMachineStore<CounterFsm>) {
    try_open(root).await.unwrap()
}

async fn try_open(
    root: &Path,
) -> Result<
    (Raft<CounterFsm>, StateMachineStore<CounterFsm>),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let log =
        FileLogStoreOf::open_with_options(root.join("log"), 0, FileLogSyncLevel::Data).unwrap();
    let sm = StateMachineStore::with_native_options(
        7,
        CounterFsm::new(),
        NativeSmOptions {
            catalog: Arc::new(SnapshotCatalog::new(root.join("snapshots"), 1)),
            max_snapshot_bytes: 1024,
            build_budget: Arc::new(tokio::sync::Semaphore::new(1)),
        },
    )
    .unwrap();
    let config = Config {
        heartbeat_interval: 100,
        election_timeout_min: 300,
        election_timeout_max: 600,
        snapshot_policy: SnapshotPolicy::Never,
        max_in_snapshot_log_to_keep: 0,
        ..Default::default()
    };
    let raft = openraft::Raft::new(
        1,
        Arc::new(config.validate().unwrap()),
        StubNetworkFactory,
        log,
        sm.clone(),
    )
    .await?;
    Ok((raft, sm))
}

#[tokio::test]
async fn native_checkpoint_plus_suffix_restores_after_prefix_is_removed() {
    let root = tempfile::tempdir().unwrap();
    let (raft, sm) = open(root.path()).await;
    raft.initialize(BTreeMap::from([(1, BasicNode::new("local"))]))
        .await
        .unwrap();
    raft.wait(Some(Duration::from_secs(5)))
        .current_leader(1, "leader")
        .await
        .unwrap();
    for i in 1..=10 {
        raft.client_write(Request::new(CounterFsm::encode_add(1, i)))
            .await
            .unwrap();
    }
    raft.trigger().snapshot().await.unwrap();
    let purged = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let metrics = raft.metrics().borrow_watched().clone();
            if let Some(purged) = metrics.purged {
                break purged;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(purged.index >= 10);
    raft.client_write(Request::new(CounterFsm::encode_add(7, 11)))
        .await
        .unwrap();
    assert_eq!(sm.with_fsm(|fsm| fsm.value(7)).await, 17);
    raft.shutdown().await.unwrap();
    drop(raft);
    drop(sm);
    {
        let mut log =
            FileLogStoreOf::open_with_options(root.path().join("log"), 0, FileLogSyncLevel::Data)
                .unwrap();
        assert_eq!(
            log.get_log_state().await.unwrap().last_purged_log_id,
            Some(purged)
        );
        let retained = log.try_get_log_entries(0..=purged.index).await.unwrap();
        assert!(
            retained.is_empty(),
            "covered prefix must actually be removed"
        );
    }
    let (reopened, sm) = open(root.path()).await;
    reopened
        .wait_for_recovery(Some(Duration::from_secs(5)))
        .await
        .unwrap();
    assert_eq!(
        sm.with_fsm(|fsm| fsm.value(7)).await,
        17,
        "fresh FSM needs snapshot and later committed suffix"
    );
    reopened.shutdown().await.unwrap();
}

#[cfg(unix)]
#[path = "native_crash_support/mod.rs"]
mod crash_support;

#[cfg(unix)]
#[tokio::test]
async fn purge_crash_cuts_keep_checkpoint_coverage_and_replayable_suffix() {
    use std::os::unix::process::ExitStatusExt;
    for cut in [
        "purge_marker_synced",
        "rewrite_data_synced",
        "rewrite_renamed",
        "rewrite_directory_synced",
    ] {
        let root = tempfile::tempdir().unwrap();
        let (raft, sm) = open(root.path()).await;
        raft.initialize(BTreeMap::from([(1, BasicNode::new("local"))]))
            .await
            .unwrap();
        raft.wait(Some(Duration::from_secs(5)))
            .current_leader(1, "leader")
            .await
            .unwrap();
        for i in 1..=10 {
            raft.client_write(Request::new(CounterFsm::encode_add(1, i)))
                .await
                .unwrap();
        }
        raft.trigger().snapshot().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if raft.metrics().borrow_watched().purged.is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        raft.client_write(Request::new(CounterFsm::encode_add(7, 11)))
            .await
            .unwrap();
        raft.shutdown().await.unwrap();
        drop(raft);
        drop(sm);
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--ignored", "--exact", "purge_crash_writer", "--nocapture"])
            .env("NATIVE_PURGE_CRASH_ROOT", root.path())
            .env("NATIVE_PURGE_CRASH_CUT", cut)
            .output()
            .unwrap();
        assert_eq!(
            output.status.signal(),
            Some(9),
            "cut {cut} not reached: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let active = SnapshotCatalog::new(root.path().join("snapshots"), 1)
            .load_native(7, 1024)
            .unwrap()
            .unwrap();
        {
            let mut log = FileLogStoreOf::open_with_options(
                root.path().join("log"),
                0,
                FileLogSyncLevel::Data,
            )
            .unwrap();
            let state = log.get_log_state().await.unwrap();
            assert!(
                state.last_purged_log_id <= active.meta.last_log_id,
                "P > S at {cut}"
            );
            let entries = log.try_get_log_entries(0..=u64::MAX).await.unwrap();
            for pair in entries.windows(2) {
                assert_eq!(
                    pair[0].log_id.index + 1,
                    pair[1].log_id.index,
                    "suffix gap at {cut}"
                );
            }
        }
        let (reopened, sm) = open(root.path()).await;
        reopened
            .wait_for_recovery(Some(Duration::from_secs(5)))
            .await
            .unwrap();
        assert_eq!(
            sm.with_fsm(|fsm| fsm.value(7)).await,
            17,
            "lost acknowledged state at {cut}"
        );
        reopened.shutdown().await.unwrap();
    }
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "child process used only by purge_crash_cuts_keep_checkpoint_coverage_and_replayable_suffix"]
async fn purge_crash_writer() {
    let root = std::path::PathBuf::from(std::env::var_os("NATIVE_PURGE_CRASH_ROOT").unwrap());
    let cut = std::env::var("NATIVE_PURGE_CRASH_CUT").unwrap();
    let subscriber = crash_support::kill_at("multiraft::native_file_log", cut);
    let _guard = tracing::subscriber::set_default(subscriber);
    let (raft, _) = open(&root).await;
    raft.wait_for_recovery(Some(Duration::from_secs(5)))
        .await
        .unwrap();
    raft.trigger().snapshot().await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    panic!("expected purge crash cut was not reached");
}

#[cfg(unix)]
#[tokio::test]
async fn install_crash_cuts_restore_acknowledged_snapshot_plus_suffix_state() {
    use std::os::unix::process::ExitStatusExt;
    for cut in [
        "staged",
        "application_restored",
        "activated",
        "bridge_updated",
    ] {
        let root = tempfile::tempdir().unwrap();
        let (raft, sm) = open(root.path()).await;
        raft.initialize(BTreeMap::from([(1, BasicNode::new("local"))]))
            .await
            .unwrap();
        raft.wait(Some(Duration::from_secs(5)))
            .current_leader(1, "leader")
            .await
            .unwrap();
        for i in 1..=10 {
            raft.client_write(Request::new(CounterFsm::encode_add(1, i)))
                .await
                .unwrap();
        }
        raft.trigger().snapshot().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if raft.metrics().borrow_watched().purged.is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        raft.client_write(Request::new(CounterFsm::encode_add(7, 11)))
            .await
            .unwrap();
        raft.shutdown().await.unwrap();
        drop(raft);
        drop(sm);
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "install_crash_writer",
                "--nocapture",
            ])
            .env("NATIVE_INSTALL_CRASH_ROOT", root.path())
            .env("NATIVE_INSTALL_CRASH_CUT", cut)
            .output()
            .unwrap();
        assert_eq!(
            output.status.signal(),
            Some(9),
            "cut {cut} not reached: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let (reopened, sm) = open(root.path()).await;
        reopened
            .wait_for_recovery(Some(Duration::from_secs(5)))
            .await
            .unwrap();
        assert_eq!(
            sm.with_fsm(|fsm| fsm.value(7)).await,
            17,
            "lost acknowledged state at install cut {cut}"
        );
        reopened.shutdown().await.unwrap();
    }
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "child process used only by install_crash_cuts_restore_acknowledged_snapshot_plus_suffix_state"]
async fn install_crash_writer() {
    use multiraft_fsm::StateMachine;
    use openraft::storage::RaftStateMachine;
    let root = std::path::PathBuf::from(std::env::var_os("NATIVE_INSTALL_CRASH_ROOT").unwrap());
    let cut = std::env::var("NATIVE_INSTALL_CRASH_CUT").unwrap();
    let (raft, mut sm) = open(&root).await;
    raft.wait_for_recovery(Some(Duration::from_secs(5)))
        .await
        .unwrap();
    // Exercise the actual install adapter without an unrelated election racing the cut.
    raft.shutdown().await.unwrap();
    let (last_log_id, last_membership) = sm.applied_state().await.unwrap();
    let bytes = sm.with_fsm(|fsm| fsm.snapshot(7)).await.unwrap();
    let meta = openraft::alias::SnapshotMetaOf::<multiraft_core::TypeConfig> {
        last_log_id,
        last_membership,
        snapshot_id: "received-committed-suffix".into(),
    };
    tracing::subscriber::set_global_default(crash_support::kill_at(
        "multiraft::native_install",
        cut,
    ))
    .unwrap();
    sm.install_snapshot(&meta, std::io::Cursor::new(bytes))
        .await
        .unwrap();
    panic!("expected install crash cut was not reached");
}

#[tokio::test]
async fn restart_rejects_missing_checkpoint_or_a_middle_committed_suffix_entry() {
    for missing_checkpoint in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let (raft, sm) = open(root.path()).await;
        raft.initialize(BTreeMap::from([(1, BasicNode::new("local"))]))
            .await
            .unwrap();
        raft.wait(Some(Duration::from_secs(5)))
            .current_leader(1, "leader")
            .await
            .unwrap();
        raft.client_write(Request::new(CounterFsm::encode_add(10, 1)))
            .await
            .unwrap();
        raft.trigger().snapshot().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if raft.metrics().borrow_watched().purged.is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let mut suffix = Vec::new();
        for (id, delta) in [(2, 1), (3, 2), (4, 4)] {
            suffix.push(
                raft.client_write(Request::new(CounterFsm::encode_add(delta, id)))
                    .await
                    .unwrap()
                    .log_id
                    .index,
            );
        }
        assert_eq!(sm.with_fsm(|fsm| fsm.value(7)).await, 17);
        raft.shutdown().await.unwrap();
        drop(raft);
        drop(sm);
        if missing_checkpoint {
            std::fs::remove_file(root.path().join("snapshots/7/native-v1/active.json")).unwrap();
        } else {
            let path = root.path().join("log/log.bin");
            let data = std::fs::read(&path).unwrap();
            let mut rewritten = Vec::new();
            let mut offset = 0;
            let mut removed = 0;
            while offset < data.len() {
                let length =
                    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                let end = offset + 4 + length;
                let entry: multiraft_core::typ::Entry =
                    bincode::deserialize(&data[offset + 4..end]).unwrap();
                if entry.log_id.index == suffix[1] {
                    removed += 1;
                } else {
                    rewritten.extend_from_slice(&data[offset..end]);
                }
                offset = end;
            }
            assert_eq!(removed, 1);
            std::fs::write(path, rewritten).unwrap();
        }
        match try_open(root.path()).await {
            Err(_) => (),
            Ok((raft, sm)) => {
                let value = sm.with_fsm(|fsm| fsm.value(7)).await;
                raft.shutdown().await.unwrap();
                panic!("incomplete recovery source was accepted, value={value}, missing_checkpoint={missing_checkpoint}");
            }
        }
    }
}
