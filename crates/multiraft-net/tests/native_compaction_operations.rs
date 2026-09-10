//! Public local-Group contract: refusal is not submission; observations prove completion.
use multiraft_core::{ClusterConfig, FileLogSyncLevel, SnapshotMode};
use multiraft_fsm::CounterFsm;
use multiraft_net::{CompactionProgress, CompactionRejection, MultiRaft};
use std::time::Duration;

#[tokio::test]
async fn disabled_group_refuses_compaction_and_unknown_group_is_distinct() {
    let node = MultiRaft::start(ClusterConfig::for_test(1, &[1]))
        .await
        .unwrap();
    node.create_group(7, &[1]).await.unwrap();
    assert_eq!(
        node.request_compaction(8).await.unwrap_err(),
        CompactionRejection::UnknownGroup
    );
    assert_eq!(
        node.request_compaction(7).await.unwrap_err(),
        CompactionRejection::Disabled
    );
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn local_compaction_observes_durable_checkpoint_and_native_purge() {
    let root = tempfile::tempdir().unwrap();
    let mut cfg = ClusterConfig::for_test(1, &[1]);
    cfg.data_dir = root.path().into();
    cfg.file_log_sync_level = FileLogSyncLevel::Data;
    cfg.snapshot_mode = SnapshotMode::NativeDurable;
    cfg.retain_log_entries = 2;
    let node = MultiRaft::start(cfg).await.unwrap();
    node.create_group(7, &[1]).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !node.is_leader(7) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    for i in 1..=10 {
        node.propose(7, CounterFsm::encode_add(1, i)).await.unwrap();
    }
    let submitted = node.request_compaction(7).await.unwrap();
    assert!(submitted.target.is_some());
    let status = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let status = node.local_storage_status(7).await.unwrap();
            if status.progress == CompactionProgress::CompletedObserved {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(status.durable_snapshot.is_some());
    assert_eq!(
        status.purged.unwrap().index,
        submitted.target.unwrap().index - 2
    );
    assert!(status.retained_log_bytes.unwrap() > 0);
    assert!(!status.no_purge_needed);
    assert_eq!(
        node.read_linearizable(7, |fsm| fsm.value(7)).await.unwrap(),
        10
    );
    node.shutdown().await.unwrap();
    assert_eq!(
        node.request_compaction(7).await.unwrap_err(),
        CompactionRejection::ShuttingDown
    );
}

#[tokio::test]
async fn retention_zero_default_and_maximum_have_explicit_no_purge_outcomes() {
    for retain in [0, 1024, 65536] {
        let root = tempfile::tempdir().unwrap();
        let mut cfg = ClusterConfig::for_test(1, &[1]);
        cfg.data_dir = root.path().into();
        cfg.file_log_sync_level = FileLogSyncLevel::Data;
        cfg.snapshot_mode = SnapshotMode::NativeDurable;
        if retain != 1024 {
            cfg.retain_log_entries = retain;
        }
        let node = MultiRaft::start(cfg.clone()).await.unwrap();
        node.create_group(7, &[1]).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !node.is_leader(7) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        node.propose(7, CounterFsm::encode_add(3, 1)).await.unwrap();
        let submitted = node.request_compaction(7).await.unwrap();
        let status = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let status = node.local_storage_status(7).await.unwrap();
                if status.progress == CompactionProgress::CompletedObserved {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        if retain == 0 {
            assert_eq!(status.purged, submitted.target);
            assert!(!status.no_purge_needed);
        } else {
            assert_eq!(status.purged, None);
            assert!(status.no_purge_needed);
        }
        node.shutdown().await.unwrap();
        // Observer-only reopening restores the checkpoint without replaying an operation.
        let reopened = MultiRaft::start(cfg).await.unwrap();
        reopened.create_group(7, &[1]).await.unwrap();
        reopened
            .wait_for_recovery(7, Duration::from_secs(5))
            .await
            .unwrap();
        let observed = reopened.local_storage_status(7).await.unwrap();
        assert!(observed.durable_snapshot.is_some());
        assert_eq!(observed.progress, CompactionProgress::Idle);
        reopened.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn known_capture_refusal_keeps_native_group_and_logs_available() {
    let root = tempfile::tempdir().unwrap();
    let mut cfg = ClusterConfig::for_test(1, &[1]);
    cfg.data_dir = root.path().into();
    cfg.file_log_sync_level = FileLogSyncLevel::Data;
    cfg.snapshot_mode = SnapshotMode::NativeDurable;
    cfg.max_snapshot_bytes = 1;
    let node = MultiRaft::start(cfg).await.unwrap();
    node.create_group(7, &[1]).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !node.is_leader(7) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    node.propose(7, CounterFsm::encode_add(3, 1)).await.unwrap();
    assert_eq!(
        node.request_compaction(7).await.unwrap_err(),
        CompactionRejection::SizeLimit
    );
    let status = node.local_storage_status(7).await.unwrap();
    assert!(status.durable_snapshot.is_none());
    assert!(status.purged.is_none());
    assert_eq!(
        node.read_linearizable(7, |fsm| fsm.value(7)).await.unwrap(),
        3
    );
    node.shutdown().await.unwrap();
}
