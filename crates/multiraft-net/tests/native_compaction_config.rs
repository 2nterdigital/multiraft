//! Reject unsafe durability before publishing any native runtime.
use multiraft_core::{ClusterConfig, FileLogSyncLevel, SnapshotMode};
use multiraft_net::MultiRaft;

#[tokio::test]
async fn durable_mode_rejects_memory_weak_sync_and_invalid_limits() {
    let root = tempfile::tempdir().unwrap();
    for case in 0..5 {
        let mut config = ClusterConfig::for_test(1, &[1]);
        config.snapshot_mode = SnapshotMode::NativeDurable;
        config.data_dir = root.path().join(format!("case-{case}"));
        config.file_log_sync_level = FileLogSyncLevel::Data;
        match case {
            0 => config.data_dir = Default::default(),
            1 => config.file_log_sync_level = FileLogSyncLevel::Os,
            2 => config.retain_log_entries = 65537,
            3 => config.max_snapshot_bytes = 0,
            4 => config.max_snapshot_bytes = 67108865,
            _ => unreachable!(),
        }
        assert!(MultiRaft::start(config).await.is_err(), "case {case}");
    }
}

#[tokio::test]
async fn invalid_checkpoint_rejects_group_in_durable_and_legacy_modes() {
    for durable in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let checkpoint = root.path().join("snapshots/7/native-v1");
        std::fs::create_dir_all(&checkpoint).unwrap();
        std::fs::write(checkpoint.join("active.json"), b"corrupt").unwrap();
        let mut config = ClusterConfig::for_test(1, &[1]);
        config.data_dir = root.path().into();
        config.file_log_sync_level = FileLogSyncLevel::Data;
        if durable {
            config.snapshot_mode = SnapshotMode::NativeDurable;
        }
        let node = MultiRaft::start(config).await.unwrap();
        assert!(
            node.create_group(7, &[1]).await.is_err(),
            "checkpoint was ignored"
        );
        node.shutdown().await.unwrap();
    }
}

#[cfg(unix)]
#[tokio::test]
async fn legacy_mode_does_not_treat_unreadable_checkpoint_identity_as_absent() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let group_root = root.path().join("snapshots/7");
    std::fs::create_dir_all(group_root.join("native-v1")).unwrap();
    let original = std::fs::metadata(&group_root).unwrap().permissions();
    std::fs::set_permissions(&group_root, std::fs::Permissions::from_mode(0o0)).unwrap();
    let mut config = ClusterConfig::for_test(1, &[1]);
    config.data_dir = root.path().into();
    config.file_log_sync_level = FileLogSyncLevel::Data;
    let node = MultiRaft::start(config).await.unwrap();
    let result = node.create_group(7, &[1]).await;
    std::fs::set_permissions(&group_root, original).unwrap();
    node.shutdown().await.unwrap();
    assert!(
        result.is_err(),
        "unknown checkpoint identity was treated as an empty root"
    );
}
