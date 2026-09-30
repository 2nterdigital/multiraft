//! Public static inputs preserve generic defaults and old consumer native profiles.
use multiraft_core::{ClusterConfig, FileLogSyncLevel, SnapshotMode};
use multiraft_fsm::CounterFsm;
use multiraft_net::{GroupConfig, NodeOwner, RuntimeConfig};
use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

#[test]
fn explicit_native_and_disabled_profiles_preserve_source_based_defaults() {
    let mut config = ClusterConfig::for_test(1, &[1]);
    assert_eq!(
        config.install_snapshot_timeout_ms,
        openraft::Config::default().install_snapshot_timeout
    );
    assert_eq!(config.install_snapshot_timeout_ms, 200);
    assert_eq!(config.snapshot_log_retention(), 0);
    config.non_durable_snapshot_log_retention =
        Some(openraft::Config::default().max_in_snapshot_log_to_keep);
    assert_eq!(config.snapshot_log_retention(), 1000);
    config.validate_snapshot_storage().unwrap();
    config.snapshot_mode = SnapshotMode::NativeDurable;
    config.data_dir = "native-profile".into();
    config.file_log_sync_level = FileLogSyncLevel::Data;
    config.non_durable_snapshot_log_retention = None;
    config.install_snapshot_timeout_ms = 5000;
    config.retain_log_entries = 17;
    config.snapshot_keep = 1;
    assert_eq!(config.snapshot_log_retention(), 17);
    assert_eq!(config.heartbeat_interval_ms, 100);
    assert_eq!(
        (
            config.election_timeout_min_ms,
            config.election_timeout_max_ms
        ),
        (300, 600)
    );
    config.validate_snapshot_storage().unwrap();
}

#[tokio::test]
async fn invalid_budget_or_retention_profile_is_rejected_before_listener_and_fsm_publication() {
    let scratch =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/issue-170/config");
    std::fs::create_dir_all(&scratch).unwrap();
    let root = tempfile::tempdir_in(scratch).unwrap();
    for case in 0..3 {
        let address = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let mut cluster = ClusterConfig::for_test(1, &[1]);
        cluster.peers = vec![(1, address)];
        match case {
            0 => cluster.install_snapshot_timeout_ms = 0,
            1 => cluster.install_snapshot_timeout_ms = 30001,
            2 => {
                cluster.snapshot_mode = SnapshotMode::NativeDurable;
                cluster.data_dir = root.path().join("native");
                cluster.file_log_sync_level = FileLogSyncLevel::Data;
                cluster.non_durable_snapshot_log_retention = Some(0);
            }
            _ => unreachable!(),
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let factory_calls = calls.clone();
        let result = NodeOwner::start(
            RuntimeConfig::new(
                cluster,
                vec![GroupConfig {
                    group_id: 7,
                    voters: vec![1],
                }],
            ),
            move |_| {
                factory_calls.fetch_add(1, Ordering::SeqCst);
                Ok(CounterFsm::new())
            },
            Instant::now() + Duration::from_secs(2),
        )
        .await;
        assert!(result.is_err(), "case {case}");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        TcpListener::bind(address).unwrap();
    }
}

#[tokio::test]
async fn production_constructor_supports_high_project_ids_without_synthetic_ports() {
    for node in [101, 65535] {
        let address = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let config = ClusterConfig::new(node, vec![(node, address)]);
        assert_eq!(config.peers, vec![(node, address)]);
        assert_eq!(config.retain_log_entries, 1024);
        assert_eq!(config.install_snapshot_timeout_ms, 200);
        assert_eq!(config.heartbeat_interval_ms, 100);
        assert_eq!(
            (
                config.election_timeout_min_ms,
                config.election_timeout_max_ms
            ),
            (300, 600)
        );
        let owner = NodeOwner::start(
            RuntimeConfig::new(
                config,
                vec![GroupConfig {
                    group_id: 7,
                    voters: vec![node],
                }],
            ),
            |_| Ok(CounterFsm::new()),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
        owner
            .shutdown(Instant::now() + Duration::from_secs(3))
            .await
            .unwrap();
        TcpListener::bind(address).unwrap();
    }
}

#[test]
fn full_native_retention_range_keeps_byte_and_profile_limits_independent() {
    for keep in [65537, u64::MAX] {
        let mut config =
            ClusterConfig::new(65535, vec![(65535, "127.0.0.1:32123".parse().unwrap())]);
        config.non_durable_snapshot_log_retention = Some(keep);
        config.validate_snapshot_storage().unwrap();
        assert_eq!(config.snapshot_log_retention(), keep);
        config.non_durable_snapshot_log_retention = None;
        config.snapshot_mode = SnapshotMode::NativeDurable;
        config.data_dir = "native-profile".into();
        config.file_log_sync_level = FileLogSyncLevel::Data;
        config.retain_log_entries = keep;
        config.validate_snapshot_storage().unwrap();
        assert_eq!(config.snapshot_log_retention(), keep);
        config.max_snapshot_bytes = 64 * 1024 * 1024 + 1;
        assert!(config.validate_snapshot_storage().is_err());
    }
}
