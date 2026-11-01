//! Managed cleanup crash trajectories use real persistent payload deletion boundaries.
use super::*;
use serde::{Deserialize, Serialize};
use std::{
    io::{BufRead, BufReader},
    process::{Command, Stdio},
};

#[derive(Serialize, Deserialize)]
struct Recovery {
    root: std::path::PathBuf,
    source: u64,
    peers: Vec<(u64, std::net::SocketAddr)>,
    operation: multiraft_core::OperationId,
    phase: String,
    checkpoint: std::path::PathBuf,
}

pub async fn child() {
    let harness = std::path::PathBuf::from(std::env::var_os("MANAGED_CLEANUP_HARNESS").unwrap());
    if std::env::var_os("MANAGED_CLEANUP_RESUME").is_some() {
        let recovery: Recovery =
            serde_json::from_slice(&std::fs::read(harness.join("recovery.json")).unwrap()).unwrap();
        let mut config = ClusterConfig::for_test(recovery.source, &[1, 2, 3]);
        config.peers = recovery.peers;
        config.data_dir = recovery.root.join(format!("node-{}", recovery.source));
        config.log_storage_backend = LogStorageBackend::RocksDb;
        config.snapshot_mode = SnapshotMode::NativeDurable;
        let live = multiraft_store::DiskFsmDb::group_path(&config.data_dir, GROUP);
        let existed = live.exists();
        assert_eq!(
            existed,
            recovery.phase == "logs",
            "FSM deletion is durable at the observed boundary"
        );
        let checkpoint_existed = recovery.checkpoint.exists();
        assert_eq!(
            checkpoint_existed,
            matches!(recovery.phase.as_str(), "logs" | "fsm"),
            "real NativeDurable checkpoint deletion is durable"
        );
        let owner = start_owned(config).await;
        let authority = Arc::new(FileAuthority::open(&recovery.root.join("catalog.json")));
        let group = owner
            .handle()
            .inspect_controlled_group(
                GROUP,
                recovery.source * 10,
                vec![recovery.source as u8],
                authority,
                deadline(),
            )
            .await
            .unwrap();
        assert_eq!(
            live.exists(),
            existed,
            "inspection never recreates deleted payload"
        );
        assert_eq!(
            recovery.checkpoint.exists(),
            checkpoint_existed,
            "inspection never recreates deleted checkpoint"
        );
        assert_eq!(
            group.receipt(deadline()).await.unwrap().stage,
            multiraft_net::ControlledGroupStage::LogsCleaned
        );
        let receipt = group
            .retire_and_cleanup_from_authority(recovery.operation, deadline())
            .await
            .unwrap();
        assert_eq!(receipt.stage, multiraft_net::ControlledGroupStage::Cleaned);
        assert_eq!(
            group
                .retire_and_cleanup_from_authority(recovery.operation, deadline())
                .await
                .unwrap(),
            receipt
        );
        let usage = group.storage_usage(deadline()).await.unwrap();
        assert_eq!(usage.fsm_file_bytes, Some(0));
        assert_eq!(usage.workspace_file_bytes, Some(0));
        assert_eq!(usage.snapshot_file_bytes, Some(0));
        assert_eq!(usage.retained_log_value_bytes, 0);
        owner.shutdown(deadline()).await.unwrap();
        std::fs::remove_dir_all(recovery.root).unwrap();
        return;
    }
    let fixture = Fixture::fresh().await;
    let leader = fixture.leader().await;
    fixture.owners[leader]
        .handle()
        .propose(GROUP, b"crash-cleanup-real-payload".to_vec(), deadline())
        .await
        .unwrap();
    let release = Arc::new(tokio::sync::Notify::new());
    let prepared = fixture
        .prepare_gated(
            transport::Gate::FinishedReply,
            Arc::new(tokio::sync::Notify::new()),
            release.clone(),
        )
        .await;
    release.notify_one();
    prepared
        .executor
        .seed_target(prepared.port.clone(), prepared.admission, deadline())
        .await
        .unwrap();
    prepared
        .executor
        .add_learner(prepared.port.clone(), deadline())
        .await
        .unwrap();
    prepared
        .executor
        .enter_joint(prepared.port.clone(), deadline())
        .await
        .unwrap();
    prepared
        .executor
        .finalize_uniform(deadline())
        .await
        .unwrap();
    prepared
        .executor
        .complete(prepared.port, deadline())
        .await
        .unwrap();
    let source = *[1, 2, 3]
        .iter()
        .find(|id| !prepared.specification.target_voters.contains(id))
        .unwrap();
    let source_index = (source - 1) as usize;
    let config = &fixture.configs[source_index];
    let mut recovery = Recovery {
        root: fixture.root.path().into(),
        source,
        peers: config.peers.clone(),
        operation: prepared.specification.operation,
        phase: std::env::var("MULTIRAFT_CLEANUP_OBSERVE_PHASE").unwrap(),
        checkpoint: std::path::PathBuf::new(),
    };
    std::fs::write(
        harness.join("recovery.json"),
        serde_json::to_vec(&recovery).unwrap(),
    )
    .unwrap();
    // Materialize the canonical checkpoint and native snapshot catalog through public
    // maintenance. Startup must validate the exact namespace left by a real crash.
    let handle = fixture.owners[source_index].handle();
    let compacted = handle.request_compaction(GROUP, deadline()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let status = handle
                .local_storage_status(GROUP, deadline())
                .await
                .unwrap();
            if status.progress == multiraft_net::CompactionProgress::CompletedObserved {
                assert!(
                    status
                        .durable_snapshot
                        .as_ref()
                        .unwrap()
                        .last_log_id
                        .as_ref()
                        .unwrap()
                        .index
                        >= compacted.target.as_ref().unwrap().index,
                    "published snapshot covers submitted applied boundary"
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("real owned snapshot completion");
    let observed = fixture.groups[source_index]
        .storage_usage(deadline())
        .await
        .unwrap();
    assert!(observed.fsm_file_bytes.unwrap() > 0);
    assert!(observed.workspace_file_bytes.unwrap() > 0);
    assert_eq!(
        observed.snapshot_file_bytes,
        Some(0),
        "this disk provider publishes snapshots in its workspace"
    );
    let live = multiraft_store::DiskFsmDb::group_path(&config.data_dir, GROUP);
    let workspace = multiraft_store::DiskFsmDb::workspace_path(&live).unwrap();
    let manifest_path = std::fs::read_dir(&workspace)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|ext| ext == "manifest"))
        .expect("real published NativeDurable checkpoint manifest");
    let manifest = multiraft_store::disk_snapshot::DiskManifest::decode(
        &std::fs::read(manifest_path).unwrap(),
        GROUP,
    )
    .unwrap();
    assert!(manifest.source.starts_with(&workspace));
    assert!(manifest.total_bytes().unwrap() > 0);
    for file in &manifest.files {
        assert_eq!(
            std::fs::metadata(manifest.source.join(&file.name))
                .unwrap()
                .len(),
            file.size
        );
    }
    recovery.checkpoint = manifest.source;
    std::fs::write(
        harness.join("recovery.json"),
        serde_json::to_vec(&recovery).unwrap(),
    )
    .unwrap();
    std::env::set_var("MULTIRAFT_CLEANUP_OBSERVE_ROOT", &config.data_dir);
    let group = fixture.groups[source_index].clone();
    let operation = prepared.specification.operation;
    let mode = std::env::var("MANAGED_CLEANUP_MODE").unwrap_or_default();
    let d = if mode == "timeout" {
        Instant::now() + Duration::from_millis(300)
    } else {
        deadline()
    };
    let waiter =
        tokio::spawn(async move { group.retire_and_cleanup_from_authority(operation, d).await });
    if mode == "cancel" || mode == "timeout" {
        let marker = config.data_dir.join("cleanup-observed-phase");
        tokio::time::timeout(Duration::from_secs(10), async {
            while !marker.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("admitted cleanup reaches log deletion");
        if mode == "cancel" {
            waiter.abort();
            assert!(waiter.await.unwrap_err().is_cancelled());
        } else {
            assert!(
                waiter.await.unwrap().is_err(),
                "admitted deadline preserves uncertainty"
            );
        }
        let group = &fixture.groups[source_index];
        assert_eq!(
            group.receipt(deadline()).await.unwrap().stage,
            multiraft_net::ControlledGroupStage::LogsCleaned
        );
        let live = multiraft_store::DiskFsmDb::group_path(&config.data_dir, GROUP);
        assert!(
            live.exists(),
            "caller cancellation cannot skip remaining deletion barrier"
        );
        let next = multiraft_core::ReplacementSpec {
            operation: multiraft_core::OperationId {
                sequence: 2,
                nonce: [12; 16],
            },
            expected_start: fixture.authority.read(GROUP).await.unwrap().membership,
            target_voters: [1, 2, 3].into(),
            target_generation: source * 10 + 1,
        };
        assert!(
            fixture.owners[source_index]
                .handle()
                .reserve_controlled_target(
                    GROUP,
                    next,
                    vec![99],
                    fixture.authority.clone(),
                    Instant::now() + Duration::from_millis(30)
                )
                .await
                .is_err(),
            "rejoin waits for actual cleanup completion"
        );
        assert!(
            group
                .retire_and_cleanup_from_authority(
                    operation,
                    Instant::now() + Duration::from_millis(30)
                )
                .await
                .is_err(),
            "another cleanup cannot bypass owned serial barrier"
        );
        std::fs::write(config.data_dir.join("cleanup-observed-release"), b"release").unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while group.receipt(deadline()).await.unwrap().stage
                != multiraft_net::ControlledGroupStage::Cleaned
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("abandoned waiter does not abandon cleanup");
        fixture.stop().await;
    } else {
        waiter.await.unwrap().unwrap();
        fixture.stop().await;
    }
}

pub fn trajectory(phase: &str, mode: &str) {
    let parent =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/cleanup-crashes");
    std::fs::create_dir_all(&parent).unwrap();
    let harness = tempfile::tempdir_in(parent).unwrap();
    let spawn = |resume: bool| {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--ignored",
                "--exact",
                "managed_cleanup_child",
                "--nocapture",
            ])
            .env("MANAGED_CLEANUP_HARNESS", harness.path())
            .env("MANAGED_CLEANUP_MODE", mode)
            .stdout(Stdio::piped())
            .stderr(Stdio::from(
                std::fs::File::create(harness.path().join(if resume {
                    "resume.stderr"
                } else {
                    "initial.stderr"
                }))
                .unwrap(),
            ));
        if resume {
            command.env("MANAGED_CLEANUP_RESUME", "1");
        } else {
            command.env("MULTIRAFT_CLEANUP_OBSERVE_PHASE", phase);
        }
        command.spawn().unwrap()
    };
    let mut child = spawn(false);
    let stdout = child.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let line = line.unwrap();
            if line.starts_with("MANAGED_CLEANUP_PHASE ") {
                let _ = tx.send(line);
            }
        }
    });
    if mode.is_empty() {
        let observed = rx.recv_timeout(Duration::from_secs(30));
        if observed.is_err() {
            let _ = child.kill();
        }
        assert_eq!(
            observed.unwrap_or_else(|error| panic!(
                "phase {phase} not observed: {error}; {}",
                std::fs::read_to_string(harness.path().join("initial.stderr")).unwrap()
            )),
            format!("MANAGED_CLEANUP_PHASE {phase}")
        );
        child.kill().unwrap();
        assert!(!child.wait().unwrap().success());
        reader.join().unwrap();
        let output = spawn(true).wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "resume failed: {}",
            String::from_utf8_lossy(&std::fs::read(harness.path().join("resume.stderr")).unwrap())
        );
    } else {
        assert!(
            child.wait().unwrap().success(),
            "managed waiter test failed: {}",
            String::from_utf8_lossy(&std::fs::read(harness.path().join("initial.stderr")).unwrap())
        );
        reader.join().unwrap();
    }
}
