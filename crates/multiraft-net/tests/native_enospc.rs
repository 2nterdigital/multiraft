//! Named ENOSPC fault injection; never fills or changes the shared host disk.
#[path = "native_lab_support/mod.rs"]
mod lab;
use multiraft_core::{ClusterConfig, FileLogSyncLevel, SnapshotMode};
use multiraft_fsm::CounterFsm;
use multiraft_net::{CompactionProgress, MultiRaft};
use std::{
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

fn config(root: &Path) -> ClusterConfig {
    let mut config = ClusterConfig::for_test(1, &[1]);
    config.data_dir = root.into();
    config.file_log_sync_level = FileLogSyncLevel::Data;
    config.snapshot_mode = SnapshotMode::NativeDurable;
    config.retain_log_entries = 0;
    config.enable_stale_queries = true;
    config
}

#[tokio::test]
#[ignore = "G1 dedicated laboratory only; explicit ENOSPC case"]
async fn enospc_snapshot_write_preserves_all_acknowledged_retained_state() {
    let base = lab::require_lab(&PathBuf::from(
        std::env::var_os("NATIVE_EVIDENCE_ROOT").unwrap(),
    ));
    let root = tempfile::Builder::new()
        .prefix("g1-enospc-")
        .tempdir_in(base)
        .unwrap()
        .keep();
    let data = root.join("data");
    std::fs::create_dir(&data).unwrap();
    let library = root.join("enospc.so");
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/native_lab_support/enospc.c");
    let compile = Command::new("cc")
        .args(["-shared", "-fPIC", "-O2", "-o"])
        .arg(&library)
        .arg(source)
        .arg("-ldl")
        .output()
        .unwrap();
    std::fs::write(root.join("compile.stdout.log"), &compile.stdout).unwrap();
    std::fs::write(root.join("compile.stderr.log"), &compile.stderr).unwrap();
    assert!(compile.status.success());
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "enospc_child", "--nocapture"])
        .env("LD_PRELOAD", &library)
        .env("NATIVE_ENOSPC_ROOT", &data)
        .output()
        .unwrap();
    std::fs::write(root.join("child.stdout.log"), &output.stdout).unwrap();
    std::fs::write(root.join("child.stderr.log"), &output.stderr).unwrap();
    assert!(
        output.status.success(),
        "ENOSPC child failed; evidence={}",
        root.display()
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("NATIVE_ENOSPC_INJECTED"));
    let reopened = MultiRaft::start(config(&data)).await.unwrap();
    reopened.create_group(7, &[1]).await.unwrap();
    let value = reopened
        .read_stale(7, |fsm| fsm.value(7))
        .await
        .unwrap()
        .value;
    assert_eq!(value, 10);
    let status = reopened.local_storage_status(7).await.unwrap();
    assert!(status.purged.is_none());
    assert!(status.durable_snapshot.is_none());
    reopened.shutdown().await.unwrap();
    println!("NATIVE_ENOSPC_ORACLE injected_write_errno=ENOSPC node=1 expected=10 purged=None durable_snapshot=None evidence={}", root.display());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "ENOSPC child invoked only by its guarded parent case"]
async fn enospc_child() {
    let root = lab::require_lab(&PathBuf::from(
        std::env::var_os("NATIVE_ENOSPC_ROOT").unwrap(),
    ));
    let node = MultiRaft::start(config(&root)).await.unwrap();
    node.create_group(7, &[1]).await.unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        while !node.is_leader(7) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    for id in 1..=10 {
        node.propose(7, CounterFsm::encode_add(1, id))
            .await
            .unwrap();
    }
    node.request_compaction(7).await.unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if node.local_storage_status(7).await.unwrap().progress
                == CompactionProgress::Unconfirmed
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let status = node.local_storage_status(7).await.unwrap();
    assert!(status.purged.is_none());
    assert!(status.durable_snapshot.is_none());
    assert_eq!(
        node.read_stale(7, |fsm| fsm.value(7)).await.unwrap().value,
        10
    );
    node.shutdown().await.unwrap();
}
