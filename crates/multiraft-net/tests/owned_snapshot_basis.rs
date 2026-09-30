//! Fixed old-source disk; fail closed, repair exact original bytes, retry same root.
use multiraft_core::{ClusterConfig, FileLogSyncLevel, MultiRaftError, SnapshotMode};
use multiraft_fsm::{ApplyOut, CounterFsm, GroupId, StateMachine};
use multiraft_net::{
    FsmFactoryContext, GroupConfig, NodeOwner, RuntimeConfig, RuntimeError, StateMachineFactory,
};
use sha2::{Digest, Sha256};
use std::fs::{self, OpenOptions};
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
struct DiskCounter {
    counter: CounterFsm,
    lease: PathBuf,
    released: Arc<AtomicUsize>,
}
impl Drop for DiskCounter {
    fn drop(&mut self) {
        fs::remove_file(&self.lease).unwrap();
        self.released.fetch_add(1, Ordering::SeqCst);
    }
}
impl StateMachine for DiskCounter {
    type Error = <CounterFsm as StateMachine>::Error;
    fn apply(&mut self, group: GroupId, index: u64, bytes: &[u8]) -> Result<ApplyOut, Self::Error> {
        self.counter.apply(group, index, bytes)
    }
    fn snapshot(&self, group: GroupId) -> Result<Vec<u8>, Self::Error> {
        self.counter.snapshot(group)
    }
    fn restore(&mut self, group: GroupId, bytes: &[u8]) -> Result<(), Self::Error> {
        self.counter.restore(group, bytes)
    }
}
#[derive(Clone)]
struct Factory {
    root: PathBuf,
    released: Arc<AtomicUsize>,
    validated: Arc<AtomicUsize>,
}
impl StateMachineFactory<DiskCounter> for Factory {
    fn create(&self, _: FsmFactoryContext) -> anyhow::Result<DiskCounter> {
        let lease = self.root.join("consumer.lease");
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lease)?;
        Ok(DiskCounter {
            counter: CounterFsm::new(),
            lease,
            released: self.released.clone(),
        })
    }
    fn validate_recovered(&self, _: FsmFactoryContext, fsm: &DiskCounter) -> anyhow::Result<()> {
        assert_eq!(
            fsm.counter.value(7),
            15,
            "original snapshot10+tail5 recovered"
        );
        self.validated.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_checkpoint_corrupt_image_and_missing_committed_tail_reject_then_exact_disk_repairs(
) {
    let fixture =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/legacy-native-alpha30");
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(fixture.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(
        manifest["source"],
        "fe832257b0c744faf93f9c7a4ab1da63d37d6a46"
    );
    let scratch = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/issue-170/basis");
    fs::create_dir_all(&scratch).unwrap();
    let image = manifest["files"]
        .as_array()
        .unwrap()
        .iter()
        .find(|file| file["path"].as_str().unwrap().ends_with("/data.bin"))
        .unwrap()["path"]
        .as_str()
        .unwrap();
    for (case, damaged) in [
        "snapshots/7/native-v1/active.json",
        image,
        "group-7/log.bin",
    ]
    .into_iter()
    .enumerate()
    {
        let root = tempfile::Builder::new()
            .prefix(&format!("basis-{case}-"))
            .tempdir_in(&scratch)
            .unwrap()
            .keep();
        for file in manifest["files"].as_array().unwrap() {
            let relative = file["path"].as_str().unwrap();
            let bytes = fs::read(fixture.join("disk").join(relative)).unwrap();
            assert_eq!(format!("{:x}", Sha256::digest(&bytes)), file["sha256"]);
            let output = root.join(relative);
            fs::create_dir_all(output.parent().unwrap()).unwrap();
            fs::write(output, bytes).unwrap();
        }
        let path = root.join(damaged);
        let original = fs::read(&path).unwrap();
        fs::write(root.join("original-basis.bin"), &original).unwrap();
        match case {
            0 => fs::remove_file(&path).unwrap(),
            1 => {
                let mut corrupt = original.clone();
                corrupt[0] ^= 1;
                fs::write(&path, &corrupt).unwrap();
                fs::write(root.join("failed-basis.bin"), corrupt).unwrap();
            }
            2 => fs::write(&path, []).unwrap(),
            _ => unreachable!(),
        }
        let address = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let mut cluster = ClusterConfig::for_test(1, &[1]);
        cluster.peers = vec![(1, address)];
        cluster.data_dir = root.clone();
        cluster.file_log_sync_level = FileLogSyncLevel::Data;
        cluster.snapshot_mode = SnapshotMode::NativeDurable;
        let factory = Factory {
            root: root.clone(),
            released: Arc::new(AtomicUsize::new(0)),
            validated: Arc::new(AtomicUsize::new(0)),
        };
        let inputs = || {
            RuntimeConfig::new(
                cluster.clone(),
                vec![GroupConfig {
                    group_id: 7,
                    voters: vec![1],
                }],
            )
        };
        let error = match NodeOwner::start(inputs(), factory.clone(), deadline()).await {
            Err(error) => error,
            Ok(owner) => {
                let _ = owner.shutdown(deadline()).await;
                panic!("incomplete required recovery basis published an owner: case{case}");
            }
        };
        assert!(
            matches!(error, RuntimeError::Source(_)),
            "source recovery rejection must survive: {error:?}"
        );
        assert_eq!(
            factory.validated.load(Ordering::SeqCst),
            0,
            "native recovery failed before consumer validation"
        );
        assert_eq!(factory.released.load(Ordering::SeqCst), 1);
        assert!(!root.join("consumer.lease").exists());
        TcpListener::bind(address).unwrap();
        fs::write(
            root.join("failed-attempt.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "case":case,"damaged_path":damaged,"source":manifest["source"],
                "original_sha256":format!("{:x}", Sha256::digest(&original)),
                "failure":"native_recovery_rejected","consumer_validations":0,"released":1,
                "repair":"same_root_exact_original_bytes_only"
            }))
            .unwrap(),
        )
        .unwrap();
        // Repair only the damaged basis, using the saved actual old bytes. Do not
        // regenerate snapshots/logs or replace the failed directory with a fixture.
        fs::write(&path, &original).unwrap();
        let owner = NodeOwner::start(inputs(), factory.clone(), deadline())
            .await
            .unwrap();
        assert_eq!(factory.validated.load(Ordering::SeqCst), 1);
        let handle = owner.handle();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match handle
                    .read_linearizable(7, deadline(), |fsm| fsm.counter.value(7))
                    .await
                {
                    Ok(value) => {
                        assert_eq!(value, 15);
                        break;
                    }
                    Err(RuntimeError::Source(MultiRaftError::NotLeader { .. })) => {
                        tokio::time::sleep(Duration::from_millis(5)).await
                    }
                    error => panic!("unexpected restored source read: {error:?}"),
                }
            }
        })
        .await
        .unwrap();
        owner.shutdown(deadline()).await.unwrap();
        assert_eq!(factory.released.load(Ordering::SeqCst), 2);
        TcpListener::bind(address).unwrap();
    }
}
