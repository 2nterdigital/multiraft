//! Owned startup validates the checked-in native snapshot plus its committed suffix.
use std::fs;
use std::io;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use multiraft_core::{ClusterConfig, FileLogSyncLevel, SnapshotMode};
use multiraft_fsm::{
    ApplyOut, CounterFsm, GroupId, StateMachine, ValidationContext, ValidationFuture,
    ValidationKind,
};
use multiraft_net::{
    FsmFactoryContext, GroupConfig, NodeOwner, RuntimeConfig, StateMachineFactory,
};
use sha2::{Digest, Sha256};
use tokio::sync::{Notify, Semaphore};
use tokio::time::Instant;

#[derive(Clone, Default)]
struct Proof {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    inputs: Arc<Mutex<Vec<(ValidationContext, i64)>>>,
    drops: Arc<AtomicUsize>,
    validated: Arc<AtomicUsize>,
    local_checks: Arc<AtomicUsize>,
    reject: bool,
    leases: PathBuf,
}
struct Consumer {
    counter: CounterFsm,
    proof: Proof,
    ready: bool,
}
impl Drop for Consumer {
    fn drop(&mut self) {
        self.proof.drops.fetch_add(1, Ordering::SeqCst);
    }
}
impl StateMachine for Consumer {
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
    fn requires_recovery_validation(&self) -> bool {
        true
    }
    fn recovery_validation(&self, context: ValidationContext) -> Option<ValidationFuture> {
        let proof = self.proof.clone();
        proof
            .inputs
            .lock()
            .unwrap()
            .push((context, self.counter.value(context.group_id)));
        let lease = tempfile::NamedTempFile::new_in(&proof.leases).unwrap();
        Some(Box::pin(async move {
            let _lease = lease;
            proof.entered.notify_one();
            proof.release.notified().await;
            if proof.reject {
                Err(io::Error::other("archive proof rejected"))
            } else {
                Ok(())
            }
        }))
    }
    fn recovery_validated(&mut self, _: ValidationContext) -> Result<(), Self::Error> {
        self.ready = true;
        self.proof.validated.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
#[derive(Clone)]
struct Factory {
    proof: Proof,
    timeout: Duration,
    budget: Option<Arc<Semaphore>>,
}
impl StateMachineFactory<Consumer> for Factory {
    fn create(&self, _: FsmFactoryContext) -> anyhow::Result<Consumer> {
        Ok(Consumer {
            counter: CounterFsm::new(),
            proof: self.proof.clone(),
            ready: false,
        })
    }
    fn validation_timeout(&self, _: FsmFactoryContext) -> Duration {
        self.timeout
    }
    fn validation_budget(&self) -> Option<Arc<Semaphore>> {
        self.budget.clone()
    }
    fn validate_recovered(&self, _: FsmFactoryContext, fsm: &Consumer) -> anyhow::Result<()> {
        assert!(!fsm.ready, "readiness follows the local consumer check");
        fsm.proof.local_checks.fetch_add(1, Ordering::SeqCst);
        anyhow::ensure!(
            fsm.counter.value(7) == 15,
            "committed suffix must precede validation"
        );
        Ok(())
    }
}
fn scratch() -> tempfile::TempDir {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/issue-4/net-tests");
    fs::create_dir_all(&root).unwrap();
    tempfile::tempdir_in(root).unwrap()
}
fn fixture() -> tempfile::TempDir {
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/legacy-native-alpha30");
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(source.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(
        manifest["source"],
        "fe832257b0c744faf93f9c7a4ab1da63d37d6a46"
    );
    let destination = scratch();
    for file in manifest["files"].as_array().unwrap() {
        let relative = file["path"].as_str().unwrap();
        let bytes = fs::read(source.join("disk").join(relative)).unwrap();
        assert_eq!(bytes.len() as u64, file["bytes"].as_u64().unwrap());
        assert_eq!(format!("{:x}", Sha256::digest(&bytes)), file["sha256"]);
        let target = destination.path().join(relative);
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::write(target, bytes).unwrap();
    }
    destination
}
fn address() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}
fn config(root: &Path, address: SocketAddr, declared: bool) -> RuntimeConfig {
    let mut cluster = ClusterConfig::for_test(1, &[1]);
    cluster.peers = vec![(1, address)];
    cluster.data_dir = root.to_owned();
    cluster.file_log_sync_level = FileLogSyncLevel::Data;
    cluster.snapshot_mode = SnapshotMode::NativeDurable;
    cluster.retain_log_entries = 2;
    RuntimeConfig::new(
        cluster,
        if declared {
            vec![GroupConfig {
                group_id: 7,
                voters: vec![1],
            }]
        } else {
            vec![]
        },
    )
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
async fn entered(proof: &Proof) {
    tokio::time::timeout(Duration::from_secs(3), proof.entered.notified())
        .await
        .unwrap();
}
async fn reclaimed(proof: &Proof, address: SocketAddr) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if proof.drops.load(Ordering::SeqCst) == 1 && TcpListener::bind(address).is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(fs::read_dir(&proof.leases).unwrap().count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_plus_committed_suffix_is_the_single_startup_proof_input() {
    let disk = fixture();
    let leases = scratch();
    let proof = Proof {
        leases: leases.path().to_owned(),
        ..Proof::default()
    };
    let port = address();
    let factory = Factory {
        proof: proof.clone(),
        timeout: Duration::from_secs(3),
        budget: None,
    };
    let start = tokio::spawn(NodeOwner::start(
        config(disk.path(), port, true),
        factory,
        deadline(),
    ));
    entered(&proof).await;
    assert!(!start.is_finished());
    let inputs = proof.inputs.lock().unwrap().clone();
    assert_eq!(
        inputs.len(),
        1,
        "existing active load must not validate a peer candidate"
    );
    assert_eq!(inputs[0].0.kind, ValidationKind::Startup);
    assert!(inputs[0].0.applied.is_some());
    assert_eq!(inputs[0].1, 15);
    assert_eq!(proof.validated.load(Ordering::SeqCst), 0);
    proof.release.notify_one();
    let owner = start.await.unwrap().unwrap();
    assert_eq!(proof.validated.load(Ordering::SeqCst), 1);
    assert_eq!(proof.local_checks.load(Ordering::SeqCst), 1);
    owner.shutdown(deadline()).await.unwrap();
    reclaimed(&proof, port).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejected_timed_out_and_cancelled_startups_do_not_publish_or_retain_resources() {
    for failure in ["reject", "timeout", "abort", "budget"] {
        let disk = fixture();
        let leases = scratch();
        let proof = Proof {
            leases: leases.path().to_owned(),
            reject: failure == "reject",
            ..Proof::default()
        };
        let port = address();
        let active = disk.path().join("snapshots/7/native-v1/active.json");
        let authority = fs::read(&active).unwrap();
        let budget = Arc::new(Semaphore::new(1));
        let occupied = if failure == "budget" {
            Some(budget.clone().acquire_owned().await.unwrap())
        } else {
            None
        };
        let start = tokio::spawn(NodeOwner::start(
            config(disk.path(), port, true),
            Factory {
                proof: proof.clone(),
                timeout: Duration::from_millis(200),
                budget: Some(budget.clone()),
            },
            deadline(),
        ));
        if failure != "budget" {
            entered(&proof).await;
        }
        match failure {
            "reject" => {
                proof.release.notify_one();
                assert!(start.await.unwrap().is_err());
            }
            "timeout" | "budget" => {
                assert!(start.await.unwrap().is_err());
            }
            "abort" => {
                start.abort();
                assert!(start.await.err().unwrap().is_cancelled());
            }
            _ => unreachable!(),
        }
        reclaimed(&proof, port).await;
        assert_eq!(proof.validated.load(Ordering::SeqCst), 0, "{failure}");
        assert_eq!(proof.local_checks.load(Ordering::SeqCst), 0, "{failure}");
        assert_eq!(fs::read(&active).unwrap(), authority, "{failure}");
        drop(occupied);
        assert_eq!(budget.available_permits(), 1, "{failure}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owner_shutdown_cancels_dynamic_group_validation_and_joins_application() {
    let disk = fixture();
    let leases = scratch();
    let proof = Proof {
        leases: leases.path().to_owned(),
        ..Proof::default()
    };
    let port = address();
    let owner = NodeOwner::start(
        config(disk.path(), port, false),
        Factory {
            proof: proof.clone(),
            timeout: Duration::from_secs(30),
            budget: None,
        },
        deadline(),
    )
    .await
    .unwrap();
    let handle = owner.handle();
    let creating_handle = handle.clone();
    let create = tokio::spawn(async move {
        creating_handle
            .create_group(
                GroupConfig {
                    group_id: 7,
                    voters: vec![1],
                },
                deadline(),
            )
            .await
    });
    entered(&proof).await;
    tokio::time::timeout(Duration::from_secs(2), owner.shutdown(deadline()))
        .await
        .expect("owner close cancels validation without waiting for its deadline")
        .unwrap();
    assert!(create.await.unwrap().is_err());
    reclaimed(&proof, port).await;
    assert_eq!(proof.validated.load(Ordering::SeqCst), 0);
    assert_eq!(proof.local_checks.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_owned_startup_prunes_crash_candidates_before_actual_suffix_proof() {
    let disk = fixture();
    let namespace = disk.path().join("snapshots/7/native-v1");
    let generations = || {
        fs::read_dir(&namespace)
            .unwrap()
            .filter(|entry| entry.as_ref().unwrap().file_type().unwrap().is_dir())
            .count()
    };
    assert_eq!(generations(), 1);
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "abandoned_candidate_writer",
            "--nocapture",
        ])
        .env("ASYNC_VALIDATION_CANDIDATE_ROOT", disk.path())
        .output()
        .unwrap();
    assert!(
        child.status.success(),
        "{}",
        String::from_utf8_lossy(&child.stderr)
    );
    assert_eq!(generations(), 2);
    let leases = scratch();
    let proof = Proof {
        leases: leases.path().to_owned(),
        ..Proof::default()
    };
    let port = address();
    let starting = tokio::spawn(NodeOwner::start(
        config(disk.path(), port, true),
        Factory {
            proof: proof.clone(),
            timeout: Duration::from_secs(3),
            budget: None,
        },
        deadline(),
    ));
    entered(&proof).await;
    assert_eq!(
        generations(),
        1,
        "legacy startup must prune validated orphan candidates"
    );
    assert_eq!(
        proof.inputs.lock().unwrap()[0].1,
        15,
        "proof binds replayed suffix"
    );
    proof.release.notify_one();
    starting
        .await
        .unwrap()
        .unwrap()
        .shutdown(deadline())
        .await
        .unwrap();
    reclaimed(&proof, port).await;
}

#[test]
#[ignore = "controlled child: durable public-catalog candidate, process exits before activation/Drop"]
fn abandoned_candidate_writer() {
    let root = PathBuf::from(std::env::var_os("ASYNC_VALIDATION_CANDIDATE_ROOT").unwrap());
    let catalog = multiraft_store::SnapshotCatalog::new(root.join("snapshots"), 1);
    let mut active = catalog.load_native(7, 64 * 1024 * 1024).unwrap().unwrap();
    active.meta.snapshot_id = "unactivated-crash-candidate".into();
    let stage = catalog
        .stage_native(7, &active.meta, &active.data, 64 * 1024 * 1024)
        .unwrap();
    // A real separate process owns these bytes. Its exit loses RAM ownership,
    // without ever publishing installation or an acknowledged application write.
    std::mem::forget(stage);
}
