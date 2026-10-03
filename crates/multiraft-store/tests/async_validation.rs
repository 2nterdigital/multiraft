//! Consumer-owned asynchronous proof, tested through the public native store seam.
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::stream;
use multiraft_core::typ::{Entry, LogId};
use multiraft_core::{Request, TypeConfig};
use multiraft_fsm::{
    ApplyOut, CaptureError, CounterFsm, GroupId, StateMachine, ValidationContext, ValidationFuture,
    ValidationKind,
};
use multiraft_store::{NativeSmOptions, SnapshotCatalog, StateMachineStore, ValidationOptions};
use openraft::alias::{LeaderIdOf, SnapshotOf};
use openraft::storage::RaftStateMachine;
use openraft::vote::RaftLeaderIdExt;
use openraft::{EntryPayload, RaftSnapshotBuilder};
use tokio::sync::{Notify, Semaphore};

#[derive(Clone, Default)]
struct Proof {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    inputs: Arc<Mutex<Vec<(ValidationContext, i64)>>>,
    reject: bool,
    panic: bool,
    lease_root: PathBuf,
}
struct CheckedCounter {
    counter: CounterFsm,
    proof: Proof,
    ready: bool,
}
impl StateMachine for CheckedCounter {
    type Error = <CounterFsm as StateMachine>::Error;
    fn apply(&mut self, group: GroupId, index: u64, bytes: &[u8]) -> Result<ApplyOut, Self::Error> {
        self.counter.apply(group, index, bytes)
    }
    fn snapshot(&self, group: GroupId) -> Result<Vec<u8>, Self::Error> {
        self.counter.snapshot(group)
    }
    fn restore(&mut self, group: GroupId, bytes: &[u8]) -> Result<(), Self::Error> {
        self.ready = false;
        self.counter.restore(group, bytes)
    }
    fn freeze_bounded(
        &self,
        group: GroupId,
        max: usize,
    ) -> Result<Vec<u8>, CaptureError<Self::Error>> {
        self.counter.freeze_bounded(group, max)
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
        let lease = tempfile::Builder::new()
            .prefix("proof-")
            .tempfile_in(&proof.lease_root)
            .unwrap();
        Some(Box::pin(async move {
            // A real file lease owned by the future proves cancellation cleanup.
            let _lease = lease;
            proof.entered.notify_one();
            proof.release.notified().await;
            assert!(!proof.panic, "controlled external proof panic");
            if proof.reject {
                return Err(io::Error::other("consumer rejected proof"));
            }
            Ok(())
        }))
    }
    fn recovery_validated(&mut self, _: ValidationContext) -> Result<(), Self::Error> {
        self.ready = true;
        Ok(())
    }
}
fn scratch() -> tempfile::TempDir {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/issue-4/store-tests");
    std::fs::create_dir_all(&root).unwrap();
    tempfile::tempdir_in(root).unwrap()
}
fn source(root: &Path) -> StateMachineStore<CounterFsm> {
    StateMachineStore::with_native_options(
        7,
        CounterFsm::new(),
        NativeSmOptions {
            catalog: Arc::new(SnapshotCatalog::new(root, 1)),
            max_snapshot_bytes: 1024,
            build_budget: Arc::new(Semaphore::new(1)),
        },
    )
    .unwrap()
}
fn receiver(
    root: &Path,
    proof: Proof,
    budget: Arc<Semaphore>,
    deadline: Duration,
) -> StateMachineStore<CheckedCounter> {
    StateMachineStore::with_native_options(
        7,
        CheckedCounter {
            counter: CounterFsm::new(),
            proof,
            ready: false,
        },
        NativeSmOptions {
            catalog: Arc::new(SnapshotCatalog::new(root, 1)),
            max_snapshot_bytes: 1024,
            build_budget: Arc::new(Semaphore::new(1)),
        },
    )
    .unwrap()
    .with_validation_options(ValidationOptions { deadline, budget })
    .unwrap()
}
fn entry(index: u64, delta: Option<i64>) -> Entry {
    Entry {
        log_id: LogId::new(LeaderIdOf::<TypeConfig>::new_committed(1, 1), index),
        payload: delta.map_or(EntryPayload::Blank, |delta| {
            EntryPayload::Normal(Request::new(CounterFsm::encode_add(delta, index)))
        }),
    }
}
async fn image(root: &Path, value: i64) -> SnapshotOf<TypeConfig, io::Cursor<Vec<u8>>> {
    let mut sm = source(root);
    sm.apply(stream::iter([
        Ok((entry(0, None), None)),
        Ok((entry(1, Some(value)), None)),
    ]))
    .await
    .unwrap();
    sm.try_create_snapshot_builder(false)
        .await
        .unwrap()
        .build_snapshot()
        .await
        .unwrap()
}
async fn entered(proof: &Proof) {
    tokio::time::timeout(Duration::from_secs(3), proof.entered.notified())
        .await
        .unwrap();
}
fn assert_no_leases(root: &Path) {
    assert_eq!(std::fs::read_dir(root).unwrap().count(), 0);
}
fn generation_dirs(root: &Path) -> usize {
    let path = root.join("7/native-v1");
    std::fs::read_dir(path)
        .map(|items| {
            items
                .filter(|item| item.as_ref().unwrap().file_type().unwrap().is_dir())
                .count()
        })
        .unwrap_or(0)
}

#[tokio::test]
async fn peer_candidate_is_hidden_until_proof_then_applied_and_provider_advance_together() {
    let disk = scratch();
    let leases = scratch();
    let proof = Proof {
        lease_root: leases.path().to_owned(),
        ..Proof::default()
    };
    let budget = Arc::new(Semaphore::new(1));
    let mut receiver = receiver(
        disk.path(),
        proof.clone(),
        budget.clone(),
        Duration::from_secs(3),
    );
    let snapshot = image(scratch().path(), 9).await;
    let mut installing = receiver.clone();
    let install = tokio::spawn(async move {
        installing
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
    });
    entered(&proof).await;
    assert_eq!(budget.available_permits(), 0);
    assert!(receiver
        .try_with_fsm(|fsm| fsm.counter.value(7))
        .await
        .is_err());
    assert_eq!(receiver.applied_state().await.unwrap().0, None);
    assert!(receiver.native_snapshot_info().await.unwrap().is_none());
    assert!(receiver.try_create_snapshot_builder(false).await.is_none());
    assert_eq!(
        proof.inputs.lock().unwrap()[0].0.kind,
        ValidationKind::PeerInstall
    );
    assert_eq!(proof.inputs.lock().unwrap()[0].0.applied, Some((1, 1)));
    assert_eq!(proof.inputs.lock().unwrap()[0].1, 9);
    // Application input waits on the transition; it cannot change the frozen candidate.
    let mut applying = receiver.clone();
    let apply = tokio::spawn(async move {
        applying
            .apply(stream::iter([Ok((entry(2, Some(3)), None))]))
            .await
    });
    tokio::task::yield_now().await;
    assert!(!apply.is_finished());
    proof.release.notify_one();
    install.await.unwrap().unwrap();
    apply.await.unwrap().unwrap();
    assert_eq!(
        receiver
            .try_with_fsm(|fsm| (fsm.counter.value(7), fsm.ready))
            .await
            .unwrap(),
        (12, true)
    );
    assert_eq!(receiver.applied_state().await.unwrap().0.unwrap().index, 2);
    assert_eq!(
        receiver
            .native_snapshot_info()
            .await
            .unwrap()
            .unwrap()
            .meta
            .last_log_id
            .unwrap()
            .index,
        1
    );
    assert_eq!(budget.available_permits(), 1);
    assert_no_leases(leases.path());
}

#[tokio::test]
async fn reject_timeout_caller_abort_and_owner_close_release_candidate_and_budget() {
    for failure in ["reject", "panic", "timeout", "abort", "close"] {
        let disk = scratch();
        let leases = scratch();
        let proof = Proof {
            lease_root: leases.path().to_owned(),
            reject: failure == "reject",
            panic: failure == "panic",
            ..Proof::default()
        };
        let budget = Arc::new(Semaphore::new(1));
        let mut receiver = receiver(
            disk.path(),
            proof.clone(),
            budget.clone(),
            Duration::from_millis(150),
        );
        let snapshot = image(scratch().path(), 9).await;
        let mut installing = receiver.clone();
        let install = tokio::spawn(async move {
            installing
                .install_snapshot(&snapshot.meta, snapshot.snapshot)
                .await
        });
        entered(&proof).await;
        match failure {
            "reject" | "panic" => {
                proof.release.notify_one();
                assert!(install.await.unwrap().is_err());
            }
            "timeout" => {
                assert_eq!(
                    install.await.unwrap().unwrap_err().kind(),
                    io::ErrorKind::TimedOut
                );
            }
            "abort" => {
                install.abort();
                assert!(install.await.unwrap_err().is_cancelled());
            }
            "close" => {
                receiver.close_native_intake();
                assert!(install.await.unwrap().is_err());
            }
            _ => unreachable!(),
        }
        receiver.wait_native_quiescent().await;
        assert!(
            receiver
                .try_with_fsm(|fsm| fsm.counter.value(7))
                .await
                .is_err(),
            "{failure}"
        );
        assert_eq!(receiver.applied_state().await.unwrap().0, None, "{failure}");
        assert!(
            receiver.native_snapshot_info().await.unwrap().is_none(),
            "{failure}"
        );
        assert!(receiver
            .apply(stream::iter([Ok((entry(0, None), None))]))
            .await
            .is_err());
        assert_eq!(budget.available_permits(), 1, "{failure}");
        assert_eq!(generation_dirs(disk.path()), 0, "{failure}");
        assert_no_leases(leases.path());
    }
}

#[tokio::test]
async fn repeated_rejected_installs_preserve_old_durable_provider_without_staged_growth() {
    let disk = scratch();
    let mut initial = source(disk.path());
    initial
        .apply(stream::iter([
            Ok((entry(0, None), None)),
            Ok((entry(1, Some(5)), None)),
        ]))
        .await
        .unwrap();
    let old = initial
        .try_create_snapshot_builder(false)
        .await
        .unwrap()
        .build_snapshot()
        .await
        .unwrap()
        .meta;
    drop(initial);
    for _ in 0..4 {
        let leases = scratch();
        let proof = Proof {
            lease_root: leases.path().to_owned(),
            reject: true,
            ..Proof::default()
        };
        let receiver = receiver(
            disk.path(),
            proof.clone(),
            Arc::new(Semaphore::new(1)),
            Duration::from_secs(3),
        );
        let source_disk = scratch();
        let mut source = source(source_disk.path());
        source
            .apply(stream::iter([
                Ok((entry(0, None), None)),
                Ok((entry(1, Some(5)), None)),
                Ok((entry(2, Some(4)), None)),
            ]))
            .await
            .unwrap();
        let snapshot = source
            .try_create_snapshot_builder(false)
            .await
            .unwrap()
            .build_snapshot()
            .await
            .unwrap();
        let mut installing = receiver.clone();
        let install = tokio::spawn(async move {
            installing
                .install_snapshot(&snapshot.meta, snapshot.snapshot)
                .await
        });
        entered(&proof).await;
        assert_eq!(
            receiver.native_snapshot_info().await.unwrap().unwrap().meta,
            old
        );
        proof.release.notify_one();
        assert!(install.await.unwrap().is_err());
        receiver.wait_native_quiescent().await;
        assert_eq!(
            receiver.native_snapshot_info().await.unwrap().unwrap().meta,
            old
        );
        assert_eq!(generation_dirs(disk.path()), 1);
        assert_no_leases(leases.path());
    }
}

#[tokio::test]
async fn concurrent_install_waits_for_previous_proof_and_publishes_its_own_generation() {
    let disk = scratch();
    let leases = scratch();
    let proof = Proof {
        lease_root: leases.path().to_owned(),
        ..Proof::default()
    };
    let receiver = receiver(
        disk.path(),
        proof.clone(),
        Arc::new(Semaphore::new(1)),
        Duration::from_secs(3),
    );
    let first = image(scratch().path(), 9).await;
    let source_disk = scratch();
    let mut source = source(source_disk.path());
    source
        .apply(stream::iter([
            Ok((entry(0, None), None)),
            Ok((entry(1, Some(9)), None)),
            Ok((entry(2, Some(4)), None)),
        ]))
        .await
        .unwrap();
    let second = source
        .try_create_snapshot_builder(false)
        .await
        .unwrap()
        .build_snapshot()
        .await
        .unwrap();
    let mut a = receiver.clone();
    let install_a =
        tokio::spawn(async move { a.install_snapshot(&first.meta, first.snapshot).await });
    entered(&proof).await;
    let mut b = receiver.clone();
    let install_b =
        tokio::spawn(async move { b.install_snapshot(&second.meta, second.snapshot).await });
    tokio::task::yield_now().await;
    assert!(!install_b.is_finished());
    assert_eq!(proof.inputs.lock().unwrap().len(), 1);
    proof.release.notify_one();
    install_a.await.unwrap().unwrap();
    entered(&proof).await;
    let inputs = proof.inputs.lock().unwrap().clone();
    assert_eq!(inputs.len(), 2);
    assert!(inputs[1].0.generation > inputs[0].0.generation);
    assert_eq!(inputs[1].0.applied, Some((2, 1)));
    assert_eq!(inputs[1].1, 13);
    assert!(receiver
        .try_with_fsm(|fsm| fsm.counter.value(7))
        .await
        .is_err());
    assert_eq!(
        receiver
            .native_snapshot_info()
            .await
            .unwrap()
            .unwrap()
            .meta
            .last_log_id
            .unwrap()
            .index,
        1
    );
    proof.release.notify_one();
    install_b.await.unwrap().unwrap();
    assert_eq!(
        receiver
            .try_with_fsm(|fsm| fsm.counter.value(7))
            .await
            .unwrap(),
        13
    );
    assert_eq!(
        receiver
            .native_snapshot_info()
            .await
            .unwrap()
            .unwrap()
            .meta
            .last_log_id
            .unwrap()
            .index,
        2
    );
    assert_no_leases(leases.path());
}

#[tokio::test]
async fn deadline_includes_shared_admission_wait_and_drops_unpolled_proof_inputs() {
    let disk = scratch();
    let leases = scratch();
    let proof = Proof {
        lease_root: leases.path().to_owned(),
        ..Proof::default()
    };
    let budget = Arc::new(Semaphore::new(1));
    let busy = budget.clone().acquire_owned().await.unwrap();
    let mut receiver = receiver(
        disk.path(),
        proof.clone(),
        budget.clone(),
        Duration::from_millis(100),
    );
    let snapshot = image(scratch().path(), 9).await;
    assert_eq!(
        receiver
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::TimedOut
    );
    assert_eq!(budget.available_permits(), 0);
    assert_eq!(generation_dirs(disk.path()), 0);
    assert_no_leases(leases.path());
    drop(busy);
    assert_eq!(budget.available_permits(), 1);
}

#[tokio::test]
async fn default_consumer_does_not_wait_for_external_validator_admission() {
    let disk = scratch();
    let source = scratch();
    let budget = Arc::new(Semaphore::new(0));
    let mut store = StateMachineStore::with_native_options(
        7,
        CounterFsm::new(),
        NativeSmOptions {
            catalog: Arc::new(SnapshotCatalog::new(disk.path(), 1)),
            max_snapshot_bytes: 1024,
            build_budget: Arc::new(Semaphore::new(1)),
        },
    )
    .unwrap()
    .with_validation_options(ValidationOptions {
        deadline: Duration::from_millis(20),
        budget: budget.clone(),
    })
    .unwrap();
    store.validate_recovery(|_| Ok(())).await.unwrap();
    let snapshot = image(source.path(), 9).await;
    store
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    assert_eq!(store.try_with_fsm(|fsm| fsm.value(7)).await.unwrap(), 9);
    assert_eq!(budget.available_permits(), 0);
}
