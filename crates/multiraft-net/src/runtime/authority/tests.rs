//! Public runtime authority on actual registration, with bounded cancellation ownership.
use super::*;
#[path = "../../../tests/local_authority_support/disk.rs"]
mod disk;
use disk::Disk;
use multiraft_core::{LogStorageBackend, SnapshotMode};
use std::sync::{atomic::AtomicBool, Condvar};

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}
async fn owner(root: std::path::PathBuf) -> NodeOwner<Disk> {
    let mut cluster = ClusterConfig::for_test(1, &[1]);
    cluster.data_dir = root.clone();
    cluster.log_storage_backend = LogStorageBackend::RocksDb;
    cluster.snapshot_mode = SnapshotMode::NativeDurable;
    let mut config = RuntimeConfig::new(cluster, vec![]);
    config.transport = RuntimeTransport::InProcess(SharedFabric::new());
    config.max_inflight = 1;
    NodeOwner::start(
        config,
        move |context: crate::FsmFactoryContext| Ok(Disk::open(&root, context.group_id())?),
        deadline(),
    )
    .await
    .unwrap()
}
fn scratch() -> tempfile::TempDir {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/local-authority");
    std::fs::create_dir_all(&root).unwrap();
    tempfile::tempdir_in(root).unwrap()
}
async fn registered(owner: &NodeOwner<Disk>) {
    // Simulate a later native publication, absent from the original runtime declarations.
    owner.shared.node.create_group(7, &[1]).await.unwrap();
    let until = deadline();
    loop {
        match owner
            .handle()
            .local_authoritative_membership(7, until)
            .await
        {
            Ok(_) => return,
            Err(LocalAuthorityError::NotLeader { .. } | LocalAuthorityError::Unavailable)
                if Instant::now() < until =>
            {
                tokio::time::sleep(Duration::from_millis(10)).await
            }
            other => panic!("later registered group must form authority: {other:?}"),
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn authority_uses_native_registration_instead_of_startup_ready_declarations() {
    let root = scratch();
    let owner = owner(root.path().to_owned()).await;
    assert_eq!(
        owner
            .handle()
            .local_authoritative_membership(7, deadline())
            .await,
        Err(LocalAuthorityError::LocalMissing)
    );
    registered(&owner).await;
    assert_eq!(
        owner
            .handle()
            .local_authoritative_membership(7, deadline())
            .await
            .unwrap()
            .group_id,
        7
    );
    owner.shutdown(deadline()).await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn waiting_read_deadline_and_owner_drop_release_admission_and_native_resources() {
    let root = scratch();
    let owner = owner(root.path().to_owned()).await;
    registered(&owner).await;
    let handle = owner.handle();
    let weak = handle.shared.clone();
    let gate = Arc::new((Mutex::new(false), Condvar::new(), AtomicBool::new(false)));
    let blocker_shared = owner.shared.clone();
    let blocker_gate = gate.clone();
    let blocker = tokio::spawn(async move {
        blocker_shared
            .node
            .with_fsm(7, |_| {
                let (open, changed, entered) = &*blocker_gate;
                entered.store(true, Ordering::Release);
                let mut open = open.lock().unwrap();
                while !*open {
                    let (guard, timeout) =
                        changed.wait_timeout(open, Duration::from_secs(5)).unwrap();
                    open = guard;
                    assert!(!timeout.timed_out(), "test gate must be released");
                }
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !gate.2.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        handle
            .local_authoritative_membership(7, Instant::now() + Duration::from_millis(30))
            .await,
        Err(LocalAuthorityError::Deadline)
    );
    // The expired call released its sole permit. A new call is admitted and waits.
    let request_handle = handle.clone();
    let request = tokio::spawn(async move {
        request_handle
            .local_authoritative_membership(7, deadline())
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if owner.shared.slots.available_permits() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        handle.local_authoritative_membership(7, deadline()).await,
        Err(LocalAuthorityError::Busy)
    );
    drop(owner);
    let result = tokio::time::timeout(Duration::from_secs(2), request)
        .await
        .unwrap()
        .unwrap();
    *gate.0.lock().unwrap() = true;
    gate.1.notify_all();
    blocker.await.unwrap();
    assert_eq!(result, Err(LocalAuthorityError::Closed));
    tokio::time::timeout(Duration::from_secs(5), async {
        while weak.upgrade().is_some() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("native owner, FSM and request tasks must be reclaimed");
}
