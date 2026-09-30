//! Owned public maintenance: exact old disk, bounded refusal and cancellation seams.
mod owned_maintenance_support;
use multiraft_core::{MultiRaftError, SnapshotMode};
use multiraft_fsm::CounterFsm;
use multiraft_net::{
    CompactionProgress, CompactionRejection, NodeOwner, RuntimeError, RuntimePhase,
};
use owned_maintenance_support::*;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::Instant;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn old_source_checkpoint_tail_new_compaction_and_restart_use_only_owned_api() {
    let disk = old_disk();
    let mut factory = Factory::new(disk.path());
    let address = address();
    let owner = factory.start(address, &[GROUP]).await;
    let handle = owner.handle();
    assert_eq!(read(&handle, GROUP).await, 15);
    handle
        .propose(GROUP, CounterFsm::encode_add(2, 12), deadline())
        .await
        .unwrap();
    let submitted = handle.request_compaction(GROUP, deadline()).await.unwrap();
    let observation = completed(&handle, GROUP).await;
    assert!(observation.durable_snapshot.is_some());
    assert!(observation.native_snapshot.is_some());
    assert!(observation.purged.is_some());
    assert!(observation.retained_log_bytes.unwrap() > 0);
    assert_eq!(
        observation.purged.unwrap().index,
        submitted.target.unwrap().index - 2
    );
    assert!(!observation.no_purge_needed);
    // Completion classification belongs to the library; consumer observes it.
    handle
        .propose(GROUP, CounterFsm::encode_add(3, 13), deadline())
        .await
        .unwrap();
    assert_eq!(read(&handle, GROUP).await, 20);
    owner.shutdown(deadline()).await.unwrap();
    reusable(&factory, address, 1);
    factory.expected = 20;
    let restarted = factory.start(address, &[GROUP]).await;
    assert_eq!(read(&restarted.handle(), GROUP).await, 20);
    assert_eq!(
        status(&restarted.handle(), GROUP).await.progress,
        CompactionProgress::Idle
    );
    restarted.shutdown(deadline()).await.unwrap();
    reusable(&factory, address, 2);
    assert_eq!(factory.legacy_serializations.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn capture_cancel_keeps_same_group_and_other_group_node_budget_busy_until_done() {
    let disk = old_disk();
    let mut factory = Factory::new(disk.path());
    let gate = Gate::default();
    factory.gate = Some(gate.clone());
    let address = address();
    let owner = factory.start(address, &[GROUP, 8]).await;
    let handle = owner.handle();
    let request_handle = handle.clone();
    let waiter =
        tokio::spawn(async move { request_handle.request_compaction(GROUP, deadline()).await });
    gate.entered().await;
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    for group in [GROUP, 8] {
        assert!(matches!(
            handle.request_compaction(group, deadline()).await,
            Err(RuntimeError::MaintenanceRejected(CompactionRejection::Busy))
        ));
    }
    assert_eq!(
        status(&handle, GROUP).await.progress,
        CompactionProgress::Preparing
    );
    gate.release();
    completed(&handle, GROUP).await;
    assert_eq!(read(&handle, GROUP).await, 15);
    handle.request_compaction(8, deadline()).await.unwrap();
    let empty = completed(&handle, 8).await;
    assert!(empty.no_purge_needed);
    owner.shutdown(deadline()).await.unwrap();
    reusable(&factory, address, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn elapsed_capture_budget_does_not_trigger_late_and_retains_busy_until_real_return() {
    let disk = old_disk();
    let mut factory = Factory::new(disk.path());
    let gate = Gate::default();
    factory.gate = Some(gate.clone());
    let address = address();
    let owner = factory.start(address, &[GROUP]).await;
    let handle = owner.handle();
    let before = status(&handle, GROUP).await;
    let expires = Instant::now() + Duration::from_millis(200);
    let request_handle = handle.clone();
    let waiter =
        tokio::spawn(async move { request_handle.request_compaction(GROUP, expires).await });
    gate.entered().await;
    assert!(matches!(
        waiter.await.unwrap(),
        Err(RuntimeError::Deadline {
            phase: RuntimePhase::Compaction,
            outcome_unknown: true
        })
    ));
    assert!(matches!(
        handle.request_compaction(GROUP, deadline()).await,
        Err(RuntimeError::MaintenanceRejected(CompactionRejection::Busy))
    ));
    gate.release();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let after = status(&handle, GROUP).await;
            if after.progress == CompactionProgress::Idle {
                assert_eq!(after.native_snapshot, before.native_snapshot);
                assert_eq!(after.purged, before.purged);
                assert_eq!(
                    after.durable_snapshot.unwrap().snapshot_id,
                    before.durable_snapshot.clone().unwrap().snapshot_id
                );
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    owner.shutdown(deadline()).await.unwrap();
    reusable(&factory, address, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn known_disabled_unsupported_size_refusals_keep_native_group_available_without_legacy_dump()
{
    for reason in [
        CompactionRejection::Disabled,
        CompactionRejection::UnsupportedCapture,
        CompactionRejection::SizeLimit,
    ] {
        let disk = old_disk();
        let mut factory = Factory::new(disk.path());
        factory.unsupported = reason == CompactionRejection::UnsupportedCapture;
        let address = address();
        // Disabled uses a fresh Group/data root because old fixture requires native provider.
        let group = if reason == CompactionRejection::Disabled {
            8
        } else {
            GROUP
        };
        let mut config = factory.config(address, &[group]);
        if reason == CompactionRejection::Disabled {
            config.cluster.snapshot_mode = SnapshotMode::Disabled;
        } else if reason == CompactionRejection::SizeLimit {
            // Existing old snapshot needs its previous cap during startup. Use a
            // fresh Group whose first capture is refused under the byte budget.
            config.cluster.max_snapshot_bytes = 1;
            config.groups[0].group_id = 8;
        }
        let group = config.groups[0].group_id;
        let owner = NodeOwner::start(config, factory.clone(), deadline())
            .await
            .unwrap();
        let handle = owner.handle();
        assert_eq!(
            read(&handle, group).await,
            if group == GROUP { 15 } else { 0 }
        );
        assert!(matches!(handle.request_compaction(group, deadline()).await,
            Err(RuntimeError::MaintenanceRejected(actual)) if actual == reason));
        assert_eq!(
            read(&handle, group).await,
            if group == GROUP { 15 } else { 0 }
        );
        assert_eq!(factory.legacy_serializations.load(Ordering::SeqCst), 0);
        assert!(matches!(
            handle.request_compaction(99, deadline()).await,
            Err(RuntimeError::Source(MultiRaftError::UnknownGroup(99)))
        ));
        assert!(matches!(
            handle.local_storage_status(group, Instant::now()).await,
            Err(RuntimeError::Deadline {
                phase: RuntimePhase::Admission,
                outcome_unknown: false
            })
        ));
        owner.shutdown(deadline()).await.unwrap();
        reusable(&factory, address, 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn capture_panic_marks_unconfirmed_and_cleanup_releases_actual_fsm_even_when_join_fails() {
    let disk = old_disk();
    let mut factory = Factory::new(disk.path());
    factory.panic_capture = true;
    let address = address();
    let owner = factory.start(address, &[GROUP]).await;
    let handle = owner.handle();
    assert!(matches!(
        handle.request_compaction(GROUP, deadline()).await,
        Err(RuntimeError::MaintenanceRejected(
            CompactionRejection::SubmissionFailed
        ))
    ));
    assert_eq!(
        status(&handle, GROUP).await.progress,
        CompactionProgress::Unconfirmed
    );
    assert_eq!(read(&handle, GROUP).await, 15);
    assert!(owner.shutdown(deadline()).await.is_err());
    reusable(&factory, address, 1);
    factory.panic_capture = false;
    factory
        .start(address, &[GROUP])
        .await
        .shutdown(deadline())
        .await
        .unwrap();
    reusable(&factory, address, 2);
}
