//! No native handles: actual 5-MiB peer install, bounded interruption and cold replay.
mod owned_snapshot_support;
use multiraft_net::RuntimeError;
use owned_snapshot_support::*;
use std::collections::BTreeSet;
use std::sync::atomic::Ordering;
use std::time::Duration;

async fn make_lagging(cluster: &mut Cluster) -> usize {
    cluster.start_all().await;
    for _ in 0..3 {
        cluster.add(1).await;
    }
    cluster.wait_all(3).await;
    let leader = cluster.leader().await;
    let victim = (leader + 1) % 3;
    assert!(cluster.status(victim).await.durable_snapshot.is_none());
    cluster.stop(victim, false).await;
    for _ in 0..10 {
        cluster.add(1).await;
    }
    cluster.wait_all(13).await;
    cluster.compact_live().await;
    victim
}
async fn assert_membership_preserved(cluster: &Cluster, victim: usize) {
    let (observed, _) = cluster
        .handle(victim)
        .observe_group(GROUP, deadline())
        .await
        .unwrap();
    assert_eq!(
        observed.committed_membership.voter_configs,
        vec![BTreeSet::from([1, 2, 3])]
    );
    assert!(observed.committed_membership.learner_ids.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn public_peer_install_survives_owner_drop_then_tail_recovers_without_live_peers() {
    let mut cluster = Cluster::new("peer-drop-");
    let victim = make_lagging(&mut cluster).await;
    let gate = Gate::default();
    cluster.factories[victim].gate = Some(gate.clone());
    cluster.factories[victim].minimum = 3;
    let starting = tokio::spawn(multiraft_net::NodeOwner::start(
        cluster.config(victim),
        cluster.factories[victim].clone(),
        deadline(),
    ));
    gate.entered().await;
    // The owned restore has a full bounded image and may outlive its API waiter.
    // Keep heartbeat/election unchanged; the explicit install budget is5 seconds.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let old_handle = if starting.is_finished() {
        let owner = starting.await.unwrap().unwrap();
        let handle = owner.handle();
        drop(owner);
        assert!(matches!(
            handle.propose(GROUP, vec![], deadline()).await,
            Err(RuntimeError::Closed)
        ));
        Some(handle)
    } else {
        // Recovery races the real incoming install. An unpublished start still
        // owns rollback and cannot hand the consumer any executable capability.
        starting.abort();
        assert!(starting.await.err().unwrap().is_cancelled());
        None
    };
    assert_eq!(
        cluster.factories[victim]
            .stats
            .released
            .load(Ordering::SeqCst),
        1
    );
    assert!(cluster.factories[victim]
        .root
        .join("consumer.lease")
        .exists());
    gate.release();
    cluster.dropped_reclaimed(victim, 2).await;
    if let Some(handle) = old_handle {
        assert!(matches!(
            handle.local_storage_status(GROUP, deadline()).await,
            Err(RuntimeError::Closed)
        ));
    }
    cluster.factories[victim].gate = None;
    cluster.start(victim).await;
    cluster.wait_value(victim, 13).await;
    let installed = cluster.status(victim).await;
    assert_eq!(
        installed.durable_snapshot.unwrap().bytes,
        IMAGE_BYTES as u64
    );
    assert!(installed.native_snapshot.is_some());
    assert!(
        cluster.factories[victim]
            .stats
            .restored
            .load(Ordering::SeqCst)
            > 0
    );
    assert_membership_preserved(&cluster, victim).await;
    cluster.add(7).await;
    cluster.wait_all(20).await;
    let leader = cluster.leader().await;
    assert_eq!(
        cluster
            .handle(leader)
            .read_linearizable(GROUP, deadline(), |fsm| fsm.value)
            .await
            .unwrap(),
        20
    );
    cluster.stop_all().await;
    // All original peer listeners are closed. Local native checkpoint plus actual
    // tail must pass the consumer's public recovery callback with exactvalue20.
    append_uncommitted_tail(&cluster.factories[victim].root);
    cluster.factories[victim].exact = Some(20);
    cluster.start(victim).await;
    assert_eq!(
        *cluster.factories[victim]
            .stats
            .validations
            .lock()
            .unwrap()
            .last()
            .unwrap(),
        20
    );
    assert_membership_preserved(&cluster, victim).await;
    assert!(cluster
        .handle(victim)
        .read_linearizable(GROUP, deadline(), |fsm| fsm.value)
        .await
        .is_err());
    cluster.stop(victim, false).await;
    assert_eq!(
        cluster.factories[victim]
            .stats
            .released
            .load(Ordering::SeqCst),
        4
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn restore_refusal_does_not_publish_checkpoint_and_same_disk_can_recover_and_retry_peer() {
    let mut cluster = Cluster::new("peer-refusal-");
    let victim = make_lagging(&mut cluster).await;
    cluster.factories[victim]
        .stats
        .reject_restore
        .store(true, Ordering::SeqCst);
    cluster.factories[victim].minimum = 3;
    let starting = tokio::spawn(multiraft_net::NodeOwner::start(
        cluster.config(victim),
        cluster.factories[victim].clone(),
        deadline(),
    ));
    tokio::time::timeout(
        Duration::from_secs(8),
        cluster.factories[victim].stats.rejection.notified(),
    )
    .await
    .unwrap();
    match starting.await.unwrap() {
        Ok(owner) => {
            cluster.owners[victim] = Some(owner);
            assert!(cluster
                .handle(victim)
                .read_linearizable(GROUP, deadline(), |fsm| fsm.value)
                .await
                .is_err());
            assert!(cluster.status(victim).await.durable_snapshot.is_none());
            cluster.stop(victim, true).await;
        }
        Err(error) => {
            assert!(matches!(error, RuntimeError::Source(_)));
            cluster.dropped_reclaimed(victim, 2).await;
        }
    }
    // Inspect only the consumer-owned filesystem outcome: no active checkpoint
    // was published after its restore refused. Keep staged failed generations.
    assert!(!cluster.factories[victim]
        .root
        .join("snapshots/7/native-v1/active.json")
        .exists());
    assert_eq!(
        cluster.factories[victim]
            .stats
            .released
            .load(Ordering::SeqCst),
        2
    );
    // The real rejected data directory and its staged generations stay in place.
    cluster.factories[victim]
        .stats
        .reject_restore
        .store(false, Ordering::SeqCst);
    cluster.start(victim).await;
    cluster.wait_value(victim, 13).await;
    assert_eq!(
        cluster.status(victim).await.durable_snapshot.unwrap().bytes,
        IMAGE_BYTES as u64
    );
    assert_membership_preserved(&cluster, victim).await;
    cluster.add(7).await;
    cluster.wait_all(20).await;
    cluster.stop_all().await;
    cluster.factories[victim].exact = Some(20);
    cluster.start(victim).await;
    cluster.stop(victim, false).await;
    assert_eq!(
        cluster.factories[victim]
            .stats
            .released
            .load(Ordering::SeqCst),
        4
    );
}
