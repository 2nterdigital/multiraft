//! Independent application/provider and real gRPC; no raw Raft/test probes.
mod startup_support;
use futures::FutureExt;
use multiraft_core::ClusterConfig;
use multiraft_net::{
    ElectionSource, ElectionSourceConfig, ElectionSourceEvent as Event, ElectionSourceRead as Read,
    ElectionSourceRecord, GroupConfig, NodeOwner, RuntimeConfig, RuntimeError, SourceFact,
    SourceUnknown,
};
use startup_support::*;
use std::time::Duration;
use tokio::time::timeout;

fn source(boot: &str, capacity: usize) -> ElectionSource {
    ElectionSource::new(ElectionSourceConfig {
        run_id: "consumer-run".into(),
        boot_id: boot.into(),
        capacity,
    })
    .unwrap()
}
fn config(root: &std::path::Path, id: u64, peers: &[(u64, std::net::SocketAddr)]) -> RuntimeConfig {
    let ids: Vec<_> = peers.iter().map(|(id, _)| *id).collect();
    let mut c = ClusterConfig::for_test(id, &ids);
    c.peers = peers.to_vec();
    c.data_dir = root.to_owned();
    c.file_log_sync_level = multiraft_core::FileLogSyncLevel::Data;
    RuntimeConfig::new(
        c,
        vec![GroupConfig {
            group_id: 7,
            voters: ids,
        }],
    )
}
fn records(source: &ElectionSource) -> Vec<ElectionSourceRecord> {
    let mut rx = source.subscribe();
    let mut values = vec![];
    while let Some(read) = rx.try_recv() {
        match read {
            Read::Record(record) => values.push(*record),
            Read::Lagged { .. } | Read::NativeDropped { .. } => (),
            Read::Closed => break,
        }
    }
    values
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_grpc_source_preserves_vote_time_membership_and_rpc_scope_without_inferred_campaigns()
{
    let roots: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let peers: Vec<_> = (1..=3).map(|id| (id, addr())).collect();
    let sources: Vec<_> = (1..=3)
        .map(|id| source(&format!("boot-{id}"), 512))
        .collect();
    let mut owners = futures::future::join_all((0..3).map(|i| {
        NodeOwner::start_with_election_source(
            config(roots[i].path(), i as u64 + 1, &peers),
            Factory::new(roots[i].path()),
            deadline(),
            sources[i].clone(),
        )
    }))
    .await
    .into_iter()
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
    let validation = std::panic::AssertUnwindSafe(async {
    let handles: Vec<_> = owners.iter().map(|o| o.handle()).collect();
    let lead = leader(&handles, 7).await;
    // Opaque payload must never appear in a source fact; source enums contain no bytes.
    let payload = b"consumer-private-command".to_vec();
    lead.propose(7, payload.clone(), deadline()).await.unwrap();
    for (i, handle) in handles.iter().enumerate() {
        let point = handle.sample_election_state(7, deadline()).await.unwrap();
        assert!(point.vote.committed);
        assert_eq!(point.vote.node_id, lead.node_id());
        assert!(point.vote_last_modified_age.is_some());
        let SourceFact::Known(membership) = &point.effective_membership else {
            panic!("RF3 membership")
        };
        assert_eq!(membership.voter_configs.len(), 1);
        assert_eq!(
            membership.voter_configs[0]
                .iter()
                .copied()
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(
            point.actual_random_timeout,
            SourceFact::Unknown(SourceUnknown::PublicPointNotExposed)
        );
        assert_eq!(
            point.lease_enabled,
            SourceFact::Unknown(SourceUnknown::PublicPointNotExposed)
        );
        assert_eq!(
            point.greater_log,
            SourceFact::Unknown(SourceUnknown::PublicPointNotExposed)
        );
        let status = sources[i].status();
        assert_eq!(status.attached_node, Some(handle.node_id()));
        assert_eq!(status.capabilities.native_origin, SourceFact::Known(()));
        assert!(!status.closed);
    }
    // An actual inbound RPC's native decision is distinct from its grant bit.
    let denied_node = handles
        .iter()
        .find(|h| h.node_id() != lead.node_id())
        .unwrap()
        .node_id();
    let wire = multiraft_net::GrpcRouter::from_config(&config(roots[0].path(), 1, &peers).cluster);
    let request = openraft::raft::VoteRequest::<multiraft_core::TypeConfig>::new(
        openraft::Vote::new(100, 42),
        None,
    );
    let reply = openraft_multi::GroupRouter::vote(
        &wire,
        denied_node,
        7,
        request,
        openraft::network::RPCOption::new(Duration::from_secs(1)),
    )
    .await
    .unwrap();
    assert!(!reply.vote_granted);
    wire.close();
    wire.join().await.unwrap();
    assert!(records(&sources[(denied_node - 1) as usize]).iter().any(|r| matches!(&r.event,
        Event::Native { event } if matches!(event.kind, multiraft_net::NativeElectionKind::VoteRequestProcessed {
            disposition: multiraft_net::VoteRequestDisposition::LeaseNotExpired, .. }))));
    let voters = [1, 2, 3];
    let context = || {
        multiraft_net::ControlContext::new(multiraft_net::ControlInvocationId([9; 16]), deadline())
    };
    let sample = timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(sample) = lead.read_group_control_sample(7, &voters, context()).await {
                if sample
                    .target_qualifications
                    .values()
                    .any(|q| matches!(q, multiraft_net::TargetQualification::Qualified { .. }))
                {
                    break sample;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let target = *sample
        .target_qualifications
        .iter()
        .find(|(_, q)| matches!(q, multiraft_net::TargetQualification::Qualified { .. }))
        .unwrap()
        .0;
    let preconditions = multiraft_net::GroupControlPreconditions {
        group_id: 7,
        expected_source: lead.node_id(),
        observed_vote: sample.flushed_vote,
        effective_membership: sample.effective_membership,
        committed_membership: sample.committed_membership,
        target,
    };
    assert!(matches!(
        lead.try_transfer_group_leader(&preconditions, &voters, context())
            .await
            .result,
        multiraft_net::GroupControlRequestResult::TriggerQueued { .. }
    ));
    timeout(Duration::from_secs(2), async {
        loop {
            if sources.iter().flat_map(records).any(|r| {
                matches!(
                    r.event,
                    Event::TransferRpcFinished {
                        result: SourceFact::Known(multiraft_net::TransferRpcResult::Accepted),
                        ..
                    }
                )
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // The transfer target's own native entry, not the queued RPC, proves origin.
    timeout(Duration::from_secs(2), async {
        loop {
            let seen = records(&sources[(target - 1) as usize]).iter().any(|r| matches!(&r.event,
                Event::Native { event } if matches!(event.kind, multiraft_net::NativeElectionKind::CampaignStarted {
                    origin: multiraft_net::CampaignOrigin::LeadershipTransfer, .. })));
            if seen { break; }
            tokio::task::yield_now().await;
        }
    }).await.unwrap();
    let current = timeout(Duration::from_secs(2), async {
        loop {
            let target_handle = &handles[(target - 1) as usize];
            if target_handle
                .read_linearizable(7, deadline(), |_| ())
                .await
                .is_ok()
            {
                break target;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let index = owners
        .iter()
        .position(|o| o.handle().node_id() == current)
        .unwrap();
    owners.remove(index).shutdown(deadline()).await.unwrap();
    let survivors: Vec<_> = handles
        .iter()
        .filter(|h| h.node_id() != current)
        .cloned()
        .collect();
    leader(&survivors, 7)
        .await
        .propose(7, payload, deadline())
        .await
        .unwrap();
    let all: Vec<_> = sources.iter().flat_map(records).collect();
    assert!(all
        .iter()
        .any(|r| matches!(r.event, Event::InitializeStarted)));
    assert!(all
        .iter()
        .any(|r| matches!(r.event, Event::InitializeFinished { .. })));
    assert!(all
        .iter()
        .any(|r| matches!(r.event, Event::VoteRpcStarted { .. })));
    assert!(all.iter().any(|r| matches!(
        r.event,
        Event::VoteRpcFinished {
            response: SourceFact::Known(_),
            ..
        }
    )));
    assert!(all.iter().any(|r| matches!(&r.event, Event::Native { event } if matches!(event.kind,
        multiraft_net::NativeElectionKind::CampaignStarted { origin: multiraft_net::CampaignOrigin::Initialize, .. }))));
    assert!(all.iter().any(|r| matches!(&r.event, Event::Native { event } if matches!(event.kind,
        multiraft_net::NativeElectionKind::CampaignStarted { origin: multiraft_net::CampaignOrigin::AutomaticTimeout, .. }))));
    for s in &sources {
        let native: Vec<_> = records(s)
            .into_iter()
            .filter_map(|r| match r.event {
                Event::Native { event } => Some((r.native_round, event)),
                _ => None,
            })
            .collect();
        assert!(native.iter().any(|(_, event)| matches!(
            event.kind,
            multiraft_net::NativeElectionKind::Started { .. }
        )));
        for (round, event) in native {
            match event.kind {
                multiraft_net::NativeElectionKind::CampaignStarted {
                    campaign_id,
                    election_timeout_after,
                    ..
                } => {
                    assert_eq!(round, SourceFact::Known(campaign_id));
                    assert!((Duration::from_millis(300)..Duration::from_millis(600))
                        .contains(&election_timeout_after));
                }
                multiraft_net::NativeElectionKind::VoteResponse {
                    disposition: multiraft_net::VoteResponseDisposition::Granted { .. },
                    campaign_id,
                    ..
                }
                | multiraft_net::NativeElectionKind::QuorumGranted { campaign_id, .. }
                | multiraft_net::NativeElectionKind::LeaderEstablished { campaign_id, .. } => {
                    assert!(
                        campaign_id.is_some(),
                        "matching native campaign is observed on this normal trace"
                    );
                }
                _ => (),
            }
        }
    }
    for (i, s) in sources.iter().enumerate() {
        for r in records(s) {
            assert_eq!(r.run_id.as_ref(), "consumer-run");
            assert_eq!(r.boot_id.as_ref(), format!("boot-{}", i + 1));
            assert_eq!(r.local_node_id, i as u64 + 1);
            if r.group_id.is_some() {
                assert_eq!(r.group_id, Some(7));
            }
            if !matches!(r.event, Event::Native { .. }) {
                assert_eq!(
                    r.native_round,
                    SourceFact::Unknown(SourceUnknown::NotObserved)
                );
            }
            if let Event::VoteRpcFinished {
                response: SourceFact::Known(response),
                ..
            } = r.event
            {
                assert_eq!(
                    response.native_consumed,
                    SourceFact::Unknown(SourceUnknown::NotObserved)
                );
            }
        }
    }
    }).catch_unwind().await;
    let cleanup =
        futures::future::join_all(owners.into_iter().map(|owner| owner.shutdown(deadline()))).await;
    if cleanup.iter().any(Result::is_err) {
        for root in roots {
            eprintln!("unreleased_consumer_root={}", root.keep().display());
        }
        if let Err(original) = validation {
            std::panic::resume_unwind(original);
        }
        panic!("owned cleanup failed: {cleanup:?}");
    }
    if let Err(original) = validation {
        std::panic::resume_unwind(original);
    }

    for s in &sources {
        assert!(s.status().closed);
        assert_eq!(s.status().active_attempts, 0);
    }
    for (i, root) in roots.iter().enumerate() {
        reusable(root.path(), std::slice::from_ref(&peers[i]));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_lag_repeated_receivers_and_cancelled_accepted_sample_release_actual_resources() {
    let root = tempfile::tempdir().unwrap();
    let peers = vec![(1, addr())];
    let s = source("bounded-boot", 32);
    let mut rx = s.subscribe();
    assert!(
        rx.recv().now_or_never().is_none(),
        "cancelled waiting receive"
    );
    let owner = NodeOwner::start_with_election_source(
        config(root.path(), 1, &peers),
        Factory::new(root.path()),
        deadline(),
        s.clone(),
    )
    .await
    .unwrap();
    let handle = owner.handle();
    let validation = std::panic::AssertUnwindSafe(async {
        // First poll admits and registers a retained job; dropping the caller waiter
        // cannot remove it or prevent its terminal source record.
        assert!(handle
            .sample_election_state(7, deadline())
            .now_or_never()
            .is_none());
        timeout(Duration::from_secs(2), async {
            loop {
                if records(&s)
                    .iter()
                    .any(|r| matches!(r.event, Event::StatePoint { .. }))
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        for _ in 0..20 {
            handle.sample_election_state(7, deadline()).await.unwrap();
        }
        let a = records(&s);
        let b = records(&s);
        assert_eq!(a, b, "identities support dedup across subscribers");
        assert!(s.status().evicted > 0);
        let missing = s.status().last_sequence - s.status().retained as u64;
        assert_eq!(
            rx.recv().await,
            Read::Lagged {
                first_missing: 1,
                last_missing: missing
            }
        );
        assert_eq!(s.status().active_attempts, 0);
        for _ in 0..s.status().retained {
            assert!(matches!(rx.recv().await, Read::Record(_)));
        }
        assert!(rx.recv().now_or_never().is_none());
    })
    .catch_unwind()
    .await;
    let cleanup = owner.shutdown(deadline()).await;
    if cleanup.is_err() {
        eprintln!("unreleased_consumer_root={}", root.keep().display());
        if let Err(original) = validation {
            std::panic::resume_unwind(original);
        }
        panic!("owned cleanup failed: {cleanup:?}");
    }
    if let Err(original) = validation {
        std::panic::resume_unwind(original);
    }

    assert!(matches!(rx.recv().await, Read::Record(record) if record.event == Event::Closed));
    assert_eq!(rx.recv().await, Read::Closed);
    assert!(matches!(
        handle.sample_election_state(7, deadline()).await,
        Err(RuntimeError::Closed)
    ));
    reusable(root.path(), &peers);
    assert_eq!(s.status().receivers, 1);
    drop(rx);
    assert_eq!(s.status().receivers, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn construction_failure_and_owner_drop_close_source_without_retaining_provider_or_port() {
    let root = tempfile::tempdir().unwrap();
    let peers = vec![(1, addr())];
    let s = source("failed-boot", 32);
    let mut factory = Factory::new(root.path());
    factory.reject = Some(7);
    assert!(NodeOwner::start_with_election_source(
        config(root.path(), 1, &peers),
        factory,
        deadline(),
        s.clone()
    )
    .await
    .is_err());
    assert!(s.status().closed);
    reusable(root.path(), &peers);
    // Reusing a source is rejected before opening a listener or constructing FSM.
    assert!(matches!(
        NodeOwner::start_with_election_source(
            config(root.path(), 1, &peers),
            Factory::new(root.path()),
            deadline(),
            s.clone()
        )
        .await,
        Err(RuntimeError::InvalidConfig(_))
    ));
    let live = source("drop-boot", 32);
    let owner = NodeOwner::start_with_election_source(
        config(root.path(), 1, &peers),
        Factory::new(root.path()),
        deadline(),
        live.clone(),
    )
    .await
    .unwrap();
    let _receiver = live.subscribe();
    drop(owner);
    timeout(Duration::from_secs(3), async {
        while !live.status().closed {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    reusable(root.path(), &peers);
}

#[test]
fn invalid_identities_and_capacity_rejected_and_unattached_source_is_not_zero_campaigns() {
    for (identity, capacity) in [("payload / credential", 2), ("ok", 0), ("ok", 8193)] {
        assert!(ElectionSource::new(ElectionSourceConfig {
            run_id: identity.into(),
            boot_id: "boot".into(),
            capacity
        })
        .is_err());
    }
    let s = source("not-attached", 1);
    let status = s.status();
    assert_eq!(status.attached_node, None);
    assert!(!status.closed);
    assert_eq!(status.capabilities.native_round, SourceFact::Known(()));
    assert!(s.subscribe().try_recv().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn twenty_group_burst_reports_source_coverage_and_releases_all_resources() {
    let roots: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let peers: Vec<_> = (1..=3).map(|id| (id, addr())).collect();
    let sources: Vec<_> = (1..=3)
        .map(|id| source(&format!("burst-{id}"), 4096))
        .collect();
    let owners = futures::future::join_all((0..3).map(|i| {
        let mut c = config(roots[i].path(), i as u64 + 1, &peers);
        c.groups.clear();
        NodeOwner::start_with_election_source(
            c,
            Factory::new(roots[i].path()),
            deadline(),
            sources[i].clone(),
        )
    }))
    .await
    .into_iter()
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
    let validation = std::panic::AssertUnwindSafe(async {
    let handles: Vec<_> = owners.iter().map(|o| o.handle()).collect();
    let inputs = || multiraft_net::StartupBatch {
        input_digest: None,
        groups: (7..27)
            .map(|id| multiraft_net::StartupGroup {
                group: GroupConfig {
                    group_id: id,
                    voters: vec![1, 2, 3],
                },
                preferred_initializer: Some(id % 3 + 1),
            })
            .collect(),
        grace: Duration::from_millis(500),
        recovery_timeout: Duration::from_secs(5),
    };
    futures::future::join_all(
        handles
            .iter()
            .map(|h| h.start_groups_with_preference(inputs())),
    )
    .await
    .into_iter()
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
    for group in 7..27 {
        leader(&handles, group)
            .await
            .propose(group, vec![1], deadline())
            .await
            .unwrap();
    }
    let stats: Vec<_> = sources.iter().map(|s| {
        let status = s.status();
        let starts = records(s).iter().filter(|r| matches!(&r.event,
            Event::Native { event } if matches!(event.kind, multiraft_net::NativeElectionKind::Started { .. }))).count();
        if status.native_dropped == 0 { assert_eq!(starts, 20); }
        assert_eq!(status.evicted, 0, "capacity must cover this bounded window");
        (status.attached_node, status.native_received, status.native_dropped, starts)
    }).collect();
    eprintln!("twenty_group_source_coverage={stats:?}");
    // Loss stays explicit; this fixture never relabels a partial window complete.
    }).catch_unwind().await;
    let cleanup =
        futures::future::join_all(owners.into_iter().map(|owner| owner.shutdown(deadline()))).await;
    if cleanup.iter().any(Result::is_err) {
        for root in roots {
            eprintln!("unreleased_consumer_root={}", root.keep().display());
        }
        if let Err(original) = validation {
            std::panic::resume_unwind(original);
        }
        panic!("owned cleanup failed: {cleanup:?}");
    }
    if let Err(original) = validation {
        std::panic::resume_unwind(original);
    }
    for (i, root) in roots.iter().enumerate() {
        reusable(root.path(), std::slice::from_ref(&peers[i]));
    }
    for s in sources {
        let final_status = s.status();
        assert!(final_status.closed);
        assert_eq!(final_status.active_attempts, 0);
        assert_eq!(final_status.native_pending, 0);
        assert_eq!(final_status.native_dropped, 0);
        assert_eq!(
            final_status.native_received,
            records(&s)
                .iter()
                .filter(|r| matches!(r.event, Event::Native { .. }))
                .count() as u64
        );
    }
}
