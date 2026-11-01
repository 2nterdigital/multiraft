//! Native-only real OLD recovery at original /data paths, without App, TiKV or initialization.
#![allow(dead_code)]
#[path = "legacy_adoption_support/opaque.rs"]
mod support;
use multiraft_net::controlled_startup as authority;
#[path = "../../multiraft-net/tests/startup_authority_support/catalog.rs"]
mod catalog;
use authority::{StartupAuthority, StartupAuthorityResult, StartupCatalog, StartupRequest};
use multiraft_core::{ClusterConfig, LogStorageBackend, SnapshotMode};
use multiraft_net::{
    ControlledOpen, ControlledOpenMode, FsmFactoryContext, NodeOwner, RuntimeConfig,
};
use multiraft_slot::adoption::{adopt_existing, inspect_existing};
use std::{path::Path, sync::Arc, time::Duration};
fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(30)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "run only in network-isolated Native test container with disposable copy mounted /data"]
async fn original_native_core_recovers_real_memberships_and_all_business_bytes_without_initialize()
{
    assert_eq!(std::env::var("NATIVE_ADOPTION_DISPOSABLE").unwrap(), "1");
    let root = Path::new("/data/state/slot");
    assert!(Path::new("/data/NATIVE-TEST-COPY").is_file());
    let before = support::business_payloads(root);
    assert_eq!(
        before.0,
        "78058cd782cbf4b514b6f59f0356cb9992343468b5bdb2c0f9371434318e0296"
    );
    assert_eq!(before.1, 98);
    let evidence = inspect_existing(root).unwrap();
    let authorization = support::authorization(&evidence);
    let first = &evidence.replicas()[0];
    let blocker = root.join(format!(
        "controlled-adoption/group-{}.anchor.json",
        first.group
    ));
    std::fs::create_dir_all(&blocker).unwrap();
    assert!(adopt_existing(&evidence, &authorization).is_err());
    let fingerprint = evidence
        .transport_fingerprint(&authorization.startup)
        .unwrap();
    let mut config = ClusterConfig::for_test(1, &[1, 2, 3]);
    config.data_dir = root.to_owned();
    config.log_storage_backend = LogStorageBackend::RocksDb;
    config.snapshot_mode = SnapshotMode::NativeDurable;
    config.raft_transport_fingerprint = Some(fingerprint);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    config.peers = vec![
        (1, address),
        (2, "127.0.0.1:17302".parse().unwrap()),
        (3, "127.0.0.1:17303".parse().unwrap()),
    ];
    let incomplete = NodeOwner::start(
        RuntimeConfig::new(config.clone(), vec![]),
        move |ctx: FsmFactoryContext| {
            support::OpaqueLegacyFsm::open(root, ctx.group_id()).map_err(Into::into)
        },
        deadline(),
    )
    .await
    .unwrap();
    let half_authority = Arc::new(catalog::FileAuthority::provision(
        Path::new("/data/half-conversion-catalog.json"),
        StartupCatalog {
            group_id: first.group,
            revision: 1,
            membership: first.membership.clone(),
            authorized: [(1, 1), (2, 1), (3, 1)].into(),
            active_operation: None,
            admissions: vec![],
            completed: vec![],
        },
    ));
    let half_admission = match half_authority
        .admit(StartupRequest {
            group_id: first.group,
            node_id: 1,
            generation: 1,
            request_id: [6; 16],
        })
        .await
        .unwrap()
    {
        StartupAuthorityResult::Applied(admission) => admission,
        _ => panic!("fixture admission"),
    };
    assert!(
        incomplete
            .handle()
            .open_controlled_group(
                ControlledOpen {
                    group_id: first.group,
                    generation: 1,
                    identity: evidence
                        .replica_identity(first.group, &authorization.startup.identity)
                        .unwrap(),
                    mode: ControlledOpenMode::Recover
                },
                half_authority,
                half_admission,
                deadline()
            )
            .await
            .is_err(),
        "normal Recover must refuse incomplete adoption"
    );
    assert!(
        incomplete
            .handle()
            .local_group_status(first.group, deadline())
            .await
            .is_err(),
        "no Core is constructed for half conversion"
    );
    incomplete.shutdown(deadline()).await.unwrap();
    std::fs::remove_dir(blocker).unwrap();
    let receipt = adopt_existing(&inspect_existing(root).unwrap(), &authorization).unwrap();
    let owner = NodeOwner::start(
        RuntimeConfig::new(config, vec![]),
        move |ctx: FsmFactoryContext| {
            support::OpaqueLegacyFsm::open(root, ctx.group_id()).map_err(Into::into)
        },
        deadline(),
    )
    .await
    .unwrap();
    let handle = owner.handle();
    std::fs::create_dir_all("/data/native-test-catalogs").unwrap();
    for original in evidence.replicas() {
        assert_eq!(
            original.committed,
            Some(original.applied),
            "this opaque factory cannot replay a business suffix"
        );
        let authority = Arc::new(catalog::FileAuthority::provision(
            &Path::new("/data/native-test-catalogs").join(format!("{}.json", original.group)),
            StartupCatalog {
                group_id: original.group,
                revision: 1,
                membership: original.membership.clone(),
                authorized: [(1, 1), (2, 1), (3, 1)].into(),
                active_operation: None,
                admissions: vec![],
                completed: vec![],
            },
        ));
        let admission = match authority
            .admit(StartupRequest {
                group_id: original.group,
                node_id: 1,
                generation: 1,
                request_id: [7; 16],
            })
            .await
            .unwrap()
        {
            StartupAuthorityResult::Applied(admission) => admission,
            _ => panic!("catalog must admit original generation"),
        };
        let group = handle
            .open_controlled_group(
                ControlledOpen {
                    group_id: original.group,
                    generation: 1,
                    identity: receipt.identities[&original.group].clone(),
                    mode: ControlledOpenMode::Recover,
                },
                authority,
                admission,
                deadline(),
            )
            .await
            .unwrap();
        assert_eq!(group.receipt(deadline()).await.unwrap().generation, 1);
        let view = handle
            .try_read_applied_with_watermark(original.group, deadline(), |_| {
                Ok::<_, std::io::Error>(())
            })
            .await
            .unwrap();
        let applied = view.applied.unwrap();
        assert_eq!(
            (applied.term, applied.node_id, applied.index),
            (
                original.applied.leader_id.term,
                original.applied.leader_id.node_id,
                original.applied.index
            )
        );
        let (observed, _) = handle
            .observe_group(original.group, deadline())
            .await
            .unwrap();
        let membership = *original.membership.log_id();
        assert_eq!(
            observed
                .committed_membership
                .log_id
                .map(|id| (id.term, id.node_id, id.index)),
            membership.map(|id| (id.leader_id.term, id.leader_id.node_id, id.index))
        );
        assert_eq!(
            &observed.committed_membership.voter_configs,
            original.membership.membership().get_joint_config()
        );
        assert!(
            group.genesis_basis(deadline()).await.is_err(),
            "adopted replicas must never masquerade as fresh Genesis"
        );
    }
    owner.shutdown(deadline()).await.unwrap();
    let completed = inspect_existing(root)
        .expect("completed adoption remains inspectable after legitimate Native open");
    assert_eq!(
        adopt_existing(&completed, &authorization).expect(
            "completed same-authorization replay must not require OLD physical engine files"
        ),
        receipt
    );

    assert_eq!(support::business_payloads(root),before,"all original pending, full first-result, Presence and sibling checkpoint rows remain exact");
}
