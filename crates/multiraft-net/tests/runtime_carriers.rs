//! Verified carrier catalogs add runtime connectivity without constructing native Groups.
#![allow(dead_code)]
use multiraft_net::{CarrierCatalog, CarrierPeer, CarrierService, GrpcPeerChannelPool};
fn peer(id: u64, raft: u16) -> CarrierPeer {
    CarrierPeer {
        node_id: id,
        identity: vec![id as u8],
        raft: format!("127.0.0.1:{raft}").parse().unwrap(),
        application_rpc: format!("127.0.0.1:{}", raft + 100).parse().unwrap(),
    }
}
#[tokio::test]
async fn unknown_runtime_peer_can_be_installed_and_versions_remain_immutable() {
    let pool = GrpcPeerChannelPool::with_carrier_config(
        vec![(1, "127.0.0.1:20001".parse().unwrap())],
        None,
        Some([7; 32]),
        CarrierService::Raft,
    );
    assert!(pool.lease(4).await.is_err());
    let catalog = CarrierCatalog {
        version: 1,
        scope: Some([7; 32]),
        peers: vec![peer(1, 20001), peer(4, 20004)],
    };
    let installed = pool.install_carrier_catalog(&catalog).unwrap();
    assert_eq!(installed.version, 1);
    assert_eq!(pool.install_carrier_catalog(&catalog).unwrap(), installed);
    let mut changed = catalog.clone();
    changed.peers[1].raft = "127.0.0.1:20005".parse().unwrap();
    assert!(pool.install_carrier_catalog(&changed).is_err());
    changed.version = 2;
    pool.install_carrier_catalog(&changed).unwrap();
    assert!(pool.install_carrier_catalog(&catalog).is_err());
    changed.version = 3;
    changed.peers[1].identity = vec![99];
    assert!(pool.install_carrier_catalog(&changed).is_err());
    pool.shutdown().await;
    assert!(pool.install_carrier_catalog(&catalog).is_err());
    assert!(pool.carrier_catalog().is_err());
}

use multiraft_net::controlled_startup as authority;
#[path = "startup_authority_support/catalog.rs"]
mod catalog;
#[path = "controlled_protocol_support/disk.rs"]
mod disk;
#[path = "owned_controlled_support/fixture.rs"]
mod fixture;
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn running_original_sources_add_an_unknown_candidate_only_after_carrier_install() {
    use fixture::{deadline, Fixture, LocalTransport, GROUP};
    use multiraft_net::portable::SourceTargetPort;
    use std::sync::Arc;
    let fixture = Fixture::without_candidate().await;
    let prepared = fixture
        .prepare_gated(
            fixture::transport::Gate::FirstChunk,
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
        )
        .await;
    let port = SourceTargetPort::new(
        prepared.endpoint.identity().clone(),
        prepared.specification.clone(),
        Arc::new(LocalTransport {
            endpoint: prepared.endpoint.clone(),
        }),
    )
    .unwrap();
    prepared
        .executor
        .seed_target(port.clone(), prepared.admission.clone(), deadline())
        .await
        .unwrap();
    let failure = prepared
        .executor
        .add_learner(port.clone(), deadline())
        .await
        .unwrap_err();
    assert!(
        failure
            .to_string()
            .contains("target peer address is unknown"),
        "actual unknown4 gate: {failure}"
    );
    let group = &fixture.groups[prepared.leader];
    let before = group.membership(deadline()).await.unwrap();
    let ledger = group.local_ledger(deadline()).await.unwrap();
    let catalog = CarrierCatalog {
        version: 1,
        scope: Some([7; 32]),
        peers: fixture.configs[3]
            .peers
            .iter()
            .map(|(id, address)| CarrierPeer {
                node_id: *id,
                identity: vec![*id as u8],
                raft: *address,
                application_rpc: format!("127.0.0.1:{}", *id).parse().unwrap(),
            })
            .collect(),
    };
    for owner in &fixture.owners {
        assert_eq!(
            owner.handle().carrier_scope(deadline()).await.unwrap(),
            Some([7; 32])
        );
        assert!(owner
            .handle()
            .carrier_catalog(deadline())
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            owner
                .handle()
                .install_carrier_catalog(catalog.clone(), deadline())
                .await
                .unwrap(),
            catalog.validated_install().unwrap()
        );
        assert!(
            owner
                .handle()
                .local_group_status(GROUP + 1, deadline())
                .await
                .is_err(),
            "catalog installation creates no Group"
        );
    }
    assert_eq!(
        group.membership(deadline()).await.unwrap(),
        before,
        "transport installation cannot change native membership"
    );
    assert_eq!(
        group.local_ledger(deadline()).await.unwrap(),
        ledger,
        "transport installation cannot change native high-water/operation state"
    );
    prepared
        .executor
        .add_learner(port.clone(), deadline())
        .await
        .expect("actual native learner4 is reachable through installed runtime carrier");
    let after = group.membership(deadline()).await.unwrap();
    assert_eq!(
        after.membership().learner_ids().collect::<Vec<_>>(),
        vec![4]
    );
    prepared
        .executor
        .enter_joint(port.clone(), deadline())
        .await
        .unwrap();
    prepared
        .executor
        .finalize_uniform(deadline())
        .await
        .unwrap();
    prepared.executor.complete(port, deadline()).await.unwrap();
    // Relocate the same real learner/voter process while the existing source Raft
    // clients retain the old StoredMembership address. Only carriers may change.
    let membership = group.membership(deadline()).await.unwrap();
    let identity = prepared.endpoint.identity().consumption.clone();
    let mut fixture = fixture;
    fixture.owners.remove(3).shutdown(deadline()).await.unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let relocated = listener.local_addr().unwrap();
    drop(listener);
    let mut candidate_config = fixture.configs[3].clone();
    candidate_config
        .peers
        .iter_mut()
        .find(|(node, _)| *node == 4)
        .unwrap()
        .1 = relocated;
    let candidate = fixture::start_owned(candidate_config).await;
    use authority::{StartupAuthority, StartupAuthorityResult, StartupRequest};
    use multiraft_net::{ControlledOpen, ControlledOpenMode};
    let admission = match fixture
        .authority
        .admit(StartupRequest {
            group_id: GROUP,
            node_id: 4,
            generation: 40,
            request_id: [95; 16],
        })
        .await
        .unwrap()
    {
        StartupAuthorityResult::Applied(value) => value,
        other => panic!("{other:?}"),
    };
    candidate
        .handle()
        .open_controlled_group(
            ControlledOpen {
                group_id: GROUP,
                generation: 40,
                identity,
                mode: ControlledOpenMode::Recover,
            },
            fixture.authority.clone(),
            admission,
            deadline(),
        )
        .await
        .unwrap();
    fixture.owners.push(candidate);
    let mut newer = catalog.clone();
    newer.version = 2;
    newer
        .peers
        .iter_mut()
        .find(|peer| peer.node_id == 4)
        .unwrap()
        .raft = relocated;
    for owner in &fixture.owners {
        owner
            .handle()
            .install_carrier_catalog(newer.clone(), deadline())
            .await
            .unwrap();
    }
    let payload = b"existing-native-client-follows-current-carrier".to_vec();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            for owner in &fixture.owners {
                let handle = owner.handle();
                if handle
                    .local_group_status(GROUP, deadline())
                    .await
                    .is_ok_and(|status| status.leader_hint == Some(handle.node_id()))
                    && handle
                        .propose(GROUP, payload.clone(), deadline())
                        .await
                        .is_ok()
                {
                    return;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("current native leader proposes through existing shared network clients");
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let actual = fixture.owners[3]
                .handle()
                .try_read_applied(GROUP, deadline(), |image| image.payloads())
                .await
                .unwrap();
            if actual.contains(&payload) {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("relocated real replica receives body despite old BasicNode address");
    assert_eq!(
        fixture.groups[prepared.leader]
            .membership(deadline())
            .await
            .unwrap(),
        membership,
        "relocation never rewrites full StoredMembership metadata"
    );
    fixture.stop().await;
}

#[test]
fn rejected_catalogs_preserve_content_scope_and_retired_identity_pins() {
    use multiraft_net::CarrierError;
    let pool =
        GrpcPeerChannelPool::with_carrier_config(vec![], None, Some([7; 32]), CarrierService::Raft);
    let catalog = CarrierCatalog {
        version: 1,
        scope: Some([7; 32]),
        peers: vec![peer(1, 20001), peer(4, 20004)],
    };
    let first = pool.install_carrier_catalog(&catalog).unwrap();
    let mut reordered = catalog.clone();
    reordered.peers.reverse();
    assert_eq!(
        reordered.validated_install().unwrap(),
        first,
        "canonical content is independent of input ordering"
    );
    assert_eq!(pool.install_carrier_catalog(&reordered).unwrap(), first);
    let mut wrong = catalog.clone();
    wrong.version = 2;
    wrong.scope = Some([8; 32]);
    assert!(matches!(
        pool.install_carrier_catalog(&wrong),
        Err(CarrierError::ScopeMismatch)
    ));
    wrong = catalog.clone();
    wrong.version = 2;
    wrong.peers.push(wrong.peers[0].clone());
    assert!(pool.install_carrier_catalog(&wrong).is_err());
    wrong = catalog.clone();
    wrong.version = 2;
    wrong.peers[1].raft = wrong.peers[0].raft;
    assert!(pool.install_carrier_catalog(&wrong).is_err());
    wrong = catalog.clone();
    wrong.version = 2;
    wrong.peers[1].identity.clear();
    assert!(pool.install_carrier_catalog(&wrong).is_err());
    wrong = catalog.clone();
    wrong.version = 2;
    wrong.peers[1].identity = vec![1; 4097];
    assert!(pool.install_carrier_catalog(&wrong).is_err());
    wrong = catalog.clone();
    wrong.version = 2;
    wrong.peers[1].node_id = 0;
    assert!(pool.install_carrier_catalog(&wrong).is_err());
    assert_eq!(
        pool.carrier_catalog().unwrap().unwrap(),
        catalog,
        "all rejected input leaves verified state unchanged"
    );
    let mut removed = catalog.clone();
    removed.version = 2;
    removed.peers.pop();
    pool.install_carrier_catalog(&removed).unwrap();
    wrong = catalog.clone();
    wrong.version = 3;
    wrong.peers[1].identity = vec![99];
    assert!(
        matches!(
            pool.install_carrier_catalog(&wrong),
            Err(CarrierError::IdentityMismatch { node: 4 })
        ),
        "removing a carrier never clears its persistent identity pin"
    );
    wrong.peers[1].identity = vec![4];
    pool.install_carrier_catalog(&wrong).unwrap();
}

#[test]
fn lossy_ipv6_flow_information_is_rejected_before_cross_component_digest_acknowledgement() {
    use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};
    let mut catalog = CarrierCatalog {
        version: 1,
        scope: Some([7; 32]),
        peers: vec![peer(4, 20004)],
    };
    catalog.peers[0].raft = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 20004, 9, 0));
    assert!(catalog.validated_install().is_err(),"serde SocketAddr omits IPv6 flowinfo, so it cannot be acknowledged as exact full carrier content");
}
