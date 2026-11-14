//! Local listener authority is independent from an installed peer catalog.
use super::*;
use std::net::TcpListener;

fn moved(node_id: u64) -> NodeAddress {
    NodeAddress {
        node_id,
        raft: address(),
        application_rpc: address(),
    }
}
fn assert_bound(node: &NodeAddress) {
    assert!(TcpListener::bind(node.raft).is_err(), "native listener");
    assert!(
        TcpListener::bind(node.application_rpc).is_err(),
        "Slot RPC listener"
    );
}
fn assert_free(node: &NodeAddress) {
    assert!(TcpListener::bind(node.raft).is_ok(), "old native socket");
    assert!(
        TcpListener::bind(node.application_rpc).is_ok(),
        "old Slot RPC socket"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn cold_candidate_preserves_local_listener_authority_and_exact_catalog() {
    for explicit_bind in [false, true] {
        let mut f = Fixture::fresh_sources().await;
        let old = moved(4);
        let owner = candidate(&f, old.clone()).await;
        assert!(owner.shutdown(deadline()).await.complete);
        let manifest_path = f.root.join("node-4").join(MANIFEST_FILE_NAME);
        let manifest = std::fs::read(&manifest_path).unwrap();
        let desired = moved(4);
        let mut bound = if explicit_bind {
            moved(4)
        } else {
            desired.clone()
        };
        if explicit_bind {
            bound.raft.set_ip(std::net::Ipv4Addr::UNSPECIFIED.into());
            bound
                .application_rpc
                .set_ip(std::net::Ipv4Addr::UNSPECIFIED.into());
        }
        let mut startup = f.startup(4, SlotStartupDisposition::Candidate, 2);
        startup.startup.directory.nodes = vec![desired.clone()];
        let domain = startup.transport_domain(&f.layout, 4).unwrap();
        let v4 = catalog(&f, old.clone(), 4);
        let original_bytes = serde_json::to_vec(&v4).unwrap();
        let expected = v4.validated_install().unwrap();
        let mut config = SlotNodeConfig::new(f.layout.clone(), 4, f.root.join("node-4"));
        if explicit_bind {
            config.transport = Some(SlotTransportConfig {
                nodes: v4
                    .peers
                    .iter()
                    .map(|peer| SlotTransportNode {
                        node_id: peer.node_id,
                        raft: peer.raft.to_string(),
                        application_rpc: peer.application_rpc.to_string(),
                    })
                    .collect(),
                raft_bind: bound.raft,
                application_rpc_bind: bound.application_rpc,
            });
        }
        let owner = SlotNodeOwner::<DiskImage>::start_controlled_with_carriers(
            config,
            startup,
            v4.clone(),
            |_| panic!("cold candidate must construct zero Core/FSMs"),
            deadline(),
        )
        .await
        .unwrap();
        let handle = owner.handle();
        let actual = handle.local_transport_addresses().unwrap();
        assert_eq!(actual.advertised, desired);
        assert_eq!(actual.raft_bind, bound.raft);
        assert_eq!(actual.application_rpc_bind, bound.application_rpc);
        assert_eq!(actual.directory_version, 4);
        assert_eq!(handle.transport_domain().unwrap(), domain);
        assert_eq!(
            handle.carrier_catalog(deadline()).await.unwrap(),
            Some(expected)
        );
        assert_eq!(serde_json::to_vec(&v4).unwrap(), original_bytes);
        assert_bound(&bound);
        assert_free(&old);
        assert_eq!(std::fs::read(&manifest_path).unwrap(), manifest);
        assert_eq!(
            handle.local_role(SlotId(0), deadline()).await.unwrap().role,
            LocalSlotRole::Missing
        );
        // A fresh registration later replaces the stale self row without restarting listeners.
        let mut v5 = v4;
        v5.version = 5;
        let row = v5.peers.iter_mut().find(|peer| peer.node_id == 4).unwrap();
        row.raft = desired.raft;
        row.application_rpc = desired.application_rpc;
        f.owners.push(owner);
        for id in 1..=4 {
            assert_eq!(
                f.handle(id)
                    .install_carrier_catalog(v5.clone(), deadline())
                    .await
                    .unwrap(),
                v5.validated_install().unwrap()
            );
        }
        let k = key(&f, SlotId(0));
        f.handle(4)
            .propose(&k, b"fresh-local-registration".to_vec(), deadline())
            .await
            .unwrap();
        let payload = f
            .handle(4)
            .read_linearizable(&k, b"", deadline())
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Vec<Vec<u8>>>(&payload).unwrap(),
            vec![b"fresh-local-registration".to_vec()]
        );
        assert_eq!(f.handle(4).local_transport_addresses().unwrap(), actual);
        assert_bound(&bound);
        f.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn cold_source_preserves_identity_generation_and_local_addresses() {
    let mut f = Fixture::fresh_sources().await;
    let old = f.directory.nodes[0].clone();
    let node4 = moved(4);
    let v4 = catalog(&f, node4, 4);
    let manifest_path = f.root.join("node-1").join(MANIFEST_FILE_NAME);
    let manifest = std::fs::read(&manifest_path).unwrap();
    let slot = SlotId(0);
    let current = leader(&f, slot).await;
    let membership = f
        .handle(current)
        .local_authoritative_membership(slot, deadline())
        .await
        .unwrap()
        .membership;
    let owner = f.owners.remove(0);
    assert!(owner.shutdown(deadline()).await.complete);
    let desired = moved(1);
    let mut startup = f.startup(1, SlotStartupDisposition::RecoverExisting, 2);
    startup.startup.directory.nodes[0] = desired.clone();
    let domain = startup.transport_domain(&f.layout, 1).unwrap();
    let path = f.root.join("node-1");
    let fsm_path = path.clone();
    let owner = SlotNodeOwner::start_controlled_with_carriers(
        SlotNodeConfig::new(f.layout.clone(), 1, path),
        startup,
        v4.clone(),
        move |ctx: multiraft_net::FsmFactoryContext| {
            DiskImage::open(&fsm_path, ctx.group_id()).map_err(Into::into)
        },
        deadline(),
    )
    .await
    .unwrap();
    assert_eq!(
        owner
            .handle()
            .local_transport_addresses()
            .unwrap()
            .advertised,
        desired
    );
    assert_bound(&desired);
    assert_free(&old);
    assert_eq!(owner.handle().transport_domain().unwrap(), domain);
    assert_eq!(
        owner.handle().carrier_catalog(deadline()).await.unwrap(),
        Some(v4.validated_install().unwrap())
    );
    assert_eq!(std::fs::read(&manifest_path).unwrap(), manifest);
    f.owners.insert(0, owner);
    let mut v5 = v4;
    v5.version = 5;
    let row = v5.peers.iter_mut().find(|peer| peer.node_id == 1).unwrap();
    row.raft = desired.raft;
    row.application_rpc = desired.application_rpc;
    for id in 1..=3 {
        f.handle(id)
            .install_carrier_catalog(v5.clone(), deadline())
            .await
            .unwrap();
    }
    let current = leader(&f, slot).await;
    if current != 1 {
        transfer(&f, slot, current, 1).await;
    }
    assert_eq!(
        f.handle(1)
            .local_authoritative_membership(slot, deadline())
            .await
            .unwrap()
            .membership,
        membership
    );
    let k = key(&f, slot);
    f.handle(2)
        .propose(&k, b"recovered-source-new-address".to_vec(), deadline())
        .await
        .unwrap();
    let payload = f
        .handle(2)
        .read_linearizable(&k, b"", deadline())
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Vec<Vec<u8>>>(&payload).unwrap(),
        vec![b"recovered-source-new-address".to_vec()]
    );
    f.stop().await;
}
