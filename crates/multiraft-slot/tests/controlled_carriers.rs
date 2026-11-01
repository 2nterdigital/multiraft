//! Post-start carrier installation through the controlled Slot boundary.
mod controlled_support;
use controlled_support::disk::DiskImage;
use controlled_support::*;
use multiraft_net::{CarrierCatalog, CarrierPeer};
use multiraft_slot::*;
fn catalog(f: &Fixture, extra: NodeAddress, version: u64) -> CarrierCatalog {
    let mut peers = f
        .directory
        .nodes
        .iter()
        .map(|n| CarrierPeer {
            node_id: n.node_id,
            identity: format!("persistent-passport-{}", n.node_id).into_bytes(),
            raft: n.raft,
            application_rpc: n.application_rpc,
        })
        .collect::<Vec<_>>();
    peers.push(CarrierPeer {
        node_id: extra.node_id,
        identity: b"persistent-passport-4".to_vec(),
        raft: extra.raft,
        application_rpc: extra.application_rpc,
    });
    CarrierCatalog {
        version,
        scope: Some(f.handle(1).transport_domain().unwrap()),
        peers,
    }
}
async fn candidate(f: &Fixture, node: NodeAddress) -> SlotNodeOwner<DiskImage> {
    let mut startup = f.startup(4, SlotStartupDisposition::Candidate, 1);
    startup.startup.directory.nodes = vec![node];
    let path = f.root.join("node-4");
    let fsm_path = path.clone();
    let calls = f.factory_calls.clone();
    let before = calls.load(std::sync::atomic::Ordering::Relaxed);
    let owner = SlotNodeOwner::start_controlled(
        SlotNodeConfig::new(f.layout.clone(), 4, &path),
        startup,
        move |ctx: multiraft_net::FsmFactoryContext| {
            calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            DiskImage::open(&fsm_path, ctx.group_id()).map_err(Into::into)
        },
        deadline(),
    )
    .await
    .unwrap();
    assert_eq!(
        f.factory_calls.load(std::sync::atomic::Ordering::Relaxed),
        before,
        "candidate startup must have zero factory calls"
    );
    owner
}
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn install_ack_requires_both_pools_and_changes_no_native_authority() {
    let mut f = Fixture::fresh_sources().await;
    assert!(f.directory.nodes.iter().all(|node| node.node_id != 4));
    for id in 1..=3 {
        assert_eq!(
            f.handle(id).carrier_catalog(deadline()).await.unwrap(),
            None
        );
    }
    let node = NodeAddress {
        node_id: 4,
        raft: address(),
        application_rpc: address(),
    };
    let added = candidate(&f, node.clone()).await;
    f.owners.push(added);
    assert_eq!(
        f.handle(4)
            .local_role(SlotId(0), deadline())
            .await
            .unwrap()
            .role,
        LocalSlotRole::Missing
    );
    let before = f.factory_calls.load(std::sync::atomic::Ordering::Relaxed);
    let snapshot = catalog(&f, node, 4);
    let expected = snapshot.validated_install().unwrap();
    for id in 1..=4 {
        assert_eq!(
            f.handle(id)
                .install_carrier_catalog(snapshot.clone(), deadline())
                .await
                .unwrap(),
            expected
        );
        assert_eq!(
            f.handle(id).carrier_catalog(deadline()).await.unwrap(),
            Some(expected)
        );
    }
    assert_eq!(
        f.factory_calls.load(std::sync::atomic::Ordering::Relaxed),
        before
    );
    assert!(!f.handle(4).status().accepting);
    assert_eq!(
        f.handle(4)
            .local_role(SlotId(0), deadline())
            .await
            .unwrap()
            .role,
        LocalSlotRole::Missing
    );
    let mut replay = snapshot.clone();
    replay.peers.reverse();
    assert_eq!(
        f.handle(1)
            .install_carrier_catalog(replay, deadline())
            .await
            .unwrap(),
        expected
    );
    let mut stale = snapshot.clone();
    stale.version = 3;
    let mut conflict = snapshot.clone();
    conflict.peers[0].application_rpc = address();
    let mut identity = snapshot.clone();
    identity.version = 5;
    identity.peers[0].identity = b"different-persistent-passport".to_vec();
    let mut scope = snapshot.clone();
    scope.version = 5;
    scope.scope = Some([99; 32]);
    for rejected in [stale, conflict, identity, scope] {
        assert!(f
            .handle(1)
            .install_carrier_catalog(rejected, deadline())
            .await
            .is_err());
        assert_eq!(
            f.handle(1).carrier_catalog(deadline()).await.unwrap(),
            Some(expected)
        );
    }
    let retained = f.handle(4);
    let closing = f.owners.pop().unwrap();
    let mut next = snapshot;
    next.version = 5;
    let (_outcome, closed) = tokio::join!(
        retained.install_carrier_catalog(next, deadline()),
        closing.shutdown(deadline())
    );
    assert!(closed.complete);
    assert!(retained.carrier_catalog(deadline()).await.is_err());
    assert!(retained
        .install_carrier_catalog(
            catalog(
                &f,
                NodeAddress {
                    node_id: 4,
                    raft: address(),
                    application_rpc: address()
                },
                6
            ),
            deadline()
        )
        .await
        .is_err());
    f.stop().await;
}

use multiraft_core::{OperationId, ReplacementSpec};
use multiraft_net::controlled_startup::{
    StartupAuthority, StartupAuthorityResult, StartupRequest, StartupTargetGeneration,
};
use multiraft_net::portable::{
    NativeTargetOutcome, NativeTargetTransport, NativeTargetTransportError, SourceTargetPort,
};
use std::sync::Arc;
struct TargetTransport {
    endpoint: SlotTargetEndpoint<DiskImage>,
}
impl NativeTargetTransport for TargetTransport {
    fn peer(&self) -> (u64, Option<[u8; 32]>) {
        let peer = self.endpoint.identity();
        (peer.node, peer.domain)
    }
    fn exchange(
        &self,
        bytes: Vec<u8>,
    ) -> multiraft_net::controlled_startup::StartupAuthorityFuture<
        '_,
        Result<Vec<u8>, NativeTargetTransportError>,
    > {
        Box::pin(async move {
            self.endpoint
                .dispatch(bytes, deadline())
                .await
                .map_err(|e| NativeTargetTransportError {
                    outcome: NativeTargetOutcome::Unknown,
                    message: e.to_string(),
                })
        })
    }
}
async fn leader(f: &Fixture, slot: SlotId) -> u64 {
    let until = deadline();
    loop {
        for id in 1..=f.owners.len() as u64 {
            if f.handle(id)
                .local_role(slot, until)
                .await
                .unwrap()
                .leader_eligible
            {
                return id;
            }
        }
        assert!(tokio::time::Instant::now() < until);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}
async fn transfer(f: &Fixture, slot: SlotId, source: u64, target: u64) {
    use multiraft_net::{
        ControlContext, ControlInvocationId, GroupControlPreconditions, GroupControlRequestResult,
        TargetQualification,
    };
    let context = ControlContext::new(ControlInvocationId([8; 16]), deadline());
    let sample = loop {
        let sample = f
            .handle(source)
            .read_group_control_sample(slot, context)
            .await
            .unwrap();
        if matches!(
            sample.target_qualifications.get(&target),
            Some(TargetQualification::Qualified { .. })
        ) {
            break sample;
        }
        assert!(tokio::time::Instant::now() < context.deadline);
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    };
    let result = f
        .handle(source)
        .try_transfer_group_leader(
            slot,
            GroupControlPreconditions {
                group_id: sample.group_id,
                expected_source: source,
                observed_vote: sample.flushed_vote,
                effective_membership: sample.effective_membership,
                committed_membership: sample.committed_membership,
                target,
            },
            context,
        )
        .await
        .unwrap();
    assert!(
        matches!(
            result.result,
            GroupControlRequestResult::TriggerQueued { .. }
        ),
        "{result:?}"
    );
    let until = deadline();
    while !f
        .handle(target)
        .local_role(slot, until)
        .await
        .unwrap()
        .leader_eligible
    {
        assert!(tokio::time::Instant::now() < until);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}
fn key(f: &Fixture, slot: SlotId) -> Vec<u8> {
    (0..10000)
        .map(|n| format!("carrier-key-{n}").into_bytes())
        .find(|key| f.layout.slot_for_key(key).unwrap() == slot)
        .unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn post_start_fourth_node_replaces_source_and_new_address_serves_removed_client() {
    let mut f = Fixture::fresh_sources().await;
    let slot = SlotId(0);
    let group = f.layout.group_for_slot(slot).unwrap();
    let k = key(&f, slot);
    let source_leader = leader(&f, slot).await;
    let before = f
        .handle(source_leader)
        .local_authoritative_membership(slot, deadline())
        .await
        .unwrap();
    let manifest = std::fs::read(f.root.join("node-1").join(MANIFEST_FILE_NAME)).unwrap();
    let node4 = NodeAddress {
        node_id: 4,
        raft: address(),
        application_rpc: address(),
    };
    assert!(f.directory.nodes.iter().all(|node| node.node_id != 4));
    let candidate = candidate(&f, node4.clone()).await;
    f.owners.push(candidate);
    assert!(f
        .handle(4)
        .propose(
            &k,
            b"uninstalled-candidate".to_vec(),
            tokio::time::Instant::now() + std::time::Duration::from_millis(200)
        )
        .await
        .is_err());
    let v4 = catalog(&f, node4.clone(), 4);
    for id in 1..=4 {
        f.handle(id)
            .install_carrier_catalog(v4.clone(), deadline())
            .await
            .unwrap();
    }
    let after = f
        .handle(source_leader)
        .local_authoritative_membership(slot, deadline())
        .await
        .unwrap();
    assert_eq!(before.membership, after.membership);
    assert_eq!(
        before.ledger, after.ledger,
        "carrier install cannot change native high-water/operation state"
    );
    f.handle(4)
        .propose(&k, b"pending-full-opaque-body".to_vec(), deadline())
        .await
        .unwrap();
    let mut current = leader(&f, slot).await;
    if current == 3 {
        transfer(&f, slot, 3, 1).await;
        current = 1;
    }
    let control = f.handle(current).controlled(slot).unwrap();
    let starting = control.membership(deadline()).await.unwrap();
    f.authority.0[&group]
        .bootstrap_membership(starting.clone())
        .unwrap();
    let spec = ReplacementSpec {
        operation: OperationId {
            sequence: 1,
            nonce: [18; 16],
        },
        expected_start: starting,
        target_voters: [1, 2, 4].into(),
        target_generation: 40,
    };
    let target = f
        .handle(4)
        .reserve_controlled_target(slot, spec.clone(), vec![4], deadline())
        .await
        .unwrap();
    let receipt = target.receipt(deadline()).await.unwrap();
    f.authority.0[&group]
        .consume_target(StartupTargetGeneration {
            node_id: 4,
            generation: 40,
            consumption_id: receipt.identity.clone(),
        })
        .unwrap();
    let endpoint = target
        .target_endpoint(spec.clone(), deadline())
        .await
        .unwrap();
    let port = SourceTargetPort::new(
        endpoint.identity().clone(),
        spec.clone(),
        Arc::new(TargetTransport { endpoint }),
    )
    .unwrap();
    let op = control
        .begin(spec.clone(), 1, port.clone(), deadline())
        .await
        .unwrap();
    let admission = match f
        .authority
        .admit(StartupRequest {
            group_id: group,
            node_id: 4,
            generation: 40,
            request_id: [40; 16],
        })
        .await
        .unwrap()
    {
        StartupAuthorityResult::Applied(a) => a,
        other => panic!("{other:?}"),
    };
    op.seed_target(port.clone(), admission, deadline())
        .await
        .unwrap();
    op.add_learner(port.clone(), deadline()).await.unwrap();
    assert_eq!(
        f.handle(4).local_role(slot, deadline()).await.unwrap().role,
        LocalSlotRole::Learner
    );
    op.enter_joint(port.clone(), deadline()).await.unwrap();
    op.finalize_uniform(deadline()).await.unwrap();
    op.complete(port, deadline()).await.unwrap();
    f.handle(3)
        .controlled(slot)
        .unwrap()
        .retire_from_authority(spec.operation, deadline())
        .await
        .unwrap();
    transfer(&f, slot, current, 4).await;
    f.handle(3)
        .propose(&k, b"retired-client-to-new-leader".to_vec(), deadline())
        .await
        .unwrap();
    let payload = f
        .handle(3)
        .read_linearizable(&k, b"", deadline())
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Vec<Vec<u8>>>(&payload).unwrap(),
        vec![
            b"pending-full-opaque-body".to_vec(),
            b"retired-client-to-new-leader".to_vec()
        ]
    );
    let membership = f
        .handle(4)
        .local_authoritative_membership(slot, deadline())
        .await
        .unwrap()
        .membership;
    // Restart only node4 at new actual sockets. No membership/Genesis rewrite grants the move.
    let old = f.owners.pop().unwrap();
    assert!(old.shutdown(deadline()).await.complete);
    let mut sparse = f.startup(4, SlotStartupDisposition::Candidate, 4);
    sparse.startup.directory.nodes = vec![node4];
    let refused = SlotNodeOwner::<DiskImage>::start_controlled(
        SlotNodeConfig::new(f.layout.clone(), 4, f.root.join("node-4")),
        sparse,
        |_| panic!("authorized target cannot open from sparse candidate carrier declarations"),
        deadline(),
    )
    .await;
    assert!(
        matches!(refused, Err(RuntimeError::InvalidConfig)),
        "authorized/consumed local generation requires full carrier coverage before recovery"
    );
    let mut v5 = v4.clone();
    v5.version = 5;
    let row = v5.peers.iter_mut().find(|peer| peer.node_id == 4).unwrap();
    row.raft = address();
    row.application_rpc = address();
    let new_rpc = row.application_rpc;
    let mut startup = f.startup(4, SlotStartupDisposition::Candidate, 4);
    startup.replicas = vec![ControlledSlotReplica {
        slot,
        generation: 40,
        identity: receipt.identity,
        request_id: [44; 16],
    }];
    let root = f.root.join("node-4");
    let path = root.clone();
    let reopened = SlotNodeOwner::start_controlled_with_carriers(
        SlotNodeConfig::new(f.layout.clone(), 4, &root),
        startup,
        v5.clone(),
        move |ctx: multiraft_net::FsmFactoryContext| {
            DiskImage::open(&path, ctx.group_id()).map_err(Into::into)
        },
        deadline(),
    )
    .await
    .unwrap();
    assert_eq!(
        reopened
            .handle()
            .local_transport_addresses()
            .unwrap()
            .advertised
            .application_rpc,
        new_rpc
    );
    assert_eq!(
        reopened
            .handle()
            .local_transport_addresses()
            .unwrap()
            .directory_version,
        5,
        "cold startup reports the version of the address set actually used"
    );
    assert_eq!(
        reopened.handle().carrier_catalog(deadline()).await.unwrap(),
        Some(v5.validated_install().unwrap())
    );
    f.owners.push(reopened);
    for id in 1..=3 {
        f.handle(id)
            .install_carrier_catalog(v5.clone(), deadline())
            .await
            .unwrap();
    }
    let current = leader(&f, slot).await;
    if current != 4 {
        transfer(&f, slot, current, 4).await;
    }
    assert_eq!(
        f.handle(4)
            .local_authoritative_membership(slot, deadline())
            .await
            .unwrap()
            .membership,
        membership,
        "dial address changes must preserve full native membership metadata"
    );
    f.handle(3)
        .propose(&k, b"new-actual-address".to_vec(), deadline())
        .await
        .unwrap();
    let payload = f
        .handle(3)
        .read_linearizable(&k, b"", deadline())
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Vec<Vec<u8>>>(&payload).unwrap(),
        vec![
            b"pending-full-opaque-body".to_vec(),
            b"retired-client-to-new-leader".to_vec(),
            b"new-actual-address".to_vec()
        ]
    );
    assert_eq!(
        std::fs::read(f.root.join("node-1").join(MANIFEST_FILE_NAME)).unwrap(),
        manifest
    );
    assert_eq!(
        f.handle(3).local_role(slot, deadline()).await.unwrap().role,
        LocalSlotRole::Retired
    );
    f.stop().await;
}
