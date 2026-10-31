//! Controlled startup and replacement through the fixed Slot public boundary.
mod controlled_support;
use controlled_support::*;
use multiraft_slot::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn controlled_genesis_and_cold_provisional_single_source_need_no_quorum() {
    let mut fixture = Fixture::fresh().await;
    let mut bases = std::collections::BTreeMap::new();
    for id in 1..=3 {
        for a in &fixture.layout.config().slots {
            let proof = fixture
                .handle(id)
                .controlled(a.slot)
                .unwrap()
                .genesis_basis(deadline())
                .await
                .unwrap();
            assert_eq!(proof.generation(), id * 10);
            assert_eq!(
                proof
                    .configuration()
                    .membership()
                    .voter_ids()
                    .collect::<Vec<_>>(),
                vec![1, 2, 3]
            );
            if let Some(expected) = bases.get(&a.slot) {
                assert_eq!(proof.configuration(), expected);
            } else {
                bases.insert(a.slot, proof.configuration().clone());
            }
        }
    }
    let retained = fixture.handle(1).controlled(SlotId(0)).unwrap();
    fixture.stop_owners().await;
    assert!(retained.receipt(deadline()).await.is_err());
    let source = fixture
        .start(1, SlotStartupDisposition::RecoverExisting, 2)
        .await
        .unwrap();
    for a in &fixture.layout.config().slots {
        let role = source
            .handle()
            .local_role(a.slot, deadline())
            .await
            .unwrap();
        assert_eq!(role.role, LocalSlotRole::Voter);
        assert_eq!(role.generation, Some(10));
        source
            .handle()
            .controlled(a.slot)
            .unwrap()
            .genesis_basis(deadline())
            .await
            .unwrap();
    }
    fixture.owners.push(source);
    fixture.stop().await;
}

use authority::{
    StartupAuthority, StartupAuthorityResult, StartupRequest, StartupTargetGeneration,
};
use multiraft_core::{OperationId, ReplacementSpec};
use multiraft_net::controlled_startup as authority;
use multiraft_net::portable::{
    NativeTargetOutcome, NativeTargetTransport, NativeTargetTransportError, SourceTargetPort,
};
use std::sync::Arc;
struct Transport {
    endpoint: SlotTargetEndpoint<controlled_support::disk::DiskImage>,
}
impl NativeTargetTransport for Transport {
    fn peer(&self) -> (u64, Option<[u8; 32]>) {
        let id = self.endpoint.identity();
        (id.node, id.domain)
    }
    fn exchange(
        &self,
        request: Vec<u8>,
    ) -> authority::StartupAuthorityFuture<'_, Result<Vec<u8>, NativeTargetTransportError>> {
        Box::pin(async move {
            self.endpoint
                .dispatch(request, deadline())
                .await
                .map_err(|e| NativeTargetTransportError {
                    outcome: NativeTargetOutcome::Unknown,
                    message: e.to_string(),
                })
        })
    }
}
async fn leader(f: &Fixture, slot: SlotId) -> u64 {
    let d = deadline();
    loop {
        for id in 1..=3 {
            let role = f.handle(id).local_role(slot, d).await.unwrap();
            if role.leader_eligible {
                return id;
            }
        }
        assert!(tokio::time::Instant::now() < d);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}
fn key(f: &Fixture, slot: SlotId) -> Vec<u8> {
    (0..10000)
        .map(|n| format!("key-{n}").into_bytes())
        .find(|k| f.layout.slot_for_key(k).unwrap() == slot)
        .unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn candidate_internal_routes_and_actual_learner_joint_uniform_roles_preserve_payload() {
    let mut f = Fixture::fresh().await;
    let slot = SlotId(0);
    let group = f.layout.group_for_slot(slot).unwrap();
    let k = key(&f, slot);
    let candidate = f
        .start(4, SlotStartupDisposition::Candidate, 1)
        .await
        .unwrap();
    f.owners.push(candidate);
    let target_handle = f.handle(4);
    assert!(
        !target_handle.status().accepting,
        "candidate does not gain external intake authority"
    );
    for a in &f.layout.config().slots {
        assert_eq!(
            target_handle
                .local_role(a.slot, deadline())
                .await
                .unwrap()
                .role,
            LocalSlotRole::Missing
        );
    }
    // Router-only candidate may already use trusted discovery and the internal Slot RPC path.
    target_handle
        .propose(&k, b"pending-opaque-body".to_vec(), deadline())
        .await
        .unwrap();
    let leader = leader(&f, slot).await;
    let source = if leader == 1 { 3 } else { 1 };
    let old = f.handle(leader).controlled(slot).unwrap();
    let membership = old.membership(deadline()).await.unwrap();
    f.authority.0[&group]
        .bootstrap_membership(membership.clone())
        .unwrap();
    let spec = ReplacementSpec {
        operation: OperationId {
            sequence: 1,
            nonce: [9; 16],
        },
        expected_start: membership,
        target_voters: [1, 2, 3, 4]
            .into_iter()
            .filter(|id| *id != source)
            .collect(),
        target_generation: 40,
    };
    let target = target_handle
        .reserve_controlled_target(slot, spec.clone(), vec![4], deadline())
        .await
        .unwrap();
    let receipt = target.receipt(deadline()).await.unwrap();
    assert_eq!(
        target_handle
            .local_role(slot, deadline())
            .await
            .unwrap()
            .role,
        LocalSlotRole::Missing
    );
    f.authority.0[&group]
        .consume_target(StartupTargetGeneration {
            node_id: 4,
            generation: 40,
            consumption_id: receipt.identity,
        })
        .unwrap();
    let endpoint = target
        .target_endpoint(spec.clone(), deadline())
        .await
        .unwrap();
    let again = target
        .target_endpoint(spec.clone(), deadline())
        .await
        .unwrap();
    assert_eq!(endpoint.identity(), again.identity());
    let retained = endpoint.clone();
    let port = SourceTargetPort::new(
        endpoint.identity().clone(),
        spec.clone(),
        Arc::new(Transport { endpoint }),
    )
    .unwrap();
    let operation = old
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
    operation
        .seed_target(port.clone(), admission, deadline())
        .await
        .unwrap();
    assert_eq!(
        target_handle
            .local_role(slot, deadline())
            .await
            .unwrap()
            .role,
        LocalSlotRole::Missing
    );
    assert!(
        !target_handle
            .local_role(slot, deadline())
            .await
            .unwrap()
            .leader_eligible
    );
    let payload = target_handle
        .read_applied(slot, b"", deadline())
        .await
        .unwrap()
        .value;
    assert_eq!(
        serde_json::from_slice::<Vec<Vec<u8>>>(&payload).unwrap(),
        vec![b"pending-opaque-body".to_vec()]
    );
    operation
        .add_learner(port.clone(), deadline())
        .await
        .unwrap();
    assert_eq!(target_handle.local_role(slot,deadline()).await.unwrap().role,LocalSlotRole::Learner);
    operation
        .enter_joint(port.clone(), deadline())
        .await
        .unwrap();
    let joint = target_handle.local_role(slot, deadline()).await.unwrap();
    assert_eq!(joint.role, LocalSlotRole::Voter);
    assert_eq!(
        joint
            .membership
            .unwrap()
            .effective_membership
            .voter_configs
            .len(),
        2
    );
    operation.finalize_uniform(deadline()).await.unwrap();
    operation.complete(port, deadline()).await.unwrap();
    let removed = f.handle(source);
    removed
        .controlled(slot)
        .unwrap()
        .retire_from_authority(spec.operation, deadline())
        .await
        .unwrap();
    assert_eq!(
        removed.local_role(slot, deadline()).await.unwrap().role,
        LocalSlotRole::Retired
    );
    removed
        .propose(&k, b"through-removed-source".to_vec(), deadline())
        .await
        .unwrap();
    let payload = removed
        .read_linearizable(&k, b"", deadline())
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Vec<Vec<u8>>>(&payload).unwrap(),
        vec![
            b"pending-opaque-body".to_vec(),
            b"through-removed-source".to_vec()
        ]
    );
    assert_eq!(removed.layout().digest(), f.layout.digest());
    assert!(!target_handle.status().accepting);
    f.stop().await;
    assert!(retained.dispatch(vec![], deadline()).await.is_err());
}
