//! Controlled startup and replacement through the fixed Slot public boundary.
mod controlled_support;
use controlled_support::*;
use multiraft_slot::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn controlled_genesis_and_cold_sources_need_no_quorum_in_both_catalog_phases() {
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
    let second = fixture
        .start(2, SlotStartupDisposition::RecoverExisting, 2)
        .await
        .unwrap();
    for a in &fixture.layout.config().slots {
        let proof = second
            .handle()
            .controlled(a.slot)
            .unwrap()
            .genesis_basis(deadline())
            .await
            .unwrap();
        assert_eq!(proof.configuration(), &bases[&a.slot]);
        let group = fixture.layout.group_for_slot(a.slot).unwrap();
        assert!(
            fixture
                .authority
                .read(group)
                .await
                .unwrap()
                .membership
                .log_id()
                .is_none(),
            "cold recovery must never finalize a provisional catalog"
        );
    }
    fixture.owners.push(source);
    fixture.owners.push(second);
    fixture.stop_owners().await;
    for a in &fixture.layout.config().slots {
        let group = fixture.layout.group_for_slot(a.slot).unwrap();
        fixture.authority.0[&group]
            .bootstrap_membership(bases[&a.slot].clone())
            .unwrap();
    }
    let finalized = fixture
        .start(1, SlotStartupDisposition::RecoverExisting, 3)
        .await
        .unwrap();
    for a in &fixture.layout.config().slots {
        let proof = finalized
            .handle()
            .controlled(a.slot)
            .unwrap()
            .genesis_basis(deadline())
            .await
            .unwrap();
        assert_eq!(proof.configuration(), &bases[&a.slot]);
        assert_eq!(
            finalized
                .handle()
                .local_role(a.slot, deadline())
                .await
                .unwrap()
                .generation,
            Some(10)
        );
    }
    fixture.owners.push(finalized);
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
    let before = f.factory_calls.load(std::sync::atomic::Ordering::Relaxed);
    let candidate = f
        .start(4, SlotStartupDisposition::Candidate, 1)
        .await
        .unwrap();
    f.owners.push(candidate);
    assert_eq!(
        f.factory_calls.load(std::sync::atomic::Ordering::Relaxed),
        before,
        "unapproved candidate has zero factory calls"
    );
    let mut ordinary = f.startup(4, SlotStartupDisposition::Candidate, 2).startup;
    ordinary.identity.protocol_version = SLOT_PROTOCOL_VERSION;
    let ordinary_result = SlotNodeOwner::<controlled_support::disk::DiskImage>::start_strict(
        SlotNodeConfig::new(f.layout.clone(), 4, f.root.join("node-4")),
        ordinary,
        |_| panic!("ordinary fallback must reject before construction"),
        deadline(),
    )
    .await;
    assert!(
        matches!(ordinary_result, Err(RuntimeError::ManifestMismatch)),
        "persisted controlled profile must reject ordinary fallback"
    );
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
    assert_eq!(
        target_handle
            .local_role(slot, deadline())
            .await
            .unwrap()
            .role,
        LocalSlotRole::Learner
    );
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
    transfer(&f, slot, leader, 4).await;
    let until = deadline();
    while !target_handle
        .local_role(slot, until)
        .await
        .unwrap()
        .leader_eligible
    {
        assert!(
            tokio::time::Instant::now() < until,
            "new voter must become actual leader"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
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
    let retired = removed.controlled(slot).unwrap();
    let generation = source * 10;
    let usage = retired.storage_usage(generation, deadline()).await.unwrap();
    assert_eq!(
        usage.receipt.stage,
        multiraft_net::ControlledGroupStage::Retired
    );
    assert!(usage.fsm_file_bytes.unwrap() > 0);
    assert!(retired
        .retire_and_cleanup_from_authority(generation + 1, spec.operation, deadline())
        .await
        .is_err());
    let sibling = SlotId(1);
    let sibling_key = key(&f, sibling);
    removed
        .propose(&sibling_key, b"sibling-before-cleanup".to_vec(), deadline())
        .await
        .unwrap();
    let cleaned = retired
        .retire_and_cleanup_from_authority(generation, spec.operation, deadline())
        .await
        .unwrap();
    assert_eq!(cleaned.stage, multiraft_net::ControlledGroupStage::Cleaned);
    assert_eq!(
        retired
            .retire_and_cleanup_from_authority(generation, spec.operation, deadline())
            .await
            .unwrap(),
        cleaned
    );
    removed
        .propose(&sibling_key, b"sibling-after-cleanup".to_vec(), deadline())
        .await
        .unwrap();
    let usage = retired.storage_usage(generation, deadline()).await.unwrap();
    assert_eq!(usage.retained_log_value_bytes, 0);
    assert_eq!(usage.fsm_file_bytes, Some(0));
    let stopped = f.owners.remove((source - 1) as usize);
    assert!(stopped.shutdown(deadline()).await.complete);
    let reopened = f
        .start(source, SlotStartupDisposition::RecoverExisting, 3)
        .await
        .unwrap();
    assert_eq!(
        reopened
            .handle()
            .local_role(slot, deadline())
            .await
            .unwrap()
            .role,
        LocalSlotRole::Retired
    );
    assert!(
        reopened
            .handle()
            .local_group_status(slot, deadline())
            .await
            .is_err(),
        "excluded cold source must not construct old Core"
    );
    reopened
        .handle()
        .propose(&k, b"cold-retired-source".to_vec(), deadline())
        .await
        .unwrap();
    f.owners.insert((source - 1) as usize, reopened);
    let payload = f
        .handle(source)
        .read_linearizable(&sibling_key, b"", deadline())
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Vec<Vec<u8>>>(&payload).unwrap(),
        vec![
            b"sibling-before-cleanup".to_vec(),
            b"sibling-after-cleanup".to_vec()
        ]
    );
    let fresh_facade = f.handle(source).controlled(slot).unwrap();
    let next = ReplacementSpec {
        operation: OperationId {
            sequence: 2,
            nonce: [10; 16],
        },
        expected_start: target.membership(deadline()).await.unwrap(),
        target_voters: [1, 2, 3].into(),
        target_generation: generation + 1,
    };
    let rejoined = f
        .handle(source)
        .reserve_controlled_target(slot, next, vec![99], deadline())
        .await
        .unwrap();
    assert!(fresh_facade
        .retire_and_cleanup_from_authority(generation, spec.operation, deadline())
        .await
        .is_err());
    assert!(fresh_facade
        .storage_usage(generation, deadline())
        .await
        .is_err());
    assert_eq!(
        rejoined.receipt(deadline()).await.unwrap().generation,
        generation + 1
    );
    assert_eq!(
        rejoined.receipt(deadline()).await.unwrap().stage,
        multiraft_net::ControlledGroupStage::Reserved
    );

    let receipt = target.receipt(deadline()).await.unwrap();
    let stopped_target = f.owners.remove(3);
    assert!(stopped_target.shutdown(deadline()).await.complete);
    let mut resume = f.startup(4, SlotStartupDisposition::Candidate, 4);
    resume.replicas = vec![ControlledSlotReplica {
        slot,
        generation: 40,
        identity: receipt.identity,
        request_id: [44; 16],
    }];
    let fsm_root = f.root.join("node-4");
    let path = fsm_root.clone();
    let resumed = SlotNodeOwner::start_controlled(
        SlotNodeConfig::new(f.layout.clone(), 4, &fsm_root),
        resume,
        move |ctx: multiraft_net::FsmFactoryContext| {
            controlled_support::disk::DiskImage::open(&path, ctx.group_id()).map_err(Into::into)
        },
        deadline(),
    )
    .await
    .unwrap();
    let resumed_role = resumed.handle().local_role(slot, deadline()).await.unwrap();
    assert_eq!(resumed_role.role, LocalSlotRole::Voter);
    assert_eq!(resumed_role.generation, Some(40));
    assert_eq!(
        resumed
            .handle()
            .local_role(SlotId(1), deadline())
            .await
            .unwrap()
            .role,
        LocalSlotRole::Missing
    );
    let restored = resumed
        .handle()
        .read_applied(slot, b"", deadline())
        .await
        .unwrap()
        .value;
    assert_eq!(
        serde_json::from_slice::<Vec<Vec<u8>>>(&restored).unwrap(),
        vec![
            b"pending-opaque-body".to_vec(),
            b"through-removed-source".to_vec(),
            b"cold-retired-source".to_vec()
        ]
    );
    f.owners.push(resumed);
    assert_eq!(removed.layout().digest(), f.layout.digest());
    assert!(!target_handle.status().accepting);
    f.stop().await;
    assert!(retained.dispatch(vec![], deadline()).await.is_err());
}

async fn transfer(f: &Fixture, slot: SlotId, source: u64, target: u64) {
    use multiraft_net::{
        ControlContext, ControlInvocationId, GroupControlPreconditions, GroupControlRequestResult,
        TargetQualification,
    };
    let context = ControlContext::new(ControlInvocationId([7; 16]), deadline());
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
        assert!(
            tokio::time::Instant::now() < context.deadline,
            "target requires fresh native acknowledgement"
        );
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    };
    let outcome = f
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
            outcome.result,
            GroupControlRequestResult::TriggerQueued { .. }
        ),
        "{outcome:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn losing_every_local_voter_keeps_the_fixed_slot_router_and_listeners_alive() {
    let mut f = Fixture::fresh().await;
    let candidate = f
        .start(4, SlotStartupDisposition::Candidate, 1)
        .await
        .unwrap();
    f.owners.push(candidate);
    let source = f.handle(1);
    for number in 0..12 {
        let slot = SlotId(number);
        let group = f.layout.group_for_slot(slot).unwrap();
        let mut leader = leader(&f, slot).await;
        if leader == 1 {
            transfer(&f, slot, 1, 2).await;
            let until = deadline();
            while !f
                .handle(2)
                .local_role(slot, until)
                .await
                .unwrap()
                .leader_eligible
            {
                assert!(tokio::time::Instant::now() < until);
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            leader = 2;
        }
        let control = f.handle(leader).controlled(slot).unwrap();
        let membership = control.membership(deadline()).await.unwrap();
        f.authority.0[&group]
            .bootstrap_membership(membership.clone())
            .unwrap();
        let spec = ReplacementSpec {
            operation: OperationId {
                sequence: u64::from(number) + 1,
                nonce: [number as u8 + 1; 16],
            },
            expected_start: membership,
            target_voters: [2, 3, 4].into(),
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
                consumption_id: receipt.identity,
            })
            .unwrap();
        let endpoint = target
            .target_endpoint(spec.clone(), deadline())
            .await
            .unwrap();
        let port = SourceTargetPort::new(
            endpoint.identity().clone(),
            spec.clone(),
            Arc::new(Transport { endpoint }),
        )
        .unwrap();
        let operation = control
            .begin(spec.clone(), 1, port.clone(), deadline())
            .await
            .unwrap();
        let admission = match f
            .authority
            .admit(StartupRequest {
                group_id: group,
                node_id: 4,
                generation: 40,
                request_id: [number as u8 + 50; 16],
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
        operation
            .add_learner(port.clone(), deadline())
            .await
            .unwrap();
        operation
            .enter_joint(port.clone(), deadline())
            .await
            .unwrap();
        operation.finalize_uniform(deadline()).await.unwrap();
        operation.complete(port, deadline()).await.unwrap();
        source
            .controlled(slot)
            .unwrap()
            .retire_from_authority(spec.operation, deadline())
            .await
            .unwrap();
    }
    assert!(source
        .local_voter_slots(deadline())
        .await
        .unwrap()
        .is_empty());
    assert!(
        source.status().accepting,
        "losing local voter ownership must not close the node"
    );
    assert!(source.local_transport_addresses().is_ok());
    for number in 0..12 {
        let slot = SlotId(number);
        let k = key(&f, slot);
        assert_eq!(
            source.local_role(slot, deadline()).await.unwrap().role,
            LocalSlotRole::Retired
        );
        source
            .propose(&k, b"router-without-voters".to_vec(), deadline())
            .await
            .unwrap();
        let payload = source.read_linearizable(&k, b"", deadline()).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<Vec<Vec<u8>>>(&payload).unwrap(),
            vec![b"router-without-voters".to_vec()]
        );
    }
    assert_eq!(
        f.handle(4)
            .local_voter_slots(deadline())
            .await
            .unwrap()
            .len(),
        12
    );
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_receipt_without_its_actual_genesis_basis_cannot_recover_or_initialize() {
    let mut f = Fixture::fresh().await;
    f.stop_owners().await;
    let anchor = f
        .root
        .join("node-1/controlled-bootstrap/group-100.anchor.json");
    let bytes = std::fs::read(&anchor).unwrap();
    std::fs::remove_file(&anchor).unwrap();
    let rejected = f.start(1, SlotStartupDisposition::RecoverExisting, 2).await;
    assert!(rejected.is_err(), "missing native basis must fail closed");
    assert!(
        !anchor.exists(),
        "recovery must not synthesize an anchor or initialize"
    );
    std::fs::write(&anchor, bytes).unwrap();
    let recovered = f
        .start(1, SlotStartupDisposition::RecoverExisting, 3)
        .await
        .unwrap();
    recovered
        .handle()
        .controlled(SlotId(0))
        .unwrap()
        .genesis_basis(deadline())
        .await
        .unwrap();
    f.owners.push(recovered);
    f.stop().await;
}
