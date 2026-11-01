//! A replaced source can retain a real old joint Core until explicit retirement.
mod controlled_support;
use controlled_support::{deadline, disk::DiskImage, Fixture};
use multiraft_core::{OperationId, ReplacementSpec};
use multiraft_net::{
    controlled_startup::{
        self as authority, StartupAuthority, StartupAuthorityResult, StartupRequest,
        StartupTargetGeneration,
    },
    portable::{
        NativeTargetOutcome, NativeTargetTransport, NativeTargetTransportError, SourceTargetPort,
    },
};
use multiraft_slot::*;
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;

struct TargetTransport(SlotTargetEndpoint<DiskImage>);
impl NativeTargetTransport for TargetTransport {
    fn peer(&self) -> (u64, Option<[u8; 32]>) {
        let identity = self.0.identity();
        (identity.node, identity.domain)
    }
    fn exchange(
        &self,
        request: Vec<u8>,
    ) -> authority::StartupAuthorityFuture<'_, Result<Vec<u8>, NativeTargetTransportError>> {
        Box::pin(async move {
            self.0
                .dispatch(request, deadline())
                .await
                .map_err(|error| NativeTargetTransportError {
                    outcome: NativeTargetOutcome::Unknown,
                    message: error.to_string(),
                })
        })
    }
}

async fn original_leader(fixture: &Fixture, slot: SlotId) -> u64 {
    let limit = deadline();
    loop {
        for node in 1..=3 {
            if fixture
                .handle(node)
                .local_role(slot, limit)
                .await
                .unwrap()
                .leader_eligible
            {
                return node;
            }
        }
        assert!(
            Instant::now() < limit,
            "original RF3 leader must be elected"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn unretired_stale_joint_source_routes_via_current_voters_without_duplicate_effects() {
    let mut fixture = Fixture::fresh().await;
    let slot = SlotId(0);
    let group = fixture.layout.group_for_slot(slot).unwrap();
    let key = (0..10000)
        .map(|index| format!("route-{index}").into_bytes())
        .find(|key| fixture.layout.slot_for_key(key).unwrap() == slot)
        .unwrap();
    let candidate = fixture
        .start(4, SlotStartupDisposition::Candidate, 1)
        .await
        .unwrap();
    fixture.owners.push(candidate);
    let leader = original_leader(&fixture, slot).await;
    let source = if leader == 1 { 3 } else { 1 };
    let source_handle = fixture.handle(source);
    fixture
        .handle(leader)
        .propose(&key, b"before-replacement".to_vec(), deadline())
        .await
        .unwrap();
    let control = fixture.handle(leader).controlled(slot).unwrap();
    let membership = control.membership(deadline()).await.unwrap();
    fixture.authority.0[&group]
        .bootstrap_membership(membership.clone())
        .unwrap();
    let specification = ReplacementSpec {
        operation: OperationId {
            sequence: 1,
            nonce: [27; 16],
        },
        expected_start: membership,
        target_voters: [1, 2, 3, 4]
            .into_iter()
            .filter(|node| *node != source)
            .collect(),
        target_generation: 40,
    };
    let target = fixture
        .handle(4)
        .reserve_controlled_target(slot, specification.clone(), vec![4], deadline())
        .await
        .unwrap();
    let receipt = target.receipt(deadline()).await.unwrap();
    fixture.authority.0[&group]
        .consume_target(StartupTargetGeneration {
            node_id: 4,
            generation: 40,
            consumption_id: receipt.identity,
        })
        .unwrap();
    let endpoint = target
        .target_endpoint(specification.clone(), deadline())
        .await
        .unwrap();
    let port = SourceTargetPort::new(
        endpoint.identity().clone(),
        specification.clone(),
        Arc::new(TargetTransport(endpoint)),
    )
    .unwrap();
    let operation = control
        .begin(specification.clone(), 1, port.clone(), deadline())
        .await
        .unwrap();
    let admission = match fixture
        .authority
        .admit(StartupRequest {
            group_id: group,
            node_id: 4,
            generation: 40,
            request_id: [41; 16],
        })
        .await
        .unwrap()
    {
        StartupAuthorityResult::Applied(admission) => admission,
        other => panic!("target admission: {other:?}"),
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
    let limit = deadline();
    loop {
        let role = source_handle.local_role(slot, limit).await.unwrap();
        if role
            .membership
            .is_some_and(|sample| sample.committed_membership.voter_configs.len() == 2)
        {
            break;
        }
        assert!(
            Instant::now() < limit,
            "removed source must actually apply the committed joint membership"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    operation.finalize_uniform(deadline()).await.unwrap();
    operation.complete(port, deadline()).await.unwrap();
    let limit = deadline();
    loop {
        let role = source_handle.local_role(slot, limit).await.unwrap();
        if role.role == LocalSlotRole::Voter
            && role.membership.as_ref().is_some_and(|sample| {
                sample.committed_membership.voter_configs.len() == 2 && sample.leader_hint.is_none()
            })
        {
            eprintln!("actual stale source {source}: {:?}", role.membership);
            break;
        }
        assert!(
            Instant::now() < limit,
            "test requires an actual unretired joint source with no local leader: {role:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let current = fixture
        .handle(leader)
        .local_authoritative_membership(slot, deadline())
        .await
        .unwrap();
    assert_eq!(
        current.membership.membership().get_joint_config(),
        std::slice::from_ref(&specification.target_voters)
    );
    let hint = source_handle
        .leader_hint(slot, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(
        hint,
        Some(leader),
        "old joint local role must not exhaust the routing deadline"
    );
    let value = source_handle
        .read_linearizable(&key, b"", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Vec<Vec<u8>>>(&value).unwrap(),
        vec![b"before-replacement".to_vec()]
    );
    source_handle
        .propose(
            &key,
            b"one-forwarded-write".to_vec(),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    let value = fixture
        .handle(leader)
        .read_linearizable(&key, b"", deadline())
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Vec<Vec<u8>>>(&value).unwrap(),
        vec![
            b"before-replacement".to_vec(),
            b"one-forwarded-write".to_vec()
        ]
    );
    assert_eq!(
        source_handle
            .local_role(slot, deadline())
            .await
            .unwrap()
            .role,
        LocalSlotRole::Voter,
        "routing discovery must not falsify the source's actual old local membership"
    );
    fixture.stop().await;
}
