//! Controlled Groups enter through owned handles from their first native startup.
#![allow(dead_code)]
#[path = "startup_authority_support/catalog.rs"]
mod catalog;
#[path = "controlled_protocol_support/disk.rs"]
mod disk;
#[path = "owned_controlled_support/fixture.rs"]
mod owned_controlled_support;
use multiraft_net::controlled_startup as authority;
use owned_controlled_support::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owned_bootstrap_exports_actual_basis_and_retained_handles_close() {
    let fixture = Fixture::fresh().await;
    let mut membership = None;
    for group in &fixture.groups {
        let proof = group
            .genesis_basis(deadline())
            .await
            .expect("actual durable Genesis basis");
        assert_eq!(proof.group_id(), GROUP);
        assert_eq!(proof.generation(), group.generation());
        assert_eq!(
            proof
                .configuration()
                .membership()
                .voter_ids()
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        if let Some(expected) = &membership {
            assert_eq!(proof.configuration(), expected);
        }
        membership = Some(proof.configuration().clone());
    }
    let retained = fixture.groups[0].clone();
    fixture.stop().await;
    assert!(
        retained.receipt(deadline()).await.is_err(),
        "retained handle cannot bypass closed runtime"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn provisional_catalog_recovers_first_source_alone_then_second_without_initialize() {
    use authority::StartupAuthority;
    let fixture = Fixture::fresh().await;
    let expected = fixture.groups[0]
        .genesis_basis(deadline())
        .await
        .unwrap()
        .configuration()
        .clone();
    let Fixture {
        root,
        authority,
        owners,
        groups,
        configs,
    } = fixture;
    drop(groups);
    for owner in owners {
        owner.shutdown(deadline()).await.unwrap();
    }
    assert!(
        authority
            .read(GROUP)
            .await
            .unwrap()
            .membership
            .log_id()
            .is_none(),
        "catalog remains provisional"
    );
    let first = start_owned(configs[0].clone()).await;
    let first_group = recover(&first, authority.clone(), 1, 71)
        .await
        .expect("local recovery without another live source");
    assert_eq!(
        first_group
            .genesis_basis(deadline())
            .await
            .unwrap()
            .configuration(),
        &expected
    );
    let second = start_owned(configs[1].clone()).await;
    let second_group = recover(&second, authority.clone(), 2, 72)
        .await
        .expect("second original recovers without initialize");
    assert_eq!(
        second_group
            .genesis_basis(deadline())
            .await
            .unwrap()
            .configuration(),
        &expected
    );
    assert!(
        authority
            .read(GROUP)
            .await
            .unwrap()
            .membership
            .log_id()
            .is_none(),
        "recovery never finalizes or reconstructs catalog"
    );
    first.shutdown(deadline()).await.unwrap();
    second.shutdown(deadline()).await.unwrap();
    drop(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn basis_rejects_missing_or_forged_anchor_and_full_node_metadata() {
    let fixture = Fixture::fresh().await;
    let anchor = fixture.configs[0]
        .data_dir
        .join("controlled-bootstrap/group-7.anchor.json");
    let original = std::fs::read(&anchor).unwrap();
    std::fs::remove_file(&anchor).unwrap();
    assert!(
        fixture.groups[0].genesis_basis(deadline()).await.is_err(),
        "receipt cannot replace anchor"
    );
    for field in [
        "node",
        "group",
        "generation",
        "identity",
        "fingerprint",
        "members",
    ] {
        let mut changed: serde_json::Value = serde_json::from_slice(&original).unwrap();
        match field {
            "identity" => changed["intent"][field] = serde_json::json!([99]),
            "fingerprint" => changed["intent"][field] = serde_json::json!(vec![9; 32]),
            "members" => changed["intent"][field]["1"]["addr"] = serde_json::json!("forged:1"),
            _ => changed["intent"][field] = serde_json::json!(99),
        }
        std::fs::write(&anchor, serde_json::to_vec(&changed).unwrap()).unwrap();
        assert!(
            fixture.groups[0].genesis_basis(deadline()).await.is_err(),
            "forged {field} must fail actual basis verification"
        );
    }
    let intent_path = fixture.configs[0]
        .data_dir
        .join("controlled-bootstrap/group-7.json");
    let original_intent = std::fs::read(&intent_path).unwrap();
    let mut matched: serde_json::Value = serde_json::from_slice(&original).unwrap();
    matched["intent"]["members"]["1"]["addr"] = serde_json::json!("forged:1");
    std::fs::write(
        &intent_path,
        serde_json::to_vec(&matched["intent"]).unwrap(),
    )
    .unwrap();
    std::fs::write(&anchor, serde_json::to_vec(&matched).unwrap()).unwrap();
    assert!(
        fixture.groups[0].genesis_basis(deadline()).await.is_err(),
        "matching forged intent+anchor cannot override actual native node metadata"
    );
    std::fs::write(&intent_path, original_intent).unwrap();
    let mut stale: serde_json::Value = serde_json::from_slice(&original).unwrap();
    stale["configuration"]["log_id"]["index"] = serde_json::json!(999999);
    std::fs::write(&anchor, serde_json::to_vec(&stale).unwrap()).unwrap();
    assert!(
        fixture.groups[0].genesis_basis(deadline()).await.is_err(),
        "full native configuration identity must match"
    );
    std::fs::write(&anchor, original).unwrap();
    fixture.groups[0]
        .genesis_basis(deadline())
        .await
        .expect("restored genuine basis");
    fixture.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_replacement_streams_seed_and_retires_without_escaping_native_capabilities() {
    use authority::{
        StartupAuthority, StartupAuthorityResult, StartupRequest, StartupTargetGeneration,
    };
    use multiraft_core::{OperationId, ReplacementSpec};
    use multiraft_net::portable::SourceTargetPort;
    use std::sync::Arc;
    let fixture = Fixture::fresh().await;
    let leader = fixture.leader().await;
    let source = if leader == 0 { 3 } else { 1 };
    let membership = fixture.groups[leader].membership(deadline()).await.unwrap();
    fixture
        .authority
        .bootstrap_membership(membership.clone())
        .unwrap();
    let specification = ReplacementSpec {
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
    let target = fixture.owners[3]
        .handle()
        .reserve_controlled_target(
            GROUP,
            specification.clone(),
            vec![4],
            fixture.authority.clone(),
            deadline(),
        )
        .await
        .unwrap();
    let receipt = target.receipt(deadline()).await.unwrap();
    assert!(
        target.storage_usage(deadline()).await.is_err(),
        "unconstructed target usage is unavailable"
    );
    assert!(
        !multiraft_store::DiskFsmDb::group_path(&fixture.configs[3].data_dir, GROUP).exists(),
        "usage must not initialize target storage"
    );
    fixture
        .authority
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
    let retained_endpoint = endpoint.clone();
    let port = SourceTargetPort::new(
        endpoint.identity().clone(),
        specification.clone(),
        Arc::new(LocalTransport { endpoint }),
    )
    .unwrap();
    let executor = fixture.groups[leader]
        .begin(specification.clone(), 1, port.clone(), deadline())
        .await
        .unwrap();
    let admission = match fixture
        .authority
        .admit(StartupRequest {
            group_id: GROUP,
            node_id: 4,
            generation: 40,
            request_id: [40; 16],
        })
        .await
        .unwrap()
    {
        StartupAuthorityResult::Applied(value) => value,
        other => panic!("{other:?}"),
    };
    executor
        .seed_target(port.clone(), admission, deadline())
        .await
        .unwrap();
    fixture.owners[3]
        .handle()
        .local_group_status(GROUP, deadline())
        .await
        .expect("target readiness follows actual native seed completion");
    executor
        .add_learner(port.clone(), deadline())
        .await
        .unwrap();
    assert!(
        fixture.groups[leader]
            .genesis_basis(deadline())
            .await
            .is_err(),
        "later learner configuration cannot export original Genesis capability"
    );
    let newer = fixture.groups[leader]
        .claim(specification.operation, 2, deadline())
        .await
        .unwrap();
    assert!(
        executor
            .enter_joint(port.clone(), deadline())
            .await
            .is_err(),
        "old managed executor remains fenced"
    );
    newer.enter_joint(port.clone(), deadline()).await.unwrap();
    newer.finalize_uniform(deadline()).await.unwrap();
    let proof = newer.complete(port, deadline()).await.unwrap();
    assert_eq!(
        proof
            .configuration
            .membership()
            .voter_ids()
            .collect::<std::collections::BTreeSet<_>>(),
        specification.target_voters
    );
    let source_index = (source - 1) as usize;
    fixture.groups[source_index]
        .retire_from_authority(specification.operation, deadline())
        .await
        .unwrap();
    assert!(
        fixture.owners[source_index]
            .handle()
            .local_group_status(GROUP, deadline())
            .await
            .is_err(),
        "actual retirement removes readiness"
    );
    let retired = &fixture.groups[source_index];
    let before = retired.storage_usage(deadline()).await.unwrap();
    assert_eq!(
        before.receipt.stage,
        multiraft_net::ControlledGroupStage::Retired
    );
    assert!(before.fsm_file_bytes.unwrap() > 0);
    let cleaned = retired
        .retire_and_cleanup_from_authority(specification.operation, deadline())
        .await
        .unwrap();
    assert_eq!(cleaned.generation, retired.generation());
    assert_eq!(cleaned.stage, multiraft_net::ControlledGroupStage::Cleaned);
    let after = retired.storage_usage(deadline()).await.unwrap();
    assert_eq!(after.retained_log_value_bytes, 0);
    assert_eq!(after.fsm_file_bytes, Some(0));
    assert_eq!(after.workspace_file_bytes, Some(0));
    assert_eq!(
        retired
            .retire_and_cleanup_from_authority(specification.operation, deadline())
            .await
            .unwrap(),
        cleaned
    );
    let old_endpoint_spec = ReplacementSpec {
        operation: OperationId {
            sequence: 2,
            nonce: [10; 16],
        },
        expected_start: proof.configuration.clone(),
        target_voters: [1, 2, 3].into(),
        target_generation: retired.generation(),
    };
    let old_endpoint = retired
        .target_endpoint(old_endpoint_spec.clone(), deadline())
        .await
        .unwrap();
    let old_request = multiraft_net::portable::NativeTargetRequest {
        scope: multiraft_net::portable::NativeTargetScope {
            version: multiraft_net::portable::NATIVE_TARGET_VERSION,
            target: old_endpoint.identity().clone(),
            specification: old_endpoint_spec,
            correlation: [55; 16],
        },
        command: multiraft_net::portable::NativeTargetCommand::Observe {
            configuration: None,
            barrier: None,
        },
    };
    let next = ReplacementSpec {
        operation: OperationId {
            sequence: 2,
            nonce: [10; 16],
        },
        expected_start: proof.configuration.clone(),
        target_voters: [1, 2, 3].into(),
        target_generation: retired.generation() + 1,
    };
    let rejoined = fixture.owners[source_index]
        .handle()
        .reserve_controlled_target(
            GROUP,
            next.clone(),
            vec![99],
            fixture.authority.clone(),
            deadline(),
        )
        .await
        .expect("same runtime replaces cleaned generation");
    assert_eq!(rejoined.generation(), cleaned.generation + 1);
    assert!(retired.receipt(deadline()).await.is_err());
    assert!(retired.storage_usage(deadline()).await.is_err());
    assert!(retired
        .retire_and_cleanup_from_authority(specification.operation, deadline())
        .await
        .is_err());
    assert_eq!(
        rejoined.receipt(deadline()).await.unwrap().stage,
        multiraft_net::ControlledGroupStage::Reserved
    );
    fixture
        .authority
        .consume_target(StartupTargetGeneration {
            node_id: source,
            generation: rejoined.generation(),
            consumption_id: rejoined.receipt(deadline()).await.unwrap().identity,
        })
        .unwrap();
    let endpoint = rejoined
        .target_endpoint(next.clone(), deadline())
        .await
        .unwrap();
    let port = SourceTargetPort::new(
        endpoint.identity().clone(),
        next.clone(),
        Arc::new(LocalTransport { endpoint }),
    )
    .unwrap();
    let operation = fixture.groups[leader]
        .begin(next, 1, port.clone(), deadline())
        .await
        .unwrap();
    let admission = match fixture
        .authority
        .admit(StartupRequest {
            group_id: GROUP,
            node_id: source,
            generation: rejoined.generation(),
            request_id: [55; 16],
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
    fixture.owners[leader]
        .handle()
        .propose(GROUP, b"new-generation-payload".to_vec(), deadline())
        .await
        .unwrap();
    let response: multiraft_net::portable::NativeTargetReply = serde_json::from_slice(
        &old_endpoint
            .dispatch(serde_json::to_vec(&old_request).unwrap(), deadline())
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        response.outcome,
        multiraft_net::portable::NativeTargetOutcome::Rejected
    );
    assert!(retired
        .retire_and_cleanup_from_authority(specification.operation, deadline())
        .await
        .is_err());
    let usage = rejoined.storage_usage(deadline()).await.unwrap();
    assert_eq!(
        usage.receipt.stage,
        multiraft_net::ControlledGroupStage::Opened
    );
    assert!(
        usage.fsm_file_bytes.unwrap() > 0,
        "old cleanup leaves new FSM intact"
    );
    let payloads = fixture.owners[leader]
        .handle()
        .read_linearizable(GROUP, deadline(), |fsm| fsm.payloads())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(payloads, vec![b"new-generation-payload".to_vec()]);
    fixture.stop().await;
    assert!(
        retained_endpoint
            .dispatch(vec![], deadline())
            .await
            .is_err(),
        "retained endpoint cannot bypass runtime close"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn canceled_first_open_waiter_still_publishes_actual_owned_completion() {
    let fixture = Fixture::startup(true).await;
    fixture.owners[0]
        .handle()
        .local_group_status(GROUP, deadline())
        .await
        .expect("ready after genuine late completion");
    fixture.groups[0]
        .genesis_basis(deadline())
        .await
        .expect("late completion retains actual basis");
    let retained = fixture.groups[0].clone();
    let Fixture {
        root,
        owners,
        groups,
        configs,
        ..
    } = fixture;
    drop(groups);
    for owner in owners {
        owner.shutdown(deadline()).await.unwrap();
    }
    assert!(retained.genesis_basis(deadline()).await.is_err());
    // Retain the actual data root: recovery reopens the same FSM, log lease and listener.
    let owner = start_owned(configs[0].clone()).await;
    let authority = std::sync::Arc::new(catalog::FileAuthority::open(
        &root.path().join("catalog.json"),
    ));
    recover(&owner, authority, 1, 90)
        .await
        .unwrap()
        .genesis_basis(deadline())
        .await
        .unwrap();
    owner.shutdown(deadline()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn canceled_midchunk_seed_and_retained_endpoint_release_actual_target_storage_on_shutdown() {
    cancellation::seed_shutdown(transport::Gate::FirstChunk).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_finish_reply_publishes_ready_and_retained_endpoint_cannot_pin_native_storage() {
    cancellation::seed_shutdown(transport::Gate::FinishedReply).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejected_open_cannot_poison_a_later_valid_target_generation() {
    use authority::{StartupAuthority, StartupAuthorityResult, StartupRequest};
    use multiraft_net::{ControlledOpen, ControlledOpenMode};
    use std::sync::Arc;
    let fixture = Fixture::fresh().await;
    let admission = match fixture
        .authority
        .admit(StartupRequest {
            group_id: GROUP,
            node_id: 1,
            generation: 10,
            request_id: [99; 16],
        })
        .await
        .unwrap()
    {
        StartupAuthorityResult::Applied(value) => value,
        other => panic!("{other:?}"),
    };
    assert!(fixture.owners[3]
        .handle()
        .open_controlled_group(
            ControlledOpen {
                group_id: GROUP,
                generation: 99,
                identity: vec![99],
                mode: ControlledOpenMode::Recover
            },
            fixture.authority.clone(),
            admission,
            deadline()
        )
        .await
        .is_err());
    fixture
        .prepare_gated(
            transport::Gate::FirstChunk,
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
        )
        .await;
    fixture.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forged_anchor_without_original_native_vote_cannot_mint_a_genesis_basis() {
    let fixture = Fixture::fresh().await;
    let path = fixture.configs[0]
        .data_dir
        .join("controlled-bootstrap/group-7.anchor.json");
    let original = std::fs::read(&path).unwrap();
    let mut anchor: serde_json::Value = serde_json::from_slice(&original).unwrap();
    anchor["vote"] = serde_json::Value::Null;
    std::fs::write(&path, serde_json::to_vec(&anchor).unwrap()).unwrap();
    assert!(
        fixture.groups[0].genesis_basis(deadline()).await.is_err(),
        "a fabricated vote-free anchor cannot export a sealed proof"
    );
    std::fs::write(&path, original).unwrap();
    fixture.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn purged_payload_deletion_with_retained_receipts_anchor_and_vote_cannot_recover() {
    use multiraft_store::DiskFsmDb;
    let fixture = Fixture::fresh().await;
    let leader = fixture.leader().await;
    let handle = fixture.owners[leader].handle();
    handle
        .propose(
            GROUP,
            b"durable-body-must-not-disappear".to_vec(),
            deadline(),
        )
        .await
        .unwrap();
    handle.request_compaction(GROUP, deadline()).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let state = handle
                .local_storage_status(GROUP, deadline())
                .await
                .unwrap();
            if state.purged.is_some()
                && state.progress == multiraft_net::CompactionProgress::CompletedObserved
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    fixture.groups[leader]
        .genesis_basis(deadline())
        .await
        .expect("actual basis remains valid through native purge");
    let Fixture {
        root,
        authority,
        owners,
        groups,
        configs,
    } = fixture;
    drop(groups);
    for owner in owners {
        owner.shutdown(deadline()).await.unwrap();
    }
    let config = configs[leader].clone();
    let live = DiskFsmDb::group_path(&config.data_dir, GROUP);
    let workspace = DiskFsmDb::workspace_path(&live).unwrap();
    std::fs::remove_dir_all(&live).unwrap();
    std::fs::remove_dir_all(&workspace).unwrap();
    let owner = start_owned(config).await;
    assert!(
        recover(&owner, authority.clone(), (leader + 1) as u64, 92)
            .await
            .is_err(),
        "Opened receipt/anchor/native vote cannot reconstruct purged application body"
    );
    assert!(
        owner
            .handle()
            .local_group_status(GROUP, deadline())
            .await
            .is_err(),
        "no partial recovery publication or BootstrapNew fallback"
    );
    let inspected = owner
        .handle()
        .inspect_controlled_group(
            GROUP,
            ((leader + 1) * 10) as u64,
            vec![(leader + 1) as u8],
            authority,
            deadline(),
        )
        .await
        .unwrap();
    assert!(
        inspected.genesis_basis(deadline()).await.is_err(),
        "receipt-only cold inspection cannot mint proof"
    );
    owner.shutdown(deadline()).await.unwrap();
    drop(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_repeated_timeouts_wait_for_real_blocked_seed_install_and_release_fsm() {
    cancellation::blocked_install_repeated_shutdown_timeouts().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[ignore = "self-spawned managed cleanup trajectory"]
async fn managed_cleanup_child() {
    cleanup::child().await;
}
#[test]
fn managed_cleanup_survives_sigkill_after_each_cleanup_boundary() {
    for phase in ["logs", "fsm", "workspace", "snapshots", "legacy"] {
        cleanup::trajectory(phase, "");
    }
}
#[test]
fn admitted_managed_cleanup_survives_caller_timeout_and_cancellation() {
    for mode in ["timeout", "cancel"] {
        cleanup::trajectory("logs", mode);
    }
}
