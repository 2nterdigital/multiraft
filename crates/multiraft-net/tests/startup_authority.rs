#[path = "../src/controlled_startup.rs"]
mod authority;
#[path = "startup_authority_support/catalog.rs"]
mod catalog;

use authority::*;
use catalog::FileAuthority;
use multiraft_core::{typ::StoredMembership, OperationId, OperationRecord, ReplacementSpec};
use openraft::{testing::log_id, BasicNode, Membership};
use std::{collections::BTreeMap, path::Path};

fn membership(index: u64, voters: &[u64]) -> StoredMembership {
    StoredMembership::new(
        Some(log_id::<multiraft_core::TypeConfig>(1, 1, index)),
        Membership::new(
            vec![voters.iter().copied().collect()],
            voters
                .iter()
                .map(|&id| (id, BasicNode::default()))
                .collect::<BTreeMap<_, _>>(),
        )
        .expect("valid fixture membership"),
    )
}
fn provision(path: &Path) -> FileAuthority {
    let authority = FileAuthority::provision(
        path,
        StartupCatalog {
            group_id: 7,
            revision: 0,
            membership: membership(1, &[1, 2, 3]),
            authorized: [(1, 10), (2, 20), (3, 30)].into(),
            active_operation: None,
            admissions: vec![],
            completed: vec![],
        },
    );
    authority.consume_target(binding().target).unwrap();
    authority
}
fn request(node_id: u64, generation: u64, nonce: u8) -> StartupRequest {
    StartupRequest {
        group_id: 7,
        node_id,
        generation,
        request_id: [nonce; 16],
    }
}
fn binding() -> StartupBinding {
    StartupBinding {
        group_id: 7,
        specification: ReplacementSpec {
            operation: OperationId {
                sequence: 1,
                nonce: [9; 16],
            },
            expected_start: membership(1, &[1, 2, 3]),
            target_voters: [2, 3, 4].into(),
            target_generation: 40,
        },
        source: 1,
        target: StartupTargetGeneration {
            node_id: 4,
            generation: 40,
            consumption_id: b"durably-consumed-generation-40".to_vec(),
        },
    }
}
fn terminal(binding: &StartupBinding) -> StartupTerminalProof {
    StartupTerminalProof {
        group_id: 7,
        native_record: OperationRecord {
            specification: binding.specification.clone(),
            executor_epoch: 2,
            terminal: Some(multiraft_core::TerminalProof {
                configuration: membership(8, &[2, 3, 4]),
                barrier: log_id::<multiraft_core::TypeConfig>(1, 1, 9),
                target_open: multiraft_core::TargetOpenReceipt {
                    operation: binding.specification.operation,
                    target_node: 4,
                    generation: 40,
                    applied: log_id::<multiraft_core::TypeConfig>(1, 1, 8),
                    receipt: binding.target.consumption_id.clone(),
                },
            }),
        },
        committed_membership: membership(8, &[2, 3, 4]),
        applied_membership: membership(8, &[2, 3, 4]),
        applied_log: log_id::<multiraft_core::TypeConfig>(1, 1, 9),
        target_opened: binding.target.clone(),
    }
}
fn applied<T: std::fmt::Debug>(result: StartupAuthorityResult<T>) -> T {
    match result {
        StartupAuthorityResult::Applied(value) => value,
        other => panic!("expected durable applied outcome, got {other:?}"),
    }
}
fn denied<T: std::fmt::Debug>(result: StartupAuthorityResult<T>) {
    assert!(
        matches!(
            result,
            StartupAuthorityResult::Rejected(StartupAuthorityRejection::UnauthorizedGeneration)
        ),
        "expected unauthorized generation, got {result:?}"
    );
}
fn directory() -> tempfile::TempDir {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp");
    std::fs::create_dir_all(&root).unwrap();
    tempfile::tempdir_in(root).unwrap()
}

#[tokio::test]
async fn binding_survives_reopen_and_serializes_cold_start_admission() {
    let directory = directory();
    let path = directory.path().join("catalog.json");
    let authority: Box<dyn StartupAuthority> = Box::new(provision(&path));
    let old = applied(authority.admit(request(1, 10, 1)).await.unwrap());
    applied(authority.bind_active(binding()).await.unwrap());
    drop(authority);

    let reopened = FileAuthority::open(&path);
    denied(reopened.admit(request(1, 10, 2)).await.unwrap());
    denied(reopened.admit(request(4, 41, 3)).await.unwrap());
    for (node, generation, nonce) in [(2, 20, 4), (3, 30, 5), (4, 40, 6)] {
        applied(
            reopened
                .admit(request(node, generation, nonce))
                .await
                .unwrap(),
        );
    }
    let catalog = reopened.read(7).await.unwrap();
    assert_eq!(catalog.active_operation, Some(binding()));
    assert!(
        catalog.admissions.contains(&old),
        "pre-binding source remains potentially live"
    );
    assert_eq!(
        applied(reopened.admit(request(1, 10, 1)).await.unwrap()),
        old,
        "readback recovers admission, never a new physical start"
    );
}

#[tokio::test]
async fn uncertain_terminal_handoff_reopens_with_atomic_retirement_and_cold_start_permissions() {
    let directory = directory();
    let path = directory.path().join("catalog.json");
    let authority = provision(&path);
    let delayed = applied(authority.admit(request(1, 10, 1)).await.unwrap());
    let binding = binding();
    applied(authority.bind_active(binding.clone()).await.unwrap());
    let proof = terminal(&binding);
    authority.lose_next_handoff_reply();
    assert_eq!(
        authority.terminal_handoff(proof.clone()).await.unwrap(),
        StartupAuthorityResult::Unknown
    );
    drop(authority);

    let reopened = FileAuthority::open(&path);
    let readback = reopened.read(7).await.unwrap();
    assert_eq!(readback.active_operation, None);
    assert_eq!(readback.authorized, [(2, 20), (3, 30), (4, 40)].into());
    assert_eq!(readback.completed, vec![proof.clone()]);
    assert!(
        readback.admissions.contains(&delayed),
        "late physical source open is still tracked; native config provides exclusion"
    );
    denied(reopened.admit(request(1, 10, 7)).await.unwrap());
    for (node, generation, nonce) in [(2, 20, 8), (3, 30, 9), (4, 40, 10)] {
        applied(
            reopened
                .admit(request(node, generation, nonce))
                .await
                .unwrap(),
        );
    }
    assert_eq!(
        applied(reopened.terminal_handoff(proof.clone()).await.unwrap()),
        proof
    );
    assert_eq!(
        reopened.bind_active(binding).await.unwrap(),
        StartupAuthorityResult::Rejected(StartupAuthorityRejection::StaleOperation)
    );
}

#[tokio::test]
async fn request_identities_and_denials_remain_immutable_after_restart() {
    let directory = directory();
    let path = directory.path().join("catalog.json");
    let authority = provision(&path);
    denied(authority.admit(request(4, 40, 1)).await.unwrap());
    applied(authority.bind_active(binding()).await.unwrap());
    drop(authority);
    let reopened = FileAuthority::open(&path);
    denied(reopened.admit(request(4, 40, 1)).await.unwrap());
    assert_eq!(
        reopened.admit(request(2, 20, 1)).await.unwrap(),
        StartupAuthorityResult::Rejected(StartupAuthorityRejection::IdentityConflict)
    );
    applied(reopened.admit(request(4, 40, 2)).await.unwrap());
    let mut changed = binding();
    changed.target.consumption_id = b"different-consumption".to_vec();
    assert_eq!(
        reopened.bind_active(changed).await.unwrap(),
        StartupAuthorityResult::Rejected(StartupAuthorityRejection::ActiveOperation)
    );
}

#[tokio::test]
async fn effective_only_or_wrong_operation_or_unconsumed_generation_cannot_finalize() {
    let directory = directory();
    let path = directory.path().join("catalog.json");
    let authority = provision(&path);
    let binding = binding();
    applied(authority.bind_active(binding.clone()).await.unwrap());
    let proof = terminal(&binding);
    let mut unapplied = proof.clone();
    unapplied.applied_membership = membership(1, &[1, 2, 3]);
    let mut wrong_operation = proof.clone();
    wrong_operation.native_record.specification.operation.nonce = [8; 16];
    let mut wrong_target = proof.clone();
    wrong_target.target_opened.generation = 41;
    let mut no_native_terminal = proof.clone();
    no_native_terminal.native_record.terminal = None;
    let mut wrong_log = proof;
    wrong_log.applied_log = log_id::<multiraft_core::TypeConfig>(2, 2, 8);
    for invalid in [
        unapplied,
        wrong_operation,
        wrong_target,
        wrong_log,
        no_native_terminal,
    ] {
        assert_eq!(
            authority.terminal_handoff(invalid).await.unwrap(),
            StartupAuthorityResult::Rejected(StartupAuthorityRejection::InvalidProof)
        );
    }
    let readback = FileAuthority::open(&path).read(7).await.unwrap();
    assert_eq!(readback.active_operation, Some(binding));
    assert!(readback.completed.is_empty());
    assert_eq!(readback.authorized, [(1, 10), (2, 20), (3, 30)].into());
}

#[test]
fn independent_handles_linearize_source_start_against_binding() {
    for iteration in 0..12 {
        let directory = directory();
        let path = directory.path().join("catalog.json");
        drop(provision(&path));
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let source_path = path.clone();
        let source_barrier = barrier.clone();
        let source = std::thread::spawn(move || {
            source_barrier.wait();
            futures::executor::block_on(FileAuthority::open(&source_path).admit(request(1, 10, 1)))
                .unwrap()
        });
        barrier.wait();
        applied(
            futures::executor::block_on(FileAuthority::open(&path).bind_active(binding())).unwrap(),
        );
        let result = source.join().unwrap();
        let catalog = futures::executor::block_on(FileAuthority::open(&path).read(7)).unwrap();
        match result {
            StartupAuthorityResult::Applied(admission) => {
                assert_eq!(admission.active_operation, None, "iteration {iteration}");
                assert!(catalog.admissions.contains(&admission));
            }
            StartupAuthorityResult::Rejected(StartupAuthorityRejection::UnauthorizedGeneration) => {
                assert!(catalog.admissions.is_empty())
            }
            other => panic!("unexpected raced outcome {other:?}"),
        }
    }
}

#[tokio::test]
async fn caller_claimed_target_consumption_is_not_a_durable_receipt() {
    let directory = directory();
    let path = directory.path().join("catalog.json");
    let authority = provision(&path);
    let mut forged = binding();
    forged.target.consumption_id = b"not-in-durable-registry".to_vec();
    assert_eq!(
        authority.bind_active(forged).await.unwrap(),
        StartupAuthorityResult::Rejected(StartupAuthorityRejection::IdentityConflict)
    );
    assert!(FileAuthority::open(&path)
        .read(7)
        .await
        .unwrap()
        .active_operation
        .is_none());
}

#[tokio::test]
async fn bootstrap_provisioning_records_native_identity_once_before_binding() {
    let directory = directory();
    let path = directory.path().join("catalog.json");
    let authority = provision(&path);
    let actual = membership(3, &[1, 2, 3]);
    authority.bootstrap_membership(actual.clone()).unwrap();
    drop(authority);
    let reopened = FileAuthority::open(&path);
    assert_eq!(reopened.read(7).await.unwrap().membership, actual);
    let mut binding = binding();
    binding.specification.expected_start = actual.clone();
    applied(reopened.bind_active(binding).await.unwrap());
    assert!(reopened.bootstrap_membership(actual).is_err());
}
