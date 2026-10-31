//! Native controlled replacement trajectories over real RocksDB, disk FSM and gRPC.
#[path = "controlled_protocol_support/admission_refusals.rs"]
mod admission_refusals;
#[path = "controlled_protocol_support/application_barrier.rs"]
mod application_barrier;
#[path = "controlled_protocol_support/binding_gate.rs"]
mod binding_gate;
#[path = "controlled_protocol_support/cancellation.rs"]
mod cancellation;
#[path = "startup_authority_support/catalog.rs"]
mod catalog;
#[path = "controlled_protocol_support/cluster.rs"]
mod cluster;
#[path = "controlled_protocol_support/crash.rs"]
mod crash;
#[path = "controlled_protocol_support/ingress.rs"]
mod ingress;
#[path = "controlled_protocol_support/lifecycle.rs"]
mod lifecycle;
#[path = "controlled_protocol_support/process.rs"]
mod process;
#[path = "controlled_protocol_support/recovery.rs"]
mod recovery;
#[path = "controlled_protocol_support/seed_publication.rs"]
mod seed_publication;
#[path = "controlled_protocol_support/session.rs"]
mod session;
#[path = "controlled_protocol_support/terminal_gate.rs"]
mod terminal_gate;

use authority::StartupAuthorityResult;
use cluster::{deadline, GROUP};
use multiraft_net::controlled_startup as authority;
use session::Session;

fn admitted<T: std::fmt::Debug>(outcome: StartupAuthorityResult<T>) -> T {
    match outcome {
        StartupAuthorityResult::Applied(value) => value,
        other => panic!("expected durable startup admission: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn departed_begin_waiter_keeps_actual_native_work_owned_until_completion() {
    cancellation::run(false).await;
    cancellation::run(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pending_opaque_application_blocks_joint_and_caller_timeout_retains_owned_work() {
    application_barrier::run().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unseeded_fresh_target_cannot_grant_delayed_old_native_vote_over_real_grpc() {
    ingress::unseeded_target_rejects_old_vote().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_leader_handoff_cleans_only_its_group_and_concurrent_rejoin_opens_once() {
    lifecycle::run().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_source_regains_leadership_in_new_term_and_rejects_old_sealed_token() {
    recovery::regained_leader_rejects_old_incarnation().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preadmitted_delayed_source_stays_unknown_and_cannot_recover_old_commit_quorum() {
    recovery::delayed_source_cannot_restore_old_commit_quorum().await;
}

#[test]
fn committed_joint_survives_sigkill_then_same_operation_continues() {
    let root = cluster::scratch("joint-sigkill-");
    let initial = process::Process::start(&root, "joint", false);
    initial.wait_phase("joint");
    initial.kill();
    process::Process::start(&root, "joint", true).finish();
}

#[test]
fn target_opening_sigkill_requires_fresh_admission_and_duplicate_never_constructs_twice() {
    let root = cluster::scratch("opening-sigkill-");
    let initial = process::Process::start(&root, "opening", false);
    initial.wait_phase("opening");
    initial.kill();
    process::Process::start(&root, "opening", true).finish();
}

#[test]
fn uniform_without_terminal_survives_sigkill_without_fabricating_completion() {
    let root = cluster::scratch("uniform-sigkill-");
    let initial = process::Process::start(&root, "uniform", false);
    initial.wait_phase("uniform");
    initial.kill();
    process::Process::start(&root, "uniform", true).finish();
}

#[test]
fn terminal_survives_sigkill_checkpoint_and_log_purge_then_all_current_voters_cold_start() {
    let root = cluster::scratch("terminal-sigkill-");
    let initial = process::Process::start(&root, "terminal", false);
    initial.wait_phase("terminal");
    initial.kill();
    process::Process::start(&root, "terminal", true).finish();
}

#[test]
fn native_complete_before_catalog_handoff_sigkill_recovers_sealed_proof_and_cleans_source() {
    let root = cluster::scratch("terminal-before-handoff-sigkill-");
    let initial = process::Process::start(&root, "terminal_before_handoff", false);
    initial.wait_phase("terminal_before_handoff");
    initial.kill();
    process::Process::start(&root, "terminal_before_handoff", true).finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "self-spawned real native cluster child; executed by crash trajectory parents"]
async fn controlled_protocol_child() {
    let root = std::path::PathBuf::from(
        std::env::var_os("CONTROLLED_PROTOCOL_ROOT").expect("child evidence root"),
    );
    let phase = std::env::var("CONTROLLED_PROTOCOL_SCENARIO").expect("child protocol phase");
    let resume = std::env::var("CONTROLLED_PROTOCOL_RESUME").expect("child boot mode") == "1";
    if phase == "seed_install" {
        seed_publication::child(&root, resume).await;
    } else {
        crash::run(&root, &phase, resume).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controlled_bootstrap_replicates_opaque_application_bytes_through_real_grpc() {
    let root = cluster::scratch("bootstrap-");
    let session = Session::fresh(&root).await;
    let leader = session.leader().await;
    // These bytes resemble a control envelope but remain opaque application data.
    let opaque = br#"{"control":{"Begin":{"executor_epoch":999}},"data":[0,255]}"#.to_vec();
    session.nodes[leader]
        .propose(GROUP, opaque.clone())
        .await
        .expect("commit opaque application bytes");
    for node in session.nodes.iter().take(3) {
        cluster::wait_payloads(node, std::slice::from_ref(&opaque)).await;
    }
    session.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn learner_installs_typed_ledger_snapshot_and_tail_before_exact_terminal() {
    let root = cluster::scratch("snapshot-tail-terminal-");
    let mut session = Session::fresh(&root).await;
    let initial = b"opaque-before-native-begin".to_vec();
    let first_leader = session.leader().await;
    session.nodes[first_leader]
        .propose(GROUP, initial.clone())
        .await
        .expect("commit snapshot application prefix");
    let (leader, specification, old_executor) = session.prepare(false).await;
    cluster::wait_payloads(&session.nodes[3], std::slice::from_ref(&initial)).await;
    let restored = session
        .group(3)
        .local_ledger(deadline())
        .await
        .expect("read target durable-applied ledger");
    let active = restored
        .current
        .expect("typed v2 operation survived actual snapshot install");
    assert_eq!(active.specification, specification);
    assert_eq!(active.executor_epoch, 1);
    assert!(
        active.terminal.is_none(),
        "snapshot does not fabricate completion"
    );
    let executor = session
        .group(leader)
        .claim(specification.operation, 2, deadline())
        .await
        .expect("claim active operation with higher executor epoch");
    assert!(
        session
            .group(leader)
            .add_learner(&old_executor, session.group(3), deadline())
            .await
            .is_err(),
        "late learner request from old epoch rejected"
    );
    assert!(
        session
            .group(leader)
            .enter_joint(&old_executor, session.group(3), deadline())
            .await
            .is_err(),
        "late joint request from old epoch rejected"
    );
    let tail = br#"{"control":{"Complete":{"executor_epoch":999}},"opaque":[255]}"#.to_vec();
    session.nodes[leader]
        .propose(GROUP, tail.clone())
        .await
        .expect("commit opaque snapshot tail");
    cluster::wait_payloads(&session.nodes[3], &[initial, tail]).await;
    session
        .group(leader)
        .enter_joint(&executor, session.group(3), deadline())
        .await
        .expect("commit guarded joint configuration");
    let joint = session
        .group(leader)
        .membership(deadline())
        .await
        .expect("read committed joint");
    assert_eq!(
        joint.membership().get_joint_config().len(),
        2,
        "joint boundary must exclude automatic uniform completion"
    );
    assert!(session
        .group(leader)
        .ledger(deadline())
        .await
        .expect("native joint ledger")
        .current
        .expect("same active operation")
        .terminal
        .is_none());
    session
        .group(leader)
        .finalize_uniform(&executor, deadline())
        .await
        .expect("commit final uniform independently");
    let membership = session
        .group(leader)
        .membership(deadline())
        .await
        .expect("read exact committed final configuration");
    assert_eq!(
        membership.membership().get_joint_config(),
        &vec![specification.target_voters.clone()]
    );
    assert_eq!(
        membership.membership().learner_ids().count(),
        0,
        "source is fully excluded"
    );
    let terminal = session
        .group(leader)
        .complete(&executor, session.group(3), deadline())
        .await
        .expect("complete only after actual target application");
    assert_eq!(terminal.proof().configuration, membership);
    assert_eq!(terminal.proof().target_open.target_node, 4);
    assert_eq!(terminal.proof().target_open.generation, 40);
    assert!(
        terminal.proof().target_open.applied.index
            >= membership
                .log_id()
                .expect("final membership log identity")
                .index
    );
    assert!(
        terminal.proof().barrier.index
            > membership
                .log_id()
                .expect("final membership log identity")
                .index,
        "Complete has its own durable application barrier"
    );
    assert!(
        session
            .group(leader)
            .claim(specification.operation, 2, deadline())
            .await
            .is_err(),
        "terminal cannot reactivate with a higher executor epoch"
    );
    session.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn installed_checkpoint_stays_quarantined_until_seed_anchor_and_open_are_durable() {
    let root = cluster::scratch("seed-publication-");
    seed_publication::initial(&root, false).await;
}

#[test]
fn installed_checkpoint_before_seed_anchor_sigkill_resumes_same_operation_with_fresh_admission() {
    let root = cluster::scratch("seed-install-sigkill-");
    let initial = process::Process::start(&root, "seed_install", false);
    initial.wait_phase("seed_install");
    initial.kill();
    process::Process::start(&root, "seed_install", true).finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn initialized_catalog_cannot_bootstrap_empty_replacement_storage_for_original_generation() {
    admission_refusals::initialized_catalog_denies_empty_bootstrap().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retained_bootstrap_anchor_cannot_publish_missing_compacted_application_storage() {
    admission_refusals::missing_compacted_storage_denies_recovery().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_noncanonical_disk_state_cannot_claim_a_misleading_canonical_current_file() {
    admission_refusals::noncanonical_storage_is_not_owned().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wrong_transport_domain_cannot_install_seed_or_cold_cleanup_retired_source_files() {
    admission_refusals::wrong_transport_domain_preserves_seed_and_retired_storage().await;
}
