//! Independent restart paths for persisted joint, uniform, and terminal boundaries.
use super::{authority::StartupAuthority, cluster, process, session::Session};
use cluster::{deadline, GROUP};
use std::path::Path;

pub async fn run(root: &Path, phase: &str, resume: bool) {
    if resume {
        recover(root, phase).await;
    } else {
        initial(root, phase).await;
    }
}

async fn initial(root: &Path, phase: &str) {
    let mut session = Session::fresh(root).await;
    let first = session.leader().await;
    session.nodes[first]
        .propose(GROUP, b"persisted-prefix".to_vec())
        .await
        .expect("commit application prefix");
    let (leader, specification, executor) = session.prepare(false).await;
    session.nodes[leader]
        .propose(GROUP, b"snapshot-tail".to_vec())
        .await
        .expect("commit suffix after typed ledger snapshot");
    cluster::wait_payloads(
        &session.nodes[3],
        &[b"persisted-prefix".to_vec(), b"snapshot-tail".to_vec()],
    )
    .await;
    session
        .group(leader)
        .enter_joint(&executor, session.group(3), deadline())
        .await
        .expect("commit joint before crash");
    if phase == "joint" {
        assert_eq!(
            session
                .group(leader)
                .membership(deadline())
                .await
                .expect("joint membership")
                .membership()
                .get_joint_config()
                .len(),
            2
        );
        process::persisted_phase(phase);
    }
    session
        .group(leader)
        .finalize_uniform(&executor, deadline())
        .await
        .expect("commit uniform before crash");
    assert!(session
        .group(leader)
        .ledger(deadline())
        .await
        .expect("preterminal ledger")
        .current
        .expect("active operation")
        .terminal
        .is_none());
    if phase == "uniform" {
        process::persisted_phase(phase);
    }
    if phase == "terminal_before_handoff" {
        let authority = std::sync::Arc::new(super::terminal_gate::TerminalCrashAuthority(
            session.authority.clone(),
        ));
        let admission = super::admitted(
            session
                .authority
                .admit(session.request(leader, 1))
                .await
                .expect("read consumed source-leader startup receipt"),
        );
        let source = session.nodes[leader]
            .open_controlled_group(
                session.open_request(leader, multiraft_net::ControlledOpenMode::Recover),
                authority,
                admission,
                deadline(),
            )
            .await
            .expect("attach handoff gate at actual public authority seam");
        source
            .complete(&executor, session.group(3), deadline())
            .await
            .expect("child must be killed before handoff can return");
        panic!("terminal handoff crash gate was not reached");
    }
    assert_eq!(phase, "terminal");
    session.authority.lose_next_handoff_reply();
    let terminal = session
        .group(leader)
        .complete(&executor, session.group(3), deadline())
        .await
        .expect("read back actual uncertain terminal handoff");
    for id in &specification.target_voters {
        let index = (*id - 1) as usize;
        cluster::compact(&session.nodes[index]).await;
        assert_eq!(
            session
                .group(index)
                .local_ledger(deadline())
                .await
                .expect("durable terminal ledger after checkpoint and log purge")
                .current
                .expect("terminal operation")
                .terminal
                .as_ref(),
            Some(terminal.proof())
        );
    }
    process::persisted_phase(phase);
}

async fn recover(root: &Path, phase: &str) {
    let mut session = Session::recover(root).await;
    let leader = session.leader().await;
    let catalog = session
        .authority
        .read(GROUP)
        .await
        .expect("reopen independent startup catalog");
    let record = session
        .group(leader)
        .ledger(deadline())
        .await
        .expect("native replayed ledger after actual SIGKILL")
        .current
        .expect("operation retained without old call stack");
    let specification = record.specification.clone();
    let source = *[1, 2, 3]
        .iter()
        .find(|id| !specification.target_voters.contains(id))
        .expect("one excluded source");
    assert!(
        session.groups[(source - 1) as usize].is_none(),
        "fresh source startup denied across active or terminal fence"
    );
    if phase == "terminal" || phase == "terminal_before_handoff" {
        assert!(
            record.terminal.is_some(),
            "terminal survives replay/checkpoint/purge"
        );
        assert_eq!(
            catalog.active_operation.is_some(),
            phase == "terminal_before_handoff",
            "crash boundary distinguishes native Complete from catalog handoff"
        );
        assert!(
            session
                .group(leader)
                .claim(specification.operation, 2, deadline())
                .await
                .is_err(),
            "terminal cannot reactivate after replay"
        );
        let terminal = session
            .group(leader)
            .reconcile_terminal(specification.operation, deadline())
            .await
            .expect(
                "recover sealed terminal without lost executor token and reconcile real handoff",
            );
        assert_eq!(Some(terminal.proof()), record.terminal.as_ref());
        let reconciled = session
            .authority
            .read(GROUP)
            .await
            .expect("read atomic external retirement after crash reconciliation");
        assert!(reconciled.active_operation.is_none());
        assert!(reconciled
            .completed
            .iter()
            .any(|proof| proof.native_record.terminal.as_ref() == Some(terminal.proof())));
        let source_index = (source - 1) as usize;
        let retired = session.nodes[source_index]
            .inspect_controlled_group(
                GROUP,
                source * 10,
                vec![source as u8],
                session.authority.clone(),
            )
            .await
            .expect("inspect old receipt without voting or opening source Core");
        retired
            .retire_and_cleanup(&terminal, deadline())
            .await
            .expect("recovered sealed proof can actually stop and clean old source generation");
        assert_eq!(
            retired.receipt().expect("retained cleanup tombstone").stage,
            multiraft_store::ControlledGroupStage::Cleaned
        );
        session.groups[source_index] = Some(retired);
    } else {
        assert!(
            record.terminal.is_none(),
            "recovery cannot fabricate terminal from membership alone"
        );
        assert_eq!(
            catalog
                .active_operation
                .as_ref()
                .expect("operation binding retained")
                .specification,
            specification
        );
        let membership = session
            .group(leader)
            .membership(deadline())
            .await
            .expect("native committed recovery membership");
        assert_eq!(
            membership.membership().get_joint_config().len(),
            if phase == "joint" { 2 } else { 1 },
            "genuine persisted phase recovered without initialize"
        );
        let executor = session
            .group(leader)
            .claim(specification.operation, 2, deadline())
            .await
            .expect("continue same operation with higher executor epoch");
        assert!(
            session
                .group(leader)
                .claim(specification.operation, 1, deadline())
                .await
                .is_err(),
            "late old executor epoch rejected after recovery"
        );
        if phase == "opening" {
            let seed = session
                .group(leader)
                .prepare_seed(&executor, deadline())
                .await
                .expect(
                    "recover unfinished target only from a fresh committed-operation checkpoint",
                );
            let fresh_admission = super::admitted(
                session
                    .authority
                    .admit(session.request(3, 2))
                    .await
                    .expect("fresh boot admission resumes quarantined target"),
            );
            let opened = session
                .group(3)
                .open_target(seed.clone(), fresh_admission, deadline())
                .await
                .expect("verify and publish recovered target seed before ordinary RPC");
            session.groups[3] = Some(opened);
            session.target_seed = Some(seed.clone());
            let count = session.creations[3].load(std::sync::atomic::Ordering::SeqCst);
            let admission = super::admitted(
                session
                    .authority
                    .admit(session.request(3, 1))
                    .await
                    .expect("read original crashed opening admission"),
            );
            session
                .group(3)
                .open_target(seed, admission, deadline())
                .await
                .expect("duplicate old open only replays after recovery");
            assert_eq!(
                session.creations[3].load(std::sync::atomic::Ordering::SeqCst),
                count,
                "opening crash duplicate cannot construct another core"
            );
            session
                .group(leader)
                .add_learner(&executor, session.group(3), deadline())
                .await
                .expect("continue first learner admission after Opening crash");
            session.nodes[leader]
                .propose(GROUP, b"snapshot-tail".to_vec())
                .await
                .expect("commit suffix after recovered first opening");
            session
                .group(leader)
                .enter_joint(&executor, session.group(3), deadline())
                .await
                .expect("target applied barrier precedes resumed joint");
        }
        session
            .group(leader)
            .finalize_uniform(&executor, deadline())
            .await
            .expect("resume final uniform independently");
        let terminal = session
            .group(leader)
            .complete(&executor, session.group(3), deadline())
            .await
            .expect("finish recovered operation using durable target receipt");
        assert_eq!(
            terminal
                .proof()
                .configuration
                .membership()
                .get_joint_config(),
            &vec![specification.target_voters.clone()]
        );
    }
    // Every authorized current voter restarted without waiting for an application peer;
    // the recovered membership still commits a fresh write and linearizable read.
    let expected = [
        b"persisted-prefix".to_vec(),
        b"snapshot-tail".to_vec(),
        b"after-cold-restart".to_vec(),
    ];
    session.nodes[leader]
        .propose(GROUP, expected[2].clone())
        .await
        .expect("cold-restarted current voter quorum commits new data");
    for id in &specification.target_voters {
        cluster::wait_payloads(&session.nodes[(*id - 1) as usize], &expected).await;
    }
    session.nodes[leader]
        .read_linearizable(GROUP, |fsm| fsm.payloads().expect("durable rows"))
        .await
        .expect("ReadIndex succeeds with restored current quorum");
    session.stop().await;
    println!("CONTROLLED_PHASE finished");
}
