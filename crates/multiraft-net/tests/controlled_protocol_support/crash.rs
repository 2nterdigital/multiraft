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
        .enter_joint(&executor, deadline())
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
    let session = Session::recover(root).await;
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
    if phase == "terminal" {
        assert!(
            record.terminal.is_some(),
            "terminal survives replay/checkpoint/purge"
        );
        assert!(catalog.active_operation.is_none());
        assert!(
            session
                .group(leader)
                .claim(specification.operation, 2, deadline())
                .await
                .is_err(),
            "terminal cannot reactivate after replay"
        );
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
