//! Checkpoint installation is not permission to publish a target's native routes.
use super::{admitted, authority::StartupAuthority, cluster, process, session::Session};
use cluster::{deadline, GROUP};
use multiraft_core::typ::{Vote, VoteRequest};
use multiraft_core::MultiRaftError;
use multiraft_net::GrpcRouter;
use multiraft_store::ControlledGroupStage;
use openraft::network::RPCOption;
use openraft_multi::GroupRouter;
use std::{path::Path, sync::atomic::Ordering, time::Duration};

const PREFIX: &[u8] = b"before-quarantined-checkpoint";

fn gate(root: &Path) -> std::path::PathBuf {
    root.join("node-4/seed-install-gate")
        .join(GROUP.to_string())
}

fn anchor(root: &Path) -> std::path::PathBuf {
    root.join("node-4/controlled-seed")
        .join(format!("group-{GROUP}/generation-40.json"))
}

pub async fn child(root: &Path, resume: bool) {
    if resume {
        recover(root).await;
        println!("CONTROLLED_PHASE finished");
    } else {
        initial(root, true).await;
    }
}

pub async fn initial(root: &Path, crash: bool) {
    let mut session = Session::fresh(root).await;
    let first = session.leader().await;
    session.nodes[first]
        .propose(GROUP, PREFIX.to_vec())
        .await
        .expect("commit application bytes before actual seed checkpoint");
    let (leader, specification, target) = session.reserve(false).await;
    let executor = session
        .group(leader)
        .begin(specification.clone(), 1, &target, deadline())
        .await
        .expect("durable Begin binds target seed operation");
    let seed = session
        .group(leader)
        .prepare_seed(&executor, deadline())
        .await
        .expect("capture real current-operation checkpoint");
    let admission = admitted(
        session
            .authority
            .admit(session.request(3, 1))
            .await
            .expect("admit original target boot"),
    );
    session.nodes[3]
        .create_group(99, &[4])
        .await
        .expect("independent sibling remains available throughout target installation");
    std::fs::create_dir_all(gate(root)).expect("create bounded checkpoint callback gate");
    std::fs::write(gate(root).join("armed"), b"hold after real DB install")
        .expect("arm checkpoint gate before target core construction");
    let target_open = target.clone();
    let mut opening =
        tokio::spawn(async move { target_open.open_target(seed, admission, deadline()).await });
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if gate(root).join("installed").exists() { break; }
            tokio::select! {
                result = &mut opening => panic!("target open ended before install gate: {result:?}"),
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }
    }).await.expect("real checkpoint callback reached post-install boundary");
    assert!(!std::fs::read(gate(root).join("installed"))
        .expect("installed applied record")
        .is_empty());
    assert_eq!(
        session.creations[3].load(Ordering::SeqCst),
        2,
        "target Core/FSM was actually constructed in addition to sibling"
    );
    assert_eq!(
        target.receipt().expect("durable opening receipt").stage,
        ControlledGroupStage::Opening
    );
    assert!(
        !anchor(root).exists(),
        "callback is before durable seed anchor"
    );
    assert!(
        matches!(
            session.nodes[3].observe_group(GROUP),
            Err(MultiRaftError::UnknownGroup(GROUP))
        ),
        "installed but unanchored Core must remain absent from ordinary native queries"
    );
    assert_eq!(
        session.nodes[3].with_fsm(GROUP, |_| ()).await,
        None,
        "ordinary application access cannot reach quarantined installed state"
    );
    assert!(
        !opening.is_finished(),
        "checkpoint callback still blocks target publication"
    );
    let configs = cluster::configurations(root);
    let address = configs[3]
        .peers
        .iter()
        .find(|(id, _)| *id == 4)
        .expect("target gRPC address")
        .1;
    let router = GrpcRouter::new(vec![(4, address)], (leader + 1) as u64);
    let vote = session.nodes[leader]
        .observe_group(GROUP)
        .expect("source actual native observation")
        .0
        .flushed_vote;
    let request = VoteRequest::new(
        Vote::new(vote.term, vote.node_id),
        *specification.expected_start.log_id(),
    );
    router
        .vote(
            4,
            99,
            request.clone(),
            RPCOption::new(Duration::from_secs(2)),
        )
        .await
        .expect("normal sibling RPC proves same server and transport are live");
    let response = router
        .vote(
            4,
            GROUP,
            request.clone(),
            RPCOption::new(Duration::from_secs(2)),
        )
        .await;
    assert!(
        response.is_err(),
        "checkpoint alone cannot publish ordinary native RPC: {response:?}"
    );
    assert!(
        session.nodes[3].observe_group(99).is_ok(),
        "sibling remains published"
    );
    assert_eq!(
        target.receipt().expect("receipt after native probe").stage,
        ControlledGroupStage::Opening
    );
    assert!(
        !anchor(root).exists(),
        "native probe cannot advance gated seed publication"
    );
    if crash {
        process::persisted_phase("seed_install");
    }
    std::fs::write(gate(root).join("release"), b"finish installation")
        .expect("release bounded checkpoint gate");
    let opened = opening
        .await
        .expect("join owned target open")
        .expect("verified seed publishes only after callback completion");
    assert_eq!(
        opened.receipt().expect("durable target open").stage,
        ControlledGroupStage::Opened
    );
    assert!(
        anchor(root).exists(),
        "target success has a durable seed anchor"
    );
    assert!(
        session.nodes[3].observe_group(GROUP).is_ok(),
        "anchored Core is now published"
    );
    router
        .vote(4, GROUP, request, RPCOption::new(Duration::from_secs(2)))
        .await
        .expect("same normal native RPC now reaches seeded target");
    router.close();
    router.join().await.expect("join native RPC probes");
    session.groups[3] = Some(opened);
    session
        .group(leader)
        .add_learner(&executor, session.group(3), deadline())
        .await
        .expect("published seed works as actual learner");
    cluster::wait_payloads(&session.nodes[3], &[PREFIX.to_vec()]).await;
    finish(&session, leader, &executor).await;
    session.stop().await;
}

async fn recover(root: &Path) {
    assert!(
        gate(root).join("installed").exists(),
        "restart uses actual post-install disk image"
    );
    assert!(
        !anchor(root).exists(),
        "SIGKILL happened before seed anchor publication"
    );
    std::fs::write(
        gate(root).join("release"),
        b"fresh boot may complete checkpoint",
    )
    .expect("release fixture gate without changing persisted native data");
    let mut session = Session::recover(root).await;
    assert!(
        matches!(
            session.nodes[3].observe_group(GROUP),
            Err(MultiRaftError::UnknownGroup(GROUP))
        ),
        "cold recovery cannot publish a partially installed seed"
    );
    assert_eq!(
        session
            .group(3)
            .receipt()
            .expect("recovered target receipt")
            .stage,
        ControlledGroupStage::Opening
    );
    let leader = session.leader().await;
    let record = session
        .group(leader)
        .ledger(deadline())
        .await
        .expect("recover actual committed operation")
        .current
        .expect("same Begin retained");
    assert_eq!(
        record.executor_epoch, 1,
        "old committed executor survives SIGKILL"
    );
    assert!(
        record.terminal.is_none(),
        "partial install cannot fabricate completion"
    );
    let executor = session
        .group(leader)
        .claim(record.specification.operation, 2, deadline())
        .await
        .expect("fresh executor resumes same operation");
    let seed = session
        .group(leader)
        .prepare_seed(&executor, deadline())
        .await
        .expect("fresh checkpoint contains recovered operation");
    let admission = admitted(
        session
            .authority
            .admit(session.request(3, 2))
            .await
            .expect("fresh startup admission after actual process loss"),
    );
    let opened = session
        .group(3)
        .open_target(seed, admission, deadline())
        .await
        .expect("same-directory partial install resumes without clearing native state");
    assert_eq!(
        opened.receipt().expect("recovered target open").stage,
        ControlledGroupStage::Opened
    );
    assert!(
        anchor(root).exists(),
        "fresh seeded recovery durably anchors before publication"
    );
    session.groups[3] = Some(opened);
    session
        .group(leader)
        .add_learner(&executor, session.group(3), deadline())
        .await
        .expect("same recovered operation adds target learner");
    cluster::wait_payloads(&session.nodes[3], &[PREFIX.to_vec()]).await;
    finish(&session, leader, &executor).await;
    session.nodes[leader]
        .propose(GROUP, b"after-seed-install-sigkill".to_vec())
        .await
        .expect("recovered final voter quorum commits fresh application data");
    for id in &record.specification.target_voters {
        cluster::wait_payloads(
            &session.nodes[(*id - 1) as usize],
            &[PREFIX.to_vec(), b"after-seed-install-sigkill".to_vec()],
        )
        .await;
    }
    session.nodes[leader]
        .read_linearizable(GROUP, |_| ())
        .await
        .expect("recovered final quorum confirms native ReadIndex");
    session.stop().await;
}

async fn finish(session: &Session, leader: usize, executor: &multiraft_net::ControlledExecutor) {
    session
        .group(leader)
        .enter_joint(executor, session.group(3), deadline())
        .await
        .expect("seeded target applied barrier allows real joint configuration");
    session
        .group(leader)
        .finalize_uniform(executor, deadline())
        .await
        .expect("real final uniform configuration commits");
    session
        .group(leader)
        .complete(executor, session.group(3), deadline())
        .await
        .expect("same operation reaches durable native terminal proof");
}
