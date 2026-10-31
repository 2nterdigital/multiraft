//! Preadmitted sessions preserve UNKNOWN without restoring excluded consensus authority.
use super::{
    admitted,
    authority::{StartupAdmission, StartupAuthority, StartupAuthorityResult},
    cluster,
    session::Session,
};
use cluster::{deadline, GROUP};
use multiraft_net::{ControlledError, ControlledOpenMode};
use multiraft_store::ControlledGroupStage;
use std::{path::Path, time::Duration};

async fn stop(session: &mut Session, index: usize) {
    session.groups[index] = None;
    session.nodes[index]
        .shutdown()
        .await
        .expect("stop actual Core and all disk owners");
}

async fn restart(
    session: &mut Session,
    root: &Path,
    index: usize,
    admission: StartupAdmission,
    timing: (u64, u64),
) {
    let mut config = cluster::configurations(root).remove(index);
    config.election_timeout_min_ms = timing.0;
    config.election_timeout_max_ms = timing.1;
    let node = cluster::start(config, session.creations[index].clone()).await;
    let group = node
        .open_controlled_group(
            session.open_request(index, ControlledOpenMode::Recover),
            session.authority.clone(),
            admission,
            deadline(),
        )
        .await
        .expect("recover existing native group using an unused durable physical admission");
    session.nodes[index] = node;
    session.groups[index] = Some(group);
}

pub async fn regained_leader_rejects_old_incarnation() {
    let root = cluster::scratch("leader-new-term-");
    let mut session = Session::fresh(&root).await;
    let source = session.leader().await;
    let source_admission = admitted(
        session
            .authority
            .admit(session.request(source, 3))
            .await
            .expect("admit unused source physical session before binding"),
    );
    let (original, specification, old_executor) = session.prepare(true).await;
    assert_eq!(original, source);
    stop(&mut session, source).await;
    eprintln!("CONTROLLED_REELECTION source={source} stopped; await retained leader");
    let retained = session.leader().await;
    assert_ne!(retained, source);
    restart(&mut session, &root, source, source_admission, (200, 250)).await;
    let other = (0..3)
        .find(|index| *index != source && *index != retained)
        .expect("third old voter");
    let other_admission = admitted(
        session
            .authority
            .admit(session.request(other, 3))
            .await
            .expect("current retained voter may obtain fresh admission during active operation"),
    );
    stop(&mut session, other).await;
    restart(&mut session, &root, other, other_admission, (3000, 4000)).await;
    tokio::time::timeout(Duration::from_secs(20), async {
        while session.nodes[source].leader(GROUP) != Some((retained + 1) as u64)
            || session.nodes[other].leader(GROUP) != Some((retained + 1) as u64)
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("both restarted voters receive real newer-leader AppendEntries before handoff");
    let barrier = b"new-leader-incarnation-applied-barrier".to_vec();
    session.nodes[retained]
        .propose(GROUP, barrier.clone())
        .await
        .expect("commit independent catch-up barrier in the retained leader term");
    cluster::wait_payloads(&session.nodes[source], std::slice::from_ref(&barrier)).await;
    cluster::wait_payloads(&session.nodes[other], std::slice::from_ref(&barrier)).await;
    stop(&mut session, retained).await;
    eprintln!("CONTROLLED_REELECTION retained={retained} stopped; source={source} other={other}; observations={:?}", session.nodes.iter().map(|node| (node.node_id(), node.leader(GROUP), node.is_leader(GROUP))).collect::<Vec<_>>());
    assert_eq!(
        session.leader().await,
        source,
        "the same source node regains actual leadership in a later term"
    );
    let stale = session
        .group(source)
        .enter_joint(&old_executor, session.group(3), deadline())
        .await;
    assert!(
        matches!(
            stale,
            Err(ControlledError::Rejected("leader incarnation changed"))
        ),
        "old sealed token must fail specifically at native incarnation fence: {stale:?}"
    );
    let executor = session
        .group(source)
        .claim(specification.operation, 2, deadline())
        .await
        .expect("new leader incarnation explicitly claims higher epoch");
    session
        .group(source)
        .enter_joint(&executor, session.group(3), deadline())
        .await
        .expect("new token actually commits guarded membership using restored quorum");
    session.stop().await;
}

pub async fn delayed_source_cannot_restore_old_commit_quorum() {
    let root = cluster::scratch("late-preadmitted-source-");
    let mut session = Session::fresh(&root).await;
    let mut unused = Vec::new();
    for index in 0..3 {
        unused.push(admitted(
            session
                .authority
                .admit(session.request(index, 3))
                .await
                .expect("persist unused original voter admission before binding"),
        ));
    }
    let before = session.leader().await;
    session.nodes[before]
        .propose(GROUP, b"old-prefix".to_vec())
        .await
        .expect("commit independently observable old application prefix");
    let (leader, specification, executor) = session.prepare(false).await;
    let source_id = *[1, 2, 3]
        .iter()
        .find(|id| !specification.target_voters.contains(id))
        .expect("moving source");
    let source = (source_id - 1) as usize;
    let delayed = unused[source].clone();
    stop(&mut session, source).await;
    session
        .group(leader)
        .enter_joint(&executor, session.group(3), deadline())
        .await
        .expect("surviving old majority plus new majority commit joint");
    session
        .group(leader)
        .finalize_uniform(&executor, deadline())
        .await
        .expect("fully exclude offline source");
    session
        .group(leader)
        .complete(&executor, session.group(3), deadline())
        .await
        .expect("complete without claiming absent source stopped");
    assert!(matches!(
        session
            .authority
            .admit(session.request(source, 4))
            .await
            .expect("fresh retired source admission"),
        StartupAuthorityResult::Rejected(_)
    ));
    for index in 0..4 {
        if index != source && index != leader {
            stop(&mut session, index).await;
        }
    }
    restart(&mut session, &root, source, delayed.clone(), (200, 250)).await;
    let old = session
        .group(source)
        .local_ledger(deadline())
        .await
        .expect("local old native ledger remains an observation only")
        .current
        .expect("source old operation");
    assert!(
        old.terminal.is_none(),
        "source actually recovered preterminal data rather than rebuilding from management intent"
    );
    tokio::time::sleep(Duration::from_millis(750)).await;
    let outcome = tokio::time::timeout(
        Duration::from_millis(500),
        session.nodes[source].propose(GROUP, b"must-not-commit".to_vec()),
    )
    .await;
    assert!(
        !matches!(outcome, Ok(Ok(_))),
        "old source plus one old voter must not regain a committing quorum"
    );
    cluster::wait_payloads(&session.nodes[leader], &[b"old-prefix".to_vec()]).await;
    assert_eq!(
        session
            .group(source)
            .receipt()
            .expect("source physical state still independently observed")
            .stage,
        ControlledGroupStage::Opened,
        "catalog retirement cannot assert a local stop barrier"
    );
    assert!(session
        .authority
        .read(GROUP)
        .await
        .expect("read retained uncertainty evidence")
        .admissions
        .contains(&delayed));
    session.stop().await;
}
