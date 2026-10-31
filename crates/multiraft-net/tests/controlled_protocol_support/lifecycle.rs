//! Public source-leader handoff, shared-engine cleanup, and concurrent generation rejoin.
use super::{
    admitted,
    authority::{
        StartupAuthority, StartupAuthorityResult, StartupRequest, StartupTargetGeneration,
    },
    cluster,
    session::Session,
};
use cluster::{deadline, GROUP};
use multiraft_core::{OperationId, ReplacementSpec};
use multiraft_net::ControlledOpenMode;
use multiraft_store::ControlledGroupStage;
use std::{sync::atomic::Ordering, time::Duration};

pub async fn run() {
    let root = cluster::scratch("source-leader-clean-rejoin-");
    let mut session = Session::fresh(&root).await;
    let source = session.leader().await;
    let source_id = (source + 1) as u64;
    let delayed = admitted(
        session
            .authority
            .admit(session.request(source, 3))
            .await
            .expect("reserve independent unused source boot before binding"),
    );
    session.nodes[source]
        .create_group(99, &[source_id])
        .await
        .expect("create independent sibling sharing the actual RaftDB");
    tokio::time::timeout(Duration::from_secs(20), async {
        while !session.nodes[source].is_leader(99) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("sibling group elects its native leader");
    session.nodes[source]
        .propose(99, b"shared-sibling-survives".to_vec())
        .await
        .expect("commit sibling application rows");
    let (executing, specification, old_executor) = session.prepare(true).await;
    let original_seed = session
        .target_seed
        .clone()
        .expect("original one-shot target seed");
    assert_eq!(
        executing, source,
        "moving source is the actual committed leader"
    );
    let checkpoint_workspace = root.join(format!("node-{source_id}/fsm-rocksdb/group-7.snapshots"));
    assert!(
        checkpoint_workspace.exists(),
        "source really owns checkpoint workspace before cleanup"
    );
    session
        .group(source)
        .enter_joint(&old_executor, session.group(3), deadline())
        .await
        .expect("source leader commits joint");
    session
        .group(source)
        .finalize_uniform(&old_executor, deadline())
        .await
        .expect("source leader commits its own exclusion");
    let leader = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let candidate = session.leader().await;
            if candidate != source {
                break candidate;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("native leader removal eventually hands leadership to a retained voter");
    assert_ne!(
        leader, source,
        "native Raft elects a retained current voter"
    );
    assert!(
        session
            .group(leader)
            .enter_joint(&old_executor, session.group(3), deadline())
            .await
            .is_err(),
        "old source leader token cannot write through new native leader"
    );
    let executor = session
        .group(leader)
        .claim(specification.operation, 2, deadline())
        .await
        .expect("retained leader takes over same operation");
    let terminal = session
        .group(leader)
        .complete(&executor, session.group(3), deadline())
        .await
        .expect("retained leader verifies and applies exact terminal");
    assert_eq!(
        session
            .group(source)
            .receipt()
            .expect("source remains locally observed")
            .stage,
        ControlledGroupStage::Opened,
        "catalog retirement alone cannot claim source physically stopped"
    );
    assert!(matches!(
        session
            .authority
            .admit(session.request(source, 4))
            .await
            .expect("new source admission checked after terminal"),
        StartupAuthorityResult::Rejected(_)
    ));
    assert!(
        session
            .authority
            .read(GROUP)
            .await
            .expect("durable catalog")
            .admissions
            .contains(&delayed),
        "preadmitted source remains potentially live until native stop barrier"
    );
    let target_count = session.creations[3].load(Ordering::SeqCst);
    let original_admission = admitted(
        session
            .authority
            .admit(session.request(3, 1))
            .await
            .expect("replay consumed target boot admission"),
    );
    let duplicate = session
        .group(3)
        .open_target(original_seed.clone(), original_admission, deadline())
        .await
        .expect("postterminal duplicate replays the consumed target open");
    assert_eq!(
        duplicate.receipt().expect("same durable target receipt"),
        session.group(3).receipt().expect("original target receipt")
    );
    assert_eq!(
        session.creations[3].load(Ordering::SeqCst),
        target_count,
        "duplicate target open never constructs a second FSM"
    );
    session
        .group(source)
        .retire_and_cleanup(&terminal, deadline())
        .await
        .expect("actual native stop and resource release precede group cleanup");
    assert_eq!(
        session
            .group(source)
            .receipt()
            .expect("tombstone retained outside deleted namespace")
            .stage,
        ControlledGroupStage::Cleaned
    );
    assert!(session.nodes[source]
        .with_fsm(GROUP, |_| ())
        .await
        .is_none());
    assert!(
        !checkpoint_workspace.exists(),
        "Cleaned must delete canonical disk checkpoint/install/swap workspace"
    );
    assert_eq!(
        session.nodes[source]
            .with_fsm(99, |fsm| fsm.payloads().expect("sibling application rows"))
            .await,
        Some(vec![b"shared-sibling-survives".to_vec()])
    );
    assert!(
        session.nodes[source]
            .open_controlled_group(
                session.open_request(source, ControlledOpenMode::Recover),
                session.authority.clone(),
                delayed,
                deadline()
            )
            .await
            .is_err(),
        "unused preadmitted source cannot reopen a locally retired generation"
    );

    let next = ReplacementSpec {
        operation: OperationId {
            sequence: 2,
            nonce: [10; 16],
        },
        expected_start: terminal.proof().configuration.clone(),
        target_voters: specification
            .target_voters
            .iter()
            .copied()
            .filter(|id| *id != 4)
            .chain([source_id])
            .collect(),
        target_generation: source_id * 10 + 1,
    };
    let before = session.creations[source].load(Ordering::SeqCst);
    let (first, second) = tokio::join!(
        session.nodes[source].reserve_controlled_target(
            GROUP,
            next.clone(),
            vec![77],
            session.authority.clone(),
            deadline()
        ),
        session.nodes[source].reserve_controlled_target(
            GROUP,
            next.clone(),
            vec![77],
            session.authority.clone(),
            deadline()
        ),
    );
    let first = first.expect("first concurrent rejoin reservation");
    let second = second.expect("identical rejoin reservation replays");
    let identity = first.receipt().expect("new durable generation").identity;
    assert_eq!(
        second.receipt().expect("same generation replay").identity,
        identity
    );
    session
        .authority
        .consume_target(StartupTargetGeneration {
            node_id: source_id,
            generation: next.target_generation,
            consumption_id: identity,
        })
        .expect("register actual newly reserved generation");
    let next_executor = session
        .group(leader)
        .begin(next.clone(), 1, &first, deadline())
        .await
        .expect("bind second operation after exact previous terminal");
    let next_seed = session
        .group(leader)
        .prepare_seed(&next_executor, deadline())
        .await
        .expect("fresh checkpoint binds the new generation before rejoin");
    let admission = admitted(
        session
            .authority
            .admit(StartupRequest {
                group_id: GROUP,
                node_id: source_id,
                generation: next.target_generation,
                request_id: [177; 16],
            })
            .await
            .expect("admit rejoined source as new target generation"),
    );
    let (opened, replayed) = tokio::join!(
        first.open_target(next_seed.clone(), admission.clone(), deadline()),
        second.open_target(next_seed, admission, deadline())
    );
    let opened = opened.expect("open rejoined generation");
    replayed.expect("concurrent open replays same consumed receipt");
    assert_eq!(
        session.creations[source].load(Ordering::SeqCst),
        before + 1,
        "concurrent rejoin constructs exactly one actual FSM"
    );
    assert!(
        session
            .group(source)
            .retire_and_cleanup(&terminal, deadline())
            .await
            .is_err(),
        "old cleanup cannot delete a rejoined newer generation"
    );
    let old_target_admission = admitted(
        session
            .authority
            .admit(session.request(3, 1))
            .await
            .expect("old target request outcome retained"),
    );
    let count_before_replay = session.creations[3].load(Ordering::SeqCst);
    if let Ok(replayed) = session
        .group(3)
        .open_target(original_seed, old_target_admission, deadline())
        .await
    {
        assert_eq!(
            replayed.receipt().expect("read-only old receipt replay"),
            session
                .group(3)
                .receipt()
                .expect("existing old target receipt")
        );
    }
    assert_eq!(
        session.creations[3].load(Ordering::SeqCst),
        count_before_replay,
        "late old target open may only replay its consumed receipt"
    );
    assert_eq!(
        session
            .group(leader)
            .ledger(deadline())
            .await
            .expect("new operation remains authoritative")
            .current
            .expect("second active operation")
            .specification,
        next
    );
    session
        .group(leader)
        .add_learner(&next_executor, &opened, deadline())
        .await
        .expect("new generation actually receives learner replication");
    session.groups[source] = Some(opened);
    let config = cluster::configurations(&root).remove(source);
    let counter = session.creations[source].clone();
    session.stop().await;
    let reopened = cluster::start(config, counter).await;
    reopened
        .create_group(99, &[source_id])
        .await
        .expect("recover original sibling from the shared engine without rebuilding its data");
    assert_eq!(
        reopened
            .with_fsm(99, |fsm| fsm.payloads().expect("recovered sibling rows"))
            .await,
        Some(vec![b"shared-sibling-survives".to_vec()])
    );
    reopened
        .shutdown()
        .await
        .expect("release recovered sibling resources");
}
