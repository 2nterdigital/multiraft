//! Pending opaque disk application remains inside the actual target application barrier.
use super::{cluster, session::Session};
use cluster::{deadline, GROUP};
use multiraft_net::ControlledError;
use std::time::Duration;
use tokio::time::Instant;

pub async fn run() {
    let root = cluster::scratch("pending-target-application-");
    let mut session = Session::fresh(&root).await;
    let (leader, specification, executor) = session.prepare(false).await;
    let gate = root.join("node-4/application-gate");
    std::fs::create_dir_all(&gate).expect("create external application test gate");
    std::fs::write(gate.join("armed"), b"pause exactly one opaque payload")
        .expect("arm application callback");
    session.nodes[leader]
        .propose(GROUP, b"pending-opaque-application".to_vec())
        .await
        .expect("old quorum commits application command while target application is paused");
    tokio::time::timeout(Duration::from_secs(5), async {
        while !gate.join("entered").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("actual target disk callback entered before commit");
    let outcome = session
        .group(leader)
        .enter_joint(
            &executor,
            session.group(3),
            Instant::now() + Duration::from_millis(100),
        )
        .await;
    assert!(
        matches!(
            outcome,
            Err(ControlledError::Deadline {
                outcome_unknown: true
            })
        ),
        "joint cannot pass the real target application barrier: {outcome:?}"
    );
    let queued = session
        .group(leader)
        .claim(
            specification.operation,
            2,
            Instant::now() + Duration::from_millis(50),
        )
        .await;
    assert!(
        matches!(
            queued,
            Err(ControlledError::Deadline {
                outcome_unknown: false
            })
        ),
        "caller timeout cannot release membership work ownership: {queued:?}"
    );
    std::fs::write(
        gate.join("release"),
        b"apply actual durable application bytes",
    )
    .expect("release target callback");
    let membership = session
        .group(leader)
        .membership(deadline())
        .await
        .expect("owned joint eventually commits after real application release");
    assert_eq!(membership.membership().get_joint_config().len(), 2);
    cluster::wait_payloads(&session.nodes[3], &[b"pending-opaque-application".to_vec()]).await;
    session
        .group(leader)
        .finalize_uniform(&executor, deadline())
        .await
        .expect("uniform follows owned joint completion");
    session
        .group(leader)
        .complete(&executor, session.group(3), deadline())
        .await
        .expect("terminal proof includes actual target application");
    session.stop().await;
}
