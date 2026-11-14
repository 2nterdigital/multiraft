//! Caller departure cannot release the source actor's actual native command fence.
use super::{
    admitted, authority::StartupAuthority, binding_gate::BindingGate, cluster, session::Session,
};
use cluster::deadline;
use multiraft_net::{ControlledError, ControlledOpenMode};
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;

pub async fn run(abort_waiter: bool) {
    let root = cluster::scratch(if abort_waiter {
        "aborted-bind-"
    } else {
        "deadline-bind-"
    });
    let mut session = Session::fresh(&root).await;
    let (leader, specification, target) = session.reserve(false).await;
    let gate = Arc::new(BindingGate::new(session.authority.clone()));
    let admission = admitted(
        session
            .authority
            .admit(session.request(leader, 1))
            .await
            .expect("read original durable admission"),
    );
    let source = session.nodes[leader]
        .open_controlled_group(
            session.open_request(leader, ControlledOpenMode::Recover),
            gate.clone(),
            admission,
            deadline(),
        )
        .await
        .expect("attach public handle to already running core");
    let waiting_source = source.clone();
    let waiting_target = target.clone();
    let waiting_specification = specification.clone();
    let waiter_deadline = if abort_waiter {
        deadline()
    } else {
        Instant::now() + Duration::from_millis(100)
    };
    let waiter = tokio::spawn(async move {
        waiting_source
            .begin(waiting_specification, 1, &waiting_target, waiter_deadline)
            .await
    });
    tokio::time::timeout(Duration::from_secs(10), gate.entered.notified())
        .await
        .expect("actual owned Begin reaches delayed external authority");
    if abort_waiter {
        waiter.abort();
        assert!(waiter
            .await
            .expect_err("caller future is aborted")
            .is_cancelled());
    } else {
        assert!(
            matches!(
                waiter.await.expect("deadline waiter completes"),
                Err(ControlledError::Deadline {
                    outcome_unknown: true
                })
            ),
            "timeout must retain unknown in-flight work"
        );
    }
    let behind = source
        .claim(
            specification.operation,
            2,
            Instant::now() + Duration::from_millis(50),
        )
        .await;
    assert!(
        matches!(
            behind,
            Err(ControlledError::Deadline {
                outcome_unknown: false
            })
        ),
        "Claim must remain outside the retained command fence: {behind:?}"
    );
    gate.release();
    let recovered = source
        .ledger(deadline())
        .await
        .expect("original owned command commits after caller departure")
        .current
        .expect("same operation native Begin exists");
    assert_eq!(recovered.specification, specification);
    assert_eq!(
        recovered.executor_epoch, 1,
        "blocked Claim was never admitted"
    );
    source
        .claim(specification.operation, 2, deadline())
        .await
        .expect("higher epoch admitted only after actual Begin completes");
    assert!(
        source
            .claim(specification.operation, 1, deadline())
            .await
            .is_err(),
        "late old epoch rejected after handover"
    );
    session.stop().await;
}
