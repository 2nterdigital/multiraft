//! Lost seed waiters and concurrent shutdown must not retain target storage through public handles.
use super::*;
use tokio::sync::Notify;
use transport::Gate;

pub async fn seed_shutdown(gate: Gate) {
    let fixture = Fixture::fresh().await;
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let prepared = fixture
        .prepare_gated(gate, entered.clone(), release.clone())
        .await;
    let identity = prepared.endpoint.identity().clone();
    let endpoint = prepared.endpoint.clone();
    let executor = prepared.executor.clone();
    let port = prepared.port.clone();
    let admission = prepared.admission.clone();
    let waiter =
        tokio::spawn(async move { executor.seed_target(port, admission, deadline()).await });
    tokio::time::timeout(Duration::from_secs(10), entered.notified())
        .await
        .expect("actual native stream reaches requested boundary");
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    if matches!(gate, Gate::FinishedReply) {
        fixture.owners[3]
            .handle()
            .local_group_status(GROUP, deadline())
            .await
            .expect("actual target Ready while seed reply is lost");
    } else {
        assert!(
            fixture.owners[3]
                .handle()
                .local_group_status(GROUP, deadline())
                .await
                .is_err(),
            "unfinished target remains quarantined"
        );
    }
    let Fixture {
        root,
        mut owners,
        groups,
        configs,
        authority,
    } = fixture;
    let target_owner = owners.remove(3);
    let target_handle = target_owner.handle();
    let shutdown = tokio::spawn(async move { target_owner.shutdown(deadline()).await });
    // Repeated caller timeouts abandon only their waits, keeping actual shutdown owned.
    let mut shutdown = Box::pin(shutdown);
    let mut completed = None;
    for _ in 0..3 {
        if let Ok(result) = tokio::time::timeout(Duration::from_micros(1), shutdown.as_mut()).await
        {
            completed = Some(result);
            break;
        }
    }
    match completed {
        Some(result) => result,
        None => shutdown.await,
    }
    .unwrap()
    .unwrap();
    assert!(
        endpoint.dispatch(vec![], deadline()).await.is_err(),
        "retained managed endpoint is Closed"
    );
    assert!(
        target_handle
            .local_group_status(GROUP, deadline())
            .await
            .is_err(),
        "no late readiness publication"
    );
    release.notify_waiters();
    for owner in owners {
        owner.shutdown(deadline()).await.unwrap();
    }
    drop(groups);
    // Retain managed endpoint, operation and target handles while reopening real native logs,
    // application DB and the same listener. None may retain a native/FSM/storage lease.
    let node = start_owned(configs[3].clone()).await;
    if matches!(gate, Gate::FinishedReply) {
        let admission = match authority
            .admit(StartupRequest {
                group_id: GROUP,
                node_id: 4,
                generation: 40,
                request_id: [91; 16],
            })
            .await
            .unwrap()
        {
            StartupAuthorityResult::Applied(value) => value,
            other => panic!("{other:?}"),
        };
        node.handle()
            .open_controlled_group(
                ControlledOpen {
                    group_id: GROUP,
                    generation: 40,
                    identity: identity.consumption,
                    mode: ControlledOpenMode::Recover,
                },
                authority,
                admission,
                deadline(),
            )
            .await
            .expect("actual installed target locally recovers with all source handles retained");
    } else {
        let image = DiskImage::open(&configs[3].data_dir, GROUP)
            .expect("interrupted target FSM lease was actually released");
        drop(image);
    }
    node.shutdown(deadline()).await.unwrap();
    assert!(endpoint.dispatch(vec![], deadline()).await.is_err());
    drop(root);
}

pub async fn blocked_install_repeated_shutdown_timeouts() {
    use multiraft_net::portable::SourceTargetPort;
    let fixture = Fixture::fresh().await;
    let prepared = fixture
        .prepare_gated(
            Gate::FirstChunk,
            Arc::new(Notify::new()),
            Arc::new(Notify::new()),
        )
        .await;
    let endpoint = prepared.endpoint.clone();
    let port = SourceTargetPort::new(
        endpoint.identity().clone(),
        prepared.specification.clone(),
        Arc::new(LocalTransport {
            endpoint: endpoint.clone(),
        }),
    )
    .unwrap();
    let gate = fixture.configs[3].data_dir.join("seed-install-gate/7");
    std::fs::create_dir_all(&gate).unwrap();
    std::fs::write(gate.join("armed"), b"hold real durable FSM callback").unwrap();
    let executor = prepared.executor.clone();
    let admission = prepared.admission.clone();
    let waiter =
        tokio::spawn(async move { executor.seed_target(port, admission, deadline()).await });
    tokio::time::timeout(Duration::from_secs(10), async {
        while !gate.join("installed").exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("actual target durable FSM install callback is blocked");
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    let Fixture {
        root,
        mut owners,
        configs,
        groups,
        ..
    } = fixture;
    let owner = owners.remove(3);
    let handle = owner.handle();
    let mut shutdown = Box::pin(tokio::spawn(
        async move { owner.shutdown(deadline()).await },
    ));
    for _ in 0..3 {
        assert!(
            tokio::time::timeout(Duration::from_millis(5), shutdown.as_mut())
                .await
                .is_err(),
            "shutdown must await actual blocked FSM callback, not canceled request or Arc counts"
        );
    }
    assert!(endpoint.dispatch(vec![], deadline()).await.is_err());
    std::fs::write(gate.join("release"), b"release actual callback").unwrap();
    shutdown.await.unwrap().unwrap();
    assert!(
        handle.local_group_status(GROUP, deadline()).await.is_err(),
        "late install cannot publish readiness after close"
    );
    let image = DiskImage::open(&configs[3].data_dir, GROUP)
        .expect("retained managed handles cannot pin real target FSM after actual shutdown join");
    drop(image);
    let reopened = start_owned(configs[3].clone()).await;
    reopened.shutdown(deadline()).await.unwrap();
    for owner in owners {
        owner.shutdown(deadline()).await.unwrap();
    }
    drop(groups);
    drop(root);
}
