//! Independent public client/handler consumer; no Raft/FSM/business framework types.
mod application_rpc_support;
use application_rpc_support::*;
use multiraft_net::application_rpc::*;
use multiraft_net::node_rpc::{node_rpc_service_client::NodeRpcServiceClient, NodeRpcRequest};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{timeout, Instant};

#[tokio::test]
async fn local_remote_bytes_metadata_and_nested_absolute_budget_are_preserved() {
    let remote = Arc::new(Echo::default());
    let (remote_owner, _) = start(2, vec![], remote.clone()).await;
    let local = Arc::new(Echo::default());
    let (owner, handle) = start(1, vec![(2, remote_owner.local_address())], local.clone()).await;
    local.handle.set(handle.clone()).ok().unwrap();
    let original = deadline();
    let mut context = RpcContext::new(original);
    context.metadata.insert("test-context", "opaque").unwrap();
    assert_eq!(
        handle
            .call(1, call(b"local"), context.clone())
            .await
            .unwrap()
            .payload,
        b"\x07\x0blocal"
    );
    assert_eq!(
        handle
            .call(2, call(b"remote"), context.clone())
            .await
            .unwrap()
            .payload,
        b"\x07\x0bremote"
    );
    assert_eq!(
        handle
            .call(1, call(b"nested"), context)
            .await
            .unwrap()
            .payload,
        b"\x07\x0binner"
    );
    assert!(local
        .observed
        .lock()
        .unwrap()
        .iter()
        .all(|(d, m)| *d == original && m.get("test-context") == Some("opaque")));
    {
        let facts = remote.observed.lock().unwrap();
        assert_eq!(facts[0].1.get("test-context"), Some("opaque"));
        assert!(facts[0].0 <= original + Duration::from_millis(10));
    }
    owner.shutdown(deadline()).await.unwrap();
    remote_owner.shutdown(deadline()).await.unwrap();
}
#[tokio::test]
async fn failures_keep_source_stage_and_conservative_dispatch_without_retry() {
    let remote = Arc::new(Echo::default());
    let (remote_owner, _) = start(2, vec![], remote.clone()).await;
    let local = Arc::new(Echo::default());
    let (owner, handle) = start(
        1,
        vec![(2, remote_owner.local_address()), (3, address())],
        local.clone(),
    )
    .await;
    for (target, payload, expected) in [
        (1, b"reject".as_slice(), RpcErrorKind::InvalidArgument),
        (2, b"reject".as_slice(), RpcErrorKind::InvalidArgument),
    ] {
        let error = handle
            .call(target, call(payload), RpcContext::new(deadline()))
            .await
            .unwrap_err();
        assert_eq!(error.kind, expected);
        assert_eq!(error.phase, RpcPhase::Handler);
        assert_eq!(error.dispatch, RpcDispatch::MayHaveDispatched);
        assert_eq!(error.message(), "application rejected");
        if target == 2 {
            assert_eq!(error.source_node, Some(2));
        }
    }
    assert_eq!(remote.calls.load(Ordering::SeqCst), 1);
    for target in [3, 9] {
        let error = handle
            .call(target, call(b"x"), RpcContext::new(deadline()))
            .await
            .unwrap_err();
        assert_eq!(error.phase, RpcPhase::Connect);
        assert_eq!(error.dispatch, RpcDispatch::NotDispatched);
    }
    let error = handle
        .call(1, call(b"x"), RpcContext::new(Instant::now()))
        .await
        .unwrap_err();
    assert_eq!(error.kind, RpcErrorKind::DeadlineExceeded);
    assert_eq!(error.dispatch, RpcDispatch::NotDispatched);
    remote_owner.fence();
    let error = handle
        .call(2, call(b"x"), RpcContext::new(deadline()))
        .await
        .unwrap_err();
    assert_eq!(error.kind, RpcErrorKind::Closed);
    assert_eq!(error.phase, RpcPhase::Admission);
    assert_eq!(error.dispatch, RpcDispatch::NotDispatched);
    assert_eq!(error.source_node, Some(2));
    owner.shutdown(deadline()).await.unwrap();
    remote_owner.shutdown(deadline()).await.unwrap();
}
#[tokio::test]
async fn outer_caps_and_required_wire_timeout_refuse_before_handler() {
    let handler = Arc::new(Echo::default());
    let (owner, handle) = start(1, vec![], handler.clone()).await;
    let error = handle
        .call(
            1,
            RpcCall::new(7, 11, vec![0; APPLICATION_RPC_OUTER_BYTES]),
            RpcContext::new(deadline()),
        )
        .await
        .unwrap_err();
    assert_eq!(
        (error.kind, error.dispatch),
        (RpcErrorKind::ResourceExhausted, RpcDispatch::NotDispatched)
    );
    assert_eq!(handler.calls.load(Ordering::SeqCst), 0);
    let error = handle
        .call(1, call(b"oversize"), RpcContext::new(deadline()))
        .await
        .unwrap_err();
    assert_eq!(
        (error.kind, error.phase, error.dispatch),
        (
            RpcErrorKind::ResourceExhausted,
            RpcPhase::Response,
            RpcDispatch::MayHaveDispatched
        )
    );
    let mut raw = NodeRpcServiceClient::connect(format!("http://{}", owner.local_address()))
        .await
        .unwrap();
    let request = || NodeRpcRequest {
        service_id: 7,
        method_id: 11,
        payload: Vec::new(),
    };
    let missing = raw.call(request()).await.unwrap_err();
    assert_eq!(missing.code(), tonic::Code::InvalidArgument);
    assert_eq!(missing.details().len(), 12);
    let mut malformed = tonic::Request::new(request());
    malformed
        .metadata_mut()
        .insert("grpc-timeout", "bad".parse().unwrap());
    assert_eq!(
        raw.call(malformed).await.unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
    let oversized = raw
        .call(NodeRpcRequest {
            service_id: 7,
            method_id: 11,
            payload: vec![0; APPLICATION_RPC_OUTER_BYTES],
        })
        .await
        .unwrap_err();
    assert_eq!(oversized.code(), tonic::Code::ResourceExhausted);
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
    owner.shutdown(deadline()).await.unwrap();
}
#[tokio::test]
async fn caller_cancellation_drops_local_and_remote_handler_futures() {
    let remote = Arc::new(Blocking::default());
    let (remote_owner, _) = start(2, vec![], remote.clone()).await;
    let local = Arc::new(Blocking::default());
    let (owner, handle) = start(1, vec![(2, remote_owner.local_address())], local.clone()).await;
    for (target, handler) in [(1, &local), (2, &remote)] {
        let handle = handle.clone();
        let request = tokio::spawn(async move {
            handle
                .call(
                    target,
                    call(b"pending"),
                    RpcContext::new(Instant::now() + Duration::from_secs(60)),
                )
                .await
        });
        timeout(Duration::from_secs(2), handler.started.notified())
            .await
            .unwrap();
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        until_dropped(&handler.dropped, 1).await;
    }
    owner.shutdown(deadline()).await.unwrap();
    remote_owner.shutdown(deadline()).await.unwrap();
}
#[tokio::test]
async fn canceled_shutdown_wait_retains_drain_and_ports_until_actual_handler_end() {
    let handler = Arc::new(Blocking::default());
    let (owner, handle) = start(1, vec![], handler.clone()).await;
    let address = owner.local_address();
    let calling = handle.clone();
    let request = tokio::spawn(async move {
        calling
            .call(1, call(b"pending"), RpcContext::new(deadline()))
            .await
    });
    timeout(Duration::from_secs(2), handler.started.notified())
        .await
        .unwrap();
    let shutdown = tokio::spawn(owner.shutdown(deadline()));
    loop {
        if handle
            .call(9, call(b"probe"), RpcContext::new(deadline()))
            .await
            .unwrap_err()
            .kind
            == RpcErrorKind::Closed
        {
            break;
        }
        tokio::task::yield_now().await;
    }
    shutdown.abort();
    assert!(shutdown.await.unwrap_err().is_cancelled());
    assert_eq!(handler.dropped.load(Ordering::SeqCst), 0);
    handler.release.notify_one();
    assert_eq!(request.await.unwrap().unwrap().payload, b"done");
    reusable(address).await;
    assert_eq!(
        handle
            .call(1, call(b"late"), RpcContext::new(deadline()))
            .await
            .unwrap_err()
            .kind,
        RpcErrorKind::Closed
    );
}
#[tokio::test]
async fn dropped_owner_fences_weak_calls_and_cancels_admitted_work_on_origin_runtime() {
    let handler = Arc::new(Blocking::default());
    let (owner, handle) = start(1, vec![], handler.clone()).await;
    let address = owner.local_address();
    let calling = handle.clone();
    let request = tokio::spawn(async move {
        calling
            .call(
                1,
                call(b"pending"),
                RpcContext::new(Instant::now() + Duration::from_secs(60)),
            )
            .await
    });
    timeout(Duration::from_secs(2), handler.started.notified())
        .await
        .unwrap();
    std::thread::spawn(move || drop(owner)).join().unwrap();
    let error = request.await.unwrap().unwrap_err();
    assert_eq!(
        (error.kind, error.dispatch),
        (RpcErrorKind::Closed, RpcDispatch::MayHaveDispatched)
    );
    until_dropped(&handler.dropped, 1).await;
    reusable(address).await;
    assert_eq!(
        handle
            .call(1, call(b"late"), RpcContext::new(deadline()))
            .await
            .unwrap_err()
            .dispatch,
        RpcDispatch::NotDispatched
    );
}
#[tokio::test]
async fn finite_admission_and_metadata_are_bounded_and_rejection_has_zero_dispatch() {
    let handler = Arc::new(Blocking::default());
    let mut inputs = config(1, vec![]);
    inputs.max_inflight = 1;
    let (owner, handle) = ApplicationRpcOwner::start(inputs, handler.clone(), deadline())
        .await
        .unwrap();
    let calling = handle.clone();
    let request = tokio::spawn(async move {
        calling
            .call(1, call(b"pending"), RpcContext::new(deadline()))
            .await
    });
    timeout(Duration::from_secs(2), handler.started.notified())
        .await
        .unwrap();
    let error = handle
        .call(1, call(b"busy"), RpcContext::new(deadline()))
        .await
        .unwrap_err();
    assert_eq!(
        (error.kind, error.phase, error.dispatch),
        (
            RpcErrorKind::ResourceExhausted,
            RpcPhase::Admission,
            RpcDispatch::NotDispatched
        )
    );
    handler.release.notify_one();
    request.await.unwrap().unwrap();
    let mut context = RpcContext::new(deadline());
    context.metadata.insert("unlisted", "value").unwrap();
    assert_eq!(
        handle
            .call(1, call(b"bad metadata"), context)
            .await
            .unwrap_err()
            .dispatch,
        RpcDispatch::NotDispatched
    );
    assert!(RpcMetadata::default().insert("grpc-timeout", "1S").is_err());
    assert!(RpcMetadata::default()
        .insert("test-context", "x".repeat(257))
        .is_err());
    owner.shutdown(deadline()).await.unwrap();
}

#[tokio::test]
async fn handler_deadline_is_unknown_dispatch_and_cancels_both_local_and_remote_work() {
    let remote = Arc::new(Blocking::default());
    let (remote_owner, _) = start(2, vec![], remote.clone()).await;
    let local = Arc::new(Blocking::default());
    let (owner, handle) = start(1, vec![(2, remote_owner.local_address())], local.clone()).await;
    for (target, handler) in [(1, &local), (2, &remote)] {
        let error = handle
            .call(
                target,
                call(b"pending"),
                RpcContext::new(Instant::now() + Duration::from_millis(50)),
            )
            .await
            .unwrap_err();
        if target == 1 {
            assert_eq!(error.kind, RpcErrorKind::DeadlineExceeded);
        } else {
            // Tonic's own grpc-timeout can win and return native CANCELLED before
            // our local absolute timer. Preserve that code, never infer zero dispatch.
            assert!(matches!(
                error.kind,
                RpcErrorKind::DeadlineExceeded | RpcErrorKind::Unavailable
            ));
            if error.kind == RpcErrorKind::Unavailable {
                assert_eq!(error.status_code, Some(1));
            }
        }
        assert_eq!(error.dispatch, RpcDispatch::MayHaveDispatched);
        assert!(matches!(
            error.phase,
            RpcPhase::Handler | RpcPhase::Dispatch
        ));
        until_dropped(&handler.dropped, 1).await;
        assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
    }
    owner.shutdown(deadline()).await.unwrap();
    remote_owner.shutdown(deadline()).await.unwrap();
}
#[tokio::test]
async fn closed_peer_generation_reconnects_only_on_next_explicit_call_and_port_is_reusable() {
    let handler = Arc::new(Echo::default());
    let (peer, _) = start(2, vec![], handler.clone()).await;
    let address = peer.local_address();
    let (owner, handle) = start(1, vec![(2, address)], Arc::new(Echo::default())).await;
    assert_eq!(
        handle
            .call(2, call(b"first"), RpcContext::new(deadline()))
            .await
            .unwrap()
            .payload,
        b"\x07\x0bfirst"
    );
    peer.fence();
    let refused = handle
        .call(2, call(b"not repeated"), RpcContext::new(deadline()))
        .await
        .unwrap_err();
    assert_eq!(refused.kind, RpcErrorKind::Closed);
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
    peer.shutdown(deadline()).await.unwrap();
    let mut inputs = config(2, vec![]);
    inputs.listen_address = address;
    let next_handler = Arc::new(Echo::default());
    let (restarted, _) = ApplicationRpcOwner::start(inputs, next_handler.clone(), deadline())
        .await
        .unwrap();
    assert_eq!(
        handle
            .call(2, call(b"second"), RpcContext::new(deadline()))
            .await
            .unwrap()
            .payload,
        b"\x07\x0bsecond"
    );
    assert_eq!(next_handler.calls.load(Ordering::SeqCst), 1);
    owner.shutdown(deadline()).await.unwrap();
    restarted.shutdown(deadline()).await.unwrap();
}
#[tokio::test]
async fn invalid_config_and_bound_port_startup_failure_publish_no_resources() {
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = occupied.local_addr().unwrap();
    let mut inputs = config(1, vec![]);
    inputs.listen_address = address;
    match ApplicationRpcOwner::start(inputs, Arc::new(Echo::default()), deadline()).await {
        Err(error) => assert_eq!(error.dispatch, RpcDispatch::NotDispatched),
        Ok(_) => panic!("occupied port must fail"),
    }
    drop(occupied);
    let mut inputs = config(1, vec![]);
    inputs.listen_address = address;
    inputs.max_inflight = 0;
    match ApplicationRpcOwner::start(inputs, Arc::new(Echo::default()), deadline()).await {
        Err(error) => assert_eq!(error.kind, RpcErrorKind::InvalidConfiguration),
        Ok(_) => panic!("invalid admission must fail"),
    }
    std::net::TcpListener::bind(address).unwrap();
}

#[tokio::test]
async fn accepted_idle_connection_cannot_detach_after_owner_stop() {
    use tokio::io::AsyncReadExt;
    let (owner, _) = start(1, vec![], Arc::new(Echo::default())).await;
    let address = owner.local_address();
    let mut idle = tokio::net::TcpStream::connect(address).await.unwrap();
    tokio::task::yield_now().await;
    owner
        .shutdown(Instant::now() + Duration::from_secs(8))
        .await
        .unwrap();
    // Tonic may have sent HTTP/2 SETTINGS before the client preface. Drain
    // those already-buffered bytes and require actual EOF after owned shutdown.
    let mut buffered = Vec::new();
    timeout(Duration::from_secs(1), idle.read_to_end(&mut buffered))
        .await
        .unwrap()
        .unwrap();
    std::net::TcpListener::bind(address).unwrap();
}
