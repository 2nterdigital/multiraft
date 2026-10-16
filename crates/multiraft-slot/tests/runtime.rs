use multiraft_fsm::{ApplyOut, CaptureError, StateMachine};
use multiraft_slot::*;
use std::{
    net::{SocketAddr, TcpListener},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Condvar, Mutex,
    },
    time::Duration,
};
use tokio::time::Instant;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(15)
}
fn address() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}
static DIR: AtomicU64 = AtomicU64::new(0);
fn directory() -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/slot-foundation/tmp");
    std::fs::create_dir_all(&root).unwrap();
    root.join(format!(
        "slot-runtime-{}-{}",
        std::process::id(),
        DIR.fetch_add(1, Ordering::Relaxed)
    ))
}
fn layout(nodes: usize, slots: u32, hosted: Vec<u64>) -> ClusterLayout {
    ClusterLayout::new(LayoutConfig {
        cluster_id: "runtime-test".into(),
        format_version: FORMAT_VERSION,
        hash_version: HashVersion::Sha256V1,
        slot_count: slots,
        group_id_base: 100,
        nodes: (1..=nodes as u64)
            .map(|node_id| NodeAddress {
                node_id,
                raft: address(),
                application_rpc: address(),
            })
            .collect(),
        slots: (0..slots)
            .map(|s| SlotAssignment {
                slot: SlotId(s),
                voters: hosted.clone(),
            })
            .collect(),
    })
    .unwrap()
}
fn key(layout: &ClusterLayout, slot: u32) -> Vec<u8> {
    (0u64..)
        .map(|n| n.to_be_bytes().to_vec())
        .find(|k| layout.slot_for_key(k).unwrap() == SlotId(slot))
        .unwrap()
}
#[derive(Default)]
struct Gate {
    entered: AtomicBool,
    open: Mutex<bool>,
    changed: Condvar,
}
impl Gate {
    fn release(&self) {
        *self.open.lock().unwrap() = true;
        self.changed.notify_all();
    }
    async fn wait(&self) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !self.entered.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }
}
struct Fsm {
    value: u64,
    gate: Arc<Gate>,
    applied: Arc<AtomicU64>,
}
impl StateMachine for Fsm {
    type Error = std::io::Error;
    fn apply(&mut self, _: u64, _: u64, data: &[u8]) -> Result<ApplyOut, Self::Error> {
        self.applied.fetch_add(1, Ordering::AcqRel);
        if data == b"gate" {
            self.gate.entered.store(true, Ordering::Release);
            let mut open = self.gate.open.lock().unwrap();
            while !*open {
                open = self.gate.changed.wait(open).unwrap();
            }
        }
        self.value += 1;
        Ok(ApplyOut {
            effects: self.value.to_be_bytes().to_vec(),
        })
    }
    fn snapshot(&self, _: u64) -> Result<Vec<u8>, Self::Error> {
        Ok(self.value.to_be_bytes().to_vec())
    }
    fn restore(&mut self, _: u64, bytes: &[u8]) -> Result<(), Self::Error> {
        self.value = u64::from_be_bytes(
            bytes
                .try_into()
                .map_err(|_| std::io::Error::other("invalid snapshot"))?,
        );
        Ok(())
    }
    fn freeze_bounded(&self, group: u64, max: usize) -> Result<Vec<u8>, CaptureError<Self::Error>> {
        if max < 8 {
            return Err(CaptureError::Refused(
                multiraft_fsm::CaptureRefusal::SizeLimit,
            ));
        }
        self.snapshot(group).map_err(CaptureError::Application)
    }
}
impl SlotStateMachine for Fsm {
    fn query(&self, _: u64, request: &[u8]) -> Result<Vec<u8>, Self::Error> {
        assert_ne!(request, b"panic", "intentional application query panic");
        Ok(self.value.to_be_bytes().to_vec())
    }
}
async fn start(
    layout: ClusterLayout,
    node: u64,
    dir: PathBuf,
    gate: Arc<Gate>,
    applied: Arc<AtomicU64>,
    requests: usize,
) -> SlotNodeOwner<Fsm> {
    let mut config = SlotNodeConfig::new(layout, node, dir);
    config.limits.per_slot_requests = requests;
    config.native.heartbeat_interval_ms = 20;
    config.native.election_timeout_min_ms = 80;
    config.native.election_timeout_max_ms = 160;
    SlotNodeOwner::start(
        config,
        move |_| {
            Ok(Fsm {
                value: 0,
                gate: gate.clone(),
                applied: applied.clone(),
            })
        },
        deadline(),
    )
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn timeout_and_drop_hold_slot_budget_until_native_completion_other_slot_progresses() {
    let layout = layout(1, 2, vec![1]);
    let k0 = key(&layout, 0);
    let k1 = key(&layout, 1);
    let gate = Arc::new(Gate::default());
    let applied = Arc::new(AtomicU64::new(0));
    let owner = start(layout, 1, directory(), gate.clone(), applied.clone(), 1).await;
    let h = owner.handle();
    let writer = h.clone();
    let key0 = k0.clone();
    let call = tokio::spawn(async move {
        writer
            .propose(
                &key0,
                b"gate".to_vec(),
                Instant::now() + Duration::from_millis(500),
            )
            .await
    });
    gate.wait().await;
    assert_eq!(call.await.unwrap(), Err(RuntimeError::Unknown));
    assert_eq!(h.inflight().slots[0].requests, 1);
    assert_eq!(
        h.propose(&k0, vec![1], deadline()).await,
        Err(RuntimeError::Busy)
    );
    assert!(h.propose(&k1, vec![1], deadline()).await.is_ok());
    gate.release();
    tokio::time::timeout(Duration::from_secs(5), async {
        while h.inflight().global.requests != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let committed = h.propose(&k0, vec![1], deadline()).await.unwrap();
    let local = h.read_applied(SlotId(0), &[], deadline()).await.unwrap();
    assert_eq!(local.value, committed.effects);
    assert_eq!(local.applied.unwrap().index, committed.index);
    assert_eq!(local.applied.unwrap().term, committed.term);
    assert_eq!(
        u64::from_be_bytes(
            h.read_linearizable(&k0, &[], deadline())
                .await
                .unwrap()
                .try_into()
                .unwrap()
        ),
        2
    );
    let report = owner.shutdown(deadline()).await;
    assert!(report.complete, "{report:?}");
    assert_eq!(report.remaining.global.requests, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn dropped_caller_and_canceled_shutdown_retain_cleanup_and_allow_reopen() {
    let layout = layout(1, 1, vec![1]);
    let k = key(&layout, 0);
    let dir = directory();
    let gate = Arc::new(Gate::default());
    let applied = Arc::new(AtomicU64::new(0));
    let owner = start(
        layout.clone(),
        1,
        dir.clone(),
        gate.clone(),
        applied.clone(),
        1,
    )
    .await;
    let h = owner.handle();
    let writer = h.clone();
    let task = tokio::spawn(async move { writer.propose(&k, b"gate".to_vec(), deadline()).await });
    gate.wait().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(h.inflight().global.requests, 1);
    let shutdown = tokio::spawn(owner.shutdown(deadline()));
    tokio::time::sleep(Duration::from_millis(30)).await;
    shutdown.abort();
    let _ = shutdown.await;
    gate.release();
    tokio::time::timeout(Duration::from_secs(5), async {
        while h.inflight().global.requests > 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(!h.status().accepting);
    // Native cleanup includes listener and database release; retry startup only after
    // the canceled shutdown's independently retained cleanup can finish.
    tokio::time::sleep(Duration::from_millis(150)).await;
    let reopened = start(layout, 1, dir, gate, applied, 1).await;
    assert_eq!(
        reopened
            .handle()
            .read_applied(SlotId(0), &[], deadline())
            .await
            .unwrap()
            .value,
        1u64.to_be_bytes()
    );
    assert!(reopened.shutdown(deadline()).await.complete);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn validation_rejects_before_apply_and_manifest_mismatch_before_native_open() {
    let layout = layout(1, 2, vec![1]);
    let k = key(&layout, 0);
    let dir = directory();
    let gate = Arc::new(Gate::default());
    let applied = Arc::new(AtomicU64::new(0));
    let owner = start(
        layout.clone(),
        1,
        dir.clone(),
        gate.clone(),
        applied.clone(),
        2,
    )
    .await;
    let h = owner.handle();
    assert_eq!(
        h.propose_expected(&k, SlotId(1), vec![1], deadline()).await,
        Err(RuntimeError::InvalidRequest)
    );
    assert_eq!(
        h.propose(&k, vec![0; MAX_COMMAND_BYTES + 1], deadline())
            .await,
        Err(RuntimeError::PayloadTooLarge)
    );
    assert_eq!(
        h.propose(&vec![0; MAX_KEY_LEN + 1], vec![1], deadline())
            .await,
        Err(RuntimeError::PayloadTooLarge)
    );
    assert_eq!(applied.load(Ordering::Acquire), 0);
    assert!(owner.shutdown(deadline()).await.complete);
    let mut changed = layout.config().clone();
    changed.cluster_id = "other".into();
    let config = SlotNodeConfig::new(ClusterLayout::new(changed).unwrap(), 1, dir);
    assert!(matches!(
        SlotNodeOwner::start(
            config,
            move |_| Ok(Fsm {
                value: 0,
                gate: gate.clone(),
                applied: applied.clone()
            }),
            deadline()
        )
        .await,
        Err(RuntimeError::ManifestMismatch)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn real_grpc_nonhosted_node_routes_single_write_and_linearizable_read() {
    let layout = layout(4, 2, vec![1, 2, 3]);
    let k = key(&layout, 0);
    let mut owners = Vec::new();
    let applied = Arc::new(AtomicU64::new(0));
    for node in 1..=4 {
        owners.push(
            start(
                layout.clone(),
                node,
                directory(),
                Arc::new(Gate::default()),
                applied.clone(),
                4,
            )
            .await,
        );
    }
    let h = owners[3].handle();
    let result = h.propose(&k, b"one".to_vec(), deadline()).await.unwrap();
    assert_eq!(result.effects, 1u64.to_be_bytes());
    assert_eq!(
        h.read_linearizable(&k, &[], deadline()).await.unwrap(),
        1u64.to_be_bytes()
    );
    assert!(h
        .leader_hint(SlotId(0), deadline())
        .await
        .unwrap()
        .is_some());
    // Exactly one command applied on each voter; the router hosts no FSM.
    tokio::time::timeout(Duration::from_secs(5), async {
        while applied.load(Ordering::Acquire) < 3 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(applied.load(Ordering::Acquire), 3);
    for owner in owners {
        let r = owner.shutdown(deadline()).await;
        assert!(r.complete, "{r:?}");
    }
}

#[test]
fn unsupported_native_modes_are_rejected() {
    let layout = layout(1, 1, vec![1]);
    let mut c = SlotNodeConfig::new(layout, 1, directory());
    c.native.snapshot_mode = multiraft_core::SnapshotMode::Disabled;
    // Startup validates before constructing any listener or data directory.
    let runtime = tokio::runtime::Runtime::new().unwrap();
    assert!(matches!(
        runtime.block_on(SlotNodeOwner::start(
            c,
            |_| Ok(Fsm {
                value: 0,
                gate: Arc::new(Gate::default()),
                applied: Arc::new(AtomicU64::new(0))
            }),
            deadline()
        )),
        Err(RuntimeError::InvalidConfig)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn remote_unknown_is_not_retried_and_destination_retains_admission() {
    let layout = layout(2, 1, vec![1]);
    let k = key(&layout, 0);
    let gate = Arc::new(Gate::default());
    let applied = Arc::new(AtomicU64::new(0));
    let destination = start(
        layout.clone(),
        1,
        directory(),
        gate.clone(),
        applied.clone(),
        1,
    )
    .await;
    let router = start(
        layout,
        2,
        directory(),
        Arc::new(Gate::default()),
        applied.clone(),
        2,
    )
    .await;
    let h = router.handle();
    let remote = destination.handle();
    let writer = h.clone();
    let key0 = k.clone();
    let call = tokio::spawn(async move {
        writer
            .propose(
                &key0,
                b"gate".to_vec(),
                Instant::now() + Duration::from_millis(500),
            )
            .await
    });
    gate.wait().await;
    assert_eq!(call.await.unwrap(), Err(RuntimeError::Unknown));
    assert_eq!(remote.inflight().global.requests, 1);
    assert_eq!(
        h.propose(&k, b"second".to_vec(), deadline()).await,
        Err(RuntimeError::Busy)
    );
    assert_eq!(applied.load(Ordering::Acquire), 1);
    gate.release();
    tokio::time::timeout(Duration::from_secs(5), async {
        while remote.inflight().global.requests > 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        h.read_linearizable(&k, &[], deadline()).await.unwrap(),
        1u64.to_be_bytes()
    );
    assert_eq!(applied.load(Ordering::Acquire), 1);
    assert!(router.shutdown(deadline()).await.complete);
    assert!(destination.shutdown(deadline()).await.complete);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn applied_read_remains_explicitly_local_when_quorum_read_fails() {
    let layout = layout(3, 1, vec![1, 2, 3]);
    let k = key(&layout, 0);
    let mut owners = Vec::new();
    for n in 1..=3 {
        owners.push(
            start(
                layout.clone(),
                n,
                directory(),
                Arc::new(Gate::default()),
                Arc::new(AtomicU64::new(0)),
                4,
            )
            .await,
        );
    }
    let h = owners[0].handle();
    h.propose(&k, vec![1], deadline()).await.unwrap();
    let leader = h.leader_hint(SlotId(0), deadline()).await.unwrap().unwrap();
    let leader_owner = owners.remove((leader - 1) as usize);
    for owner in owners {
        assert!(owner.shutdown(deadline()).await.complete);
    }
    let h = leader_owner.handle();
    assert_eq!(
        h.read_applied(SlotId(0), &[], deadline())
            .await
            .unwrap()
            .value,
        1u64.to_be_bytes()
    );
    let failure = h
        .read_linearizable(&k, &[], Instant::now() + Duration::from_millis(250))
        .await
        .unwrap_err();
    assert!(
        !failure.outcome_unknown(),
        "read must not be a write-unknown: {failure:?}"
    );
    assert!(leader_owner.shutdown(deadline()).await.complete);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rpc_layout_slot_and_size_validation_never_apply() {
    use bincode::Options;
    use multiraft_net::application_rpc::*;
    use serde::{Deserialize, Serialize};
    #[derive(Serialize)]
    struct Request {
        version: u32,
        cluster: String,
        digest: ManifestDigest,
        slot: SlotId,
        key: Vec<u8>,
        operation: Op,
    }
    #[allow(dead_code)]
    #[derive(Serialize)]
    enum Op {
        Hint,
        SlotHint,
        Write(Vec<u8>),
        Read(Vec<u8>),
    }
    #[derive(Deserialize)]
    struct Reply {
        version: u32,
        digest: ManifestDigest,
        result: Result<Value, RuntimeError>,
    }
    #[allow(dead_code)]
    #[derive(Deserialize)]
    enum Value {
        Hint(Option<u64>),
        Applied(SlotApplied),
        Read(Vec<u8>),
    }
    struct Reject;
    impl ApplicationRpcHandler for Reject {
        fn call(&self, _: RpcContext, _: RpcCall) -> RpcFuture<'_> {
            Box::pin(async {
                Err(RpcHandlerError::new(
                    RpcErrorKind::Unimplemented,
                    "test caller",
                ))
            })
        }
    }
    let layout = layout(1, 2, vec![1]);
    let k = key(&layout, 0);
    let applied = Arc::new(AtomicU64::new(0));
    let owner = start(
        layout.clone(),
        1,
        directory(),
        Arc::new(Gate::default()),
        applied.clone(),
        4,
    )
    .await;
    let config = ApplicationRpcConfig::new(
        99,
        "127.0.0.1:0".parse().unwrap(),
        vec![(1, layout.node(1).unwrap().application_rpc)],
    );
    let (caller, rpc) = ApplicationRpcOwner::start(config, Arc::new(Reject), deadline())
        .await
        .unwrap();
    for (slot, digest, command, expected) in [
        (
            SlotId(1),
            layout.digest(),
            vec![1],
            RuntimeError::InvalidRequest,
        ),
        (
            SlotId(0),
            ManifestDigest([0; 32]),
            vec![1],
            RuntimeError::LayoutMismatch,
        ),
        (
            SlotId(0),
            layout.digest(),
            vec![0; MAX_COMMAND_BYTES + 1],
            RuntimeError::PayloadTooLarge,
        ),
    ] {
        let payload = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .serialize(&Request {
                version: 1,
                cluster: layout.cluster_id().into(),
                digest,
                slot,
                key: k.clone(),
                operation: Op::Write(command),
            })
            .unwrap();
        let reply = rpc
            .call(
                1,
                RpcCall::new(0x534c4f54, 1, payload),
                RpcContext::new(deadline()),
            )
            .await
            .unwrap();
        let reply: Reply = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .deserialize(&reply.payload)
            .unwrap();
        assert_eq!(reply.version, 1);
        assert_eq!(reply.digest, layout.digest());
        assert!(matches!(reply.result, Err(ref e) if *e == expected));
    }
    assert_eq!(applied.load(Ordering::Acquire), 0);
    caller.shutdown(deadline()).await.unwrap();
    assert!(owner.shutdown(deadline()).await.complete);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partial_group_start_failure_rolls_back_and_existing_data_can_reopen() {
    let layout = layout(1, 2, vec![1]);
    let dir = directory();
    let gate = Arc::new(Gate::default());
    let applied = Arc::new(AtomicU64::new(0));
    let config = SlotNodeConfig::new(layout.clone(), 1, dir.clone());
    let g = gate.clone();
    let a = applied.clone();
    let result = SlotNodeOwner::start(
        config,
        move |context: multiraft_net::FsmFactoryContext| {
            if context.group_id() == 101 {
                return Err(anyhow::anyhow!("intentional second-group failure"));
            }
            Ok(Fsm {
                value: 0,
                gate: g.clone(),
                applied: a.clone(),
            })
        },
        deadline(),
    )
    .await;
    assert!(result.is_err());
    assert!(dir.join(MANIFEST_FILE_NAME).is_file());
    let reopened = start(layout, 1, dir, gate, applied, 4).await;
    assert!(reopened.shutdown(deadline()).await.complete);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn application_query_panic_fences_before_releasing_admission() {
    let layout = layout(1, 1, vec![1]);
    let k = key(&layout, 0);
    let owner = start(
        layout,
        1,
        directory(),
        Arc::new(Gate::default()),
        Arc::new(AtomicU64::new(0)),
        1,
    )
    .await;
    let h = owner.handle();
    assert_eq!(
        h.read_linearizable(&k, b"panic", deadline()).await,
        Err(RuntimeError::TaskPanicked)
    );
    assert!(!h.status().accepting);
    assert_eq!(h.inflight().global.requests, 0);
    assert_eq!(
        h.propose(&k, vec![1], deadline()).await,
        Err(RuntimeError::Closed)
    );
    let report = owner.shutdown(deadline()).await;
    assert_eq!(report.error, Some(RuntimeError::TaskPanicked));
    assert_eq!(report.remaining.global.requests, 0);
}

#[test]
fn owner_drop_outside_runtime_context_uses_origin_runtime_cleanup() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let layout = layout(1, 1, vec![1]);
    let dir = directory();
    let gate = Arc::new(Gate::default());
    let applied = Arc::new(AtomicU64::new(0));
    let owner = runtime.block_on(start(
        layout.clone(),
        1,
        dir.clone(),
        gate.clone(),
        applied.clone(),
        1,
    ));
    let h = owner.handle();
    drop(owner);
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), async {
            while h.status().local_recovery_ready {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let reopened = start(layout, 1, dir, gate, applied, 1).await;
        assert!(reopened.shutdown(deadline()).await.complete);
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn nonhosted_routes_past_failed_first_voter_and_reserves_probe_budget() {
    let layout = layout(4, 1, vec![1, 2, 3]);
    let k = key(&layout, 0);
    let mut owners = Vec::new();
    for node in 1..=4 {
        owners.push(
            start(
                layout.clone(),
                node,
                directory(),
                Arc::new(Gate::default()),
                Arc::new(AtomicU64::new(0)),
                4,
            )
            .await,
        );
    }
    let first = owners.remove(0);
    assert!(first.shutdown(deadline()).await.complete);
    let h2 = owners[0].handle();
    let h3 = owners[1].handle();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let a = h2.leader_hint(SlotId(0), deadline()).await.unwrap();
            let b = h3.leader_hint(SlotId(0), deadline()).await.unwrap();
            if a == b && a.is_some_and(|id| id == 2 || id == 3) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let router = owners[2].handle();
    let result = router
        .propose(&k, vec![1], Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    assert_eq!(result.effects, 1u64.to_be_bytes());
    assert!(router
        .leader_hint(SlotId(0), Instant::now() + Duration::from_secs(3))
        .await
        .unwrap()
        .is_some_and(|id| id == 2 || id == 3));
    // A dead peer that accepts TCP but never responds cannot consume the whole
    // request deadline: the later voter probe and ReadIndex still get a budget.
    let listener = tokio::net::TcpListener::bind(layout.node(1).unwrap().application_rpc)
        .await
        .unwrap();
    let blackhole = tokio::spawn(async move {
        let mut connections = Vec::new();
        loop {
            connections.push(listener.accept().await.unwrap().0);
        }
    });
    let value = router
        .read_linearizable(&k, &[], Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    assert_eq!(value, 1u64.to_be_bytes());
    blackhole.abort();
    let _ = blackhole.await;
    for owner in owners {
        assert!(owner.shutdown(deadline()).await.complete);
    }
}
