//! M2 gate: a wiped voter rejoins a formed Group without initializing.
//!
//! `InitializePolicy::Forbidden` skips pristine initialize and grace fallback
//! and waits (OpenRaft `wait_for_recovery`) until the first observed leader
//! commit is applied. Persisted Forbidden Groups restart normally. The wiped
//! voter models OpenRaft's documented rejoin: the leader side has
//! `allow_log_reversion` enabled; there is no application vote protection.
mod startup_support;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use multiraft_core::{ClusterConfig, StartupProvenance};
use multiraft_net::{
    InitializeDisposition, InitializePolicy, MultiRaft, NodeOwner, RuntimeHandle, StartupPhase,
};
use startup_support::*;
use tokio::time::{timeout, Instant};
use tracing::field::{Field, Visit};
use tracing::{Event, Metadata, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;
use tracing_subscriber::Layer;

/// Rejoin budget: within the contract's 30 s ceiling.
const REJOIN_BUDGET: Duration = Duration::from_secs(25);
const FORM_BUDGET: Duration = Duration::from_secs(5);
const CAPTURED_TARGETS: [&str; 1] = ["multiraft::startup"];

// ---------------------------------------------------------------- tracing capture

#[derive(Clone, Default)]
struct Capture {
    events: Arc<Mutex<Vec<BTreeMap<String, String>>>>,
}

impl Capture {
    fn global() -> &'static Capture {
        static CAPTURE: OnceLock<Capture> = OnceLock::new();
        CAPTURE.get_or_init(|| {
            let capture = Capture::default();
            tracing::subscriber::set_global_default(
                tracing_subscriber::registry().with(capture.clone()),
            )
            .expect("install test tracing capture");
            capture
        })
    }
    fn mark(&self) -> usize {
        self.events.lock().unwrap().len()
    }
    /// Events since `mark` with the given target, node, group and phase.
    fn count(&self, mark: usize, target: &str, node: u64, group: u64, phase: &str) -> usize {
        let node = node.to_string();
        let group = group.to_string();
        self.events.lock().unwrap()[mark..]
            .iter()
            .filter(|e| {
                e.get("target").map(String::as_str) == Some(target)
                    && e.get("node_id") == Some(&node)
                    && e.get("group_id") == Some(&group)
                    && e.get("phase").map(String::as_str) == Some(phase)
            })
            .count()
    }
}

#[derive(Default)]
struct Fields(BTreeMap<String, String>);
impl Visit for Fields {
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().to_owned(), value.to_string());
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.insert(field.name().to_owned(), value.to_string());
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
}

impl<S: Subscriber> Layer<S> for Capture {
    fn enabled(&self, metadata: &Metadata<'_>, _: Context<'_, S>) -> bool {
        CAPTURED_TARGETS.contains(&metadata.target())
    }
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        let target = event.metadata().target();
        if !CAPTURED_TARGETS.contains(&target) {
            return;
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        fields.0.insert("target".to_owned(), target.to_owned());
        self.events.lock().unwrap().push(fields.0);
    }
}

// ---------------------------------------------------------------- cluster helpers

struct Cluster {
    roots: Vec<tempfile::TempDir>,
    peers: Vec<(u64, SocketAddr)>,
    owners: Vec<Option<NodeOwner<Bytes>>>,
}

impl Cluster {
    async fn start(group: u64) -> Self {
        let roots: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
        let peers = vec![(1, addr()), (2, addr()), (3, addr())];
        let mut cluster = Self {
            owners: vec![None, None, None],
            roots,
            peers,
        };
        for id in 1..=3 {
            cluster.boot(id).await;
        }
        let formed = || batch(&[group], &[1, 2, 3], Some(1), FORM_BUDGET);
        let (h1, h2, h3) = (cluster.handle(1), cluster.handle(2), cluster.handle(3));
        let (a, b, c) = tokio::join!(
            h1.start_groups_with_preference(formed()),
            h2.start_groups_with_preference(formed()),
            h3.start_groups_with_preference(formed()),
        );
        for report in [a, b, c] {
            let report = report.unwrap();
            assert_eq!(report.groups[0].phase, StartupPhase::Ready);
        }
        cluster
    }
    fn root(&self, id: u64) -> &Path {
        self.roots[(id - 1) as usize].path()
    }
    async fn boot(&mut self, id: u64) {
        let root = self.root(id).to_owned();
        let owner = start_native(&root, id, &self.peers, Factory::new(&root)).await;
        self.owners[(id - 1) as usize] = Some(owner);
    }
    fn handle(&self, id: u64) -> RuntimeHandle<Bytes> {
        self.owners[(id - 1) as usize]
            .as_ref()
            .expect("node is running")
            .handle()
    }
    fn live(&self, ids: &[u64]) -> Vec<RuntimeHandle<Bytes>> {
        ids.iter().map(|id| self.handle(*id)).collect()
    }
    async fn stop(&mut self, id: u64) {
        let owner = self.owners[(id - 1) as usize]
            .take()
            .expect("node is running");
        owner.shutdown(deadline()).await.unwrap();
    }
    fn wipe(&self, id: u64) {
        let root: PathBuf = self.root(id).to_owned();
        std::fs::remove_dir_all(&root).unwrap();
        std::fs::create_dir_all(&root).unwrap();
    }
    async fn shutdown_all(mut self) {
        for id in 1..=3 {
            if self.owners[(id - 1) as usize].is_some() {
                self.stop(id).await;
            }
        }
    }
}

fn value(n: u64) -> Vec<u8> {
    n.to_le_bytes().to_vec()
}

fn decode(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes.try_into().expect("u64 value"))
}

/// Propose `from..=to` in order through a confirmed leader.
async fn propose_range(live: &[RuntimeHandle<Bytes>], group: u64, from: u64, to: u64) {
    let leader = leader(live, group).await;
    for n in from..=to {
        let applied = leader.propose(group, value(n), deadline()).await.unwrap();
        assert_eq!(decode(&applied.effects), n);
    }
}

async fn applied_index(handle: &RuntimeHandle<Bytes>, group: u64) -> Option<u64> {
    handle
        .local_group_status(group, deadline())
        .await
        .unwrap()
        .last_applied
        .map(|id| id.index)
}

/// Request native compaction until the durable snapshot purges through `min`.
async fn compact_through(handles: &[RuntimeHandle<Bytes>], group: u64, min: u64) {
    for handle in handles {
        timeout(Duration::from_secs(10), async {
            loop {
                let status = handle
                    .local_storage_status(group, deadline())
                    .await
                    .unwrap();
                if status.durable_snapshot.is_some()
                    && status.purged.is_some_and(|purged| purged.index >= min)
                {
                    return;
                }
                let _ = handle.request_compaction(group, deadline()).await;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("node {} did not purge through {min}", handle.node_id()));
    }
}

/// Wait until every handle has applied at least `index`.
async fn await_applied(handles: &[RuntimeHandle<Bytes>], group: u64, index: u64) {
    for handle in handles {
        timeout(Duration::from_secs(10), async {
            while applied_index(handle, group).await.is_none_or(|i| i < index) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("node {} did not apply {index}", handle.node_id()));
    }
}

async fn local_value(handle: &RuntimeHandle<Bytes>, group: u64) -> u64 {
    let bytes = handle
        .try_read_applied(group, deadline(), |fsm| {
            Ok::<_, std::convert::Infallible>(fsm.value.clone())
        })
        .await
        .unwrap();
    decode(&bytes)
}

// ---------------------------------------------------------------- tests

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn wiped_voter_rejoins_via_native_snapshot_without_initialize() {
    const GROUP: u64 = 41;
    const N: u64 = 40;
    const M: u64 = 25;
    let capture = Capture::global();
    let mut cluster = Cluster::start(GROUP).await;

    propose_range(&cluster.live(&[1, 2, 3]), GROUP, 1, N).await;
    let first = applied_index(
        &leader(&cluster.live(&[1, 2, 3]), GROUP).await.clone(),
        GROUP,
    )
    .await
    .unwrap();
    compact_through(&cluster.live(&[1, 2, 3]), GROUP, first).await;

    // Victim is node 1, the preferred initializer, leader or not: its loss
    // forces 2/3 to re-elect, and it must never initialize again.
    cluster.stop(1).await;
    cluster.wipe(1);
    propose_range(&cluster.live(&[2, 3]), GROUP, N + 1, N + M).await;
    let survivor = leader(&cluster.live(&[2, 3]), GROUP).await.clone();
    let committed = applied_index(&survivor, GROUP).await.unwrap();
    compact_through(&cluster.live(&[2, 3]), GROUP, committed).await;

    let mark = capture.mark();
    cluster.boot(1).await;
    let victim = cluster.handle(1);
    let begun = Instant::now();
    let report = victim
        .start_groups_with_preference(batch_with_policy(
            &[GROUP],
            &[1, 2, 3],
            Some(1),
            REJOIN_BUDGET,
            InitializePolicy::Forbidden,
        ))
        .await
        .unwrap();
    let rejoin = begun.elapsed();
    let g = &report.groups[0];
    assert_eq!(g.phase, StartupPhase::Ready);
    assert_eq!(g.provenance, Some(StartupProvenance::Pristine));
    assert_eq!(g.initialization, InitializeDisposition::NotDispatched);
    assert!(!g.fallback);
    assert_eq!(
        capture.count(mark, "multiraft::startup", 1, GROUP, "initialize_dispatch"),
        0,
        "Forbidden rejoin must not dispatch initialize"
    );

    // Ready means applied >= first observed leader commit, which covers all N+M.
    let applied = applied_index(&victim, GROUP).await.unwrap();
    assert!(
        applied >= committed,
        "victim applied {applied} < committed {committed}"
    );
    assert_eq!(local_value(&victim, GROUP).await, N + M);
    let storage = victim
        .local_storage_status(GROUP, deadline())
        .await
        .unwrap();
    assert!(
        storage.durable_snapshot.is_some(),
        "catch-up across the purged prefix must install a native snapshot"
    );

    let all = cluster.live(&[1, 2, 3]);
    let authority = leader(&all, GROUP).await.clone();
    assert_eq!(
        decode(
            &authority
                .read_linearizable(GROUP, deadline(), |fsm| fsm.value.clone())
                .await
                .unwrap()
        ),
        N + M
    );

    // Stop the remaining non-leader survivor: the rejoined victim must now be
    // part of every commit quorum.
    let leader_id = authority.node_id();
    let stopped = if leader_id == 1 {
        // The victim itself leads; stop one survivor, the other plus victim commit.
        2
    } else {
        [2, 3]
            .into_iter()
            .find(|id| *id != leader_id)
            .expect("a non-leader survivor")
    };
    // Stop only after every node has applied the leader's index, so the stop
    // never races replication of the already-acknowledged entries.
    let leader_applied = applied_index(&authority, GROUP).await.unwrap();
    await_applied(&all, GROUP, leader_applied).await;
    cluster.stop(stopped).await;
    let remaining: Vec<u64> = [1, 2, 3]
        .into_iter()
        .filter(|id| cluster.owners[(*id - 1) as usize].is_some())
        .collect();
    assert!(remaining.contains(&1));
    propose_range(&cluster.live(&remaining), GROUP, N + M + 1, N + M + 1).await;
    timeout(Duration::from_secs(5), async {
        while local_value(&victim, GROUP).await != N + M + 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("victim applies the quorum commit");
    eprintln!("rejoin via snapshot completed in {rejoin:?}");
    cluster.shutdown_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn forbidden_persisted_groups_cold_restart_without_initialize() {
    const GROUP: u64 = 43;
    let capture = Capture::global();
    let mut cluster = Cluster::start(GROUP).await;
    propose_range(&cluster.live(&[1, 2, 3]), GROUP, 1, 5).await;
    for id in 1..=3 {
        cluster.stop(id).await;
    }

    // A full cold restart under Forbidden must still elect: persisted voters
    // only recover and never initialize.
    let mark = capture.mark();
    for id in 1..=3 {
        cluster.boot(id).await;
    }
    let forbidden = || {
        batch_with_policy(
            &[GROUP],
            &[1, 2, 3],
            Some(1),
            FORM_BUDGET,
            InitializePolicy::Forbidden,
        )
    };
    let (h1, h2, h3) = (cluster.handle(1), cluster.handle(2), cluster.handle(3));
    let (r1, r2, r3) = tokio::join!(
        h1.start_groups_with_preference(forbidden()),
        h2.start_groups_with_preference(forbidden()),
        h3.start_groups_with_preference(forbidden()),
    );
    for report in [r1, r2, r3] {
        let report = report.unwrap();
        let g = &report.groups[0];
        assert_eq!(g.phase, StartupPhase::Ready);
        assert_eq!(g.provenance, Some(StartupProvenance::Persisted));
        assert_eq!(g.initialization, InitializeDisposition::NotDispatched);
        assert!(!g.fallback);
    }
    let all = cluster.live(&[1, 2, 3]);
    assert_eq!(
        decode(
            &leader(&all, GROUP)
                .await
                .read_linearizable(GROUP, deadline(), |fsm| fsm.value.clone())
                .await
                .unwrap()
        ),
        5
    );
    for id in 1..=3 {
        assert_eq!(
            capture.count(mark, "multiraft::startup", id, GROUP, "initialize_dispatch"),
            0
        );
    }
    cluster.shutdown_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forbidden_pristine_without_leader_fails_at_budget_without_ready() {
    const GROUP: u64 = 44;
    let capture = Capture::global();
    let root = tempfile::tempdir().unwrap();
    let peers = vec![(1, addr()), (2, addr()), (3, addr())];
    let owner = start_native(root.path(), 1, &peers, Factory::new(root.path())).await;
    let mark = capture.mark();
    let budget = Duration::from_millis(800);
    let begun = Instant::now();
    let failure = owner
        .handle()
        .start_groups_with_preference(batch_with_policy(
            &[GROUP],
            &[1, 2, 3],
            Some(1),
            budget,
            InitializePolicy::Forbidden,
        ))
        .await
        .expect_err("no leader exists: Forbidden must not report ready");
    assert!(begun.elapsed() >= budget);
    assert_eq!(failure.phase, StartupPhase::NativeWait);
    let g = &failure.report.groups[0];
    assert_eq!(g.provenance, Some(StartupProvenance::Pristine));
    assert_eq!(g.initialization, InitializeDisposition::NotDispatched);
    assert!(!g.fallback);
    assert_ne!(g.phase, StartupPhase::Ready);
    assert_eq!(
        capture.count(mark, "multiraft::startup", 1, GROUP, "initialize_dispatch"),
        0
    );
    let _ = owner.shutdown(deadline()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn grpc_facade_forbidden_create_group_never_initializes() {
    const GROUP: u64 = 45;
    let peers = vec![(1, addr()), (2, addr()), (3, addr())];
    let ids = [1, 2, 3];
    let mut nodes = Vec::new();
    for id in ids {
        let mut config = ClusterConfig::for_test(id, &ids);
        config.peers = peers.clone();
        config.data_dir = Default::default();
        nodes.push(MultiRaft::start_grpc(config).await.unwrap());
    }
    // Node 3 alone, Forbidden: no initialize, so no leader and no recovery.
    nodes[2]
        .create_group_with_policy(GROUP, &ids, InitializePolicy::Forbidden)
        .await
        .unwrap();
    assert!(nodes[2]
        .wait_for_recovery(GROUP, Duration::from_millis(500))
        .await
        .is_err());
    assert!(
        !nodes[2].is_initialized(GROUP).await.unwrap(),
        "Forbidden facade Group must not initialize itself"
    );
    // Nodes 1 and 2 form the Group; node 3 is caught up by their leader.
    for node in &nodes[..2] {
        node.create_group(GROUP, &ids).await.unwrap();
    }
    let leader_id = multiraft_net::wait_for_leader(&nodes, GROUP, Duration::from_secs(10))
        .await
        .expect("leader");
    let leader = nodes.iter().find(|n| n.node_id() == leader_id).unwrap();
    leader
        .propose(GROUP, multiraft_fsm::CounterFsm::encode_add(9, 1))
        .await
        .unwrap();
    nodes[2]
        .wait_for_recovery(GROUP, Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(
        nodes[2].with_fsm(GROUP, |fsm| fsm.value(GROUP)).await,
        Some(9)
    );
    for node in &nodes {
        node.shutdown().await.unwrap();
    }
}
