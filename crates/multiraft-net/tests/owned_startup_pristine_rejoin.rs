//! M2 gate: a wiped voter rejoins a formed Group without initializing.
//!
//! `InitializePolicy::Forbidden` skips pristine initialize and grace fallback,
//! waits until the first observed leader commit is applied, and arms the inbound
//! rejoin vote guard meanwhile. Persisted Forbidden Groups restart normally.
mod startup_support;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use multiraft_core::{ClusterConfig, FileLogSyncLevel, SnapshotMode, StartupProvenance};
use multiraft_net::{
    GroupServerState, InitializeDisposition, InitializePolicy, MultiRaft, NodeOwner, RuntimeConfig,
    RuntimeHandle, RuntimeTransport, SharedFabric, StartupPhase,
};
use startup_support::*;
use tokio::time::{timeout, Instant};
use tracing::field::{Field, Visit};
use tracing::{Event, Metadata, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;
use tracing_subscriber::Layer;

/// Rejoin budget: within the contract's 30 s ceiling, long enough for the
/// no-leader window in the vote guard test.
const REJOIN_BUDGET: Duration = Duration::from_secs(25);
const FORM_BUDGET: Duration = Duration::from_secs(5);
const CAPTURED_TARGETS: [&str; 2] = ["multiraft::startup", "multiraft::rejoin_guard"];

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
    /// `rejoin_guard_armed` decisions since `mark` for one node/group.
    fn armed(&self, mark: usize, node: u64, group: u64) -> Vec<bool> {
        let (node, group) = (node.to_string(), group.to_string());
        self.events.lock().unwrap()[mark..]
            .iter()
            .filter(|e| {
                e.get("phase").map(String::as_str) == Some("rejoin_guard")
                    && e.get("node_id") == Some(&node)
                    && e.get("group_id") == Some(&group)
            })
            .map(|e| e.get("rejoin_guard_armed").map(String::as_str) == Some("true"))
            .collect()
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
    /// `Some` runs every node on one shared in-process fabric instead of gRPC.
    fabric: Option<SharedFabric>,
}

impl Cluster {
    async fn start(group: u64) -> Self {
        Self::start_on(group, None).await
    }
    async fn start_on(group: u64, fabric: Option<SharedFabric>) -> Self {
        let roots: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
        let peers = vec![(1, addr()), (2, addr()), (3, addr())];
        let mut cluster = Self {
            owners: vec![None, None, None],
            roots,
            peers,
            fabric,
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
        let factory = Factory::new(self.root(id));
        self.boot_with(id, factory).await;
    }
    async fn boot_with(&mut self, id: u64, factory: Factory) {
        let root = self.root(id).to_owned();
        let owner = match &self.fabric {
            None => start_native(&root, id, &self.peers, factory).await,
            Some(fabric) => {
                let ids: Vec<_> = self.peers.iter().map(|(id, _)| *id).collect();
                let mut config = ClusterConfig::for_test(id, &ids);
                config.peers = self.peers.clone();
                config.data_dir = root.clone();
                config.file_log_sync_level = FileLogSyncLevel::Data;
                config.snapshot_mode = SnapshotMode::NativeDurable;
                config.retain_log_entries = 0;
                let mut runtime = RuntimeConfig::new(config, vec![]);
                runtime.transport = RuntimeTransport::InProcess(fabric.clone());
                NodeOwner::start(runtime, factory, deadline())
                    .await
                    .unwrap()
            }
        };
        self.owners[(id - 1) as usize] = Some(owner);
    }
    /// Take a node out for a concurrent shutdown.
    fn take(&mut self, id: u64) -> NodeOwner<Bytes> {
        self.owners[(id - 1) as usize]
            .take()
            .expect("node is running")
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
    assert_eq!(capture.armed(mark, 1, GROUP), vec![true]);
    assert_eq!(
        capture.count(mark, "multiraft::rejoin_guard", 1, GROUP, "release"),
        1,
        "startup completes only after the guard anchor is applied"
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
    let other = [2, 3]
        .into_iter()
        .find(|id| *id != leader_id)
        .expect("a non-leader survivor");
    if leader_id == 1 {
        // The victim itself leads; stop one survivor, the other plus victim commit.
        cluster.stop(2).await;
    } else {
        cluster.stop(other).await;
    }
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
async fn vote_guard_blocks_election_through_wiped_voter_before_catch_up() {
    vote_guard_scenario(42, None).await;
}

/// Same scenario over the in-process transport (`node.rs` drops the reply).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn vote_guard_blocks_election_in_process_transport() {
    vote_guard_scenario(48, Some(SharedFabric::new())).await;
}

async fn vote_guard_scenario(group: u64, fabric: Option<SharedFabric>) {
    const SHARED: u64 = 10;
    const AB_ONLY: u64 = 20;
    const WINDOW: Duration = Duration::from_secs(4);
    let capture = Capture::global();
    let mut cluster = Cluster::start_on(group, fabric).await;

    propose_range(&cluster.live(&[1, 2, 3]), group, 1, SHARED).await;
    let a = leader(&cluster.live(&[1, 2, 3]), group).await.node_id();
    let mut others = [1, 2, 3].into_iter().filter(|id| *id != a);
    let (b, c) = (others.next().unwrap(), others.next().unwrap());

    // C lags: entries SHARED+1..=SHARED+AB_ONLY commit on A+B only.
    cluster.stop(c).await;
    propose_range(&cluster.live(&[a, b]), group, SHARED + 1, SHARED + AB_ONLY).await;
    // B loses its disk; A crashes before B can hear from it.
    cluster.stop(b).await;
    cluster.wipe(b);
    cluster.stop(a).await;

    // C restarts from persisted state (Forbidden + persisted = normal restart).
    cluster.boot(c).await;
    let report = cluster
        .handle(c)
        .start_groups_with_preference(batch_with_policy(
            &[group],
            &[1, 2, 3],
            Some(1),
            FORM_BUDGET,
            InitializePolicy::Forbidden,
        ))
        .await
        .unwrap();
    assert_eq!(
        report.groups[0].provenance,
        Some(StartupProvenance::Persisted)
    );
    assert_eq!(local_value(&cluster.handle(c), group).await, SHARED);

    // B rejoins wiped and Forbidden; its startup waits for a leader.
    let mark = capture.mark();
    cluster.boot(b).await;
    let wiped = cluster.handle(b);
    let rejoin = tokio::spawn(async move {
        wiped
            .start_groups_with_preference(batch_with_policy(
                &[group],
                &[1, 2, 3],
                Some(1),
                REJOIN_BUDGET,
                InitializePolicy::Forbidden,
            ))
            .await
    });

    // Without the guard, C would win B's vote here and truncate A's entries.
    let lagging = cluster.handle(c);
    let window_end = Instant::now() + WINDOW;
    while Instant::now() < window_end {
        let status = lagging.local_group_status(group, deadline()).await.unwrap();
        assert_ne!(
            status.server_state,
            GroupServerState::Leader,
            "lagging node {c} won an election through the wiped voter {b}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        !rejoin.is_finished(),
        "wiped voter cannot recover without a leader"
    );
    let dropped = capture.count(mark, "multiraft::rejoin_guard", b, group, "drop_vote");
    assert!(
        dropped > 0,
        "the wiped voter must have dropped C's vote requests"
    );
    assert_eq!(
        capture.count(mark, "multiraft::rejoin_guard", b, group, "release"),
        0
    );

    // A returns: A+C elect, B catches up, and every committed entry survives.
    let a_returned = Instant::now();
    cluster.boot(a).await;
    cluster
        .handle(a)
        .start_groups_with_preference(batch_with_policy(
            &[group],
            &[1, 2, 3],
            Some(1),
            FORM_BUDGET,
            InitializePolicy::Forbidden,
        ))
        .await
        .unwrap();
    let report = rejoin.await.unwrap().unwrap();
    let caught_up = a_returned.elapsed();
    let g = &report.groups[0];
    assert_eq!(g.provenance, Some(StartupProvenance::Pristine));
    assert_eq!(g.initialization, InitializeDisposition::NotDispatched);
    assert!(!g.fallback);
    assert_eq!(
        capture.count(mark, "multiraft::startup", b, group, "initialize_dispatch"),
        0
    );
    assert_eq!(
        capture.count(mark, "multiraft::rejoin_guard", b, group, "release"),
        1
    );

    let all = cluster.live(&[1, 2, 3]);
    let authority = leader(&all, group).await;
    assert_eq!(
        decode(
            &authority
                .read_linearizable(group, deadline(), |fsm| fsm.value.clone())
                .await
                .unwrap()
        ),
        SHARED + AB_ONLY,
        "entries committed on A+B were lost"
    );
    propose_range(&all, group, SHARED + AB_ONLY + 1, SHARED + AB_ONLY + 1).await;
    for handle in &all {
        timeout(Duration::from_secs(5), async {
            while local_value(handle, group).await != SHARED + AB_ONLY + 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("every replica converges");
    }
    eprintln!(
        "guard window {WINDOW:?}: dropped={dropped}; recovery after A returned {caught_up:?}"
    );
    cluster.shutdown_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn forbidden_persisted_groups_cold_restart_without_guard() {
    const GROUP: u64 = 43;
    let capture = Capture::global();
    let mut cluster = Cluster::start(GROUP).await;
    propose_range(&cluster.live(&[1, 2, 3]), GROUP, 1, 5).await;
    for id in 1..=3 {
        cluster.stop(id).await;
    }

    // A full cold restart under Forbidden must still elect: persisted voters
    // never arm the guard (arming them would deadlock every election).
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
            capture.count(mark, "multiraft::rejoin_guard", id, GROUP, "drop_vote"),
            0
        );
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
    let capture = Capture::global();
    let peers = vec![(1, addr()), (2, addr()), (3, addr())];
    let ids = [1, 2, 3];
    let mut nodes = Vec::new();
    for id in ids {
        let mut config = ClusterConfig::for_test(id, &ids);
        config.peers = peers.clone();
        config.data_dir = Default::default();
        nodes.push(MultiRaft::start_grpc(config).await.unwrap());
    }
    let mark = capture.mark();
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
    // The facade decided the guard from pristine storage before publication.
    assert_eq!(
        capture.armed(mark, 3, GROUP),
        vec![true],
        "Forbidden pristine facade Group arms the rejoin guard"
    );
    for node in &nodes {
        node.shutdown().await.unwrap();
    }
}

/// A guarded voter that got membership from a snapshot but not the committed
/// tail must not campaign: elections stay disabled until the guard releases.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn guarded_voter_with_snapshot_membership_never_campaigns() {
    const GROUP: u64 = 46;
    const SHARED: u64 = 10;
    const SNAPSHOT: u64 = 15;
    const TAIL: u64 = 30;
    const WINDOW: Duration = Duration::from_secs(4);
    let capture = Capture::global();
    let mut cluster = Cluster::start(GROUP).await;
    propose_range(&cluster.live(&[1, 2, 3]), GROUP, 1, SHARED).await;
    let a = leader(&cluster.live(&[1, 2, 3]), GROUP).await.node_id();
    let mut others = [1, 2, 3].into_iter().filter(|id| *id != a);
    let (b, c) = (others.next().unwrap(), others.next().unwrap());

    // C lags at SHARED. A+B commit through SNAPSHOT, purge it, then commit the
    // tail SNAPSHOT+1..=TAIL as log entries only.
    cluster.stop(c).await;
    propose_range(&cluster.live(&[a, b]), GROUP, SHARED + 1, SNAPSHOT).await;
    let snapshot_index = applied_index(&cluster.handle(a), GROUP).await.unwrap();
    compact_through(&cluster.live(&[a]), GROUP, snapshot_index).await;
    propose_range(&cluster.live(&[a, b]), GROUP, SNAPSHOT + 1, TAIL).await;

    // B rejoins wiped; hold its snapshot restore so A can be stopped before it
    // sends any entry after the snapshot.
    cluster.stop(b).await;
    cluster.wipe(b);
    let mark = capture.mark();
    let gate = Gate::default();
    let _release = Release(vec![gate.clone()]);
    let mut factory = Factory::new(cluster.root(b));
    factory.restore = Some((GROUP, gate.clone()));
    cluster.boot_with(b, factory).await;
    let wiped = cluster.handle(b);
    let rejoin = tokio::spawn(async move {
        wiped
            .start_groups_with_preference(batch_with_policy(
                &[GROUP],
                &[1, 2, 3],
                Some(1),
                REJOIN_BUDGET,
                InitializePolicy::Forbidden,
            ))
            .await
    });
    timeout(Duration::from_secs(10), gate.entered.notified())
        .await
        .expect("B receives the snapshot");
    // Owned shutdown fences new outbound RPCs before stopping native Raft.
    let crashed = cluster.take(a);
    let stopping = tokio::spawn(async move { crashed.shutdown(deadline()).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    gate.release();
    stopping.await.unwrap().unwrap();
    assert!(
        capture.count(mark, "multiraft::rejoin_guard", b, GROUP, "anchor") > 0,
        "B observed the leader commit and snapshot"
    );

    // C restarts; B now has the snapshot membership (log = snapshot) but not the tail.
    cluster.boot(c).await;
    cluster
        .handle(c)
        .start_groups_with_preference(batch_with_policy(
            &[GROUP],
            &[1, 2, 3],
            Some(1),
            FORM_BUDGET,
            InitializePolicy::Forbidden,
        ))
        .await
        .unwrap();
    let lagging = cluster.handle(c);
    let window_end = Instant::now() + WINDOW;
    while Instant::now() < window_end {
        let status = lagging.local_group_status(GROUP, deadline()).await.unwrap();
        assert_ne!(
            status.leader_hint,
            Some(b),
            "guarded voter {b} campaigned and won with only the snapshot prefix"
        );
        assert_ne!(status.server_state, GroupServerState::Leader);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(!rejoin.is_finished());
    assert_eq!(
        capture.count(mark, "multiraft::rejoin_guard", b, GROUP, "release"),
        0,
        "B never reached the anchor without A"
    );

    // A returns: data intact, B caught up and released.
    cluster.boot(a).await;
    cluster
        .handle(a)
        .start_groups_with_preference(batch_with_policy(
            &[GROUP],
            &[1, 2, 3],
            Some(1),
            FORM_BUDGET,
            InitializePolicy::Forbidden,
        ))
        .await
        .unwrap();
    let report = rejoin.await.unwrap().unwrap();
    assert_eq!(report.groups[0].phase, StartupPhase::Ready);
    assert_eq!(
        capture.count(mark, "multiraft::rejoin_guard", b, GROUP, "release"),
        1
    );
    let all = cluster.live(&[1, 2, 3]);
    assert_eq!(
        decode(
            &leader(&all, GROUP)
                .await
                .read_linearizable(GROUP, deadline(), |fsm| fsm.value.clone())
                .await
                .unwrap()
        ),
        TAIL,
        "entries committed on A+B were lost"
    );
    cluster.shutdown_all().await;
}

/// Continuous writes during rejoin: the per-term anchor does not chase the
/// commit, so owned startup reaches Ready with the guard released, and the
/// rejoined voter then participates in a normal election.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn guard_releases_before_ready_under_continuous_writes() {
    const GROUP: u64 = 47;
    let capture = Capture::global();
    let mut cluster = Cluster::start(GROUP).await;
    propose_range(&cluster.live(&[1, 2, 3]), GROUP, 1, 20).await;
    let a = leader(&cluster.live(&[1, 2, 3]), GROUP).await.node_id();
    let b = [1, 2, 3].into_iter().find(|id| *id != a).unwrap();
    let c = [1, 2, 3]
        .into_iter()
        .find(|id| *id != a && *id != b)
        .unwrap();
    cluster.stop(b).await;
    cluster.wipe(b);

    // Eight concurrent writers keep the commit index moving during the rejoin.
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    let writing = Arc::new(AtomicBool::new(true));
    let acked_index = Arc::new(AtomicU64::new(0));
    let writes = Arc::new(AtomicU64::new(0));
    let leader_handle = leader(&cluster.live(&[a, c]), GROUP).await.clone();
    let mut writers = Vec::new();
    for w in 0..8u64 {
        let (writing, acked_index, writes) = (writing.clone(), acked_index.clone(), writes.clone());
        let leader = leader_handle.clone();
        writers.push(tokio::spawn(async move {
            let mut n = 0;
            while writing.load(Ordering::Acquire) {
                n += 1;
                let applied = leader
                    .propose(GROUP, value(1_000_000 * (w + 1) + n), deadline())
                    .await
                    .unwrap();
                acked_index.fetch_max(applied.index, Ordering::AcqRel);
                writes.fetch_add(1, Ordering::AcqRel);
            }
        }));
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mark = capture.mark();
    cluster.boot(b).await;
    let before = writes.load(Ordering::Acquire);
    let report = cluster
        .handle(b)
        .start_groups_with_preference(batch_with_policy(
            &[GROUP],
            &[1, 2, 3],
            Some(1),
            REJOIN_BUDGET,
            InitializePolicy::Forbidden,
        ))
        .await
        .unwrap();
    let during = writes.load(Ordering::Acquire) - before;
    assert_eq!(report.groups[0].phase, StartupPhase::Ready);
    assert_eq!(
        capture.count(mark, "multiraft::rejoin_guard", b, GROUP, "release"),
        1,
        "the guard must be released before Ready even while commits advance"
    );
    assert!(during > 0, "commits advanced during the rejoin");
    tokio::time::sleep(Duration::from_millis(200)).await;
    writing.store(false, Ordering::Release);
    for writer in writers {
        writer.await.unwrap();
    }
    let last_index = acked_index.load(Ordering::Acquire);

    // Kill the leader: the rejoined voter and the other survivor must elect.
    let leader_id = leader(&cluster.live(&[1, 2, 3]), GROUP).await.node_id();
    let victim = if leader_id == b { a } else { leader_id };
    cluster.stop(victim).await;
    let survivors: Vec<u64> = [1, 2, 3].into_iter().filter(|id| *id != victim).collect();
    assert!(survivors.contains(&b));
    let live = cluster.live(&survivors);
    let elected = leader(&live, GROUP).await.clone();
    elected
        .read_linearizable(GROUP, deadline(), |_| ())
        .await
        .unwrap();
    let applied = applied_index(&elected, GROUP).await.unwrap();
    assert!(
        applied >= last_index,
        "acknowledged index {last_index} lost (applied {applied})"
    );
    let marker = elected.propose(GROUP, value(7), deadline()).await.unwrap();
    assert!(marker.index > last_index);
    eprintln!(
        "continuous writes: {during} commits during rejoin, {} total, last acked index {last_index}; new leader {}",
        writes.load(Ordering::Acquire),
        elected.node_id()
    );
    cluster.shutdown_all().await;
}
