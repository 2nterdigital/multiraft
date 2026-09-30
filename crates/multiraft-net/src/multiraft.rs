//! Public MultiRaft facade over in-process [`Router`] or cross-process [`GrpcRouter`].
//!
//! Lives in `multiraft-net` (not `multiraft-core`) to avoid a core↔net dependency
//! cycle: net already depends on core for [`TypeConfig`].
//!
//! # In-process cluster
//!
//! - [`MultiRaft::start`] starts **one** node with its own [`Router`].
//! - [`MultiRaft::start_cluster`] starts N nodes sharing one [`Router`] (preferred for tests).
//! - [`SharedFabric`] exposes the shared [`Router`] + glue so chaos tests can
//!   `shutdown` a node and [`SharedFabric::start_node`] it again with the same
//!   `node_id` / `data_dir` / peers.
//!
//! # Cross-process (gRPC)
//!
//! - [`MultiRaft::start_grpc`] binds a tonic server on this node's peer addr and
//!   uses [`GrpcRouter`] for outbound Raft RPCs.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;

#[cfg(test)]
use std::sync::atomic::AtomicUsize;
#[cfg(test)]
use std::sync::atomic::Ordering as AtomicOrdering;

use crate::group_control::GroupControlSample;
use crate::group_control::GroupControlSampleError;
use crate::group_observation::initial_group_observation;
use crate::group_observation::GroupObservation;
use crate::group_observation::GroupObservationReceiver;
use crate::grpc::GrpcRouter;
use crate::grpc::GrpcServer;
use crate::network::GrpcNetworkFactory;
use crate::network::NetworkFactory;
use crate::node::GroupApp;
use crate::node::GroupMap;
use crate::node::Node;
use crate::router::Router;
use crate::snapshot_fetch::pull_snapshot_chunked;
use crate::standby_throttle::StandbyThrottle;
use crate::FsmFactoryContext;
use crate::GroupControlLayoutObservation;
use crate::GroupControlPreconditions;
use crate::GroupControlRequestEcho;
use crate::GroupControlRequestResult;
use crate::StateMachineFactory;
use multiraft_core::ClusterConfig;
use multiraft_core::GroupId;
use multiraft_core::MultiRaftError;
use multiraft_core::NodeId;
use multiraft_core::NodeRole;
use multiraft_core::ProposeApplied;
use multiraft_core::ProposeOk;
use multiraft_core::RecoverOutcome;
use multiraft_core::Request;
use multiraft_core::SnapshotAdvertisement;
use multiraft_core::SnapshotMode;
use multiraft_core::StaleRead;
use multiraft_core::TypeConfig;
use multiraft_core::STANDBY_SNAPSHOT_TRIGGER;
use multiraft_fsm::CounterFsm;
use multiraft_fsm::StateMachine;
use multiraft_store::CatalogEntry;
use multiraft_store::FileLogStoreOf;
use multiraft_store::FileLogStreamOptions;
use multiraft_store::MemLogStore;
use multiraft_store::Raft;
use multiraft_store::SmOptions;
use multiraft_store::SnapshotCatalog;
use multiraft_store::StateMachineStore;
use multiraft_store::TriggerCb;
use openraft::async_runtime::WatchReceiver;
use openraft::error::InitializeError;
use openraft::error::RaftError;
use openraft::type_config::TypeConfigExt;
use openraft::BasicNode;
use openraft::ChangeMembers;
use openraft::Config;
use openraft::ReadPolicy;

#[cfg(test)]
use tokio::sync::Notify;

mod application;
mod group_start;
mod lifecycle;
mod maintenance;
mod membership;
mod read;
mod recovery;
pub(crate) mod tasks;
mod transport_start;
pub use maintenance::{
    CompactionProgress, CompactionRejection, CompactionSubmission, DurableSnapshotObservation,
    LocalStorageStatus, NATIVE_SNAPSHOT_COMPACTION_CONTRACT,
};
mod snapshot_runtime;
use snapshot_runtime::SnapshotRuntime;

type LeaderCb = Arc<dyn Fn(u64, Option<u64>) + Send + Sync + 'static>;
type SnapshotReadyCb = Arc<dyn Fn(SnapshotAdvertisement) + Send + Sync + 'static>;

#[cfg(test)]
#[allow(dead_code)]
#[derive(Clone, Default)]
struct DaisySpawnProbe {
    spawn_attempts: Arc<AtomicUsize>,
    spawned: Arc<Notify>,
    permit_first_tick: Arc<Notify>,
    ticks: Arc<AtomicUsize>,
}

/// Coordinates `create_group` so membership is initialized once all peers are local.
#[derive(Clone, Default)]
struct ClusterGlue {
    /// group -> nodes that have created the local raft
    ready: Arc<Mutex<HashMap<GroupId, HashSet<NodeId>>>>,
    /// groups that already claimed initialize
    claimed_init: Arc<Mutex<HashSet<GroupId>>>,
}

impl ClusterGlue {
    fn mark_ready(&self, group: GroupId, node_id: NodeId, members: &[NodeId]) -> bool {
        let mut ready = self.ready.lock().unwrap();
        let set = ready.entry(group).or_default();
        set.insert(node_id);
        members.iter().all(|m| set.contains(m))
    }

    fn try_claim_init(&self, group: GroupId) -> bool {
        self.claimed_init.lock().unwrap().insert(group)
    }
}

/// Shared in-process fabric: one [`Router`] + cluster glue for many nodes.
///
/// Use this when a test needs to restart a node after [`MultiRaft::shutdown`]
/// without losing the peer mesh (unregister on shutdown; re-register on start).
#[derive(Clone, Default)]
pub struct SharedFabric {
    router: Router,
    glue: ClusterGlue,
}

impl SharedFabric {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn router(&self) -> &Router {
        &self.router
    }

    pub async fn start_node(&self, config: ClusterConfig) -> anyhow::Result<MultiRaft> {
        let factory: Arc<dyn StateMachineFactory<CounterFsm>> =
            Arc::new(|_| Ok::<CounterFsm, anyhow::Error>(CounterFsm::new()));
        MultiRaft::start_inner(config, self.router.clone(), self.glue.clone(), factory).await
    }

    async fn start_node_with_factory_arc<S: StateMachine>(
        &self,
        config: ClusterConfig,
        factory: Arc<dyn StateMachineFactory<S>>,
    ) -> anyhow::Result<MultiRaft<S>> {
        MultiRaft::start_inner(config, self.router.clone(), self.glue.clone(), factory).await
    }

    /// Start one in-process node using the supplied state-machine factory.
    pub async fn start_node_with_factory<S: StateMachine>(
        &self,
        config: ClusterConfig,
        factory: impl StateMachineFactory<S>,
    ) -> anyhow::Result<MultiRaft<S>> {
        self.start_node_with_factory_arc(config, Arc::new(factory))
            .await
    }
}

enum NetBackend {
    InProcess { router: Router, glue: ClusterGlue },
    Grpc { router: GrpcRouter },
}

/// Multi-Raft handle for one node (many groups).
pub struct MultiRaft<S: StateMachine = CounterFsm> {
    node_id: NodeId,
    config: ClusterConfig,
    net: NetBackend,
    groups: GroupMap<S>,
    tasks: tasks::OwnedTasks,
    reads: read::ReadRuntime,
    fsm_releases: Mutex<Vec<multiraft_store::StateMachineRelease>>,
    // Immutable native storage basis captured before successful Group construction.
    construction_recovery: Mutex<BTreeMap<GroupId, Option<openraft::alias::LogIdOf<TypeConfig>>>>,
    ingress_tasks: Arc<tasks::OwnedTasks>,
    listener_stop: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    ingress_accepting: Arc<std::sync::atomic::AtomicBool>,
    fsm_factory: Arc<dyn StateMachineFactory<S>>,
    leader_cbs: Arc<Mutex<Vec<LeaderCb>>>,
    snapshot_rt: Arc<SnapshotRuntime>,
    /// Standby node ids for replication throttle (shared with Router / GrpcRouter).
    standby_throttle: StandbyThrottle,
    #[cfg(test)]
    daisy_spawn_probe: Option<DaisySpawnProbe>,
}

impl MultiRaft<CounterFsm> {
    /// Start a single in-process node with a private [`Router`].
    pub async fn start(config: ClusterConfig) -> anyhow::Result<Self> {
        Self::start_with_factory(config, |_| {
            Ok::<CounterFsm, anyhow::Error>(CounterFsm::new())
        })
        .await
    }

    /// Start N nodes sharing one [`Router`] (in-process multi-node harness).
    ///
    /// `SocketAddr` peers in each config are unused; nodes are linked via the shared router.
    /// Internally uses [`SharedFabric`]; prefer that type when tests need node restart.
    pub async fn start_cluster(configs: Vec<ClusterConfig>) -> anyhow::Result<Vec<Self>> {
        Self::start_cluster_with_factory(configs, |_| {
            Ok::<CounterFsm, anyhow::Error>(CounterFsm::new())
        })
        .await
    }

    /// Start one node with cross-process tonic transport.
    ///
    /// Binds a gRPC server on this node's address from `config.peers` and uses
    /// [`GrpcRouter`] for outbound Raft RPCs to other peers.
    pub async fn start_grpc(config: ClusterConfig) -> anyhow::Result<Self> {
        Self::start_grpc_with_factory(config, |_| {
            Ok::<CounterFsm, anyhow::Error>(CounterFsm::new())
        })
        .await
    }
}

impl<S: StateMachine> MultiRaft<S> {
    /// Start N nodes sharing one in-process router and state-machine factory.
    pub async fn start_cluster_with_factory(
        configs: Vec<ClusterConfig>,
        factory: impl StateMachineFactory<S>,
    ) -> anyhow::Result<Vec<Self>> {
        let fabric = SharedFabric::new();
        let factory: Arc<dyn StateMachineFactory<S>> = Arc::new(factory);
        let mut nodes = Vec::with_capacity(configs.len());
        for config in configs {
            nodes.push(
                fabric
                    .start_node_with_factory_arc(config, factory.clone())
                    .await?,
            );
        }
        Ok(nodes)
    }

    /// Start one in-process node with a private [`Router`] and state-machine factory.
    ///
    /// Retains `factory` as a [`StateMachineFactory`] and invokes it later when
    /// [`Self::create_group`] constructs a local group.
    ///
    /// # Errors
    ///
    /// This constructor does not invoke `factory`; factory errors are returned later
    /// by [`Self::create_group`].
    pub async fn start_with_factory(
        config: ClusterConfig,
        factory: impl StateMachineFactory<S>,
    ) -> anyhow::Result<Self> {
        Self::start_inner(
            config,
            Router::new(),
            ClusterGlue::default(),
            Arc::new(factory),
        )
        .await
    }

    /// Start one node with cross-process tonic transport and a state-machine factory.
    pub async fn start_grpc_with_factory(
        config: ClusterConfig,
        factory: impl StateMachineFactory<S>,
    ) -> anyhow::Result<Self> {
        Self::start_grpc_inner(config, Arc::new(factory)).await
    }

    /// Standby ids currently subject to replication throttle.
    pub fn standby_throttle_ids(&self) -> HashSet<NodeId> {
        self.standby_throttle.standby_ids()
    }

    /// Newest local snapshot advertisement for `group` by `(last_term, last_index)`.
    pub fn best_snapshot_ad(&self, group: u64) -> Option<SnapshotAdvertisement> {
        self.snapshot_ads()
            .into_iter()
            .filter(|a| a.group == group && !a.fetch_url.is_empty())
            .max_by_key(|a| (a.last_term, a.last_index))
    }

    /// Live Standby snapshot installation is contained.
    ///
    /// # Errors
    ///
    /// Returns [`MultiRaftError::LiveSnapshotInstallUnsupported`] before HTTP,
    /// Raft, or FSM mutation.
    pub async fn pull_and_install_snapshot(
        &self,
        _group: u64,
        _fetch_url: &str,
    ) -> Result<(), MultiRaftError> {
        Err(MultiRaftError::LiveSnapshotInstallUnsupported)
    }

    /// Fetch snapshot bytes via chunked Range download (resume temp under data_dir / temp).
    pub async fn fetch_snapshot_bytes(
        &self,
        fetch_url: &str,
    ) -> Result<crate::snapshot_fetch::FetchedSnapshot, anyhow::Error> {
        let chunk = self.config.snapshot_fetch_chunk_bytes.max(1);
        let temp_dir = if self.config.data_dir.as_os_str().is_empty() {
            std::env::temp_dir().join("multiraft-snap-fetch")
        } else {
            self.config.data_dir.join("snap-fetch-tmp")
        };
        pull_snapshot_chunked(fetch_url, chunk, &temp_dir).await
    }

    /// Live Standby advertisement recovery is contained.
    ///
    /// # Errors
    ///
    /// Returns [`MultiRaftError::LiveSnapshotInstallUnsupported`] before reading
    /// advertisements or performing network, Raft, or FSM effects.
    #[allow(clippy::needless_return)]
    pub async fn try_recover_from_standby_ads(
        &self,
        _group: u64,
    ) -> Result<RecoverOutcome, MultiRaftError> {
        return Err(MultiRaftError::LiveSnapshotInstallUnsupported);
    }

    /// Live daisy snapshot synchronization is contained.
    ///
    /// # Errors
    ///
    /// Returns [`MultiRaftError::LiveSnapshotInstallUnsupported`] before reading
    /// daisy configuration or contacting an upstream.
    #[allow(clippy::needless_return)]
    pub async fn sync_from_daisy_upstream(
        &self,
        _group: u64,
    ) -> Result<RecoverOutcome, MultiRaftError> {
        return Err(MultiRaftError::LiveSnapshotInstallUnsupported);
    }

    /// Live daisy background synchronization is contained.
    ///
    /// Returns [`MultiRaftError::LiveSnapshotInstallUnsupported`] before reading
    /// configuration or creating a task.
    pub fn spawn_daisy_sync_loop(&self, _groups: Vec<u64>) -> Result<(), MultiRaftError> {
        Err(MultiRaftError::LiveSnapshotInstallUnsupported)
    }

    #[cfg(test)]
    fn set_daisy_spawn_probe_for_test(&mut self, probe: DaisySpawnProbe) {
        self.daisy_spawn_probe = Some(probe);
    }

    /// Observe this node's latest normalized control-plane state for a local Raft Group.
    ///
    /// The initial sample can become stale immediately and must not be treated as
    /// permission to perform business reads or writes.
    ///
    /// # Errors
    ///
    /// Returns [`MultiRaftError::UnknownGroup`] when this process has no local
    /// instance for `group`. Returns [`MultiRaftError::ObservationClosed`] if the
    /// initial server-metrics sample is already terminal.
    pub fn observe_group(
        &self,
        group: GroupId,
    ) -> Result<(GroupObservation, GroupObservationReceiver), MultiRaftError> {
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        let raw = raft.server_metrics();
        initial_group_observation(group, raw).map_err(MultiRaftError::from)
    }

    /// Leader proposes the magic standby-snapshot trigger log entry.
    pub async fn trigger_standby_snapshot(&self, group: u64) -> Result<ProposeOk, MultiRaftError> {
        self.propose(group, STANDBY_SNAPSHOT_TRIGGER.to_vec()).await
    }

    /// Record a snapshot advertisement (persisted when `data_dir` is set).
    pub fn record_snapshot_ad(&self, ad: SnapshotAdvertisement) {
        self.snapshot_rt.record_ad(ad);
    }

    /// Return all locally known snapshot advertisements.
    pub fn snapshot_ads(&self) -> Vec<SnapshotAdvertisement> {
        self.snapshot_rt.ads.lock().unwrap().clone()
    }

    /// Optional callback when a Standby finishes an async snapshot.
    pub fn on_snapshot_ready<F>(&self, cb: F)
    where
        F: Fn(SnapshotAdvertisement) + Send + Sync + 'static,
    {
        *self.snapshot_rt.on_snapshot_ready.lock().unwrap() = Some(Arc::new(cb));
    }

    /// Test hook: artificial delay inside `spawn_blocking` serialize/fsync.
    pub fn set_snapshot_serialize_delay(&self, delay: Option<Duration>) {
        *self.snapshot_rt.serialize_delay.lock().unwrap() = delay;
    }

    /// Durable catalog for this node (StandbyOffload + data_dir), if any.
    pub fn snapshot_catalog(&self) -> Option<Arc<SnapshotCatalog>> {
        self.snapshot_rt.catalog.clone()
    }

    /// Latest catalog entry for `group` on this node.
    pub fn latest_catalog_entry(&self, group: GroupId) -> Option<CatalogEntry> {
        self.snapshot_rt
            .catalog
            .as_ref()?
            .latest(group)
            .ok()
            .flatten()
    }

    /// Direct durable snapshot installation is contained.
    ///
    /// # Errors
    ///
    /// Returns [`MultiRaftError::LiveSnapshotInstallUnsupported`] before Raft or
    /// FSM effects.
    #[allow(clippy::needless_return)]
    pub async fn install_durable_snapshot(
        &self,
        _group: GroupId,
        _last_index: u64,
        _last_term: u64,
        _data: Vec<u8>,
    ) -> Result<(), MultiRaftError> {
        return Err(MultiRaftError::LiveSnapshotInstallUnsupported);
    }

    /// Standby catalog installation is contained.
    ///
    /// # Errors
    ///
    /// Returns [`MultiRaftError::LiveSnapshotInstallUnsupported`] before reading
    /// the catalog or mutating Raft/FSM state.
    #[allow(clippy::needless_return)]
    pub async fn try_install_from_standby_catalog(
        &self,
        _group: GroupId,
        _standby_catalog: &SnapshotCatalog,
    ) -> Result<(), MultiRaftError> {
        return Err(MultiRaftError::LiveSnapshotInstallUnsupported);
    }

    async fn try_initialize(
        &self,
        group: GroupId,
        members: &[NodeId],
    ) -> Result<(), MultiRaftError> {
        let raft = self
            .raft(group)
            .ok_or(MultiRaftError::UnknownGroup(group))?;
        let nodes = self.membership_nodes(members);
        match raft.initialize(nodes).await {
            Ok(()) => Ok(()),
            Err(RaftError::APIError(InitializeError::NotAllowed(_))) => Ok(()),
            Err(e) => Err(MultiRaftError::Other(anyhow::anyhow!(
                "initialize group: {e}"
            ))),
        }
    }

    /// Best-effort control sample for one existing local Group.
    ///
    /// This combines two public state point reads, one ReadIndex confirmation
    /// and one public metrics read from the same existing Raft handle. It does
    /// not read the application FSM and does not create missing groups.
    pub async fn read_group_control_sample(
        &self,
        group: GroupId,
        expected_voters: &[NodeId],
        max_sample_age: Duration,
        max_target_ack_age: Duration,
    ) -> Result<GroupControlSample, GroupControlSampleError> {
        let raft = self
            .raft(group)
            .ok_or(GroupControlSampleError::UnknownGroup { group_id: group })?;

        crate::group_control::read_group_control_sample(
            &raft,
            group,
            self.node_id,
            expected_voters,
            max_sample_age,
            max_target_ack_age,
        )
        .await
    }

    /// Submit one best-effort leadership transfer request after rechecking the
    /// observed control preconditions against a fresh public sample.
    pub async fn try_transfer_group_leader(
        &self,
        preconditions: &GroupControlPreconditions,
        expected_voters: &[NodeId],
        max_sample_age: Duration,
        max_target_ack_age: Duration,
    ) -> GroupControlRequestResult {
        let group = preconditions.group_id;
        let raft = match self.raft(group) {
            Some(raft) => raft,
            None => {
                return GroupControlRequestResult::PrecheckRejected {
                    echo: preconditions.echo(),
                    reason: crate::group_control::GroupControlPrecheckRejection::Sample(
                        GroupControlSampleError::UnknownGroup { group_id: group },
                    ),
                };
            }
        };

        crate::group_control::submit_group_control_transfer(
            &raft,
            group,
            self.node_id,
            preconditions,
            expected_voters,
            max_sample_age,
            max_target_ack_age,
        )
        .await
    }

    /// Classify the current independently observed layout for a previous
    /// wrapper request. This does not reinterpret the request result.
    pub async fn observe_group_control_layout(
        &self,
        echo: GroupControlRequestEcho,
        expected_voters: &[NodeId],
        max_sample_age: Duration,
        max_target_ack_age: Duration,
    ) -> GroupControlLayoutObservation {
        let sample_result = self
            .read_group_control_sample(
                echo.group_id,
                expected_voters,
                max_sample_age,
                max_target_ack_age,
            )
            .await;
        crate::group_control::classify_group_control_layout(echo, sample_result.as_ref())
    }

    pub fn is_leader(&self, group: u64) -> bool {
        self.raft(group).map(|r| r.is_leader()).unwrap_or(false)
    }

    pub fn leader(&self, group: u64) -> Option<u64> {
        self.raft(group)
            .and_then(|r| r.metrics().borrow_watched().current_leader)
    }

    /// Register a leader-change callback `(group_id, current_leader)`.
    ///
    /// Best-effort metrics watcher per group (spawned on create_group and for
    /// groups already present when this is called).
    pub fn on_leader_change<F>(&self, cb: F)
    where
        F: Fn(u64, Option<u64>) + Send + Sync + 'static,
    {
        let cb: LeaderCb = Arc::new(cb);
        self.leader_cbs.lock().unwrap().push(cb);

        let groups: Vec<(GroupId, Raft<S>)> = self
            .groups
            .lock()
            .unwrap()
            .iter()
            .map(|(&gid, app)| (gid, app.raft.clone()))
            .collect();

        for (gid, raft) in groups {
            self.spawn_leader_watch(gid, raft, self.leader_cbs.clone());
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// In-process shared [`Router`] (panics if this node was started with gRPC).
    pub fn router(&self) -> &Router {
        match &self.net {
            NetBackend::InProcess { router, .. } => router,
            NetBackend::Grpc { .. } => {
                panic!("router() is only available for in-process MultiRaft::start / start_cluster")
            }
        }
    }

    /// Distinct peer links: in-process router channels or gRPC peer channels.
    pub fn unique_peer_links(&self) -> usize {
        match &self.net {
            NetBackend::InProcess { router, .. } => router.unique_peer_links(),
            NetBackend::Grpc { router } => router.unique_peer_links(),
        }
    }

    /// Shut down all local Raft groups cleanly (flush / stop core tasks).
    ///
    /// For in-process mode, also unregisters this node from the shared [`Router`]
    /// so peers observe it as unreachable (used by demo admin leader-loss simulation).
    pub async fn shutdown(&self) -> Result<(), MultiRaftError> {
        tokio::time::timeout(Duration::from_secs(30), self.shutdown_owned_groups())
            .await
            .map_err(|_| {
                MultiRaftError::Other(anyhow::anyhow!(
                    "native shutdown did not quiesce within deadline"
                ))
            })?
    }

    fn raft(&self, group: GroupId) -> Option<Raft<S>> {
        self.groups
            .lock()
            .unwrap()
            .get(&group)
            .map(|g| g.raft.clone())
    }
}

/// Wait until any handle reports a leader for `group` (test helper).
pub async fn wait_for_leader(
    nodes: &[MultiRaft],
    group: GroupId,
    timeout: Duration,
) -> Option<NodeId> {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        for n in nodes {
            if let Some(leader) = n.leader(group) {
                if nodes.iter().any(|x| x.is_leader(group)) {
                    return Some(leader);
                }
            }
        }
        TypeConfig::sleep(Duration::from_millis(50)).await;
    }
    None
}

const MEMBERSHIP_RETRY_TIMEOUT: Duration = Duration::from_secs(15);
const MEMBERSHIP_RETRY_INTERVAL: Duration = Duration::from_millis(50);

fn transient_membership_err(label: &str, e: &impl std::fmt::Display) -> MultiRaftError {
    MultiRaftError::Other(anyhow::anyhow!(
        "{label}: {e} (retry after membership settles)"
    ))
}

fn membership_err_retryable(err: &MultiRaftError) -> bool {
    matches!(err, MultiRaftError::Other(e) if e.to_string().contains("configuration change"))
}

async fn retry_on_membership_pending<F, Fut>(mut op: F) -> Result<(), MultiRaftError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), MultiRaftError>>,
{
    let deadline = std::time::Instant::now() + MEMBERSHIP_RETRY_TIMEOUT;
    loop {
        match op().await {
            Ok(()) => return Ok(()),
            Err(e) if membership_err_retryable(&e) => {
                if std::time::Instant::now() >= deadline {
                    return Err(e);
                }
                tokio::time::sleep(MEMBERSHIP_RETRY_INTERVAL).await;
            }
            Err(e) => return Err(e),
        }
    }
}

#[cfg(test)]
mod tests;
