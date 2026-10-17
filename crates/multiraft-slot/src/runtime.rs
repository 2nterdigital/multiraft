//! Owned fixed-slot runtime. Handles retain no idle native or listener resources.
use crate::limits::{Budget, MAX_COMMAND_BYTES, MAX_EFFECTS_BYTES, MAX_QUERY_BYTES};
use crate::runtime_error::{native, startup_failure};
use crate::startup::{local_voter_slots, SlotStartupPlan};
use crate::{
    ClusterLayout, InflightStatus, NodeId, RuntimeError, ShutdownReport, SlotId, SlotLimits,
    SlotManifest, SlotStatus,
};
use futures::FutureExt;
use multiraft_core::{ClusterConfig, LogStorageBackend, NodeRole, SnapshotMode};
use multiraft_fsm::StateMachine;
use multiraft_net::{
    application_rpc::{ApplicationRpcConfig, ApplicationRpcHandle, ApplicationRpcOwner},
    GroupConfig, NodeOwner, RuntimeConfig, RuntimeHandle, StartupBatch, StartupGroup,
    StartupReport, StateMachineFactory,
};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, Weak,
    },
    time::Duration,
};
use tokio::{
    sync::{oneshot, watch},
    task::JoinSet,
    time::Instant,
};

/// Bound for confirming native release after a failed startup.
const CLEANUP: Duration = Duration::from_secs(60);

/// Application-owned query semantics. Callbacks must be bounded and nonblocking.
/// Applications validate that command/query payload identity agrees with the routing key.
pub trait SlotStateMachine: StateMachine {
    fn query(&self, group: u64, request: &[u8]) -> Result<Vec<u8>, Self::Error>;
}

pub struct SlotNodeConfig {
    pub layout: ClusterLayout,
    pub node_id: NodeId,
    pub data_dir: PathBuf,
    /// Timings and native snapshot policy limits may be tuned. Identity/storage
    /// fields are checked against the verified manifest before opening the node.
    pub native: ClusterConfig,
    pub limits: SlotLimits,
    /// Per-slot initialize policy, bootstrap grace and recovery budget for the
    /// native preference startup. The default allows initialization on every
    /// slot with a 1000 ms grace; see [`SlotStartupPlan`].
    pub startup: SlotStartupPlan,
}
impl SlotNodeConfig {
    pub fn new(layout: ClusterLayout, node_id: NodeId, data_dir: impl Into<PathBuf>) -> Self {
        let data_dir = data_dir.into();
        let mut native = ClusterConfig::new(
            node_id,
            layout.nodes().iter().map(|n| (n.node_id, n.raft)).collect(),
        );
        native.data_dir = data_dir.clone();
        native.log_storage_backend = LogStorageBackend::RocksDb;
        native.snapshot_mode = SnapshotMode::NativeDurable;
        native.raft_transport_fingerprint = Some(layout.digest().0);
        Self {
            layout,
            node_id,
            data_dir,
            native,
            limits: SlotLimits::default(),
            startup: SlotStartupPlan::default(),
        }
    }
    pub(crate) fn validate(&self) -> Result<(), RuntimeError> {
        self.limits.validate()?;
        self.layout
            .node(self.node_id)
            .map_err(|_| RuntimeError::InvalidConfig)?;
        if !self
            .startup
            .validate(&local_voter_slots(&self.layout, self.node_id))
        {
            return Err(RuntimeError::InvalidConfig);
        }
        let c = &self.native;
        let peers: Vec<_> = self
            .layout
            .nodes()
            .iter()
            .map(|n| (n.node_id, n.raft))
            .collect();
        if self.data_dir.as_os_str().is_empty()
            || c.data_dir != self.data_dir
            || c.node_id != self.node_id
            || c.peers != peers
            || c.raft_transport_fingerprint != Some(self.layout.digest().0)
            || c.log_storage_backend != LogStorageBackend::RocksDb
            || c.snapshot_mode != SnapshotMode::NativeDurable
            || c.role != NodeRole::Voter
            || c.daisy_upstream_base.is_some()
            || !c.standby_node_ids.is_empty()
            || c.admin_advertise_addr.is_some()
            || c.validate_snapshot_storage().is_err()
        {
            return Err(RuntimeError::InvalidConfig);
        }
        Ok(())
    }
}

/// Exact committed-entry result. `effects` are opaque application bytes.
#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SlotApplied {
    pub slot: SlotId,
    pub index: u64,
    pub term: u64,
    pub effects: Vec<u8>,
}

impl std::fmt::Debug for SlotApplied {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlotApplied")
            .field("slot", &self.slot)
            .field("index", &self.index)
            .field("term", &self.term)
            .field("effects_bytes", &self.effects.len())
            .finish()
    }
}

pub struct SlotNodeOwner<S: SlotStateMachine> {
    shared: Arc<Shared<S>>,
    startup: StartupReport,
}
pub struct SlotHandle<S: SlotStateMachine> {
    shared: Weak<Shared<S>>,
    layout: ClusterLayout,
    node_id: NodeId,
    budget: Arc<Budget>,
}
impl<S: SlotStateMachine> Clone for SlotHandle<S> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            layout: self.layout.clone(),
            node_id: self.node_id,
            budget: self.budget.clone(),
        }
    }
}
struct Owners<S: SlotStateMachine> {
    native: NodeOwner<S>,
    rpc: ApplicationRpcOwner,
}
pub(crate) struct Shared<S: SlotStateMachine> {
    pub layout: ClusterLayout,
    pub node_id: NodeId,
    pub native: RuntimeHandle<S>,
    rpc: Mutex<Option<ApplicationRpcHandle>>,
    owners: Mutex<Option<Owners<S>>>,
    accepting: AtomicBool,
    cleaning: AtomicBool,
    pub(crate) budget: Arc<Budget>,
    tasks: Mutex<JoinSet<()>>,
    done: watch::Sender<Option<ShutdownReport>>,
    unknown: AtomicU64,
    panicked: AtomicBool,
    runtime: tokio::runtime::Handle,
}
impl<S: SlotStateMachine> SlotNodeOwner<S> {
    /// Verify or create the slot manifest, start the native node, bring up all
    /// local voter slots with one native `start_groups_with_preference` batch
    /// (see [`SlotStartupPlan`]), then start the application RPC listener.
    ///
    /// Returns after local recovery of every voter slot, which is neither
    /// quorum nor leader readiness. The batch's per-group budget is the plan's
    /// `recovery_timeout` clamped to the time left before `deadline`, counted
    /// from each group's native registration.
    ///
    /// # Errors
    ///
    /// Any group failing or timing out fails the whole start with
    /// [`RuntimeError::StartupFailed`] carrying the native cause; everything
    /// already started (groups, listener, database) is released first. No
    /// retry or fallback beyond the native grace is attempted.
    pub async fn start(
        config: SlotNodeConfig,
        factory: impl StateMachineFactory<S>,
        deadline: Instant,
    ) -> Result<Self, RuntimeError> {
        config.validate()?;
        if Instant::now() >= deadline {
            return Err(RuntimeError::Deadline);
        }
        let manifest =
            SlotManifest::open_or_create(&config.data_dir, &config.layout, config.node_id)
                .map_err(|_| RuntimeError::ManifestMismatch)?;
        let layout = manifest.layout().clone();
        let mut groups = Vec::new();
        for assignment in layout
            .config()
            .slots
            .iter()
            .filter(|a| a.voters.contains(&config.node_id))
        {
            let group = GroupConfig {
                group_id: layout
                    .group_for_slot(assignment.slot)
                    .map_err(|_| RuntimeError::InvalidConfig)?,
                voters: assignment.voters.clone(),
            };
            groups.push(
                StartupGroup::new(group, Some(assignment.preferred_initializer))
                    .with_initialize(config.startup.policy(assignment.slot)),
            );
        }
        let mut cluster = config.native;
        // Source the native compatibility domain from the disk-verified layout.
        cluster.raft_transport_fingerprint = Some(manifest.layout_digest().0);
        // Groups are declared by the batch below, never by the owner itself.
        let mut runtime = RuntimeConfig::new(cluster, Vec::new());
        // The Slot budget is the public limit, native admission remains independently bounded.
        runtime.max_inflight = config.limits.global_requests;
        let native_owner = NodeOwner::start(runtime, factory, deadline)
            .await
            .map_err(|e| native(e, false))?;
        let startup = if groups.is_empty() {
            // A router-only node hosts no voter slot and has nothing to recover.
            StartupReport {
                input_digest: Some(manifest.layout_digest().0),
                node_id: config.node_id,
                configuration_verified: false,
                groups: Vec::new(),
            }
        } else {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let recovery_timeout = config.startup.recovery_timeout.min(remaining);
            if recovery_timeout.is_zero() {
                let _ = native_owner.shutdown(Instant::now() + CLEANUP).await;
                return Err(RuntimeError::Deadline);
            }
            let batch = StartupBatch {
                // Opaque native correlation; the layout digest identifies the cohort input.
                input_digest: Some(manifest.layout_digest().0),
                groups,
                grace: config.startup.bootstrap_grace,
                recovery_timeout,
            };
            // On failure the native batch already fenced and released the owner;
            // shutting it down here only confirms that release before returning.
            match native_owner
                .handle()
                .start_groups_with_preference(batch)
                .await
            {
                Ok(report) => report,
                Err(failure) => {
                    let error = startup_failure(&layout, &failure);
                    let cleanup = native_owner.shutdown(Instant::now() + CLEANUP).await;
                    tracing::warn!(target: "multiraft_slot::startup", node_id = config.node_id,
                        error = %failure, cleanup_confirmed = cleanup.is_ok(),
                        "slot native preference startup failed; owner released");
                    return Err(error);
                }
            }
        };
        let shared = Arc::new(Shared {
            native: native_owner.handle(),
            budget: Budget::new(config.limits, layout.slot_count()),
            layout,
            node_id: config.node_id,
            rpc: Mutex::new(None),
            owners: Mutex::new(None),
            accepting: AtomicBool::new(true),
            cleaning: AtomicBool::new(false),
            tasks: Mutex::new(JoinSet::new()),
            done: watch::channel(None).0,
            unknown: AtomicU64::new(0),
            panicked: AtomicBool::new(false),
            runtime: tokio::runtime::Handle::current(),
        });
        let address = shared
            .layout
            .node(config.node_id)
            .map_err(|_| RuntimeError::InvalidConfig)?
            .application_rpc;
        let peers = shared
            .layout
            .nodes()
            .iter()
            .filter(|n| n.node_id != config.node_id)
            .map(|n| (n.node_id, n.application_rpc))
            .collect();
        let rpc_config = ApplicationRpcConfig::new(config.node_id, address, peers);
        let result = ApplicationRpcOwner::start(
            rpc_config,
            Arc::new(crate::rpc::Handler {
                shared: Arc::downgrade(&shared),
            }),
            deadline,
        )
        .await;
        let (rpc_owner, rpc_handle) = match result {
            Ok(rpc) => rpc,
            Err(_) => {
                let _ = native_owner.shutdown(Instant::now() + CLEANUP).await;
                return Err(RuntimeError::Unavailable);
            }
        };
        *shared.rpc.lock().unwrap() = Some(rpc_handle);
        *shared.owners.lock().unwrap() = Some(Owners {
            native: native_owner,
            rpc: rpc_owner,
        });
        Ok(Self { shared, startup })
    }
    /// Local native startup facts of every voter slot: provenance, initialize
    /// disposition, fallback and phase. Neither quorum nor leader readiness.
    pub fn startup_report(&self) -> &StartupReport {
        &self.startup
    }
    pub fn handle(&self) -> SlotHandle<S> {
        SlotHandle {
            shared: Arc::downgrade(&self.shared),
            layout: self.shared.layout.clone(),
            node_id: self.shared.node_id,
            budget: self.shared.budget.clone(),
        }
    }
    /// Cancellation abandons only this wait. Cleanup keeps both owners and joins.
    pub async fn shutdown(self, deadline: Instant) -> ShutdownReport {
        self.shared.begin_cleanup();
        let mut done = self.shared.done.subscribe();
        let wait = async {
            loop {
                if let Some(report) = done.borrow_and_update().clone() {
                    return report;
                }
                if done.changed().await.is_err() {
                    return self.shared.report(false, Some(RuntimeError::Unavailable));
                }
            }
        };
        tokio::time::timeout_at(deadline, wait)
            .await
            .unwrap_or_else(|_| self.shared.report(false, Some(RuntimeError::Unknown)))
    }
}
impl<S: SlotStateMachine> Drop for SlotNodeOwner<S> {
    fn drop(&mut self) {
        self.shared.begin_cleanup();
    }
}
impl<S: SlotStateMachine> Shared<S> {
    fn report(&self, complete: bool, error: Option<RuntimeError>) -> ShutdownReport {
        ShutdownReport {
            complete,
            unknown_writes: self.unknown.load(Ordering::Acquire),
            remaining: self.budget.snapshot(),
            error,
        }
    }
    pub(crate) fn check(&self, deadline: Instant) -> Result<(), RuntimeError> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(RuntimeError::Closed);
        }
        if Instant::now() >= deadline {
            return Err(RuntimeError::Deadline);
        }
        Ok(())
    }
    fn begin_cleanup(self: &Arc<Self>) {
        let owners = {
            // Same lock as registration: after fencing, shutdown cannot miss a child.
            let _tasks = self.tasks.lock().unwrap();
            self.accepting.store(false, Ordering::Release);
            if self.cleaning.swap(true, Ordering::AcqRel) {
                return;
            }
            let owners = self.owners.lock().unwrap().take();
            if let Some(owners) = &owners {
                owners.rpc.fence();
            }
            owners
        };
        let shared = self.clone();
        self.runtime.spawn(async move {
            let deadline = Instant::now() + Duration::from_secs(120);
            let native_and_rpc = async {
                if let Some(owners) = owners {
                    let (n, r) = tokio::join!(
                        owners.native.shutdown(deadline),
                        owners.rpc.shutdown(deadline)
                    );
                    n.map_err(|e| native(e, false))?;
                    r.map_err(|_| RuntimeError::Unavailable)?;
                }
                Ok::<_, RuntimeError>(())
            };
            let tasks = futures::future::poll_fn(|cx| {
                let mut tasks = shared.tasks.lock().unwrap();
                loop {
                    match tasks.poll_join_next(cx) {
                        std::task::Poll::Ready(Some(result)) => {
                            if result.is_err() {
                                shared.panicked.store(true, Ordering::Release);
                            }
                        }
                        std::task::Poll::Ready(None) => return std::task::Poll::Ready(()),
                        std::task::Poll::Pending => return std::task::Poll::Pending,
                    }
                }
            });
            let (result, ()) = tokio::join!(native_and_rpc, tasks);
            let error = if shared.panicked.load(Ordering::Acquire) {
                Some(RuntimeError::TaskPanicked)
            } else {
                result.err()
            };
            shared
                .done
                .send_replace(Some(shared.report(error.is_none(), error)));
        });
    }
    pub(crate) fn rpc(&self) -> Result<ApplicationRpcHandle, RuntimeError> {
        self.rpc.lock().unwrap().clone().ok_or(RuntimeError::Closed)
    }
    fn submit(
        self: &Arc<Self>,
        slot: SlotId,
        key: Vec<u8>,
        command: Vec<u8>,
        deadline: Instant,
        routed: bool,
    ) -> Result<oneshot::Receiver<Result<SlotApplied, RuntimeError>>, RuntimeError> {
        self.check(deadline)?;
        let guard = self
            .budget
            .acquire(slot, key.len() + command.len() + MAX_EFFECTS_BYTES)?;
        let mut tasks = self.tasks.lock().unwrap();
        self.check(deadline)?;
        while let Some(result) = tasks.try_join_next() {
            if result.is_err() {
                self.accepting.store(false, Ordering::Release);
                self.panicked.store(true, Ordering::Release);
                return Err(RuntimeError::TaskPanicked);
            }
        }
        let (tx, rx) = oneshot::channel();
        let shared = self.clone();
        tasks.spawn(async move {
            let work = async {
                if routed {
                    shared.execute_local(slot, command, deadline).await
                } else {
                    crate::rpc::route_write(&shared, slot, key, command, deadline).await
                }
            };
            let result = match std::panic::AssertUnwindSafe(work).catch_unwind().await {
                Ok(r) => r,
                Err(_) => {
                    shared.panicked.store(true, Ordering::Release);
                    shared.begin_cleanup();
                    Err(RuntimeError::Unknown)
                }
            };
            if matches!(result, Err(RuntimeError::Unknown)) {
                shared.unknown.fetch_add(1, Ordering::AcqRel);
            }
            drop(guard);
            let _ = tx.send(result);
        });
        Ok(rx)
    }
    pub(crate) async fn execute_local(
        &self,
        slot: SlotId,
        command: Vec<u8>,
        deadline: Instant,
    ) -> Result<SlotApplied, RuntimeError> {
        self.check(deadline)?;
        let group = self
            .layout
            .group_for_slot(slot)
            .map_err(|_| RuntimeError::InvalidRequest)?;
        let receipt = self
            .native
            .submit_owned(group, command, deadline)
            .await
            .map_err(|e| native(e, true))?;
        let applied = receipt.completion().await.map_err(|e| native(e, true))?;
        if applied.effects.len() > MAX_EFFECTS_BYTES {
            return Err(RuntimeError::Unknown);
        }
        Ok(SlotApplied {
            slot,
            index: applied.index,
            term: applied.term,
            effects: applied.effects,
        })
    }
    pub(crate) async fn accept_write(
        self: &Arc<Self>,
        slot: SlotId,
        key: Vec<u8>,
        command: Vec<u8>,
        deadline: Instant,
    ) -> Result<SlotApplied, RuntimeError> {
        let rx = self.submit(slot, key, command, deadline, true)?;
        wait_write(rx, deadline).await
    }
    pub(crate) async fn local_query(
        self: &Arc<Self>,
        slot: SlotId,
        key_bytes: usize,
        query: &[u8],
        deadline: Instant,
    ) -> Result<Vec<u8>, RuntimeError> {
        self.check(deadline)?;
        let _guard = self
            .budget
            .acquire(slot, key_bytes + query.len() + MAX_EFFECTS_BYTES)?;
        self.execute_query(slot, query, deadline).await
    }
    pub(crate) async fn execute_query(
        self: &Arc<Self>,
        slot: SlotId,
        query: &[u8],
        deadline: Instant,
    ) -> Result<Vec<u8>, RuntimeError> {
        self.check(deadline)?;
        let group = self
            .layout
            .group_for_slot(slot)
            .map_err(|_| RuntimeError::InvalidRequest)?;
        let result = std::panic::AssertUnwindSafe(self.native.read_linearizable(
            group,
            deadline,
            |s| s.query(group, query),
        ))
        .catch_unwind()
        .await;
        let value = match result {
            Ok(result) => result
                .map_err(|e| native(e, false))?
                .map_err(|_| RuntimeError::Application)?,
            Err(_) => {
                self.panicked.store(true, Ordering::Release);
                self.begin_cleanup();
                return Err(RuntimeError::TaskPanicked);
            }
        };
        if value.len() > MAX_EFFECTS_BYTES {
            return Err(RuntimeError::PayloadTooLarge);
        }
        Ok(value)
    }
}
async fn wait_write(
    rx: oneshot::Receiver<Result<SlotApplied, RuntimeError>>,
    deadline: Instant,
) -> Result<SlotApplied, RuntimeError> {
    tokio::time::timeout_at(deadline, rx)
        .await
        .map_err(|_| RuntimeError::Unknown)?
        .map_err(|_| RuntimeError::Unknown)?
}
impl<S: SlotStateMachine> SlotHandle<S> {
    pub fn layout(&self) -> &ClusterLayout {
        &self.layout
    }
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }
    pub fn inflight(&self) -> InflightStatus {
        self.budget.snapshot()
    }
    pub fn status(&self) -> SlotStatus {
        let shared = self.shared.upgrade();
        SlotStatus {
            local_recovery_ready: shared.is_some(),
            accepting: shared
                .as_ref()
                .is_some_and(|s| s.accepting.load(Ordering::Acquire)),
            inflight: self.inflight(),
        }
    }
    fn shared(&self) -> Result<Arc<Shared<S>>, RuntimeError> {
        let s = self.shared.upgrade().ok_or(RuntimeError::Closed)?;
        if !s.accepting.load(Ordering::Acquire) {
            return Err(RuntimeError::Closed);
        }
        Ok(s)
    }
    fn validate(&self, key: &[u8], bytes: &[u8], max: usize) -> Result<SlotId, RuntimeError> {
        if bytes.len() > max {
            return Err(RuntimeError::PayloadTooLarge);
        }
        self.layout
            .slot_for_key(key)
            .map_err(|_| RuntimeError::PayloadTooLarge)
    }
    pub async fn propose(
        &self,
        key: &[u8],
        command: Vec<u8>,
        deadline: Instant,
    ) -> Result<SlotApplied, RuntimeError> {
        let slot = self.validate(key, &command, MAX_COMMAND_BYTES)?;
        self.propose_expected(key, slot, command, deadline).await
    }
    pub async fn propose_expected(
        &self,
        key: &[u8],
        expected_slot: SlotId,
        command: Vec<u8>,
        deadline: Instant,
    ) -> Result<SlotApplied, RuntimeError> {
        let slot = self.validate(key, &command, MAX_COMMAND_BYTES)?;
        if slot != expected_slot {
            return Err(RuntimeError::InvalidRequest);
        }
        let shared = self.shared()?;
        let rx = shared.submit(slot, key.to_vec(), command, deadline, false)?;
        wait_write(rx, deadline).await
    }
    pub async fn read_linearizable(
        &self,
        key: &[u8],
        query: &[u8],
        deadline: Instant,
    ) -> Result<Vec<u8>, RuntimeError> {
        let slot = self.validate(key, query, MAX_QUERY_BYTES)?;
        let shared = self.shared()?;
        crate::rpc::route_read(&shared, slot, key.to_vec(), query.to_vec(), deadline).await
    }
    /// Value and applied watermark sampled atomically under the same FSM lock.
    /// This local view never establishes leader authority or quorum freshness.
    pub async fn read_applied(
        &self,
        slot: SlotId,
        query: &[u8],
        deadline: Instant,
    ) -> Result<crate::AppliedView<Vec<u8>>, RuntimeError> {
        if query.len() > MAX_QUERY_BYTES {
            return Err(RuntimeError::PayloadTooLarge);
        }
        let s = self.shared()?;
        s.check(deadline)?;
        let _guard = s.budget.acquire(slot, query.len() + MAX_EFFECTS_BYTES)?;
        let group = self
            .layout
            .group_for_slot(slot)
            .map_err(|_| RuntimeError::InvalidRequest)?;
        let result = std::panic::AssertUnwindSafe(s.native.try_read_applied_with_watermark(
            group,
            deadline,
            |fsm| fsm.query(group, query),
        ))
        .catch_unwind()
        .await;
        let value = match result {
            Ok(result) => result.map_err(|_| RuntimeError::Unavailable)?,
            Err(_) => {
                s.panicked.store(true, Ordering::Release);
                s.begin_cleanup();
                return Err(RuntimeError::TaskPanicked);
            }
        };
        if value.value.len() > MAX_EFFECTS_BYTES {
            return Err(RuntimeError::PayloadTooLarge);
        }
        Ok(value)
    }
    pub async fn leader_hint(
        &self,
        slot: SlotId,
        deadline: Instant,
    ) -> Result<Option<NodeId>, RuntimeError> {
        let s = self.shared()?;
        crate::rpc::route_hint(&s, slot, deadline).await
    }
    pub async fn local_group_status(
        &self,
        slot: SlotId,
        deadline: Instant,
    ) -> Result<multiraft_net::LocalGroupStatus, RuntimeError> {
        let s = self.shared()?;
        s.native
            .local_group_status(
                self.layout
                    .group_for_slot(slot)
                    .map_err(|_| RuntimeError::InvalidRequest)?,
                deadline,
            )
            .await
            .map_err(|e| native(e, false))
    }
    pub async fn local_storage_status(
        &self,
        slot: SlotId,
        deadline: Instant,
    ) -> Result<multiraft_net::LocalStorageStatus, RuntimeError> {
        let s = self.shared()?;
        s.native
            .local_storage_status(
                self.layout
                    .group_for_slot(slot)
                    .map_err(|_| RuntimeError::InvalidRequest)?,
                deadline,
            )
            .await
            .map_err(|e| native(e, false))
    }
    pub async fn request_compaction(
        &self,
        slot: SlotId,
        deadline: Instant,
    ) -> Result<multiraft_net::CompactionSubmission, RuntimeError> {
        let s = self.shared()?;
        s.native
            .request_compaction(
                self.layout
                    .group_for_slot(slot)
                    .map_err(|_| RuntimeError::InvalidRequest)?,
                deadline,
            )
            .await
            .map_err(|e| native(e, false))
    }
}
