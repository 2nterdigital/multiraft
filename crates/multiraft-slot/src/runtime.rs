//! Owned fixed-slot runtime. Handles retain no idle native or listener resources.
use crate::limits::{Budget, MAX_COMMAND_BYTES, MAX_EFFECTS_BYTES, MAX_QUERY_BYTES};
use crate::runtime_error::native;
mod genesis;
mod node_config;
mod observation;
mod recovery;
mod start;
use crate::{
    ClusterLayout, InflightStatus, NodeId, RuntimeError, ShutdownReport, SlotId, SlotStatus,
};
use futures::FutureExt;
use multiraft_fsm::StateMachine;
use multiraft_net::{
    application_rpc::{ApplicationRpcHandle, ApplicationRpcOwner},
    NodeOwner, RuntimeHandle, StartupReport,
};
pub use node_config::SlotNodeConfig;
use std::{
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

/// Application-owned query semantics. Callbacks must be bounded and nonblocking.
/// Applications validate that command/query payload identity agrees with the routing key.
pub trait SlotStateMachine: StateMachine {
    fn query(&self, group: u64, request: &[u8]) -> Result<Vec<u8>, Self::Error>;
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
    pub transport_digest: crate::ManifestDigest,
    pub node_id: NodeId,
    pub native: RuntimeHandle<S>,
    rpc: Mutex<Option<ApplicationRpcHandle>>,
    owners: Mutex<Option<Owners<S>>>,
    accepting: AtomicBool,
    business_enabled: bool,
    transport_addresses: Option<crate::SlotTransportAddresses>,
    cleaning: AtomicBool,
    pub(crate) budget: Arc<Budget>,
    tasks: Mutex<JoinSet<()>>,
    done: watch::Sender<Option<ShutdownReport>>,
    unknown: AtomicU64,
    panicked: AtomicBool,
    runtime: tokio::runtime::Handle,
}
impl<S: SlotStateMachine> SlotNodeOwner<S> {
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
        if !self.business_enabled {
            return Err(RuntimeError::Unavailable);
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
    ) -> Result<crate::LinearizableView<Vec<u8>>, RuntimeError> {
        self.check(deadline)?;
        let _guard = self
            .budget
            .acquire(slot, key_bytes + query.len() + MAX_EFFECTS_BYTES)?;
        self.execute_query(slot, query, deadline).await
    }
    /// The value and the applied id it reflects, sampled under one FSM lock
    /// after this call's ReadIndex confirmation.
    pub(crate) async fn execute_query(
        self: &Arc<Self>,
        slot: SlotId,
        query: &[u8],
        deadline: Instant,
    ) -> Result<crate::LinearizableView<Vec<u8>>, RuntimeError> {
        self.check(deadline)?;
        let group = self
            .layout
            .group_for_slot(slot)
            .map_err(|_| RuntimeError::InvalidRequest)?;
        let result = std::panic::AssertUnwindSafe(self.native.read_linearizable_with_applied(
            group,
            deadline,
            |s| s.query(group, query),
        ))
        .catch_unwind()
        .await;
        let view = match result {
            Ok(result) => {
                let view = result.map_err(|e| native(e, false))?;
                crate::LinearizableView {
                    value: view.value.map_err(|_| RuntimeError::Application)?,
                    applied: view.applied,
                }
            }
            Err(_) => {
                self.panicked.store(true, Ordering::Release);
                self.begin_cleanup();
                return Err(RuntimeError::TaskPanicked);
            }
        };
        if view.value.len() > MAX_EFFECTS_BYTES {
            return Err(RuntimeError::PayloadTooLarge);
        }
        Ok(view)
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
    /// Return sockets confirmed by this strict startup, without asserting current reachability.
    ///
    /// # Errors
    /// Returns Closed after fencing, or Unavailable for the legacy startup entry point.
    pub fn local_transport_addresses(&self) -> Result<crate::SlotTransportAddresses, RuntimeError> {
        self.shared()?
            .transport_addresses
            .clone()
            .ok_or(RuntimeError::Unavailable)
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
                .is_some_and(|s| s.business_enabled && s.accepting.load(Ordering::Acquire)),
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
    /// Linearizable read that also returns the applied log id the value
    /// reflects, sampled under the same FSM lock on the replica that confirmed
    /// the read (possibly a forwarded leader). Same authority as
    /// [`Self::read_linearizable`]; the id is at or beyond its read point.
    /// A forwarded read needs a destination that also has this API: a peer
    /// built before it rejects the request with `InvalidRequest`.
    pub async fn read_linearizable_with_applied(
        &self,
        key: &[u8],
        query: &[u8],
        deadline: Instant,
    ) -> Result<crate::LinearizableView<Vec<u8>>, RuntimeError> {
        let slot = self.validate(key, query, MAX_QUERY_BYTES)?;
        let shared = self.shared()?;
        crate::rpc::route_read_with_applied(&shared, slot, key.to_vec(), query.to_vec(), deadline)
            .await
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
