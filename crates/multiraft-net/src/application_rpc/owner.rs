//! Listener/operation lifetime owner. Idle handles are weak; stop wait cancellation is independent.
use super::*;
use crate::multiraft::tasks::OwnedTasks;
use crate::GrpcPeerChannelPool;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::sync::{oneshot, watch, OwnedSemaphorePermit, Semaphore};

pub struct ApplicationRpcOwner {
    shared: Arc<Shared>,
    address: SocketAddr,
}
#[derive(Clone)]
pub struct ApplicationRpcHandle {
    pub(super) shared: Weak<Shared>,
}
pub(super) struct Shared {
    pub core: Arc<Core>,
    listener: OwnedTasks,
    listener_stop: Mutex<Option<oneshot::Sender<()>>>,
    cleaning: AtomicBool,
    done: watch::Sender<Option<Result<(), RpcError>>>,
    runtime: tokio::runtime::Handle,
    cleanup: Mutex<Option<tokio::task::JoinHandle<()>>>,
}
pub(super) struct Core {
    pub node: NodeId,
    pub handler: Arc<dyn ApplicationRpcHandler>,
    pub channels: GrpcPeerChannelPool,
    pub keys: BTreeSet<String>,
    pub admission: Arc<Semaphore>,
    pub accepting: AtomicBool,
    pub cancel: watch::Sender<bool>,
    pub io_stop: watch::Sender<bool>,
    limit: u32,
}
impl Core {
    pub(super) fn admit(&self) -> Result<OwnedSemaphorePermit, RpcError> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(closed(RpcPhase::Admission));
        }
        let permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|e| match e {
                tokio::sync::TryAcquireError::Closed => closed(RpcPhase::Admission),
                tokio::sync::TryAcquireError::NoPermits => RpcError::new(
                    RpcErrorKind::ResourceExhausted,
                    RpcPhase::Admission,
                    RpcDispatch::NotDispatched,
                    "application RPC admission full",
                ),
            })?;
        if !self.accepting.load(Ordering::Acquire) {
            return Err(closed(RpcPhase::Admission));
        }
        Ok(permit)
    }
}
impl ApplicationRpcOwner {
    pub async fn start(
        config: ApplicationRpcConfig,
        handler: Arc<dyn ApplicationRpcHandler>,
        deadline: Instant,
    ) -> Result<(Self, ApplicationRpcHandle), RpcError> {
        validate(&config)?;
        if Instant::now() >= deadline {
            return Err(expired(RpcPhase::Admission, RpcDispatch::NotDispatched));
        }
        let listener = tokio::time::timeout_at(
            deadline,
            tokio::net::TcpListener::bind(config.listen_address),
        )
        .await
        .map_err(|_| expired(RpcPhase::Admission, RpcDispatch::NotDispatched))?
        .map_err(|_| {
            RpcError::new(
                RpcErrorKind::Unavailable,
                RpcPhase::Admission,
                RpcDispatch::NotDispatched,
                "bind application RPC listener failed",
            )
        })?;
        let address = listener.local_addr().map_err(|_| {
            RpcError::new(
                RpcErrorKind::Internal,
                RpcPhase::Admission,
                RpcDispatch::NotDispatched,
                "read application RPC address failed",
            )
        })?;
        let (cancel, _) = watch::channel(false);
        let shared = Arc::new(Shared {
            core: Arc::new(Core {
                node: config.local_node_id,
                handler,
                channels: GrpcPeerChannelPool::new(config.peers),
                keys: config.metadata_keys,
                admission: Arc::new(Semaphore::new(config.max_inflight)),
                accepting: AtomicBool::new(true),
                cancel,
                io_stop: watch::channel(false).0,
                limit: u32::try_from(config.max_inflight).expect("validated limit"),
            }),
            listener: OwnedTasks::default(),
            listener_stop: Mutex::new(None),
            cleaning: AtomicBool::new(false),
            done: watch::channel(None).0,
            runtime: tokio::runtime::Handle::current(),
            cleanup: Mutex::new(None),
        });
        let (stop, stopped) = oneshot::channel();
        *shared.listener_stop.lock().unwrap() = Some(stop);
        let service = super::wire::RpcService {
            shared: Arc::downgrade(&shared),
            node: config.local_node_id,
            limit: shared.core.limit,
        };
        // No await after arming rollback/registration: startup cannot lose the bound listener.
        let listener_core = shared.core.clone();
        let io_stop = listener_core.io_stop.subscribe();
        shared.listener.spawn_result(async move {
            let result = super::wire::serve(listener, service, stopped, io_stop).await;
            listener_core.accepting.store(false, Ordering::Release);
            listener_core.cancel.send_replace(true);
            result
        });
        let handle = ApplicationRpcHandle {
            shared: Arc::downgrade(&shared),
        };
        Ok((Self { shared, address }, handle))
    }
    pub fn local_address(&self) -> SocketAddr {
        self.address
    }
    pub fn handle(&self) -> ApplicationRpcHandle {
        ApplicationRpcHandle {
            shared: Arc::downgrade(&self.shared),
        }
    }
    pub fn fence(&self) {
        self.shared.core.accepting.store(false, Ordering::Release);
    }
    /// Cancellation abandons only this stop wait; origin-runtime cleanup remains retained.
    pub async fn shutdown(self, deadline: Instant) -> Result<(), RpcError> {
        self.shared.begin_cleanup(false);
        let mut done = self.shared.done.subscribe();
        let result = tokio::time::timeout_at(deadline, async {
            loop {
                if let Some(result) = done.borrow_and_update().clone() {
                    return result;
                }
                done.changed()
                    .await
                    .map_err(|_| closed(RpcPhase::Shutdown))?;
            }
        })
        .await
        .map_err(|_| expired(RpcPhase::Shutdown, RpcDispatch::MayHaveDispatched))?;
        // Join remains retained by Shared, and cleanup sets completion only after all resource joins.
        result
    }
}
impl Drop for ApplicationRpcOwner {
    fn drop(&mut self) {
        self.shared.begin_cleanup(true);
    }
}
impl Shared {
    fn begin_cleanup(self: &Arc<Self>, abnormal: bool) {
        self.core.accepting.store(false, Ordering::Release);
        if self.cleaning.swap(true, Ordering::AcqRel) {
            return;
        }
        if abnormal {
            self.core.cancel.send_replace(true);
        }
        let shared = self.clone();
        let job = self.runtime.spawn(async move {
            // Fence is separate from permit acquisition: no gate is held across a handler.
            // Graceful requests keep original budgets; after30s cleanup signals cancellation.
            let drain = shared
                .core
                .admission
                .clone()
                .acquire_many_owned(shared.core.limit);
            let drained = tokio::time::timeout(Duration::from_secs(30), drain).await;
            let _drained = match drained {
                Ok(Ok(guard)) => Some(guard),
                _ => {
                    shared.core.cancel.send_replace(true);
                    shared
                        .core
                        .admission
                        .clone()
                        .acquire_many_owned(shared.core.limit)
                        .await
                        .ok()
                }
            };
            shared.core.channels.shutdown().await;
            if let Some(stop) = shared.listener_stop.lock().unwrap().take() {
                let _ = stop.send(());
            }
            // Tonic retains spawned connections via its shutdown watcher. Let
            // replies flush gracefully, then end any stalled decoder/idle IO.
            let joined =
                match tokio::time::timeout(Duration::from_secs(5), shared.listener.join()).await {
                    Ok(result) => result,
                    Err(_) => {
                        shared.core.io_stop.send_replace(true);
                        shared.listener.join().await
                    }
                };
            let result = joined.map_err(|_| {
                RpcError::new(
                    RpcErrorKind::Internal,
                    RpcPhase::Shutdown,
                    RpcDispatch::MayHaveDispatched,
                    "application RPC listener task failed",
                )
            });
            shared.done.send_replace(Some(result));
        });
        *self.cleanup.lock().unwrap() = Some(job);
    }
}
fn validate(config: &ApplicationRpcConfig) -> Result<(), RpcError> {
    let bad = || {
        RpcError::new(
            RpcErrorKind::InvalidConfiguration,
            RpcPhase::Admission,
            RpcDispatch::NotDispatched,
            "invalid application RPC configuration",
        )
    };
    if !(1..=65_536).contains(&config.max_inflight)
        || config.metadata_keys.len() > 8
        || config
            .metadata_keys
            .iter()
            .any(|k| !super::metadata::valid_key(k))
    {
        return Err(bad());
    }
    let mut nodes = BTreeSet::new();
    let mut addresses = BTreeSet::new();
    for (node, address) in &config.peers {
        if *node == config.local_node_id
            || address.ip().is_unspecified()
            || address.port() == 0
            || !nodes.insert(*node)
            || !addresses.insert(*address)
        {
            return Err(bad());
        }
    }
    Ok(())
}
pub(super) fn closed(phase: RpcPhase) -> RpcError {
    RpcError::new(
        RpcErrorKind::Closed,
        phase,
        RpcDispatch::NotDispatched,
        "application RPC transport closed",
    )
}
pub(super) fn expired(phase: RpcPhase, dispatch: RpcDispatch) -> RpcError {
    RpcError::new(
        RpcErrorKind::DeadlineExceeded,
        phase,
        dispatch,
        "application RPC absolute deadline expired",
    )
}
