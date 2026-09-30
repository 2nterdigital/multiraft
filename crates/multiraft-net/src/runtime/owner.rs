//! Publication, rollback and cancellation-independent shutdown of one node.
use super::*;
use crate::StateMachineFactory;

impl<S: StateMachine> NodeOwner<S> {
    /// Start the listener and recover every declared Group before returning handles.
    /// Failure and cancellation fence the partially built runtime and reclaim its resources.
    /// Application factories, apply/restore and query callbacks must be bounded: the
    /// runtime cannot preempt synchronous application code inside a Tokio worker.
    pub async fn start(
        config: RuntimeConfig,
        factory: impl StateMachineFactory<S>,
        deadline: Instant,
    ) -> Result<Self, RuntimeError> {
        if config.max_inflight == 0 || config.max_inflight > 65_536 {
            return Err(RuntimeError::InvalidConfig(
                "max_inflight must be in 1..=65536",
            ));
        }
        let mut group_ids = BTreeSet::new();
        let peer_ids: BTreeSet<_> = config.cluster.peers.iter().map(|(id, _)| *id).collect();
        if peer_ids.len() != config.cluster.peers.len()
            || !peer_ids.contains(&config.cluster.node_id)
        {
            return Err(RuntimeError::InvalidConfig(
                "peer identities must be unique and include local node",
            ));
        }
        for group in &config.groups {
            let voters: BTreeSet<_> = group.voters.iter().copied().collect();
            if !group_ids.insert(group.group_id)
                || voters.is_empty()
                || voters.len() != group.voters.len()
                || !voters.is_subset(&peer_ids)
            {
                return Err(RuntimeError::InvalidConfig(
                    "groups and their declared voters must be unique and known",
                ));
            }
        }
        if Instant::now() >= deadline {
            return Err(RuntimeError::Deadline {
                phase: RuntimePhase::Admission,
                outcome_unknown: false,
            });
        }
        // Constructors have no await after spawning the transport owner, so cancellation
        // can never lose a just-created listener before this owner is armed.
        let start_transport = async {
            match config.transport {
                RuntimeTransport::Grpc => {
                    MultiRaft::start_grpc_with_factory(config.cluster, factory).await
                }
                RuntimeTransport::InProcess(fabric) => {
                    fabric
                        .start_node_with_factory(config.cluster, factory)
                        .await
                }
            }
        };
        let node = tokio::time::timeout_at(deadline, start_transport)
            .await
            .map_err(|_| RuntimeError::Deadline {
                phase: RuntimePhase::GroupStart,
                outcome_unknown: false,
            })?
            .map_err(|error| RuntimeError::Source(MultiRaftError::Other(error)))?;
        let (completed, _) = watch::channel(None);
        let (abort_requests, _) = watch::channel(false);
        let owner = Self {
            shared: Arc::new(RuntimeShared {
                node,
                accepting: AtomicBool::new(true),
                admission: Arc::new(RwLock::new(())),
                slots: Arc::new(Semaphore::new(config.max_inflight)),
                ready: Mutex::new(BTreeSet::new()),
                group_creation: tokio::sync::Mutex::new(()),
                startups: OwnedTasks::default(),
                cleanup_started: AtomicBool::new(false),
                completed,
                abort_requests,
                closed: watch::channel(false).0,
                runtime: tokio::runtime::Handle::current(),
                cleanup_task: Mutex::new(None),
            }),
        };
        let handle = owner.handle();
        for group in config.groups {
            if let Err(error) = handle.create_group(group, deadline).await {
                let _ = owner.shutdown(Instant::now() + CLEANUP_TIMEOUT).await;
                return Err(error);
            }
        }
        Ok(owner)
    }

    pub fn handle(&self) -> RuntimeHandle<S> {
        RuntimeHandle {
            shared: Arc::downgrade(&self.shared),
            node_id: self.shared.node.node_id(),
        }
    }

    /// Fence new requests, drain admitted calls and join native/listener/FSM owners.
    /// Success includes the actual application FSM destructor completing, so
    /// consumers can reuse their leases/data directories and the listener port.
    /// Cancellation abandons only this wait: the one cleanup task continues.
    pub async fn shutdown(self, deadline: Instant) -> Result<(), RuntimeError> {
        self.shared.begin_cleanup();
        self.shared.wait_cleanup(deadline).await
    }
}

impl<S: StateMachine> Drop for NodeOwner<S> {
    fn drop(&mut self) {
        if self.shared.completed.borrow().is_none() {
            self.shared.abort_requests.send_replace(true);
        }
        self.shared.begin_cleanup();
    }
}

impl<S: StateMachine> RuntimeShared<S> {
    pub(super) async fn wait_cleanup(&self, deadline: Instant) -> Result<(), RuntimeError> {
        let mut completion = self.completed.subscribe();
        let wait = async {
            loop {
                if let Some(result) = completion.borrow_and_update().clone() {
                    return result.map_err(RuntimeError::ShutdownFailed);
                }
                completion
                    .changed()
                    .await
                    .map_err(|_| RuntimeError::Closed)?;
            }
        };
        let result =
            tokio::time::timeout_at(deadline, wait)
                .await
                .map_err(|_| RuntimeError::Deadline {
                    phase: RuntimePhase::Shutdown,
                    outcome_unknown: true,
                })?;
        let task = self.cleanup_task.lock().unwrap().take();
        if let Some(task) = task {
            task.await
                .map_err(|error| RuntimeError::Source(MultiRaftError::Other(error.into())))?;
        }
        result
    }

    pub(super) fn begin_cleanup(self: &Arc<Self>) {
        self.accepting.store(false, Ordering::Release);
        self.closed.send_replace(true);
        self.slots.close();
        if self.cleanup_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let shared = self.clone();
        // Owned cleanup outlives a canceled shutdown waiter. The graceful wait is
        // bounded; expiration retains rollback rather than abandoning construction.
        // Its Arc is the sole cleanup lifetime; request/observation handles remain Weak.
        let task = self.runtime.spawn(async move {
            let cleanup = async {
                // Reserve part of the cleanup budget for native stop/join. If an
                // admitted request ignores a long caller deadline, interrupt its
                // waiter; interruption never retracts a dispatched write.
                let drained =
                    tokio::time::timeout(CLEANUP_TIMEOUT / 2, shared.admission.write()).await;
                let _drained = match drained {
                    Ok(guard) => guard,
                    Err(_) => {
                        shared.abort_requests.send_replace(true);
                        shared.admission.write().await
                    }
                };
                let startup_result = shared
                    .startups
                    .join()
                    .await
                    .map_err(|error| RuntimeError::Source(MultiRaftError::Other(error)));
                // A failed/panicked owner task must not bypass native cleanup.
                let native_result = shared.node.shutdown().await.map_err(RuntimeError::Source);
                startup_result.and(native_result)
            };
            let result = match tokio::time::timeout(CLEANUP_TIMEOUT, cleanup).await {
                Ok(result) => result,
                Err(_) => {
                    shared.abort_requests.send_replace(true);
                    shared.node.abort_background();
                    // Never abort a construction task without a native owner. Its
                    // deadline cancels only native pre-worker awaits; registered FSM
                    // release witnesses remain part of this retained rollback.
                    let _drained = shared.admission.write().await;
                    let _ = shared.startups.join().await;
                    let _ = shared.node.shutdown().await;
                    Err(RuntimeError::Deadline {
                        phase: RuntimePhase::Shutdown,
                        outcome_unknown: true,
                    })
                }
            };
            shared
                .completed
                .send_replace(Some(result.map_err(Arc::new)));
        });
        *self.cleanup_task.lock().unwrap() = Some(task);
    }
}
