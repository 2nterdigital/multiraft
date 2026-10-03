//! Publication, rollback and cancellation-independent shutdown of one node.
use super::*;
use crate::StateMachineFactory;

impl<S: StateMachine> NodeOwner<S> {
    /// Start the listener and recover every declared Group before returning handles.
    /// Data/All file roots confirm the native constructor's validated checkpoint
    /// and forced committed-log basis before consumer validation. This is local
    /// recovery, not quorum/leader readiness; all business truth reads still use
    /// ReadIndex. Memory/Os retain the native cluster-tail recovery wait.
    /// Failure and cancellation fence the partially built runtime and reclaim its resources.
    /// Application factories, apply/restore and query callbacks must be bounded: the
    /// runtime cannot preempt synchronous application code inside a Tokio worker.
    pub async fn start(
        config: RuntimeConfig,
        factory: impl StateMachineFactory<S>,
        deadline: Instant,
    ) -> Result<Self, RuntimeError> {
        Self::start_inner(config, factory, deadline, None).await
    }

    /// Install source observation before constructing any native Group.
    /// Source receivers retain no native runtime or application resources.
    pub async fn start_with_election_source(
        config: RuntimeConfig,
        factory: impl StateMachineFactory<S>,
        deadline: Instant,
        source: crate::ElectionSource,
    ) -> Result<Self, RuntimeError> {
        Self::start_inner(config, factory, deadline, Some(source)).await
    }

    async fn start_inner(
        config: RuntimeConfig,
        factory: impl StateMachineFactory<S>,
        deadline: Instant,
        source: Option<crate::ElectionSource>,
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
        let source_node_id = config.cluster.node_id;
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
        let mut attachment = source
            .as_ref()
            .map(|source| source.hub.attach(source_node_id))
            .transpose()?;
        let result = tokio::time::timeout_at(deadline, start_transport)
            .await
            .map_err(|_| RuntimeError::Deadline {
                phase: RuntimePhase::GroupStart,
                outcome_unknown: false,
            })
            .and_then(|result| {
                result.map_err(|error| RuntimeError::Source(MultiRaftError::Other(error)))
            });
        let mut node = match result {
            Ok(node) => node,
            Err(error) => {
                if let Some(source) = &source {
                    source.hub.close();
                }
                return Err(error);
            }
        };
        node.election_source = source.map(|source| source.hub);
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
                source_jobs: OwnedTasks::default(),
                startup_admission: Mutex::new(startup::StartupAdmission::default()),
                cleanup_started: AtomicBool::new(false),
                completed,
                abort_requests,
                closed: watch::channel(false).0,
                runtime: tokio::runtime::Handle::current(),
                cleanup_task: Mutex::new(None),
            }),
        };
        if let Some(attachment) = &mut attachment {
            attachment.transfer();
        }
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
        self.node.cancel_pending_validation();
        self.slots.close();
        if self.cleanup_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let shared = self.clone();
        // Owned cleanup outlives a canceled shutdown waiter. The graceful wait is
        // bounded; expiration retains rollback rather than abandoning construction.
        // Its Arc is the sole cleanup lifetime; request/observation handles remain Weak.
        let task =
            self.runtime.spawn(async move {
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
                    let source_result = shared
                        .source_jobs
                        .join()
                        .await
                        .map_err(|error| RuntimeError::Source(MultiRaftError::Other(error)));
                    let startup_result = shared
                        .startups
                        .join()
                        .await
                        .map_err(|error| RuntimeError::Source(MultiRaftError::Other(error)));
                    // A failed/panicked owner task must not bypass native cleanup.
                    let native_result = shared.node.shutdown().await.map_err(RuntimeError::Source);
                    source_result.and(startup_result).and(native_result)
                };
                let graceful = match tokio::time::timeout(CLEANUP_TIMEOUT, cleanup).await {
                    Ok(result) => result,
                    Err(_) => Err(RuntimeError::Deadline {
                        phase: RuntimePhase::Shutdown,
                        outcome_unknown: true,
                    }),
                };
                let result =
                    match graceful {
                        Ok(()) => Ok(()),
                        Err(original) => {
                            shared.abort_requests.send_replace(true);
                            shared.node.abort_background();
                            // Never abort construction without its native owner. A native
                            // or graceful timeout leaves actual release unconfirmed.
                            let _drained = shared.admission.write().await;
                            let startup_result = shared.startups.join().await.map_err(|error| {
                                RuntimeError::Source(MultiRaftError::Other(error))
                            });
                            // Keep this full stop/join future IN PLACE across every native
                            // budget window. A timer expiration abandons only that poll,
                            // never retained tasks, blocking children or FSM witnesses.
                            let stop = shared.node.shutdown_owned_groups();
                            tokio::pin!(stop);
                            let native_result = loop {
                                if let Ok(result) =
                                    tokio::time::timeout(CLEANUP_TIMEOUT, stop.as_mut()).await
                                {
                                    break result.map_err(RuntimeError::Source);
                                }
                            };
                            let source_result = shared.source_jobs.join().await.map_err(|error| {
                                RuntimeError::Source(MultiRaftError::Other(error))
                            });
                            let retained = source_result.and(startup_result).and(native_result);
                            // A real graceful source failure remains the original cause.
                            // For an expired graceful wait, a subsequently observed source
                            // failure is more specific; otherwise retain its uncertainty.
                            if matches!(original, RuntimeError::Deadline { .. }) {
                                retained.and(Err(original))
                            } else {
                                Err(original)
                            }
                        }
                    };
                shared
                    .completed
                    .send_replace(Some(result.map_err(Arc::new)));
            });
        *self.cleanup_task.lock().unwrap() = Some(task);
    }
}
