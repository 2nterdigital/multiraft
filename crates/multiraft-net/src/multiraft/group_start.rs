//! Existing facade behavior, owned by this feature.
use super::*;

impl<S: StateMachine> MultiRaft<S> {
    pub(super) async fn spawn_local_group(
        &self,
        group: GroupId,
    ) -> Result<tokio::time::Instant, MultiRaftError> {
        // Every admitted mode is manual-only. Native protocol snapshots still
        // use the configured provider; Disabled has no legacy 5000-log policy.
        let snapshot_policy = openraft::SnapshotPolicy::Never;
        let snapshot_policy_name = "never";
        let storage = if self.config.data_dir.as_os_str().is_empty() {
            "memory"
        } else {
            "file"
        };
        let transport = match &self.net {
            NetBackend::InProcess { .. } => "in_process",
            NetBackend::Grpc { .. } => "grpc",
        };
        let group_directory = if self.config.data_dir.as_os_str().is_empty() {
            PathBuf::from("<memory>")
        } else {
            self.config.data_dir.join(format!("group-{group}"))
        };
        tracing::info!(
            target: "multiraft::recovery",
            operation = "group_start",
            phase = "start",
            node_id = self.node_id,
            group_id = group,
            transport,
            storage,
            directory = %group_directory.display(),
            role = ?self.config.role,
            snapshot_mode = ?self.config.snapshot_mode,
            snapshot_policy = snapshot_policy_name,
            file_log_sync_level = ?self.config.file_log_sync_level,
            install_snapshot_timeout_ms = self.config.install_snapshot_timeout_ms,
            snapshot_log_retention = self.config.snapshot_log_retention(),
            "starting local Raft group"
        );
        let config = Config {
            heartbeat_interval: self.config.heartbeat_interval_ms,
            election_timeout_min: self.config.election_timeout_min_ms,
            election_timeout_max: self.config.election_timeout_max_ms,
            install_snapshot_timeout: self.config.install_snapshot_timeout_ms,
            max_in_snapshot_log_to_keep: self.config.snapshot_log_retention(),
            snapshot_policy,
            // Wipe/restart chaos and follower catch-up can present a shorter log
            // than the leader last matched; without this openraft panics.
            allow_log_reversion: Some(true),
            api_batch_linger_ms: self.config.api_batch_linger_ms,
            api_batch_capacity: if self.config.api_batch_capacity == 0 {
                4096
            } else {
                self.config.api_batch_capacity
            },
            max_append_entries: Some(if self.config.max_append_entries == 0 {
                4096
            } else {
                self.config.max_append_entries
            }),
            max_payload_entries: if self.config.max_payload_entries == 0 {
                300
            } else {
                self.config.max_payload_entries
            },
            ..Default::default()
        };
        let config = Arc::new(config.validate().map_err(|error| {
            tracing::error!(
                target: "multiraft::recovery",
                operation = "group_start",
                phase = "error",
                node_id = self.node_id,
                group_id = group,
                transport,
                storage,
                directory = %group_directory.display(),
                error = %error,
                error_debug = ?error,
                "rejected invalid OpenRaft group configuration"
            );
            MultiRaftError::Other(error.into())
        })?);

        let context = FsmFactoryContext {
            node_id: self.node_id,
            group_id: group,
        };
        let fsm = self.fsm_factory.create(context).map_err(|source| {
            tracing::error!(
                target: "multiraft::recovery",
                operation = "group_start",
                phase = "error",
                node_id = self.node_id,
                group_id = group,
                transport,
                storage,
                directory = %group_directory.display(),
                reason_code = "application_factory_failed",
                "failed to construct application FSM"
            );
            MultiRaftError::Other(source.context(format!(
                "create FSM for node {}, group {}",
                context.node_id(),
                context.group_id(),
            )))
        })?;
        // StandbyOffload: never hot-dump FSM in openraft build_snapshot (voters or standby).
        let allow_hot_build = self.config.snapshot_mode != SnapshotMode::StandbyOffload;

        let sm_holder: Arc<OnceLock<multiraft_store::WeakStateMachineStore<S>>> =
            Arc::new(OnceLock::new());
        let on_standby_trigger = if self.config.role == NodeRole::Standby
            && self.config.snapshot_mode == SnapshotMode::StandbyOffload
        {
            let catalog = self.snapshot_rt.catalog.clone().ok_or_else(|| {
                let error = anyhow::anyhow!(
                    "StandbyOffload Standby requires non-empty data_dir for SnapshotCatalog"
                );
                tracing::error!(
                    target: "multiraft::recovery",
                    operation = "group_start",
                    phase = "error",
                    node_id = self.node_id,
                    group_id = group,
                    transport,
                    storage,
                    directory = %group_directory.display(),
                    error = %error,
                    error_debug = ?error,
                    "missing StandbyOffload snapshot catalog"
                );
                MultiRaftError::Other(error)
            })?;
            Some(standby::trigger(
                &self.snapshot_rt,
                sm_holder.clone(),
                catalog,
                self.node_id,
            ))
        } else {
            None
        };

        if self.config.snapshot_mode != SnapshotMode::NativeDurable
            && !self.config.data_dir.as_os_str().is_empty()
        {
            let native_root = self
                .config
                .data_dir
                .join("snapshots")
                .join(group.to_string())
                .join("native-v1");
            match std::fs::symlink_metadata(native_root) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
                Err(error) => return Err(MultiRaftError::Other(error.into())),
                Ok(_) => {
                    return Err(MultiRaftError::Other(anyhow::anyhow!(
                        "root requires native durable snapshot provider"
                    )))
                }
            }
        }
        let state_machine_store = if self.config.snapshot_mode == SnapshotMode::NativeDurable {
            // Every native Group startup, including the legacy facade/owner path,
            // bounds and validates orphan cleanup before loading recovery authority.
            self.snapshot_rt
                .catalog
                .as_ref()
                .expect("validated durable catalog")
                .startup_provenance(group, self.config.max_snapshot_bytes)
                .map_err(|error| MultiRaftError::Other(error.into()))?;
            StateMachineStore::with_native_options(
                group,
                fsm,
                multiraft_store::NativeSmOptions {
                    catalog: self
                        .snapshot_rt
                        .catalog
                        .clone()
                        .expect("validated durable catalog"),
                    max_snapshot_bytes: self.config.max_snapshot_bytes,
                    build_budget: self.snapshot_rt.build_budget.clone(),
                },
            )
            .map_err(|error| MultiRaftError::Other(error.into()))?
        } else {
            StateMachineStore::with_options(
                group,
                fsm,
                SmOptions {
                    allow_hot_build,
                    catalog: self.snapshot_rt.catalog.clone(),
                    on_standby_trigger,
                },
            )
        };
        let state_machine_store = state_machine_store
            .with_validation_options(multiraft_store::ValidationOptions {
                deadline: self.fsm_factory.validation_timeout(context),
                budget: self
                    .fsm_factory
                    .validation_budget()
                    .unwrap_or_else(|| self.snapshot_rt.validation_budget.clone()),
            })
            .map_err(|error| MultiRaftError::Other(error.into()))?;
        state_machine_store.begin_recovery_validation();
        if self
            .snapshot_rt
            .validation_closed
            .load(std::sync::atomic::Ordering::Acquire)
        {
            state_machine_store.cancel_pending_validation();
        }
        {
            let mut releases = self.fsm_releases.lock().unwrap();
            releases.retain(|release| !release.is_released());
            releases.push(state_machine_store.release_observer());
        }
        let _ = sm_holder.set(state_machine_store.downgrade());

        let mut durable_basis = None;
        let raft = match &self.net {
            NetBackend::InProcess { router, .. } => {
                let network = crate::election_source::ObservedFactory::new(NetworkFactory::new(router.clone(), group), group, self.election_source.clone());
                if self.config.data_dir.as_os_str().is_empty() {
                    let log_store = MemLogStore::default();
                    openraft::Raft::new_with_election_observer(
                        self.node_id,
                        config,
                        network,
                        log_store,
                        state_machine_store.clone(),
                        self.election_source.clone().map(|hub| Arc::new(crate::election_source::NativeSourceObserver { group, hub }) as Arc<dyn openraft::election_observer::ElectionObserver<TypeConfig>>),
                    )
                    .await
                } else {
                    let dir = self.config.data_dir.join(format!("group-{group}"));
                    let mut log_store = FileLogStoreOf::open_with_full_options(
                        &dir,
                        self.config.file_log_coalesce_us,
                        self.config.file_log_sync_level,
                        FileLogStreamOptions {
                            stream_buf_bytes: self.config.file_log_stream_buf_bytes,
                            stream_flush_ms: self.config.file_log_stream_flush_ms,
                            hold_overlap: self.config.file_log_hold_overlap,
                        },
                    )
                    .map_err(|error| {
                        tracing::error!(
                            target: "multiraft::recovery",
                            operation = "group_start",
                            phase = "error",
                            node_id = self.node_id,
                            group_id = group,
                            transport,
                            storage,
                            directory = %dir.display(),
                            error = %error,
                            error_debug = ?error,
                            "failed to open file-backed Raft log"
                        );
                        let message = format!(
                            "open file log for node {}, group {} at {}: {error}",
                            self.node_id, group, dir.display()
                        );
                        MultiRaftError::Other(anyhow::Error::new(error).context(message))
                    })?;
                    if recovery::durable_local_mode(&self.config) {
                        durable_basis = Some(recovery::construction_basis(group, &mut log_store, &state_machine_store).await?);
                    }
                    openraft::Raft::new_with_election_observer(
                        self.node_id,
                        config,
                        network,
                        log_store,
                        state_machine_store.clone(),
                        self.election_source.clone().map(|hub| Arc::new(crate::election_source::NativeSourceObserver { group, hub }) as Arc<dyn openraft::election_observer::ElectionObserver<TypeConfig>>),
                    )
                    .await
                }
            }
            NetBackend::Grpc { router } => {
                let network = crate::election_source::ObservedFactory::new(GrpcNetworkFactory::new(router.clone(), group), group, self.election_source.clone());
                if self.config.data_dir.as_os_str().is_empty() {
                    let log_store = MemLogStore::default();
                    openraft::Raft::new_with_election_observer(
                        self.node_id,
                        config,
                        network,
                        log_store,
                        state_machine_store.clone(),
                        self.election_source.clone().map(|hub| Arc::new(crate::election_source::NativeSourceObserver { group, hub }) as Arc<dyn openraft::election_observer::ElectionObserver<TypeConfig>>),
                    )
                    .await
                } else {
                    let dir = self.config.data_dir.join(format!("group-{group}"));
                    let mut log_store = FileLogStoreOf::open_with_full_options(
                        &dir,
                        self.config.file_log_coalesce_us,
                        self.config.file_log_sync_level,
                        FileLogStreamOptions {
                            stream_buf_bytes: self.config.file_log_stream_buf_bytes,
                            stream_flush_ms: self.config.file_log_stream_flush_ms,
                            hold_overlap: self.config.file_log_hold_overlap,
                        },
                    )
                    .map_err(|error| {
                        tracing::error!(
                            target: "multiraft::recovery",
                            operation = "group_start",
                            phase = "error",
                            node_id = self.node_id,
                            group_id = group,
                            transport,
                            storage,
                            directory = %dir.display(),
                            error = %error,
                            error_debug = ?error,
                            "failed to open file-backed Raft log"
                        );
                        let message = format!(
                            "open file log for node {}, group {} at {}: {error}",
                            self.node_id, group, dir.display()
                        );
                        MultiRaftError::Other(anyhow::Error::new(error).context(message))
                    })?;
                    if recovery::durable_local_mode(&self.config) {
                        durable_basis = Some(recovery::construction_basis(group, &mut log_store, &state_machine_store).await?);
                    }
                    openraft::Raft::new_with_election_observer(
                        self.node_id,
                        config,
                        network,
                        log_store,
                        state_machine_store.clone(),
                        self.election_source.clone().map(|hub| Arc::new(crate::election_source::NativeSourceObserver { group, hub }) as Arc<dyn openraft::election_observer::ElectionObserver<TypeConfig>>),
                    )
                    .await
                }
            }
        }
        .map_err(|error| {
            let classified = recovery::native_recovery_error(group, multiraft_core::RecoveryStage::Construct, error);
            let (recovery_phase, recovery_failure) = recovery::diagnostic_fields(&classified);
            tracing::error!(target: "multiraft::recovery", operation = "group_start", phase = "error",
                recovery_phase, recovery_failure,
                node_id = self.node_id, group_id = group, transport, storage,
                directory = %group_directory.display(), error = %classified,
                "failed to recover or start local Raft group");
            classified
        })?;

        // Do not add an await between native worker spawn and registry insertion.
        // Only successful native construction validates this captured storage basis.
        if let Some(target) = durable_basis {
            self.construction_recovery
                .lock()
                .unwrap()
                .insert(group, target);
        }
        let registered_at;
        {
            let mut g = self.groups.lock().unwrap();
            g.insert(
                group,
                GroupApp {
                    node_id: self.node_id,
                    group_id: group,
                    raft: raft.clone(),
                    state_machine: state_machine_store,
                },
            );
            registered_at = tokio::time::Instant::now();
        }

        self.spawn_leader_watch(group, raft.clone(), self.leader_cbs.clone());
        self.spawn_standby_throttle_watch(raft, self.standby_throttle.clone());
        tracing::info!(
            target: "multiraft::recovery",
            operation = "group_start",
            phase = "complete",
            node_id = self.node_id,
            group_id = group,
            transport,
            storage,
            directory = %group_directory.display(),
            "published local Raft group"
        );
        Ok(registered_at)
    }

    pub(super) fn membership_nodes(&self, members: &[NodeId]) -> BTreeMap<NodeId, BasicNode> {
        let mut nodes = BTreeMap::new();
        for &id in members {
            let addr = self
                .config
                .peers
                .iter()
                .find(|(n, _)| *n == id)
                .map(|(_, a)| a.to_string())
                .unwrap_or_default();
            nodes.insert(id, BasicNode { addr });
        }
        nodes
    }

    pub(super) fn spawn_leader_watch(
        &self,
        group: GroupId,
        raft: Raft<S>,
        cbs: Arc<Mutex<Vec<LeaderCb>>>,
    ) {
        // Exactly one owned metrics watcher per local Group; callback registration
        // reuses it rather than creating duplicate observations/tasks.
        if !self.leader_watch_groups.lock().unwrap().insert(group) {
            return;
        }
        let local_node_id = self.node_id;
        self.tasks.spawn(async move {
            let mut rx = raft.metrics();
            let mut last: Option<(Option<NodeId>, crate::VoteObservation)> = None;
            loop {
                let metrics = rx.borrow_watched().clone();
                let cur = metrics.current_leader;
                let vote = crate::VoteObservation::new(metrics.vote.leader_id().term,
                    metrics.vote.leader_id().node_id, metrics.vote.committed);
                if last.as_ref() != Some(&(cur, vote)) {
                    tracing::info!(target: "multiraft::control", group_id = group, local_node_id,
                        stage = "observation", result = if last.is_some() { "leader_vote_changed" } else { "initial_observation" },
                        leader_node_id = ?cur, previous_leader_node_id = ?last.map(|old| old.0),
                        previous_vote_term = ?last.map(|old|old.1.term), previous_vote_node_id = ?last.map(|old|old.1.node_id),
                        previous_vote_committed = ?last.map(|old|old.1.committed), vote_term = vote.term, vote_node_id = vote.node_id,
                        vote_committed = vote.committed, reason_code = "cause_unknown",
                        "native leader/vote observation; request causality unknown");
                }
                if last.as_ref().map(|old| old.0) != Some(cur) {
                    let callbacks: Vec<LeaderCb> = cbs.lock().unwrap().clone();
                    for cb in callbacks {
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| cb(group, cur)));
                    }
                }
                last = Some((cur, vote));
                if rx.changed().await.is_err() { break; }
            }
        });
    }

    /// Keep standby throttle in sync with committed learner membership so every
    /// potential leader throttles dynamically added Standbys after failover.
    pub(super) fn spawn_standby_throttle_watch(&self, raft: Raft<S>, throttle: StandbyThrottle) {
        self.tasks.spawn(async move {
            let mut rx = raft.metrics();
            let mut last_learners: Option<BTreeSet<NodeId>> = None;
            loop {
                let membership = rx.borrow_watched().membership_config.membership().clone();
                let learners: BTreeSet<NodeId> = membership.learner_ids().collect();
                if last_learners.as_ref() != Some(&learners) {
                    let voters: BTreeSet<NodeId> = membership.voter_ids().collect();
                    for &id in &learners {
                        throttle.insert(id);
                    }
                    for id in throttle.standby_ids() {
                        if voters.contains(&id) {
                            throttle.remove(id);
                        }
                    }
                    last_learners = Some(learners);
                }
                if rx.changed().await.is_err() {
                    break;
                }
            }
        });
    }
}
