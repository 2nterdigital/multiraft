//! Existing facade behavior, owned by this feature.
use super::*;

impl<S: StateMachine> MultiRaft<S> {
    pub(super) async fn spawn_local_group(&self, group: GroupId) -> Result<(), MultiRaftError> {
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
            "starting local Raft group"
        );
        let config = Config {
            heartbeat_interval: self.config.heartbeat_interval_ms,
            election_timeout_min: self.config.election_timeout_min_ms,
            election_timeout_max: self.config.election_timeout_max_ms,
            max_in_snapshot_log_to_keep: if self.config.snapshot_mode == SnapshotMode::NativeDurable
            {
                self.config.retain_log_entries
            } else {
                0
            },
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
            MultiRaftError::Other(anyhow::anyhow!(error.to_string()))
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
                error = %source,
                error_debug = ?source,
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
            let rt = self.snapshot_rt.clone();
            let holder = sm_holder.clone();
            let trigger: TriggerCb = Arc::new(move |gid, index, term| {
                let catalog = catalog.clone();
                let rt = rt.clone();
                let holder = holder.clone();
                TypeConfig::spawn(async move {
                    let Some(sm) = holder.get().and_then(|weak| weak.upgrade()) else {
                        tracing::warn!(group = gid, "standby trigger before SM ready");
                        return;
                    };
                    let delay = *rt.serialize_delay.lock().unwrap();
                    match sm
                        .build_standby_snapshot_async(&catalog, gid, index, term, delay)
                        .await
                    {
                        Ok(entry) => {
                            let fetch_url = rt
                                .admin_advertise_addr
                                .map(|addr| format!("http://{addr}/snapshots/{gid}/latest"))
                                .unwrap_or_default();
                            let ad = SnapshotAdvertisement {
                                group: gid,
                                last_index: entry.last_index,
                                last_term: entry.last_term,
                                snapshot_id: entry.snapshot_id,
                                size: entry.size,
                                sha256_hex: entry.sha256_hex,
                                fetch_url,
                            };
                            rt.record_ad(ad);
                        }
                        Err(e) => {
                            tracing::error!(
                                group = gid,
                                index,
                                term,
                                error = %e,
                                "standby async snapshot failed"
                            );
                        }
                    }
                });
            });
            Some(trigger)
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
        {
            let mut releases = self.fsm_releases.lock().unwrap();
            releases.retain(|release| !release.is_released());
            releases.push(state_machine_store.release_observer());
        }
        let _ = sm_holder.set(state_machine_store.downgrade());

        let raft = match &self.net {
            NetBackend::InProcess { router, .. } => {
                let network = NetworkFactory::new(router.clone(), group);
                if self.config.data_dir.as_os_str().is_empty() {
                    let log_store = MemLogStore::default();
                    openraft::Raft::new(
                        self.node_id,
                        config,
                        network,
                        log_store,
                        state_machine_store.clone(),
                    )
                    .await
                } else {
                    let dir = self.config.data_dir.join(format!("group-{group}"));
                    let log_store = FileLogStoreOf::open_with_full_options(
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
                        MultiRaftError::Other(anyhow::anyhow!(
                            "open file log for node {}, group {} at {}: {error}",
                            self.node_id,
                            group,
                            dir.display()
                        ))
                    })?;
                    openraft::Raft::new(
                        self.node_id,
                        config,
                        network,
                        log_store,
                        state_machine_store.clone(),
                    )
                    .await
                }
            }
            NetBackend::Grpc { router } => {
                let network = GrpcNetworkFactory::new(router.clone(), group);
                if self.config.data_dir.as_os_str().is_empty() {
                    let log_store = MemLogStore::default();
                    openraft::Raft::new(
                        self.node_id,
                        config,
                        network,
                        log_store,
                        state_machine_store.clone(),
                    )
                    .await
                } else {
                    let dir = self.config.data_dir.join(format!("group-{group}"));
                    let log_store = FileLogStoreOf::open_with_full_options(
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
                        MultiRaftError::Other(anyhow::anyhow!(
                            "open file log for node {}, group {} at {}: {error}",
                            self.node_id,
                            group,
                            dir.display()
                        ))
                    })?;
                    openraft::Raft::new(
                        self.node_id,
                        config,
                        network,
                        log_store,
                        state_machine_store.clone(),
                    )
                    .await
                }
            }
        }
        .map_err(|error| {
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
                "failed to recover or start local Raft group"
            );
            MultiRaftError::Other(anyhow::anyhow!(
                "start Raft for node {}, group {} at {}: {error}",
                self.node_id,
                group,
                group_directory.display()
            ))
        })?;

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
        Ok(())
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
