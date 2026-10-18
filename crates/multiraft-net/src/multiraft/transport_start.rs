//! Transport startup and listener ownership for the existing facade.
use super::*;

impl<S: StateMachine> MultiRaft<S> {
    pub(super) async fn start_inner(
        config: ClusterConfig,
        router: Router,
        glue: ClusterGlue,
        factory: Arc<dyn StateMachineFactory<S>>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            config.raft_transport_fingerprint.is_none() && config.transport.is_none(),
            "fingerprinted or overridden Raft transport requires gRPC"
        );
        config
            .validate_snapshot_storage()
            .map_err(anyhow::Error::msg)?;
        recovery::check_node_log(&config)?;
        let groups: GroupMap<S> = Arc::new(Mutex::new(BTreeMap::new()));
        let (node, _tx) = Node::with_groups(config.node_id, router.clone(), groups.clone());
        let tasks = tasks::OwnedTasks::default();
        tasks.spawn_result(async move {
            node.run()
                .await
                .ok_or_else(|| anyhow::anyhow!("in-process ingress worker stopped"))
        });
        let snapshot_rt = SnapshotRuntime::new(&config);
        router.throttle().apply_config(&config);
        let standby_throttle = router.throttle().clone();

        Ok(Self {
            node_id: config.node_id,
            config,
            rocks_log_db: Arc::new(Mutex::new(None)),
            log_opening: Arc::new(tokio::sync::Mutex::new(false)),
            log_open_tasks: tasks::OwnedTasks::default(),
            net: NetBackend::InProcess { router, glue },
            groups,
            tasks,
            control_slot: tokio::sync::Semaphore::new(1),
            leader_watch_groups: Mutex::new(BTreeSet::new()),
            fsm_releases: Mutex::new(Vec::new()),
            construction_recovery: Mutex::new(BTreeMap::new()),
            ingress_tasks: Arc::new(tasks::OwnedTasks::default()),
            listener_stop: Mutex::new(None),
            ingress_accepting: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            fsm_factory: factory,
            election_source: None,
            leader_cbs: Arc::new(Mutex::new(Vec::new())),
            snapshot_rt,
            standby_throttle,
            #[cfg(test)]
            daisy_spawn_probe: None,
        })
    }

    pub(super) async fn start_grpc_inner(
        config: ClusterConfig,
        factory: Arc<dyn StateMachineFactory<S>>,
    ) -> anyhow::Result<Self> {
        config
            .validate_snapshot_storage()
            .map_err(anyhow::Error::msg)?;
        let self_addr = config
            .peers
            .iter()
            .find(|(id, _)| *id == config.node_id)
            .map(|(_, a)| *a)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "start_grpc: node {} missing from config.peers",
                    config.node_id
                )
            })?;

        if let Some(transport) = &config.transport {
            transport
                .validate(&config.peers)
                .map_err(anyhow::Error::msg)?;
        }
        recovery::check_node_log(&config)?;
        let groups: GroupMap<S> = Arc::new(Mutex::new(BTreeMap::new()));
        let grpc_router = GrpcRouter::from_config(&config);
        let standby_throttle = grpc_router.throttle().clone();
        let snapshot_rt = SnapshotRuntime::new(&config);

        let groups_for_server = groups.clone();
        let snapshot_cap = config.max_snapshot_bytes;
        let fingerprint = config.raft_transport_fingerprint;
        let bind = config
            .transport
            .as_ref()
            .map_or(self_addr, |t| t.listen_address);
        let listener = tokio::net::TcpListener::bind(bind).await?;
        let tasks = tasks::OwnedTasks::default();
        let ingress_tasks = Arc::new(tasks::OwnedTasks::default());
        let ingress_accepting = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let (listener_stop, stopped) = tokio::sync::oneshot::channel();
        let service_tasks = ingress_tasks.clone();
        let service_accepting = ingress_accepting.clone();
        tasks.spawn_result(async move {
            GrpcServer::serve_owned_bound(
                listener,
                groups_for_server,
                snapshot_cap,
                service_tasks,
                service_accepting,
                fingerprint,
                async move {
                    let _ = stopped.await;
                },
            )
            .await
        });

        Ok(Self {
            node_id: config.node_id,
            config,
            rocks_log_db: Arc::new(Mutex::new(None)),
            log_opening: Arc::new(tokio::sync::Mutex::new(false)),
            log_open_tasks: tasks::OwnedTasks::default(),
            net: NetBackend::Grpc {
                router: grpc_router,
            },
            groups,
            tasks,
            control_slot: tokio::sync::Semaphore::new(1),
            leader_watch_groups: Mutex::new(BTreeSet::new()),
            fsm_releases: Mutex::new(Vec::new()),
            construction_recovery: Mutex::new(BTreeMap::new()),
            ingress_tasks,
            listener_stop: Mutex::new(Some(listener_stop)),
            ingress_accepting,
            fsm_factory: factory,
            election_source: None,
            leader_cbs: Arc::new(Mutex::new(Vec::new())),
            snapshot_rt,
            standby_throttle,
            #[cfg(test)]
            daisy_spawn_probe: None,
        })
    }
}
