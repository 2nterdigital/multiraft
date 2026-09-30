//! Transport startup and listener ownership for the existing facade.
use super::*;

impl<S: StateMachine> MultiRaft<S> {
    pub(super) async fn start_inner(
        config: ClusterConfig,
        router: Router,
        glue: ClusterGlue,
        factory: Arc<dyn StateMachineFactory<S>>,
    ) -> anyhow::Result<Self> {
        config
            .validate_snapshot_storage()
            .map_err(anyhow::Error::msg)?;
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
            net: NetBackend::InProcess { router, glue },
            groups,
            tasks,
            reads: read::ReadRuntime::default(),
            fsm_releases: Mutex::new(Vec::new()),
            ingress_tasks: Arc::new(tasks::OwnedTasks::default()),
            listener_stop: Mutex::new(None),
            ingress_accepting: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            fsm_factory: factory,
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

        let groups: GroupMap<S> = Arc::new(Mutex::new(BTreeMap::new()));
        let grpc_router = GrpcRouter::from_config(&config);
        let standby_throttle = grpc_router.throttle().clone();
        let snapshot_rt = SnapshotRuntime::new(&config);

        let groups_for_server = groups.clone();
        let snapshot_cap = config.max_snapshot_bytes;
        let listener = tokio::net::TcpListener::bind(self_addr).await?;
        let tasks = tasks::OwnedTasks::default();
        let ingress_tasks = Arc::new(tasks::OwnedTasks::default());
        let ingress_accepting = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let (listener_stop, stopped) = tokio::sync::oneshot::channel();
        let service_tasks = ingress_tasks.clone();
        let service_accepting = ingress_accepting.clone();
        tasks.spawn_result(async move {
            GrpcServer::serve_owned(
                listener,
                groups_for_server,
                snapshot_cap,
                service_tasks,
                service_accepting,
                async move {
                    let _ = stopped.await;
                },
            )
            .await
        });

        Ok(Self {
            node_id: config.node_id,
            config,
            net: NetBackend::Grpc {
                router: grpc_router,
            },
            groups,
            tasks,
            reads: read::ReadRuntime::default(),
            fsm_releases: Mutex::new(Vec::new()),
            ingress_tasks,
            listener_stop: Mutex::new(Some(listener_stop)),
            ingress_accepting,
            fsm_factory: factory,
            leader_cbs: Arc::new(Mutex::new(Vec::new())),
            snapshot_rt,
            standby_throttle,
            #[cfg(test)]
            daisy_spawn_probe: None,
        })
    }
}
