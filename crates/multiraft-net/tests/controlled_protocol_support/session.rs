//! Actual public protocol setup; recovery never initializes a persisted group.
use super::{admitted, authority, catalog::FileAuthority, cluster};
use authority::{
    StartupAuthority, StartupAuthorityResult, StartupCatalog, StartupRequest,
    StartupTargetGeneration,
};
use cluster::{deadline, DiskImage, GROUP, VOTERS};
use multiraft_core::{typ::StoredMembership, OperationId, ReplacementSpec};
use multiraft_net::{
    ControlledExecutor, ControlledGroup, ControlledOpen, ControlledOpenMode, ControlledSeed,
    MultiRaft,
};
use openraft::{BasicNode, Membership};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{atomic::AtomicUsize, Arc},
};

pub struct Session {
    pub nodes: Vec<Arc<MultiRaft<DiskImage>>>,
    pub groups: Vec<Option<ControlledGroup<DiskImage>>>,
    pub authority: Arc<FileAuthority>,
    pub creations: Vec<Arc<AtomicUsize>>,
    pub target_seed: Option<ControlledSeed>,
    identities: Vec<Vec<u8>>,
}

impl Session {
    pub async fn fresh(root: &Path) -> Self {
        let authority = Arc::new(FileAuthority::provision(
            &root.join("catalog.json"),
            StartupCatalog {
                group_id: GROUP,
                revision: 0,
                membership: StoredMembership::new(
                    None,
                    Membership::new(
                        vec![VOTERS.into()],
                        VOTERS
                            .into_iter()
                            .map(|id| (id, BasicNode::default()))
                            .collect::<BTreeMap<_, _>>(),
                    )
                    .expect("exact three bootstrap voters"),
                ),
                authorized: [(1, 10), (2, 20), (3, 30)].into(),
                active_operation: None,
                admissions: vec![],
                completed: vec![],
            },
        ));
        let mut session = Self {
            nodes: vec![],
            groups: vec![None; 4],
            authority,
            target_seed: None,
            identities: (1..=4).map(|id| vec![id]).collect(),
            creations: (0..4).map(|_| Arc::new(AtomicUsize::new(0))).collect(),
        };
        for (index, config) in cluster::configurations(root).into_iter().enumerate() {
            session
                .nodes
                .push(cluster::start(config, session.creations[index].clone()).await);
        }
        let mut openings = Vec::new();
        for index in 0..3 {
            let node = session.nodes[index].clone();
            let authority = session.authority.clone();
            let request = session.open_request(
                index,
                ControlledOpenMode::BootstrapNew {
                    voters: VOTERS.into(),
                },
            );
            let admission = admitted(
                authority
                    .admit(session.request(index, 1))
                    .await
                    .expect("durably admit bootstrap before native construction"),
            );
            openings.push(async move {
                node.open_controlled_group(request, authority, admission, deadline())
                    .await
                    .expect("bootstrap exact three voters concurrently")
            });
        }
        for (index, group) in futures::future::join_all(openings)
            .await
            .into_iter()
            .enumerate()
        {
            session.groups[index] = Some(group);
        }
        let leader = session.leader().await;
        let membership = session
            .group(leader)
            .membership(deadline())
            .await
            .expect("actual committed bootstrap membership");
        session
            .authority
            .bootstrap_membership(membership)
            .expect("finalize catalog provisioning with native fact");
        session
    }

    pub async fn recover(root: &Path) -> Self {
        let authority = Arc::new(FileAuthority::open(&root.join("catalog.json")));
        let mut session = Self {
            nodes: vec![],
            groups: vec![None; 4],
            authority,
            target_seed: None,
            identities: (1..=4).map(|id| vec![id]).collect(),
            creations: (0..4).map(|_| Arc::new(AtomicUsize::new(0))).collect(),
        };
        let catalog = session
            .authority
            .read(GROUP)
            .await
            .expect("read durable target identity");
        session.identities[3] = catalog
            .active_operation
            .as_ref()
            .map(|binding| binding.target.consumption_id.clone())
            .or_else(|| {
                catalog
                    .completed
                    .last()
                    .map(|proof| proof.target_opened.consumption_id.clone())
            })
            .expect("recover retained target generation identity");
        for (index, config) in cluster::configurations(root).into_iter().enumerate() {
            session
                .nodes
                .push(cluster::start(config, session.creations[index].clone()).await);
        }
        for index in 0..4 {
            let admission = session
                .authority
                .admit(session.request(index, 2))
                .await
                .expect("read independent durable startup authority");
            match admission {
                StartupAuthorityResult::Applied(admission) => {
                    let group = session.nodes[index]
                        .open_controlled_group(
                            session.open_request(index, ControlledOpenMode::Recover),
                            session.authority.clone(),
                            admission,
                            deadline(),
                        )
                        .await
                        .expect("recover admitted durable group without initialize");
                    session.groups[index] = Some(group);
                }
                StartupAuthorityResult::Rejected(
                    authority::StartupAuthorityRejection::UnauthorizedGeneration,
                ) => {}
                other => panic!("unexpected recovery admission: {other:?}"),
            }
        }
        session.leader().await;
        session
    }

    pub fn request(&self, index: usize, boot: u8) -> StartupRequest {
        StartupRequest {
            group_id: GROUP,
            node_id: (index + 1) as u64,
            generation: ((index + 1) * 10) as u64,
            request_id: [boot * 10 + index as u8; 16],
        }
    }

    pub fn open_request(&self, index: usize, mode: ControlledOpenMode) -> ControlledOpen {
        ControlledOpen {
            group_id: GROUP,
            generation: ((index + 1) * 10) as u64,
            identity: self.identities[index].clone(),
            mode,
        }
    }

    pub fn group(&self, index: usize) -> &ControlledGroup<DiskImage> {
        self.groups[index]
            .as_ref()
            .expect("locally opened controlled group")
    }
    pub async fn leader(&self) -> usize {
        cluster::leader(&self.nodes).await
    }

    pub async fn reserve(
        &mut self,
        replace_leader: bool,
    ) -> (usize, ReplacementSpec, ControlledGroup<DiskImage>) {
        let leader = self.leader().await;
        // Keep the executing leader in the final configuration.
        let source = if replace_leader {
            (leader + 1) as u64
        } else if leader == 0 {
            3
        } else {
            1
        };
        let specification = ReplacementSpec {
            operation: OperationId {
                sequence: 1,
                nonce: [9; 16],
            },
            expected_start: self
                .group(leader)
                .membership(deadline())
                .await
                .expect("exact committed starting configuration"),
            target_voters: VOTERS
                .into_iter()
                .filter(|id| *id != source)
                .chain([4])
                .collect(),
            target_generation: 40,
        };
        let target = self.nodes[3]
            .reserve_controlled_target(
                GROUP,
                specification.clone(),
                vec![4],
                self.authority.clone(),
                deadline(),
            )
            .await
            .expect("durably reserve target before constructing core");
        assert_eq!(
            self.nodes[3].with_fsm(GROUP, |_| ()).await,
            None,
            "reservation must not create target core or FSM"
        );
        let mut changed = specification.clone();
        let other_source = if source == 1 { 2 } else { 1 };
        changed.target_voters = VOTERS
            .into_iter()
            .filter(|id| *id != other_source)
            .chain([4])
            .collect();
        assert!(
            self.nodes[3]
                .reserve_controlled_target(
                    GROUP,
                    changed,
                    vec![4],
                    self.authority.clone(),
                    deadline()
                )
                .await
                .is_err(),
            "same operation and generation cannot change immutable target configuration"
        );
        self.authority
            .consume_target(StartupTargetGeneration {
                node_id: 4,
                generation: target.generation(),
                consumption_id: target
                    .receipt()
                    .expect("actual durable reservation receipt")
                    .identity,
            })
            .expect("register actual reserved generation in independent catalog");
        (leader, specification, target)
    }

    pub async fn prepare(
        &mut self,
        replace_leader: bool,
    ) -> (usize, ReplacementSpec, ControlledExecutor) {
        let (leader, specification, target) = self.reserve(replace_leader).await;
        let executor = self
            .group(leader)
            .begin(specification.clone(), 1, &target, deadline())
            .await
            .expect("bind startup directory then commit native Begin");
        let seed = self
            .group(leader)
            .prepare_seed(&executor, deadline())
            .await
            .expect("capture actual current-operation checkpoint before target publication");
        self.target_seed = Some(seed.clone());
        let admission = admitted(
            self.authority
                .admit(self.request(3, 1))
                .await
                .expect("admit bound target generation"),
        );
        self.identities[3] = target
            .receipt()
            .expect("retained reservation identity")
            .identity;
        self.groups[3] = Some(
            target
                .open_target(seed, admission, deadline())
                .await
                .expect("durably open consumed target once"),
        );
        // A later control-only entry and checkpoint force ordinary gRPC snapshot
        // catch-up beyond the quarantine seed, without changing application rows.
        self.group(leader)
            .claim(specification.operation, 1, deadline())
            .await
            .expect("durably replay epoch before later catch-up checkpoint");
        let cut = cluster::compact(&self.nodes[leader]).await;
        assert!(self.nodes[3].create_group(GROUP, &[4]).await.is_err(), "legacy create_group cannot initialize an already opened controlled learner as a singleton");
        self.group(leader)
            .add_learner(&executor, self.group(3), deadline())
            .await
            .expect("guarded learner addition over real gRPC");
        tokio::time::timeout(std::time::Duration::from_secs(20), async {
            loop {
                let status = self.nodes[3]
                    .local_storage_status(GROUP)
                    .await
                    .expect("target storage observation");
                if status
                    .durable_snapshot
                    .and_then(|snapshot| snapshot.last_log_id)
                    .is_some_and(|id| id.index >= cut)
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("target installs actual typed ledger snapshot over gRPC");
        (leader, specification, executor)
    }

    pub async fn stop(self) {
        drop(self.groups);
        for node in self.nodes {
            node.shutdown()
                .await
                .expect("join native tasks and release disk FSM");
        }
    }
}
