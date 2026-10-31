//! Durable one-shot construction, with startup authority before native core creation.
use super::*;
use multiraft_core::StartupProvenance;
use multiraft_store::ControlledGroupStage;
use std::io::{ErrorKind, Write};

impl<S: StateMachine> MultiRaft<S> {
    /// Reserve a target generation without constructing a core or initializing membership.
    pub async fn reserve_controlled_target(
        self: &Arc<Self>,
        group: GroupId,
        specification: ReplacementSpec,
        identity: Vec<u8>,
        authority: Arc<dyn StartupAuthority>,
        deadline: tokio::time::Instant,
    ) -> Result<ControlledGroup<S>, ControlledError> {
        self.controlled_require_storage()?;
        let starting = specification.expected_start.membership();
        let old: BTreeSet<_> = starting.voter_ids().collect();
        let target: Vec<_> = specification
            .target_voters
            .difference(&old)
            .copied()
            .collect();
        if identity.is_empty()
            || specification.operation.sequence == 0
            || specification.target_generation == 0
            || target != [self.node_id]
            || old.len() != 3
            || specification.target_voters.len() != 3
            || old.difference(&specification.target_voters).count() != 1
            || starting.get_joint_config().len() != 1
            || starting.learner_ids().next().is_some()
        {
            return Err(ControlledError::Rejected(
                "invalid target replacement reservation",
            ));
        }
        use sha2::Digest;
        let specification_bytes = serde_json::to_vec(&specification)
            .map_err(|error| ControlledError::Native(error.to_string()))?;
        let mut digest = sha2::Sha256::new();
        digest.update(b"multiraft-target-consumption-v1");
        digest.update((specification_bytes.len() as u64).to_be_bytes());
        digest.update(specification_bytes);
        digest.update(&identity);
        let identity = digest.finalize().to_vec();
        // Cleaned is a durable barrier: the old owner can perform no later mutation.
        self.ensure_node_log().await?;
        {
            let mut owners = self.group_owners.lock().expect("group owner lock poisoned");
            let db = self.controlled_db()?;
            if let Some(old) = db.lifecycle_receipt(group)? {
                if old.stage == ControlledGroupStage::Cleaned
                    && old.generation.checked_add(1) == Some(specification.target_generation)
                {
                    db.begin_rejoin(
                        group,
                        old.generation,
                        specification.target_generation,
                        &identity,
                    )?;
                    owners.insert(group, Arc::new(GroupOwner::default()));
                }
            }
        }
        self.controlled_run(group, deadline, move |node| async move {
            node.controlled_require_storage()?;
            node.ensure_node_log().await?;
            let old: BTreeSet<_> = specification
                .expected_start
                .membership()
                .voter_ids()
                .collect();
            let target: Vec<_> = specification
                .target_voters
                .difference(&old)
                .copied()
                .collect();
            if target != [node.node_id] || identity.is_empty() {
                return Err(ControlledError::Rejected(
                    "target generation identity does not match replacement",
                ));
            }
            node.controlled_db()?.reserve_controlled(
                group,
                specification.target_generation,
                &identity,
            )?;
            let mut handle = ControlledGroup::handle(
                &node,
                &ControlledOpen {
                    group_id: group,
                    generation: specification.target_generation,
                    identity,
                    mode: ControlledOpenMode::Target {
                        operation: specification.operation,
                    },
                },
                authority,
            );
            handle.target_specification = Some(specification);
            Ok(handle)
        })
        .await
    }

    /// Attach to an existing receipt for observation or cleanup, without constructing a core.
    pub async fn inspect_controlled_group(
        self: &Arc<Self>,
        group: GroupId,
        generation: u64,
        identity: Vec<u8>,
        authority: Arc<dyn StartupAuthority>,
    ) -> Result<ControlledGroup<S>, ControlledError> {
        self.ensure_node_log().await?;
        let request = ControlledOpen {
            group_id: group,
            generation,
            identity,
            mode: ControlledOpenMode::Recover,
        };
        let handle = ControlledGroup::handle(self, &request, authority);
        handle.checked_receipt()?;
        Ok(handle)
    }

    /// Open only a durable externally admitted generation. Duplicate target opens only replay.
    /// A caller deadline leaves the one-shot admission and construction job owned by this node.
    pub async fn open_controlled_group(
        self: &Arc<Self>,
        request: ControlledOpen,
        authority: Arc<dyn StartupAuthority>,
        admission: StartupAdmission,
        deadline: tokio::time::Instant,
    ) -> Result<ControlledGroup<S>, ControlledError> {
        self.controlled_require_storage()?;
        self.ensure_node_log().await?;
        let replay = tokio::time::timeout_at(
            deadline,
            self.controlled_open_replay(&request, &authority, &admission),
        )
        .await
        .map_err(|_| ControlledError::Deadline {
            outcome_unknown: false,
        })??;
        if let Some(handle) = replay {
            return Ok(handle);
        }
        self.controlled_run(request.group_id, deadline, move |node| async move {
            node.controlled_require_storage()?;
            node.ensure_node_log().await?;
            if admission.request.group_id != request.group_id
                || admission.request.node_id != node.node_id
                || admission.request.generation != request.generation
            {
                return Err(ControlledError::Rejected(
                    "startup admission identity mismatch",
                ));
            }
            let catalog = authority.read(request.group_id).await?;
            if !catalog.admissions.contains(&admission) {
                return Err(ControlledError::Rejected(
                    "startup admission is not durable",
                ));
            }
            let db = node.controlled_db()?;
            let old = db.lifecycle_receipt(request.group_id)?;
            if old.as_ref().is_some_and(|old| {
                old.generation != request.generation || old.identity != request.identity
            }) {
                return Err(ControlledError::Rejected(
                    "local generation identity mismatch",
                ));
            }
            match &request.mode {
                ControlledOpenMode::BootstrapNew { voters } => {
                    if let Some(existing) = &old {
                        bootstrap::verify(&node, &request, voters)?;
                        if !matches!(
                            existing.stage,
                            ControlledGroupStage::Reserved | ControlledGroupStage::Opening
                        ) {
                            return Ok(ControlledGroup::handle(&node, &request, authority));
                        }
                    }
                    if old.is_none()
                        && (voters.len() != 3
                            || !voters.contains(&node.node_id)
                            || *voters != catalog.membership.membership().voter_ids().collect()
                            || node.log_provenance(request.group_id)?
                                != StartupProvenance::Pristine
                            || multiraft_store::DiskFsmDb::group_path(
                                &node.config.data_dir,
                                request.group_id,
                            )
                            .exists()
                            || node
                                .config
                                .data_dir
                                .join("snapshots")
                                .join(request.group_id.to_string())
                                .exists())
                    {
                        return Err(ControlledError::Rejected(
                            "bootstrap requires a pristine exact-three admitted group",
                        ));
                    }
                    bootstrap::persist(&node, &request, voters).await?;
                    db.reserve_controlled(request.group_id, request.generation, &request.identity)?;
                }
                ControlledOpenMode::Target { operation } => {
                    let receipt = old.as_ref().ok_or(ControlledError::Rejected(
                        "target generation is not reserved",
                    ))?;
                    if receipt.stage != ControlledGroupStage::Reserved {
                        let known = catalog.active_operation.as_ref().is_some_and(|binding| {
                            binding.specification.operation == *operation
                                && binding.target.node_id == node.node_id
                                && binding.target.generation == request.generation
                                && binding.target.consumption_id == request.identity
                        }) || catalog.completed.iter().any(|completed| {
                            completed.native_record.specification.operation == *operation
                                && completed.target_opened.node_id == node.node_id
                                && completed.target_opened.generation == request.generation
                                && completed.target_opened.consumption_id == request.identity
                        });
                        if !known {
                            return Err(ControlledError::Rejected(
                                "target replay operation identity mismatch",
                            ));
                        }
                        return Ok(ControlledGroup::handle(&node, &request, authority));
                    }
                    let binding = catalog
                        .active_operation
                        .as_ref()
                        .ok_or(ControlledError::Rejected("target operation is not active"))?;
                    if binding.specification.operation != *operation
                        || binding.target.node_id != node.node_id
                        || binding.target.generation != request.generation
                        || binding.target.consumption_id != request.identity
                    {
                        return Err(ControlledError::Rejected(
                            "target consumption is not bound to this operation",
                        ));
                    }
                    let old = old.as_ref().ok_or(ControlledError::Rejected(
                        "target generation is not reserved",
                    ))?;
                    if old.stage != ControlledGroupStage::Reserved {
                        return Ok(ControlledGroup::handle(&node, &request, authority));
                    }
                }
                ControlledOpenMode::Recover => {
                    let old = old.as_ref().ok_or(ControlledError::Rejected(
                        "recovery requires an existing generation",
                    ))?;
                    if !matches!(
                        old.stage,
                        ControlledGroupStage::Opening | ControlledGroupStage::Opened
                    ) {
                        return Err(ControlledError::Rejected(
                            "retired generation cannot restart",
                        ));
                    }
                    if old.stage == ControlledGroupStage::Opening
                        && catalog.active_operation.as_ref().is_none_or(|binding| {
                            binding.target.node_id != node.node_id
                                || binding.target.generation != request.generation
                                || binding.target.consumption_id != request.identity
                        })
                    {
                        return Err(ControlledError::Rejected(
                            "unresolved opening requires the same active operation",
                        ));
                    }
                }
            }
            if node.raft(request.group_id).is_some() {
                if let ControlledOpenMode::BootstrapNew { voters } = &request.mode {
                    bootstrap::reconcile(&node, &request, voters).await?;
                    db.advance_controlled(
                        request.group_id,
                        request.generation,
                        &request.identity,
                        ControlledGroupStage::Opening,
                        ControlledGroupStage::Opened,
                    )?;
                }
                return Ok(ControlledGroup::handle(&node, &request, authority));
            }
            let root = node.config.data_dir.clone();
            let consumed_admission = admission.clone();
            let consumed =
                tokio::task::spawn_blocking(move || consume_startup(&root, &consumed_admission))
                    .await
                    .map_err(|error| ControlledError::Native(error.to_string()))??;
            if !consumed {
                return Ok(ControlledGroup::handle(&node, &request, authority));
            }
            let current = db
                .lifecycle_receipt(request.group_id)?
                .ok_or(ControlledError::Rejected("generation receipt missing"))?;
            if current.stage == ControlledGroupStage::Reserved {
                db.advance_controlled(
                    request.group_id,
                    request.generation,
                    &request.identity,
                    ControlledGroupStage::Reserved,
                    ControlledGroupStage::Opening,
                )?;
            }
            let opened = node.spawn_controlled_local_group(request.group_id).await;
            opened?;
            node.wait_for_owned_recovery(request.group_id, Duration::from_secs(30))
                .await?;
            node.validate_recovered(request.group_id).await?;
            node.ensure_recovery_running(request.group_id)?;
            if let ControlledOpenMode::BootstrapNew { voters } = &request.mode {
                bootstrap::reconcile(&node, &request, voters).await?;
            }
            // The receipt follows real native construction, never an RPC waiter.
            db.advance_controlled(
                request.group_id,
                request.generation,
                &request.identity,
                ControlledGroupStage::Opening,
                ControlledGroupStage::Opened,
            )?;
            Ok(ControlledGroup::handle(&node, &request, authority))
        })
        .await
    }
    async fn controlled_open_replay(
        self: &Arc<Self>,
        request: &ControlledOpen,
        authority: &Arc<dyn StartupAuthority>,
        admission: &StartupAdmission,
    ) -> Result<Option<ControlledGroup<S>>, ControlledError> {
        let Some(receipt) = self.controlled_db()?.lifecycle_receipt(request.group_id)? else {
            return Ok(None);
        };
        if receipt.generation != request.generation || receipt.identity != request.identity {
            return Err(ControlledError::Rejected(
                "local generation identity mismatch",
            ));
        }
        let replay = match &request.mode {
            ControlledOpenMode::Target { .. } => receipt.stage != ControlledGroupStage::Reserved,
            ControlledOpenMode::BootstrapNew { voters }
                if !matches!(
                    receipt.stage,
                    ControlledGroupStage::Reserved | ControlledGroupStage::Opening
                ) =>
            {
                bootstrap::verify(self, request, voters)?;
                true
            }
            _ => false,
        };
        if !replay {
            return Ok(None);
        }
        let catalog = authority.read(request.group_id).await?;
        if admission.request.group_id != request.group_id
            || admission.request.node_id != self.node_id
            || admission.request.generation != request.generation
            || !catalog.admissions.contains(admission)
        {
            return Err(ControlledError::Rejected(
                "startup replay admission identity mismatch",
            ));
        }
        if let ControlledOpenMode::Target { operation } = &request.mode {
            let known = catalog.active_operation.as_ref().is_some_and(|binding| {
                binding.specification.operation == *operation
                    && binding.target.node_id == self.node_id
                    && binding.target.generation == request.generation
                    && binding.target.consumption_id == request.identity
            }) || catalog.completed.iter().any(|completed| {
                completed.native_record.specification.operation == *operation
                    && completed.target_opened.node_id == self.node_id
                    && completed.target_opened.generation == request.generation
                    && completed.target_opened.consumption_id == request.identity
            });
            if !known {
                return Err(ControlledError::Rejected(
                    "target replay operation identity mismatch",
                ));
            }
        }
        Ok(Some(ControlledGroup::handle(
            self,
            request,
            authority.clone(),
        )))
    }

    fn controlled_require_storage(&self) -> Result<(), ControlledError> {
        if self.config.data_dir.as_os_str().is_empty()
            || self.config.log_storage_backend != LogStorageBackend::RocksDb
            || self.config.snapshot_mode != SnapshotMode::NativeDurable
            || self.config.role != NodeRole::Voter
        {
            return Err(ControlledError::Rejected(
                "controlled groups require voter RocksDB and NativeDurable snapshots",
            ));
        }
        Ok(())
    }
}

fn consume_startup(root: &std::path::Path, admission: &StartupAdmission) -> std::io::Result<bool> {
    let directory = root
        .join("controlled-startup")
        .join(format!("group-{}", admission.request.group_id))
        .join(format!("generation-{}", admission.request.generation));
    std::fs::create_dir_all(&directory)?;
    let path = directory.join(format!("{}.json", admission.serial));
    let bytes = serde_json::to_vec(admission).map_err(std::io::Error::other)?;
    match std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
    {
        Ok(mut file) => {
            file.write_all(&bytes)?;
            file.sync_all()?;
            std::fs::File::open(&directory)?.sync_all()?;
            // Persist newly created ancestor links as well as the receipt itself.
            for ancestor in directory
                .ancestors()
                .skip(1)
                .take_while(|ancestor| ancestor.starts_with(root))
            {
                std::fs::File::open(ancestor)?.sync_all()?;
            }
            Ok(true)
        }
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {
            if std::fs::read(path)? != bytes {
                return Err(std::io::Error::new(
                    ErrorKind::InvalidData,
                    "startup receipt identity conflict",
                ));
            }
            Ok(false)
        }
        Err(error) => Err(error),
    }
}
