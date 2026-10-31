//! Test-only durable reference adapter; never linked into the production crate.
use super::authority::*;
use multiraft_core::{GroupId, OperationId};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
};

#[derive(Serialize, Deserialize)]
struct Denial {
    request: StartupRequest,
    reason: StartupAuthorityRejection,
}
#[derive(Serialize, Deserialize)]
struct State {
    catalog: StartupCatalog,
    denials: Vec<Denial>,
    consumed_targets: Vec<StartupTargetGeneration>,
    bootstrap_finalized: bool,
}

pub struct FileAuthority {
    path: PathBuf,
    lose_handoff_reply: AtomicBool,
}
struct Lock(PathBuf);
impl Drop for Lock {
    fn drop(&mut self) {
        fs::remove_file(&self.0).expect("release fixture lock");
    }
}
impl FileAuthority {
    pub fn provision(path: &Path, catalog: StartupCatalog) -> Self {
        let adapter = Self::open(path);
        let _lock = adapter.lock().unwrap();
        assert!(!path.exists(), "fixture must not overwrite a catalog");
        adapter
            .save(&State {
                catalog,
                denials: vec![],
                consumed_targets: vec![],
                bootstrap_finalized: false,
            })
            .unwrap();
        adapter
    }
    pub fn open(path: &Path) -> Self {
        Self {
            path: path.to_owned(),
            lose_handoff_reply: AtomicBool::new(false),
        }
    }
    /// Explicit fixture provisioning synchronizes the first native committed membership.
    /// It is one-shot and cannot repair an ordinary startup or an existing operation.
    pub fn bootstrap_membership(
        &self,
        membership: multiraft_core::typ::StoredMembership,
    ) -> Result<(), StartupAuthorityError> {
        let _lock = self.lock()?;
        let mut state = self.load()?;
        let catalog = &mut state.catalog;
        let configs = membership.membership().get_joint_config();
        let voters = catalog.authorized.keys().copied().collect();
        if state.bootstrap_finalized
            || catalog.active_operation.is_some()
            || !catalog.completed.is_empty()
            || membership.log_id().is_none()
            || configs.len() != 1
            || configs[0] != voters
        {
            return Err(error(
                "bootstrap provisioning identity is unavailable or already finalized",
            ));
        }
        catalog.membership = membership;
        catalog.revision += 1;
        state.bootstrap_finalized = true;
        self.save(&state)
    }
    /// Fixture registry setup: durably consume an immutable target generation before binding.
    pub fn consume_target(
        &self,
        target: StartupTargetGeneration,
    ) -> Result<(), StartupAuthorityError> {
        let _lock = self.lock()?;
        let mut state = self.load()?;
        if target.consumption_id.is_empty() {
            return Err(error("empty consumption identity"));
        }
        if let Some(existing) = state
            .consumed_targets
            .iter()
            .find(|entry| entry.node_id == target.node_id && entry.generation == target.generation)
        {
            return if existing == &target {
                Ok(())
            } else {
                Err(error("consumption identity conflict"))
            };
        }
        state.consumed_targets.push(target);
        self.save(&state)
    }
    pub fn lose_next_handoff_reply(&self) {
        self.lose_handoff_reply.store(true, Ordering::SeqCst);
    }
    fn lock(&self) -> Result<Lock, StartupAuthorityError> {
        let path = self.path.with_extension("lock");
        // Independent adapter handles serialize through an actual filesystem lock. A crashed
        // lock holder fails closed: the fixture never expires a lock to bypass admission.
        for _ in 0..100_000 {
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(_) => return Ok(Lock(path)),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => std::thread::yield_now(),
                Err(e) => return Err(error(e)),
            }
        }
        Err(StartupAuthorityError("fixture lock unavailable".into()))
    }
    fn load(&self) -> Result<State, StartupAuthorityError> {
        serde_json::from_slice(&fs::read(&self.path).map_err(error)?).map_err(error)
    }
    fn save(&self, state: &State) -> Result<(), StartupAuthorityError> {
        let temporary = self.path.with_extension("pending");
        let mut file = File::create(&temporary).map_err(error)?;
        file.write_all(&serde_json::to_vec(state).map_err(error)?)
            .map_err(error)?;
        file.sync_all().map_err(error)?;
        fs::rename(temporary, &self.path).map_err(error)?;
        File::open(self.path.parent().unwrap())
            .map_err(error)?
            .sync_all()
            .map_err(error)
    }
    fn transaction<T>(
        &self,
        mutate: impl FnOnce(&mut State) -> StartupAuthorityResult<T>,
    ) -> Result<StartupAuthorityResult<T>, StartupAuthorityError> {
        let _lock = self.lock()?;
        let mut state = self.load()?;
        let result = mutate(&mut state);
        state.catalog.revision += 1;
        self.save(&state)?;
        Ok(result)
    }
}
fn error(e: impl std::fmt::Display) -> StartupAuthorityError {
    StartupAuthorityError(e.to_string())
}
fn rejected<T>(reason: StartupAuthorityRejection) -> StartupAuthorityResult<T> {
    StartupAuthorityResult::Rejected(reason)
}
fn operation(binding: &StartupBinding) -> OperationId {
    binding.specification.operation
}

impl StartupAuthority for FileAuthority {
    fn admit(
        &self,
        request: StartupRequest,
    ) -> StartupAuthorityFuture<
        '_,
        Result<StartupAuthorityResult<StartupAdmission>, StartupAuthorityError>,
    > {
        Box::pin(async move {
            self.transaction(|state| {
                let catalog = &mut state.catalog;
                if request.group_id != catalog.group_id {
                    return rejected(StartupAuthorityRejection::UnknownGroup);
                }
                if let Some(previous) = catalog
                    .admissions
                    .iter()
                    .find(|a| a.request.request_id == request.request_id)
                {
                    return if previous.request == request {
                        StartupAuthorityResult::Applied(previous.clone())
                    } else {
                        rejected(StartupAuthorityRejection::IdentityConflict)
                    };
                }
                if let Some(previous) = state
                    .denials
                    .iter()
                    .find(|d| d.request.request_id == request.request_id)
                {
                    return rejected(if previous.request == request {
                        previous.reason
                    } else {
                        StartupAuthorityRejection::IdentityConflict
                    });
                }
                let authorized = match &catalog.active_operation {
                    Some(binding) if binding.source == request.node_id => false,
                    Some(binding) if binding.target.node_id == request.node_id => {
                        binding.target.generation == request.generation
                    }
                    _ => catalog.authorized.get(&request.node_id) == Some(&request.generation),
                };
                if !authorized {
                    state.denials.push(Denial {
                        request,
                        reason: StartupAuthorityRejection::UnauthorizedGeneration,
                    });
                    return rejected(StartupAuthorityRejection::UnauthorizedGeneration);
                }
                let admission = StartupAdmission {
                    request,
                    serial: catalog.revision + 1,
                    active_operation: catalog.active_operation.as_ref().map(operation),
                };
                catalog.admissions.push(admission.clone());
                StartupAuthorityResult::Applied(admission)
            })
        })
    }
    fn bind_active(
        &self,
        binding: StartupBinding,
    ) -> StartupAuthorityFuture<
        '_,
        Result<StartupAuthorityResult<StartupBinding>, StartupAuthorityError>,
    > {
        Box::pin(async move {
            self.transaction(|state| {
                let catalog = &mut state.catalog;
                if catalog.group_id != binding.group_id {
                    return rejected(StartupAuthorityRejection::UnknownGroup);
                }
                if let Some(active) = &catalog.active_operation {
                    return if active == &binding {
                        StartupAuthorityResult::Applied(active.clone())
                    } else {
                        rejected(StartupAuthorityRejection::ActiveOperation)
                    };
                }
                if catalog.completed.iter().any(|p| {
                    p.native_record.specification.operation.sequence >= operation(&binding).sequence
                }) {
                    return rejected(StartupAuthorityRejection::StaleOperation);
                }
                if catalog.membership != binding.specification.expected_start {
                    return rejected(StartupAuthorityRejection::ConfigurationChanged);
                }
                let old = catalog.membership.membership().get_joint_config();
                let target = &binding.specification.target_voters;
                if old.len() != 1
                    || old[0].len() != 3
                    || target.len() != 3
                    || !old[0].contains(&binding.source)
                    || target.contains(&binding.source)
                    || old[0].contains(&binding.target.node_id)
                    || !target.contains(&binding.target.node_id)
                    || old[0].intersection(target).count() != 2
                    || binding.target.generation != binding.specification.target_generation
                    || !state.consumed_targets.contains(&binding.target)
                    || binding.target.consumption_id.is_empty()
                    || catalog.authorized.get(&binding.source) != Some(&binding.source_generation)
                {
                    return rejected(StartupAuthorityRejection::IdentityConflict);
                }
                catalog.active_operation = Some(binding.clone());
                StartupAuthorityResult::Applied(binding)
            })
        })
    }
    fn terminal_handoff(
        &self,
        proof: StartupTerminalProof,
    ) -> StartupAuthorityFuture<
        '_,
        Result<StartupAuthorityResult<StartupTerminalProof>, StartupAuthorityError>,
    > {
        Box::pin(async move {
            let result = self.transaction(|state| {
                let catalog = &mut state.catalog;
                if catalog.group_id != proof.group_id {
                    return rejected(StartupAuthorityRejection::UnknownGroup);
                }
                if let Some(previous) = catalog.completed.iter().find(|p| {
                    p.native_record.specification.operation
                        == proof.native_record.specification.operation
                }) {
                    return if previous == &proof {
                        StartupAuthorityResult::Applied(previous.clone())
                    } else {
                        rejected(StartupAuthorityRejection::IdentityConflict)
                    };
                }
                let Some(binding) = &catalog.active_operation else {
                    return rejected(StartupAuthorityRejection::InvalidProof);
                };
                let Some(native) = &proof.native_record.terminal else {
                    return rejected(StartupAuthorityRejection::InvalidProof);
                };
                let receipt = &native.target_open;
                if native.configuration != proof.committed_membership
                    || !covers_log(&proof.applied_log, &native.barrier)
                    || receipt.operation != binding.specification.operation
                    || receipt.target_node != binding.target.node_id
                    || receipt.generation != binding.target.generation
                    || receipt.receipt != binding.target.consumption_id
                    || !native
                        .configuration
                        .log_id()
                        .as_ref()
                        .is_some_and(|id| covers_log(&receipt.applied, id))
                {
                    return rejected(StartupAuthorityRejection::InvalidProof);
                }
                let membership = &proof.committed_membership;
                let configs = membership.membership().get_joint_config();
                let valid_log = membership.log_id().as_ref().is_some_and(|id| {
                    proof.applied_log.index() >= id.index()
                        && (proof.applied_log.index() != id.index() || proof.applied_log == *id)
                });
                if proof.source_generation != binding.source_generation
                    || proof.native_record.specification != binding.specification
                    || proof.applied_membership != *membership
                    || !valid_log
                    || configs != &vec![binding.specification.target_voters.clone()]
                    || proof.target_opened != binding.target
                {
                    return rejected(StartupAuthorityRejection::InvalidProof);
                }
                catalog.authorized.remove(&binding.source);
                catalog
                    .authorized
                    .insert(binding.target.node_id, binding.target.generation);
                catalog.membership = membership.clone();
                catalog.active_operation = None;
                catalog.completed.push(proof.clone());
                StartupAuthorityResult::Applied(proof)
            })?;
            if matches!(result, StartupAuthorityResult::Applied(_))
                && self.lose_handoff_reply.swap(false, Ordering::SeqCst)
            {
                Ok(StartupAuthorityResult::Unknown)
            } else {
                Ok(result)
            }
        })
    }
    fn read(
        &self,
        group_id: GroupId,
    ) -> StartupAuthorityFuture<'_, Result<StartupCatalog, StartupAuthorityError>> {
        Box::pin(async move {
            let _lock = self.lock()?;
            let state = self.load()?;
            if state.catalog.group_id != group_id {
                return Err(StartupAuthorityError("unknown group".into()));
            }
            Ok(state.catalog)
        })
    }
}

fn covers_log(actual: &multiraft_core::typ::LogId, required: &multiraft_core::typ::LogId) -> bool {
    actual.index() >= required.index() && (actual.index() != required.index() || actual == required)
}
