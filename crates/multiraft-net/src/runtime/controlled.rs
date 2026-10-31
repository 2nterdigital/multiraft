//! Managed controlled replicas retain weak ownership and hide native sealed mutation capabilities.
use super::*;
use crate::controlled_startup::{StartupAdmission, StartupAuthority};
use crate::portable::{NativeTargetEndpoint, NativeTargetIdentity, SourceTargetPort};
use crate::{
    ControlledError, ControlledExecutor, ControlledGroup, ControlledOpen, ManagedGenesisBasis,
};
use multiraft_core::{NativeLedger, OperationId, ReplacementSpec, TerminalProof};
use multiraft_store::ControlledGroupReceipt;
use std::future::Future;

mod operation;

/// Admission failures and native controlled outcomes preserve their original error classification.
#[derive(Debug, thiserror::Error)]
pub enum ManagedControlError {
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    #[error(transparent)]
    Native(#[from] ControlledError),
}

/// Weak access to exactly one durable local generation; no native node capability escapes.
pub struct ManagedControlledGroup<S: StateMachine> {
    runtime: RuntimeHandle<S>,
    group: ControlledGroup<S>,
}
impl<S: StateMachine> Clone for ManagedControlledGroup<S> {
    fn clone(&self) -> Self {
        Self {
            runtime: self.runtime.clone(),
            group: self.group.clone(),
        }
    }
}
impl<S: StateMachine> std::fmt::Debug for ManagedControlledGroup<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.group.fmt(f)
    }
}

/// One executor incarnation; all sealed native evidence remains private to this facade.
pub struct ManagedOperation<S: StateMachine> {
    group: ManagedControlledGroup<S>,
    executor: ControlledExecutor,
}
impl<S: StateMachine> Clone for ManagedOperation<S> {
    fn clone(&self) -> Self {
        Self {
            group: self.group.clone(),
            executor: self.executor.clone(),
        }
    }
}
impl<S: StateMachine> std::fmt::Debug for ManagedOperation<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagedOperation")
            .field("group", &self.group)
            .finish_non_exhaustive()
    }
}

/// Stateful portable dispatcher whose requests always pass owned runtime admission.
pub struct ManagedTargetEndpoint<S: StateMachine> {
    runtime: RuntimeHandle<S>,
    id: u64,
    identity: NativeTargetIdentity,
}
impl<S: StateMachine> Clone for ManagedTargetEndpoint<S> {
    fn clone(&self) -> Self {
        Self {
            runtime: self.runtime.clone(),
            id: self.id,
            identity: self.identity.clone(),
        }
    }
}
impl<S: StateMachine> std::fmt::Debug for ManagedTargetEndpoint<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagedTargetEndpoint")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

impl<S: StateMachine> RuntimeShared<S> {
    fn register_controlled(&self, group: GroupId, generation: u64) -> Result<(), ControlledError> {
        let mut generations = self
            .controlled_generations
            .lock()
            .expect("controlled generations lock poisoned");
        match generations.get(&group) {
            Some(actual) if *actual != generation => Err(ControlledError::Rejected(
                "runtime generation differs from controlled request",
            )),
            _ => {
                generations.insert(group, generation);
                Ok(())
            }
        }
    }
    pub(super) fn publish_controlled(&self, group: GroupId, generation: u64, opened: bool) {
        let mut generations = self
            .controlled_generations
            .lock()
            .expect("controlled generations lock poisoned");
        if opened {
            generations.entry(group).or_insert(generation);
        }
        if generations.get(&group) != Some(&generation) {
            return;
        }
        let mut ready = self.ready.lock().expect("runtime readiness lock poisoned");
        if opened && self.accepting.load(Ordering::Acquire) {
            ready.insert(group);
        } else {
            ready.remove(&group);
        }
    }
}
impl<S: StateMachine> RuntimeHandle<S> {
    // The task owns the waiter independently of caller cancellation. Shutdown cancels only
    // that waiter, releasing admission before native stop/join drains the actor's real work.
    async fn controlled_request<T, F, Fut>(
        &self,
        deadline: Instant,
        operation: F,
    ) -> Result<T, ManagedControlError>
    where
        T: Send + 'static,
        F: FnOnce(Arc<RuntimeShared<S>>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, ControlledError>> + Send + 'static,
    {
        let admitted = self.admit(deadline).await?;
        let shared = admitted.shared.clone();
        let mut closed = shared.closed.subscribe();
        let (reply, receiver) = tokio::sync::oneshot::channel();
        if !shared.controlled_jobs.spawn(async move {
            let _admitted = admitted;
            let result = if *closed.borrow_and_update() { Err(ControlledError::Closed) } else {
                tokio::select! { biased; _ = closed.changed() => Err(ControlledError::Closed), result = operation(_admitted.shared.clone()) => result }
            };
            let _ = reply.send(result);
        }) { return Err(RuntimeError::Closed.into()); }
        let result = tokio::time::timeout_at(deadline, receiver)
            .await
            .map_err(|_| ControlledError::Deadline {
                outcome_unknown: true,
            })?
            .map_err(|_| ControlledError::Closed)?;
        if Instant::now() >= deadline {
            return Err(ControlledError::Deadline {
                outcome_unknown: true,
            }
            .into());
        }
        result.map_err(Into::into)
    }
    /// Open an externally admitted controlled generation without exporting native ownership.
    pub async fn open_controlled_group(
        &self,
        request: ControlledOpen,
        authority: Arc<dyn StartupAuthority>,
        admission: StartupAdmission,
        deadline: Instant,
    ) -> Result<ManagedControlledGroup<S>, ManagedControlError> {
        let runtime = self.clone();
        self.controlled_request(deadline, move |shared| async move {
            let group = shared
                .node
                .open_controlled_group_completion(request, authority, admission, None, None)
                .await?;
            // Replay may bypass construction; only an active actual core is publishable.
            if group.receipt()?.stage == multiraft_store::ControlledGroupStage::Opened
                && shared.node.local_group_status(group.group_id()).is_ok()
            {
                shared.publish_controlled(group.group_id(), group.generation(), true);
            }
            shared.register_controlled(group.group_id(), group.generation())?;
            Ok(ManagedControlledGroup { runtime, group })
        })
        .await
    }
    /// Reserve immutable target storage without constructing or initializing a Group.
    pub async fn reserve_controlled_target(
        &self,
        group: GroupId,
        specification: ReplacementSpec,
        identity: Vec<u8>,
        authority: Arc<dyn StartupAuthority>,
        deadline: Instant,
    ) -> Result<ManagedControlledGroup<S>, ManagedControlError> {
        let runtime = self.clone();
        self.controlled_request(deadline, move |shared| async move {
            let group = shared
                .node
                .reserve_controlled_target(group, specification, identity, authority, deadline)
                .await?;
            shared.register_controlled(group.group_id(), group.generation())?;
            Ok(ManagedControlledGroup { runtime, group })
        })
        .await
    }
    /// Inspect an existing receipt without opening a core or granting ordinary request readiness.
    pub async fn inspect_controlled_group(
        &self,
        group: GroupId,
        generation: u64,
        identity: Vec<u8>,
        authority: Arc<dyn StartupAuthority>,
        deadline: Instant,
    ) -> Result<ManagedControlledGroup<S>, ManagedControlError> {
        let runtime = self.clone();
        self.controlled_request(deadline, move |shared| async move {
            let group = shared
                .node
                .inspect_controlled_group(group, generation, identity, authority)
                .await?;
            shared.register_controlled(group.group_id(), group.generation())?;
            Ok(ManagedControlledGroup { runtime, group })
        })
        .await
    }
}
impl<S: StateMachine> ManagedControlledGroup<S> {
    /// Immutable Group identity, independent of local runtime liveness.
    pub fn group_id(&self) -> GroupId {
        self.group.group_id()
    }
    /// Immutable local generation, independent of local runtime liveness.
    pub fn generation(&self) -> u64 {
        self.group.generation()
    }
    async fn request<T, F, Fut>(
        &self,
        deadline: Instant,
        action: F,
    ) -> Result<T, ManagedControlError>
    where
        T: Send + 'static,
        F: FnOnce(ControlledGroup<S>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, ControlledError>> + Send + 'static,
    {
        let group = self.group.clone();
        self.runtime
            .controlled_request(deadline, move |_| action(group))
            .await
    }
    /// Observe durable lifecycle metadata; it does not establish consensus authority.
    pub async fn receipt(
        &self,
        deadline: Instant,
    ) -> Result<ControlledGroupReceipt, ManagedControlError> {
        self.request(deadline, |group| async move { group.receipt() })
            .await
    }
    /// Read actual locally applied native ledger without quorum confirmation.
    pub async fn local_ledger(
        &self,
        deadline: Instant,
    ) -> Result<NativeLedger, ManagedControlError> {
        self.request(deadline, move |group| async move {
            group.local_ledger(deadline).await
        })
        .await
    }
    /// Read native ledger after this request's ReadIndex.
    pub async fn ledger(&self, deadline: Instant) -> Result<NativeLedger, ManagedControlError> {
        self.request(deadline, move |group| async move {
            group.ledger(deadline).await
        })
        .await
    }
    /// Read full committed native membership after this request's ReadIndex.
    pub async fn membership(
        &self,
        deadline: Instant,
    ) -> Result<multiraft_core::typ::StoredMembership, ManagedControlError> {
        self.request(deadline, move |group| async move {
            group.membership(deadline).await
        })
        .await
    }
    /// Verify actual durable original bootstrap state locally, without ReadIndex or initialize.
    pub async fn genesis_basis(
        &self,
        deadline: Instant,
    ) -> Result<ManagedGenesisBasis, ManagedControlError> {
        self.request(deadline, move |group| async move {
            group.genesis_basis(deadline).await
        })
        .await
    }
    /// Bind a stateful portable dispatcher; hosts must authenticate before dispatch.
    pub async fn target_endpoint(
        &self,
        specification: ReplacementSpec,
        deadline: Instant,
    ) -> Result<ManagedTargetEndpoint<S>, ManagedControlError> {
        let group = self.group.clone();
        let runtime = self.runtime.clone();
        self.runtime
            .controlled_request(deadline, move |shared| async move {
                let endpoint = NativeTargetEndpoint::new(group, specification)?;
                let identity = endpoint.identity().clone();
                let id = shared
                    .next_controlled_endpoint
                    .fetch_add(1, Ordering::Relaxed);
                let mut endpoints = shared
                    .controlled_endpoints
                    .lock()
                    .expect("controlled endpoint registry lock poisoned");
                if !shared.accepting.load(Ordering::Acquire) {
                    return Err(ControlledError::Closed);
                }
                endpoints.insert(id, endpoint);
                Ok(ManagedTargetEndpoint {
                    runtime,
                    id,
                    identity,
                })
            })
            .await
    }

    /// Return only terminal facts after native durable reconciliation and authority handoff.
    pub async fn reconcile_terminal(
        &self,
        operation: OperationId,
        deadline: Instant,
    ) -> Result<TerminalProof, ManagedControlError> {
        self.request(deadline, move |group| async move {
            Ok(group
                .reconcile_terminal(operation, deadline)
                .await?
                .proof()
                .clone())
        })
        .await
    }
    /// Resolve retained authority and prove actual local stop, retaining every replica file.
    pub async fn retire_from_authority(
        &self,
        operation: OperationId,
        deadline: Instant,
    ) -> Result<(), ManagedControlError> {
        self.request(deadline, move |group| async move {
            let terminal = group.retirement_from_authority(operation, deadline).await?;
            group.retire(&terminal, deadline).await
        })
        .await
    }
}
impl<S: StateMachine> ManagedTargetEndpoint<S> {
    /// Immutable portable target binding; the returned facts carry no startup authority.
    pub fn identity(&self) -> &NativeTargetIdentity {
        &self.identity
    }
    /// Dispatch an authenticated bounded request through owned admission and shutdown fencing.
    pub async fn dispatch(
        &self,
        bytes: Vec<u8>,
        deadline: Instant,
    ) -> Result<Vec<u8>, ManagedControlError> {
        let id = self.id;
        self.runtime
            .controlled_request(deadline, move |shared| async move {
                let endpoint = shared
                    .controlled_endpoints
                    .lock()
                    .expect("controlled endpoint registry lock poisoned")
                    .get(&id)
                    .cloned()
                    .ok_or(ControlledError::Closed)?;
                endpoint.dispatch(&bytes).await
            })
            .await
    }
}
