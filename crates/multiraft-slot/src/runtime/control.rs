//! Slot-addressed managed control; raw native ownership and sealed mutation evidence never escape.
use super::{Shared, SlotHandle, SlotStateMachine};
use crate::{RuntimeError, SlotId};
use multiraft_core::{NativeLedger, OperationId, ReplacementSpec, TerminalProof};
use multiraft_net::{
    controlled_startup::StartupAdmission,
    portable::{NativeTargetIdentity, SourceTargetPort},
    ControlledGroupReceipt, ManagedControlError, ManagedControlledGroup, ManagedGenesisBasis,
    ManagedOperation, ManagedTargetEndpoint,
};
use std::{future::Future, sync::Arc};
use tokio::time::Instant;

/// Slot admission failures preserve native controlled uncertainty and rejection categories.
#[derive(Debug, thiserror::Error)]
pub enum SlotControlError {
    #[error(transparent)]
    Slot(#[from] RuntimeError),
    #[error(transparent)]
    Native(#[from] ManagedControlError),
}
/// Weak, fixed-Slot control facade. No arbitrary native Group can be addressed.
pub struct SlotControlledGroup<S: SlotStateMachine> {
    handle: SlotHandle<S>,
    slot: SlotId,
}
impl<S: SlotStateMachine> Clone for SlotControlledGroup<S> {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            slot: self.slot,
        }
    }
}
impl<S: SlotStateMachine> std::fmt::Debug for SlotControlledGroup<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlotControlledGroup")
            .field("slot", &self.slot)
            .finish_non_exhaustive()
    }
}
/// One native executor incarnation, limited to the fixed replacement steps.
pub struct SlotManagedOperation<S: SlotStateMachine> {
    group: SlotControlledGroup<S>,
    operation: ManagedOperation<S>,
}
impl<S: SlotStateMachine> Clone for SlotManagedOperation<S> {
    fn clone(&self) -> Self {
        Self {
            group: self.group.clone(),
            operation: self.operation.clone(),
        }
    }
}
impl<S: SlotStateMachine> std::fmt::Debug for SlotManagedOperation<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.group.fmt(f)
    }
}
/// Cached weak portable dispatcher; actual transfer state belongs to the native runtime.
pub struct SlotTargetEndpoint<S: SlotStateMachine> {
    group: SlotControlledGroup<S>,
    endpoint: ManagedTargetEndpoint<S>,
}
impl<S: SlotStateMachine> Clone for SlotTargetEndpoint<S> {
    fn clone(&self) -> Self {
        Self {
            group: self.group.clone(),
            endpoint: self.endpoint.clone(),
        }
    }
}
impl<S: SlotStateMachine> std::fmt::Debug for SlotTargetEndpoint<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.endpoint.fmt(f)
    }
}
impl<S: SlotStateMachine> SlotHandle<S> {
    /// Address one fixed Slot's controlled generation. This does not construct a native Group.
    pub fn controlled(&self, slot: SlotId) -> Result<SlotControlledGroup<S>, SlotControlError> {
        self.layout
            .group_for_slot(slot)
            .map_err(|_| RuntimeError::InvalidRequest)?;
        if self.shared()?.controlled.is_none() {
            return Err(RuntimeError::Unavailable.into());
        }
        Ok(SlotControlledGroup {
            handle: self.clone(),
            slot,
        })
    }
    /// Reserve the plan's exact target generation without constructing a Core.
    pub async fn reserve_controlled_target(
        &self,
        slot: SlotId,
        specification: ReplacementSpec,
        identity: Vec<u8>,
        deadline: Instant,
    ) -> Result<SlotControlledGroup<S>, SlotControlError> {
        let facade = self.controlled(slot)?;
        self.control_call(slot, identity.len(), deadline, move |shared| async move {
            let controlled = shared
                .controlled
                .as_ref()
                .ok_or(RuntimeError::Unavailable)?;
            let group_id = shared
                .layout
                .group_for_slot(slot)
                .map_err(|_| RuntimeError::InvalidRequest)?;
            let group = shared
                .native
                .reserve_controlled_target(
                    group_id,
                    specification,
                    identity,
                    controlled.authority.clone(),
                    deadline,
                )
                .await?;
            controlled.groups.lock().unwrap().insert(slot, group);
            Ok(())
        })
        .await?;
        Ok(facade)
    }
    pub(super) async fn control_call<T, F, Fut>(
        &self,
        slot: SlotId,
        bytes: usize,
        deadline: Instant,
        action: F,
    ) -> Result<T, SlotControlError>
    where
        T: Send + 'static,
        F: FnOnce(Arc<Shared<S>>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, SlotControlError>> + Send + 'static,
    {
        let shared = self.shared()?;
        shared.check(deadline)?;
        let guard = shared.budget.acquire(slot, bytes)?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        {
            let mut tasks = shared.tasks.lock().unwrap();
            shared.check(deadline)?;
            let pinned = shared.clone();
            tasks.spawn(async move {
                let _guard = guard;
                let result = action(pinned).await;
                let _ = sender.send(result);
            });
        }
        tokio::time::timeout_at(deadline, receiver)
            .await
            .map_err(|_| RuntimeError::Unknown)?
            .map_err(|_| RuntimeError::Unknown)?
    }
}
impl<S: SlotStateMachine> SlotControlledGroup<S> {
    /// Immutable fixed Slot address.
    pub fn slot(&self) -> SlotId {
        self.slot
    }
    async fn call<T, F, Fut>(&self, deadline: Instant, action: F) -> Result<T, SlotControlError>
    where
        T: Send + 'static,
        F: FnOnce(ManagedControlledGroup<S>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, ManagedControlError>> + Send + 'static,
    {
        let slot = self.slot;
        self.handle
            .control_call(slot, 0, deadline, move |shared| async move {
                let group = shared
                    .controlled
                    .as_ref()
                    .ok_or(RuntimeError::Unavailable)?
                    .groups
                    .lock()
                    .unwrap()
                    .get(&slot)
                    .cloned()
                    .ok_or(RuntimeError::Unavailable)?;
                action(group).await.map_err(Into::into)
            })
            .await
    }
    /// Actual local persistent generation receipt; no consensus authority is implied.
    pub async fn receipt(&self, d: Instant) -> Result<ControlledGroupReceipt, SlotControlError> {
        self.call(d, move |g| async move { g.receipt(d).await })
            .await
    }
    /// Locally durable native ledger without ReadIndex.
    pub async fn local_ledger(&self, d: Instant) -> Result<NativeLedger, SlotControlError> {
        self.call(d, move |g| async move { g.local_ledger(d).await })
            .await
    }
    /// Native ledger after this request's ReadIndex.
    pub async fn ledger(&self, d: Instant) -> Result<NativeLedger, SlotControlError> {
        self.call(d, move |g| async move { g.ledger(d).await })
            .await
    }
    /// Full committed native membership after this request's ReadIndex.
    pub async fn membership(
        &self,
        d: Instant,
    ) -> Result<multiraft_core::typ::StoredMembership, SlotControlError> {
        self.call(d, move |g| async move { g.membership(d).await })
            .await
    }
    /// Sealed actual durable original Genesis basis, with no deserialization capability.
    pub async fn genesis_basis(&self, d: Instant) -> Result<ManagedGenesisBasis, SlotControlError> {
        self.call(d, move |g| async move { g.genesis_basis(d).await })
            .await
    }
    /// Bind the exact operation and native executor incarnation.
    pub async fn begin(
        &self,
        specification: ReplacementSpec,
        epoch: u64,
        target: SourceTargetPort,
        d: Instant,
    ) -> Result<SlotManagedOperation<S>, SlotControlError> {
        let operation = self
            .call(d, move |g| async move {
                g.begin(specification, epoch, target, d).await
            })
            .await?;
        Ok(SlotManagedOperation {
            group: self.clone(),
            operation,
        })
    }
    /// Commit the same operation's higher executor epoch before acknowledging ownership.
    pub async fn claim(
        &self,
        operation: OperationId,
        epoch: u64,
        d: Instant,
    ) -> Result<SlotManagedOperation<S>, SlotControlError> {
        let operation = self
            .call(
                d,
                move |g| async move { g.claim(operation, epoch, d).await },
            )
            .await?;
        Ok(SlotManagedOperation {
            group: self.clone(),
            operation,
        })
    }
    /// Reuse one dispatcher for the exact immutable specification across transport requests.
    pub async fn target_endpoint(
        &self,
        specification: ReplacementSpec,
        d: Instant,
    ) -> Result<SlotTargetEndpoint<S>, SlotControlError> {
        let slot = self.slot;
        let endpoint = self
            .handle
            .control_call(slot, 0, d, move |shared| async move {
                let controlled = shared
                    .controlled
                    .as_ref()
                    .ok_or(RuntimeError::Unavailable)?;
                let mut key = slot.0.to_be_bytes().to_vec();
                key.extend(
                    serde_json::to_vec(&specification).map_err(|_| RuntimeError::InvalidRequest)?,
                );
                if let Some(endpoint) = controlled.endpoints.lock().unwrap().get(&key).cloned() {
                    return Ok(endpoint);
                }
                let group = controlled
                    .groups
                    .lock()
                    .unwrap()
                    .get(&slot)
                    .cloned()
                    .ok_or(RuntimeError::Unavailable)?;
                let endpoint = group.target_endpoint(specification, d).await?;
                // Native admission owns actual transfer state. Serialize cache insertion so
                // every later request resolves the same endpoint, including concurrent binds.
                let mut endpoints = controlled.endpoints.lock().unwrap();
                Ok(endpoints.entry(key).or_insert(endpoint).clone())
            })
            .await?;
        Ok(SlotTargetEndpoint {
            group: self.clone(),
            endpoint,
        })
    }
    /// Reconcile terminal facts without minting native retirement capabilities.
    pub async fn reconcile_terminal(
        &self,
        operation: OperationId,
        d: Instant,
    ) -> Result<TerminalProof, SlotControlError> {
        self.call(d, move |g| async move {
            g.reconcile_terminal(operation, d).await
        })
        .await
    }
    /// Verify retained native authority, stop this source, and retain all files.
    pub async fn retire_from_authority(
        &self,
        operation: OperationId,
        d: Instant,
    ) -> Result<(), SlotControlError> {
        self.call(d, move |g| async move {
            g.retire_from_authority(operation, d).await
        })
        .await
    }
}
impl<S: SlotStateMachine> SlotManagedOperation<S> {
    async fn call<T, F, Fut>(&self, d: Instant, action: F) -> Result<T, SlotControlError>
    where
        T: Send + 'static,
        F: FnOnce(ManagedOperation<S>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, ManagedControlError>> + Send + 'static,
    {
        let operation = self.operation.clone();
        self.group
            .handle
            .control_call(self.group.slot, 0, d, move |_| async move {
                action(operation).await.map_err(Into::into)
            })
            .await
    }
    /// Install the actual native checkpoint through the authenticated target port.
    pub async fn seed_target(
        &self,
        target: SourceTargetPort,
        admission: StartupAdmission,
        d: Instant,
    ) -> Result<(), SlotControlError> {
        self.call(d, move |o| async move {
            o.seed_target(target, admission, d).await
        })
        .await
    }
    /// Add only the exact target learner under the native application barrier.
    pub async fn add_learner(
        &self,
        target: SourceTargetPort,
        d: Instant,
    ) -> Result<(), SlotControlError> {
        self.call(d, move |o| async move { o.add_learner(target, d).await })
            .await
    }
    /// Enter the exact old/new joint configuration after actual target catch-up.
    pub async fn enter_joint(
        &self,
        target: SourceTargetPort,
        d: Instant,
    ) -> Result<(), SlotControlError> {
        self.call(d, move |o| async move { o.enter_joint(target, d).await })
            .await
    }
    /// Finalize the operation's exact uniform target configuration.
    pub async fn finalize_uniform(&self, d: Instant) -> Result<(), SlotControlError> {
        self.call(d, move |o| async move { o.finalize_uniform(d).await })
            .await
    }
    /// Verify durable native terminal application and target receipt.
    pub async fn complete(
        &self,
        target: SourceTargetPort,
        d: Instant,
    ) -> Result<TerminalProof, SlotControlError> {
        self.call(d, move |o| async move { o.complete(target, d).await })
            .await
    }
}
impl<S: SlotStateMachine> SlotTargetEndpoint<S> {
    /// Portable target facts; these grant no native startup or retirement authority.
    pub fn identity(&self) -> &NativeTargetIdentity {
        self.endpoint.identity()
    }
    /// Dispatch bounded, authenticated bytes under Slot and native admission.
    pub async fn dispatch(&self, bytes: Vec<u8>, d: Instant) -> Result<Vec<u8>, SlotControlError> {
        let endpoint = self.endpoint.clone();
        self.group
            .handle
            .control_call(self.group.slot, bytes.len(), d, move |_| async move {
                endpoint.dispatch(bytes, d).await.map_err(Into::into)
            })
            .await
    }
}
