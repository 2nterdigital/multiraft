//! Actual application destruction, independent of native core/tick shutdown.
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

#[derive(Debug, Default)]
pub(super) struct ReleaseState {
    released: AtomicBool,
    changed: Notify,
}

/// Opaque observation of application FSM destruction. Holding it retains no FSM.
#[derive(Clone, Debug)]
pub struct StateMachineRelease {
    pub(super) state: Arc<ReleaseState>,
}

impl StateMachineRelease {
    /// True only after application FSM destruction has completed.
    pub fn is_released(&self) -> bool {
        self.state.released.load(Ordering::Acquire)
    }

    /// Wait until the application FSM's destructor has completed.
    /// Callers bound this observation with their shutdown deadline.
    pub async fn wait(&self) {
        loop {
            let changed = self.state.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.state.released.load(Ordering::Acquire) {
                return;
            }
            changed.await;
        }
    }
}

#[derive(Debug)]
pub(super) struct ReleaseOnDrop(pub(super) Arc<ReleaseState>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.released.store(true, Ordering::Release);
        self.0.changed.notify_waiters();
    }
}

type WeakTrigger = std::sync::Weak<dyn Fn(multiraft_core::GroupId, u64, u64) + Send + Sync>;

/// Weak snapshot callback reference. It cannot retain the FSM or create a callback cycle.
pub struct WeakStateMachineStore<S: multiraft_fsm::StateMachine> {
    group_id: multiraft_core::GroupId,
    inner: std::sync::Weak<futures::lock::Mutex<super::StateMachineStoreInner<S>>>,
    native: Option<std::sync::Weak<super::native::NativeRuntime>>,
    allow_hot_build: bool,
    catalog: Option<Arc<crate::SnapshotCatalog>>,
    trigger: Option<WeakTrigger>,
    release: StateMachineRelease,
}
impl<S: multiraft_fsm::StateMachine> super::StateMachineStore<S> {
    /// Create a resource-neutral weak reference for owned background callbacks.
    pub fn downgrade(&self) -> WeakStateMachineStore<S> {
        WeakStateMachineStore {
            group_id: self.group_id,
            inner: Arc::downgrade(&self.inner),
            native: self.native.as_ref().map(Arc::downgrade),
            allow_hot_build: self.allow_hot_build,
            catalog: self.catalog.clone(),
            trigger: self.on_standby_trigger.as_ref().map(Arc::downgrade),
            release: self.release.clone(),
        }
    }
}
impl<S: multiraft_fsm::StateMachine> WeakStateMachineStore<S> {
    pub fn upgrade(&self) -> Option<super::StateMachineStore<S>> {
        Some(super::StateMachineStore {
            group_id: self.group_id,
            inner: self.inner.upgrade()?,
            native: match &self.native {
                Some(native) => Some(native.upgrade()?),
                None => None,
            },
            allow_hot_build: self.allow_hot_build,
            catalog: self.catalog.clone(),
            on_standby_trigger: self.trigger.as_ref().and_then(std::sync::Weak::upgrade),
            release: self.release.clone(),
        })
    }
}
