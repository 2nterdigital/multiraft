//! Retain staged-transfer destruction until the receiving owner is quiescent.
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::Notify;

#[derive(Default)]
pub(in crate::sm_bridge) struct CleanupWork {
    active: AtomicUsize,
    changed: Notify,
}

pub(super) struct CleanupGuard(Arc<CleanupWork>);

impl CleanupWork {
    pub(super) fn retain(self: &Arc<Self>) -> CleanupGuard {
        self.active.fetch_add(1, Ordering::AcqRel);
        CleanupGuard(self.clone())
    }

    pub(in crate::sm_bridge) async fn wait(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.active.load(Ordering::Acquire) == 0 {
                return;
            }
            changed.await;
        }
    }
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        if self.0.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.changed.notify_waiters();
        }
    }
}
