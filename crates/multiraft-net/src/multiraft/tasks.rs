//! Library-owned tasks with atomic close/register and retained task failures.
use std::future::Future;
use std::sync::Mutex;
use tokio::task::{JoinError, JoinSet};

#[derive(Default)]
struct TaskState {
    closed: bool,
    tasks: JoinSet<anyhow::Result<()>>,
    first_error: Option<anyhow::Error>,
}
impl TaskState {
    fn record(&mut self, result: Result<anyhow::Result<()>, JoinError>) {
        match result {
            Ok(Err(error)) => {
                self.first_error.get_or_insert(error);
            }
            Err(error) if !error.is_cancelled() => {
                self.first_error.get_or_insert(error.into());
            }
            _ => {}
        }
    }
}
#[derive(Default)]
pub(crate) struct OwnedTasks {
    state: Mutex<TaskState>,
    joining: tokio::sync::Mutex<()>,
}

impl OwnedTasks {
    /// `false` means fenced: future is dropped without being polled or spawned.
    pub(crate) fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) -> bool {
        self.spawn_result(async move {
            future.await;
            Ok(())
        })
    }
    /// Registration and close use the same lock, so join cannot miss a late child.
    pub(crate) fn spawn_result(
        &self,
        future: impl Future<Output = anyhow::Result<()>> + Send + 'static,
    ) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return false;
        }
        while let Some(result) = state.tasks.try_join_next() {
            state.record(result);
        }
        state.tasks.spawn(future);
        true
    }
    pub(crate) fn abort(&self) {
        let mut state = self.state.lock().unwrap();
        state.closed = true;
        state.tasks.abort_all();
    }
    /// Fence registration without interrupting tasks or their non-abortable children.
    pub(crate) fn close(&self) {
        self.state.lock().unwrap().closed = true;
    }

    /// Cancellation abandons only this waiter. Handles stay in the registry so
    /// another join still waits for real completion, including blocking children.
    pub(crate) async fn join(&self) -> anyhow::Result<()> {
        self.close();
        let _joining = self.joining.lock().await;
        futures::future::poll_fn(|context| {
            let mut state = self.state.lock().unwrap();
            loop {
                match state.tasks.poll_join_next(context) {
                    std::task::Poll::Ready(Some(result)) => state.record(result),
                    std::task::Poll::Ready(None) => {
                        return std::task::Poll::Ready(
                            state.first_error.take().map_or(Ok(()), Err),
                        );
                    }
                    std::task::Poll::Pending => return std::task::Poll::Pending,
                }
            }
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Notify;

    #[tokio::test]
    async fn canceled_join_retains_task_and_fences_late_registration_until_actual_completion() {
        let tasks = Arc::new(OwnedTasks::default());
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let child_entered = entered.clone();
        let child_release = release.clone();
        let (finished, completion) = tokio::sync::oneshot::channel();
        assert!(tasks.spawn(async move {
            child_entered.notify_one();
            child_release.notified().await;
            let _ = finished.send(());
        }));
        entered.notified().await;
        let joining_tasks = tasks.clone();
        let waiter = tokio::spawn(async move { joining_tasks.join().await });
        tokio::task::yield_now().await;
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert!(!tasks.spawn(async {}));
        // A moved/dropped JoinSet would have aborted the child and closed this
        // completion receiver. Retained join waits for the real child instead.
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), tasks.join())
            .await
            .unwrap()
            .unwrap();
        completion.await.unwrap();
    }
}
