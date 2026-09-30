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
    pub(crate) fn close(&self) {
        self.state.lock().unwrap().closed = true;
    }

    pub(crate) fn abort(&self) {
        let mut state = self.state.lock().unwrap();
        state.closed = true;
        state.tasks.abort_all();
    }
    pub(crate) async fn join(&self) -> anyhow::Result<()> {
        let (mut tasks, first_error) = {
            let mut state = self.state.lock().unwrap();
            state.closed = true;
            (std::mem::take(&mut state.tasks), state.first_error.take())
        };
        let mut joined = TaskState {
            first_error,
            ..TaskState::default()
        };
        while let Some(result) = tasks.join_next().await {
            joined.record(result);
        }
        joined.first_error.map_or(Ok(()), Err)
    }
}
