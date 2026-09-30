//! A caller can join only a round that has not started; four running and one queued.
use crate::multiraft::tasks::OwnedTasks;
use std::future::Future;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

pub(super) struct ReadBarrier<T> {
    max_running: usize,
    state: Mutex<State<T>>,
}
struct State<T> {
    running: usize,
    next: Option<watch::Sender<Option<T>>>,
}
#[derive(Debug, PartialEq, Eq)]
pub(super) struct RoundAbandoned;

impl<T: Clone + Send + Sync + 'static> ReadBarrier<T> {
    pub(super) fn new(max_running: usize) -> Arc<Self> {
        assert!(max_running > 0);
        Arc::new(Self {
            max_running,
            state: Mutex::new(State {
                running: 0,
                next: None,
            }),
        })
    }
    pub(super) async fn confirm<C, F>(
        self: &Arc<Self>,
        tasks: &OwnedTasks,
        round: C,
    ) -> Result<T, RoundAbandoned>
    where
        C: Fn() -> F + Send + 'static,
        F: Future<Output = T> + Send + 'static,
    {
        let (mut outcome, publish) = {
            let mut state = self.state.lock().unwrap();
            if state.running < self.max_running {
                state.running += 1;
                let (publish, outcome) = watch::channel(None);
                (outcome, Some(publish))
            } else {
                (
                    state
                        .next
                        .get_or_insert_with(|| watch::channel(None).0)
                        .subscribe(),
                    None,
                )
            }
        };
        if let Some(publish) = publish {
            // Construct guard BEFORE spawn: rejected registration/unpolled abort must release the slot.
            // Do this outside the state lock: rejected futures are synchronously dropped by registry.
            let release = ReleaseOnUnwind {
                barrier: Arc::clone(self),
                active: true,
            };
            tasks.spawn(run(release, round, publish));
        }
        let result = match outcome.wait_for(Option::is_some).await {
            Ok(value) => Ok(value.clone().expect("published outcome")),
            Err(_) => Err(RoundAbandoned),
        };
        result
    }
}
async fn run<T, C, F>(
    mut release: ReleaseOnUnwind<T>,
    round: C,
    mut publish: watch::Sender<Option<T>>,
) where
    C: Fn() -> F,
    F: Future<Output = T>,
{
    loop {
        let outcome = round().await;
        publish.send_replace(Some(outcome));
        let mut state = release.barrier.state.lock().unwrap();
        if let Some(queued) = state.next.take() {
            publish = queued;
        } else {
            state.running -= 1;
            drop(state);
            release.active = false;
            return;
        }
    }
}
struct ReleaseOnUnwind<T> {
    barrier: Arc<ReadBarrier<T>>,
    active: bool,
}
impl<T> Drop for ReleaseOnUnwind<T> {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut state = self
            .barrier
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.running -= 1;
        if state.running == 0 {
            state.next = None;
        }
    }
}

#[cfg(test)]
mod tests;
