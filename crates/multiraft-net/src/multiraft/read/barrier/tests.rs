use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;

/// A confirmation whose rounds complete only when the test releases them.
#[derive(Clone, Default)]
struct Rounds {
    started: Arc<AtomicUsize>,
    release: Arc<Notify>,
}

impl Rounds {
    fn round(
        &self,
    ) -> impl Fn() -> futures::future::BoxFuture<'static, usize> + Clone + Send + 'static {
        let rounds = self.clone();
        move || {
            let rounds = rounds.clone();
            Box::pin(async move {
                let number = rounds.started.fetch_add(1, Ordering::SeqCst) + 1;
                rounds.release.notified().await;
                number
            })
        }
    }

    fn started(&self) -> usize {
        self.started.load(Ordering::SeqCst)
    }

    async fn until_started(&self, rounds: usize) {
        while self.started() < rounds {
            tokio::task::yield_now().await;
        }
    }
}

#[tokio::test]
async fn a_lone_caller_starts_exactly_one_round() {
    let barrier = TestBarrier::new(1);
    let rounds = Rounds::default();
    let waiter = tokio::spawn({
        let barrier = Arc::clone(&barrier);
        let round = rounds.round();
        async move { barrier.confirm(round).await }
    });
    rounds.until_started(1).await;
    rounds.release.notify_one();
    assert_eq!(waiter.await.unwrap(), Ok(1));
    assert_eq!(rounds.started(), 1);
}

#[tokio::test]
async fn callers_arriving_during_a_round_share_only_the_next_round() {
    let barrier = TestBarrier::new(1);
    let rounds = Rounds::default();
    let first = tokio::spawn({
        let barrier = Arc::clone(&barrier);
        let round = rounds.round();
        async move { barrier.confirm(round).await }
    });
    rounds.until_started(1).await;
    // These arrive after round 1 began, so round 1 must not satisfy them.
    let late: Vec<_> = (0..16)
        .map(|_| {
            let barrier = Arc::clone(&barrier);
            let round = rounds.round();
            tokio::spawn(async move { barrier.confirm(round).await })
        })
        .collect();
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        rounds.started(),
        1,
        "late callers must not start a parallel round"
    );
    rounds.release.notify_one();
    assert_eq!(first.await.unwrap(), Ok(1));
    rounds.until_started(2).await;
    rounds.release.notify_one();
    for waiter in late {
        assert_eq!(waiter.await.unwrap(), Ok(2));
    }
    assert_eq!(rounds.started(), 2);
}

#[tokio::test]
async fn a_caller_after_the_queued_round_began_waits_for_a_third_round() {
    let barrier = TestBarrier::new(1);
    let rounds = Rounds::default();
    let spawn = |barrier: &Arc<TestBarrier<usize>>| {
        let barrier = Arc::clone(barrier);
        let round = rounds.round();
        tokio::spawn(async move { barrier.confirm(round).await })
    };
    let first = spawn(&barrier);
    rounds.until_started(1).await;
    let second = spawn(&barrier);
    tokio::task::yield_now().await;
    rounds.release.notify_one();
    rounds.until_started(2).await;
    let third = spawn(&barrier);
    tokio::task::yield_now().await;
    rounds.release.notify_one();
    rounds.until_started(3).await;
    rounds.release.notify_one();
    assert_eq!(first.await.unwrap(), Ok(1));
    assert_eq!(second.await.unwrap(), Ok(2));
    assert_eq!(third.await.unwrap(), Ok(3));
}

#[tokio::test]
async fn failures_reach_every_caller_of_that_round_and_the_barrier_recovers() {
    let barrier = TestBarrier::<Result<usize, usize>>::new(1);
    let rounds = Rounds::default();
    // Round 2 fails; rounds 1 and 3 succeed.
    let round = {
        let round = rounds.round();
        move || {
            let pending = round();
            async move {
                let number = pending.await;
                if number == 2 {
                    Err(number)
                } else {
                    Ok(number)
                }
            }
        }
    };
    let spawn = |barrier: &Arc<TestBarrier<Result<usize, usize>>>| {
        let barrier = Arc::clone(barrier);
        let round = round.clone();
        tokio::spawn(async move { barrier.confirm(round).await })
    };
    let first = spawn(&barrier);
    rounds.until_started(1).await;
    let queued: Vec<_> = (0..3).map(|_| spawn(&barrier)).collect();
    tokio::task::yield_now().await;
    rounds.release.notify_one();
    rounds.until_started(2).await;
    rounds.release.notify_one();
    assert_eq!(first.await.unwrap(), Ok(Ok(1)));
    for waiter in queued {
        assert_eq!(waiter.await.unwrap(), Ok(Err(2)));
    }
    let after = spawn(&barrier);
    rounds.until_started(3).await;
    rounds.release.notify_one();
    assert_eq!(after.await.unwrap(), Ok(Ok(3)));
}

#[tokio::test]
async fn cancelling_the_starting_caller_does_not_strand_joined_callers() {
    let barrier = TestBarrier::new(1);
    let rounds = Rounds::default();
    let starter = tokio::spawn({
        let barrier = Arc::clone(&barrier);
        let round = rounds.round();
        async move { barrier.confirm(round).await }
    });
    rounds.until_started(1).await;
    let joined = tokio::spawn({
        let barrier = Arc::clone(&barrier);
        let round = rounds.round();
        async move { barrier.confirm(round).await }
    });
    tokio::task::yield_now().await;
    starter.abort();
    rounds.release.notify_one();
    rounds.until_started(2).await;
    rounds.release.notify_one();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), joined)
            .await
            .unwrap()
            .unwrap(),
        Ok(2)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn callers_racing_the_end_of_the_last_round_are_never_abandoned() {
    for limit in [1, 4] {
        let barrier = TestBarrier::<()>::new(limit);
        let callers: Vec<_> = (0..16)
            .map(|_| {
                let barrier = Arc::clone(&barrier);
                tokio::spawn(async move {
                    for _ in 0..200 {
                        barrier
                            .confirm(|| async {})
                            .await
                            .expect("a round that did not panic never abandons callers");
                    }
                })
            })
            .collect();
        for caller in callers {
            caller.await.unwrap();
        }
    }
}

#[tokio::test]
async fn callers_below_the_limit_confirm_immediately_and_later_ones_queue_once() {
    let barrier = TestBarrier::new(2);
    let rounds = Rounds::default();
    let spawn = |barrier: &Arc<TestBarrier<usize>>| {
        let barrier = Arc::clone(barrier);
        let round = rounds.round();
        tokio::spawn(async move { barrier.confirm(round).await })
    };
    let first = spawn(&barrier);
    rounds.until_started(1).await;
    // A near-simultaneous second caller keeps its own immediate round.
    let second = spawn(&barrier);
    rounds.until_started(2).await;
    // With two rounds running, later callers share one queued round.
    let queued: Vec<_> = (0..8).map(|_| spawn(&barrier)).collect();
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        rounds.started(),
        2,
        "the running limit bounds confirmation traffic"
    );
    rounds.release.notify_one();
    rounds.until_started(3).await;
    rounds.release.notify_one();
    rounds.release.notify_one();
    let mut outcomes = vec![
        first.await.unwrap().unwrap(),
        second.await.unwrap().unwrap(),
    ];
    outcomes.sort_unstable();
    assert_eq!(outcomes, [1, 2]);
    for waiter in queued {
        assert_eq!(
            waiter.await.unwrap(),
            Ok(3),
            "queued callers share the round started after them"
        );
    }
    assert_eq!(rounds.started(), 3);
}

#[tokio::test]
async fn a_panicking_round_leaves_queued_callers_to_a_surviving_round() {
    let barrier = TestBarrier::<usize>::new(2);
    let rounds = Rounds::default();
    // Round 1 panics once released; rounds 2 and 3 complete normally.
    let round = {
        let round = rounds.round();
        move || {
            let pending = round();
            async move {
                let number = pending.await;
                assert_ne!(number, 1, "simulated backend panic");
                number
            }
        }
    };
    let spawn = |barrier: &Arc<TestBarrier<usize>>| {
        let barrier = Arc::clone(barrier);
        let round = round.clone();
        tokio::spawn(async move { barrier.confirm(round).await })
    };
    let first = spawn(&barrier);
    rounds.until_started(1).await;
    let second = spawn(&barrier);
    rounds.until_started(2).await;
    let queued: Vec<_> = (0..4).map(|_| spawn(&barrier)).collect();
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    rounds.release.notify_one();
    rounds.release.notify_one();
    rounds.until_started(3).await;
    rounds.release.notify_one();
    let mut early = vec![first.await.unwrap(), second.await.unwrap()];
    early.sort_by_key(Result::is_ok);
    assert_eq!(early, [Err(RoundAbandoned), Ok(2)]);
    for waiter in queued {
        assert_eq!(waiter.await.unwrap(), Ok(3));
    }
}

#[tokio::test]
async fn a_panicking_round_abandons_its_callers_and_later_callers_start_fresh() {
    let barrier = TestBarrier::<usize>::new(1);
    let attempts = Arc::new(AtomicUsize::new(0));
    let round = {
        let attempts = Arc::clone(&attempts);
        move || {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            async move {
                assert_ne!(attempt, 0, "simulated backend panic");
                attempt
            }
        }
    };
    assert_eq!(barrier.confirm(round.clone()).await, Err(RoundAbandoned));
    assert_eq!(barrier.confirm(round).await, Ok(1));
}
struct TestBarrier<T> {
    inner: Arc<ReadBarrier<T>>,
    tasks: OwnedTasks,
}
impl<T: Clone + Send + Sync + 'static> TestBarrier<T> {
    fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: ReadBarrier::new(limit),
            tasks: OwnedTasks::default(),
        })
    }
    async fn confirm<C, F>(&self, round: C) -> Result<T, RoundAbandoned>
    where
        C: Fn() -> F + Send + 'static,
        F: Future<Output = T> + Send + 'static,
    {
        self.inner.confirm(&self.tasks, round).await
    }
}

#[tokio::test]
async fn owner_stop_rejects_unpolled_and_running_work_without_detaching() {
    let barrier = TestBarrier::<usize>::new(1);
    let rounds = Rounds::default();
    let first = tokio::spawn({
        let barrier = barrier.clone();
        let round = rounds.round();
        async move { barrier.confirm(round).await }
    });
    rounds.until_started(1).await;
    let queued = tokio::spawn({
        let barrier = barrier.clone();
        let round = rounds.round();
        async move { barrier.confirm(round).await }
    });
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    barrier.tasks.abort();
    barrier.tasks.join().await.unwrap();
    assert_eq!(first.await.unwrap(), Err(RoundAbandoned));
    assert_eq!(queued.await.unwrap(), Err(RoundAbandoned));
    assert_eq!(barrier.confirm(rounds.round()).await, Err(RoundAbandoned));
    assert_eq!(rounds.started(), 1);
    assert_eq!(barrier.inner.state.lock().unwrap().running, 0);
}
#[tokio::test(start_paused = true)]
async fn waiter_deadline_does_not_shorten_the_next_waiter_round() {
    let barrier = TestBarrier::<usize>::new(1);
    let rounds = Rounds::default();
    let first = tokio::spawn({
        let barrier = barrier.clone();
        let round = rounds.round();
        async move { tokio::time::timeout(Duration::from_secs(1), barrier.confirm(round)).await }
    });
    rounds.until_started(1).await;
    let next = tokio::spawn({
        let barrier = barrier.clone();
        let round = rounds.round();
        async move { barrier.confirm(round).await }
    });
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(first.await.unwrap().is_err());
    rounds.release.notify_one();
    rounds.until_started(2).await;
    rounds.release.notify_one();
    assert_eq!(next.await.unwrap(), Ok(2));
    barrier.tasks.join().await.unwrap();
}

#[tokio::test]
async fn four_immediate_rounds_share_exactly_one_queued_round() {
    let barrier = TestBarrier::new(4);
    let rounds = Rounds::default();
    let spawn = || {
        let barrier = barrier.clone();
        let round = rounds.round();
        tokio::spawn(async move { barrier.confirm(round).await })
    };
    let mut immediate = Vec::new();
    for number in 1..=4 {
        immediate.push(spawn());
        rounds.until_started(number).await;
    }
    let queued: Vec<_> = (0..16).map(|_| spawn()).collect();
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert_eq!(rounds.started(), 4);
    rounds.release.notify_one();
    rounds.until_started(5).await;
    for _ in 0..4 {
        rounds.release.notify_one();
        tokio::task::yield_now().await;
    }
    for waiter in immediate {
        assert!(matches!(waiter.await.unwrap(), Ok(1..=4)));
    }
    for waiter in queued {
        assert_eq!(waiter.await.unwrap(), Ok(5));
    }
    assert_eq!(rounds.started(), 5);
    barrier.tasks.join().await.unwrap();
}
#[tokio::test]
async fn last_panicking_runner_abandons_queued_waiters_and_releases_all_slots() {
    let barrier = TestBarrier::<usize>::new(1);
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let spawn = || {
        let barrier = barrier.clone();
        let started = started.clone();
        let release = release.clone();
        tokio::spawn(async move {
            barrier
                .confirm(move || {
                    let started = started.clone();
                    let release = release.clone();
                    async move {
                        started.notify_one();
                        release.notified().await;
                        panic!("controlled round panic");
                    }
                })
                .await
        })
    };
    let first = spawn();
    started.notified().await;
    let queued = spawn();
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    release.notify_one();
    assert_eq!(first.await.unwrap(), Err(RoundAbandoned));
    assert_eq!(queued.await.unwrap(), Err(RoundAbandoned));
    assert_eq!(barrier.inner.state.lock().unwrap().running, 0);
    assert!(
        barrier.tasks.join().await.is_err(),
        "registry retains panic fact without detaching task"
    );
}
