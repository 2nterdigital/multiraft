//! Lab-only ReadIndex sharing for independent group-version queries.
use super::{Shared, SlotHandle, SlotStateMachine};
use crate::limits::{Guard, MAX_EFFECTS_BYTES, MAX_QUERY_BYTES};
use crate::{runtime_error::native, RuntimeError, SlotId};
use futures::FutureExt;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    sync::{oneshot, watch, Notify, OwnedSemaphorePermit, Semaphore},
    time::{sleep_until, timeout_at, Instant},
};

const MAX_MEMBERS: usize = 8;
const MAX_BATCHES_PER_SLOT: usize = 2;
const WINDOW_ENV: &str = "ECH0_LAB_GROUP_VERSION_READ_BATCH_MS";

type QueryResult = Result<Vec<u8>, RuntimeError>;
type ReplyHold = (oneshot::Receiver<Vec<QueryResult>>, oneshot::Sender<()>);

pub(super) fn configured_window() -> Result<u64, RuntimeError> {
    match std::env::var(WINDOW_ENV).as_deref() {
        Err(std::env::VarError::NotPresent) | Ok("0") => Ok(0),
        Ok("1") => Ok(1),
        Ok("2") => Ok(2),
        _ => Err(RuntimeError::InvalidConfig),
    }
}

pub(super) struct BatchReads {
    window_ms: AtomicU64,
    slots: Vec<SlotBatches>,
    closed: watch::Sender<bool>,
}

impl BatchReads {
    pub(super) fn new(slots: u32, window_ms: u64) -> Self {
        Self {
            window_ms: AtomicU64::new(window_ms),
            slots: (0..slots).map(|_| SlotBatches::default()).collect(),
            closed: watch::channel(false).0,
        }
    }

    pub(super) fn close(&self) {
        self.closed.send_replace(true);
    }

    fn window(&self) -> Duration {
        Duration::from_millis(self.window_ms.load(Ordering::Relaxed))
    }

    fn slot(&self, slot: SlotId) -> Result<&SlotBatches, RuntimeError> {
        self.slots
            .get(slot.0 as usize)
            .ok_or(RuntimeError::InvalidRequest)
    }
}

struct SlotBatches {
    collecting: Mutex<Option<Collecting>>,
    permits: Arc<Semaphore>,
    hold: Mutex<Option<Hold>>,
}

impl Default for SlotBatches {
    fn default() -> Self {
        Self {
            collecting: Mutex::new(None),
            permits: Arc::new(Semaphore::new(MAX_BATCHES_PER_SLOT)),
            hold: Mutex::new(None),
        }
    }
}

struct Collecting {
    started: Instant,
    cutoff: Instant,
    items: Vec<Item>,
    wake: Arc<Notify>,
}

impl Collecting {
    fn new(started: Instant, window: Duration, first: Item) -> Self {
        Self {
            started,
            cutoff: started + window,
            items: vec![first],
            wake: Arc::new(Notify::new()),
        }
    }

    fn push(&mut self, item: Item, now: Instant) -> Result<(), Item> {
        self.items.retain(|item| !item.reply.is_closed());
        if now >= self.cutoff || item.deadline <= self.cutoff || self.items.len() == MAX_MEMBERS {
            return Err(item);
        }
        self.items.push(item);
        if self.items.len() == MAX_MEMBERS {
            self.wake.notify_one();
        }
        Ok(())
    }
}

struct Item {
    query: Vec<u8>,
    deadline: Instant,
    reply: oneshot::Sender<QueryResult>,
    _guard: Guard,
}

struct Waiter {
    reply: oneshot::Receiver<QueryResult>,
    wake: Arc<Notify>,
}

impl Drop for Waiter {
    fn drop(&mut self) {
        self.reply.close();
        self.wake.notify_one();
    }
}

struct Hold {
    captured: oneshot::Sender<Vec<QueryResult>>,
    release: oneshot::Receiver<()>,
}

impl<S: SlotStateMachine> SlotHandle<S> {
    /// Lab-only strict GroupVersion query. A zero window uses the standard read.
    pub async fn read_lab_group_version(
        &self,
        key: &[u8],
        query: &[u8],
        deadline: Instant,
    ) -> Result<Vec<u8>, RuntimeError> {
        let Some(shared) = self
            .shared
            .upgrade()
            .filter(|s| !s.lab_group_version_reads.window().is_zero())
        else {
            return self.read_linearizable(key, query, deadline).await;
        };
        let slot = self.validate(key, query, MAX_QUERY_BYTES)?;
        crate::rpc::route_lab_group_version_read(
            &shared,
            slot,
            key.to_vec(),
            query.to_vec(),
            deadline,
        )
        .await
    }

    #[doc(hidden)]
    pub fn set_lab_group_version_batch_window(&self, window: Duration) -> Result<(), RuntimeError> {
        let window_ms = match window {
            d if d == Duration::ZERO => 0,
            d if d == Duration::from_millis(1) => 1,
            d if d == Duration::from_millis(2) => 2,
            _ => return Err(RuntimeError::InvalidConfig),
        };
        self.shared()?
            .lab_group_version_reads
            .window_ms
            .store(window_ms, Ordering::Relaxed);
        Ok(())
    }

    /// Test hook. Observe captured replies after the FSM lock is released, then resume delivery.
    #[doc(hidden)]
    pub fn hold_next_lab_group_version_batch_reply(
        &self,
        slot: SlotId,
    ) -> Result<ReplyHold, RuntimeError> {
        let shared = self.shared()?;
        let mut hold = shared
            .lab_group_version_reads
            .slot(slot)?
            .hold
            .lock()
            .unwrap();
        if hold.is_some() || shared.lab_group_version_reads.window().is_zero() {
            return Err(RuntimeError::Busy);
        }
        let (captured, observation) = oneshot::channel();
        let (resume, release) = oneshot::channel();
        *hold = Some(Hold { captured, release });
        Ok((observation, resume))
    }
}

impl<S: SlotStateMachine> Shared<S> {
    pub(crate) async fn execute_lab_group_version_read(
        self: &Arc<Self>,
        slot: SlotId,
        query: Vec<u8>,
        guard: Guard,
        deadline: Instant,
    ) -> QueryResult {
        self.check(deadline)?;
        let window = self.lab_group_version_reads.window();
        if window.is_zero() || !S::is_lab_group_version_query(&query) {
            return Err(RuntimeError::InvalidRequest);
        }

        let (reply, receiver) = oneshot::channel();
        let item = Item {
            query,
            deadline,
            reply,
            _guard: guard,
        };
        match self.join_lab_batch(slot, window, item)? {
            Ok(wake) => {
                let mut waiter = Waiter {
                    reply: receiver,
                    wake,
                };
                timeout_at(deadline, &mut waiter.reply)
                    .await
                    .map_err(|_| RuntimeError::Deadline)?
                    .map_err(|_| RuntimeError::Unavailable)?
            }
            Err(item) => self
                .execute_query(slot, &item.query, openraft::ReadPolicy::ReadIndex, deadline)
                .await
                .map(|view| view.value),
        }
    }

    fn join_lab_batch(
        self: &Arc<Self>,
        slot: SlotId,
        window: Duration,
        item: Item,
    ) -> Result<Result<Arc<Notify>, Item>, RuntimeError> {
        let lane = self.lab_group_version_reads.slot(slot)?;
        let mut collecting = lane.collecting.lock().unwrap();
        let now = Instant::now();
        if let Some(batch) = collecting.as_mut() {
            return Ok(batch.push(item, now).map(|()| batch.wake.clone()));
        }
        if item.deadline <= now + window {
            return Ok(Err(item));
        }
        let Ok(permit) = lane.permits.clone().try_acquire_owned() else {
            return Ok(Err(item));
        };

        // This registration uses the same fence as the existing owned Slot writes.
        let mut tasks = self.tasks.lock().unwrap();
        self.check(item.deadline)?;
        while let Some(result) = tasks.try_join_next() {
            if result.is_err() {
                self.panicked.store(true, Ordering::Release);
                drop(tasks);
                drop(collecting);
                self.begin_cleanup();
                return Err(RuntimeError::TaskPanicked);
            }
        }
        let batch = Collecting::new(now, window, item);
        let wake = batch.wake.clone();
        let task_wake = wake.clone();
        let cutoff = batch.cutoff;
        *collecting = Some(batch);
        let shared = self.clone();
        tasks.spawn(async move { shared.run_lab_batch(slot, cutoff, task_wake, permit).await });
        Ok(Ok(wake))
    }

    async fn run_lab_batch(
        self: Arc<Self>,
        slot: SlotId,
        cutoff: Instant,
        wake: Arc<Notify>,
        _permit: OwnedSemaphorePermit,
    ) {
        let lane = &self.lab_group_version_reads.slots[slot.0 as usize];
        let mut closed = self.lab_group_version_reads.closed.subscribe();
        let batch = loop {
            {
                let mut collecting = lane.collecting.lock().unwrap();
                let batch = collecting
                    .as_mut()
                    .expect("the batch task exclusively owns its collecting entry");
                batch.items.retain(|item| !item.reply.is_closed());
                if *closed.borrow()
                    || batch.items.is_empty()
                    || batch.items.len() == MAX_MEMBERS
                    || Instant::now() >= cutoff
                {
                    break collecting.take().expect("the collecting entry exists");
                }
            }
            tokio::select! {
                _ = closed.changed() => {}
                _ = sleep_until(cutoff) => {}
                _ = wake.notified() => {}
            }
        };
        let Some(deadline) = batch.items.iter().map(|item| item.deadline).max() else {
            return;
        };
        if let Err(error) = self.check(deadline) {
            for item in batch.items {
                let _ = item.reply.send(Err(error.clone()));
            }
            return;
        }
        let group = match self.layout.group_for_slot(slot) {
            Ok(group) => group,
            Err(_) => {
                for item in batch.items {
                    let _ = item.reply.send(Err(RuntimeError::InvalidRequest));
                }
                return;
            }
        };

        let items = batch.items;
        multiraft_net::lab_read_batch_metrics::record_group_version_batch(
            slot.0,
            items.len() as u64,
            batch.started.elapsed().as_micros() as u64,
        );
        let results = {
            let all_cancelled = async {
                loop {
                    if items.iter().all(|item| item.reply.is_closed()) {
                        return;
                    }
                    wake.notified().await;
                }
            };
            tokio::pin!(all_cancelled);
            let query = |fsm: &S| {
                items
                    .iter()
                    .map(|item| {
                        if item.reply.is_closed() || Instant::now() >= item.deadline {
                            return Err(RuntimeError::Deadline);
                        }
                        let value = fsm
                            .query(group, &item.query)
                            .map_err(|_| RuntimeError::Application)?;
                        if value.len() > MAX_EFFECTS_BYTES {
                            return Err(RuntimeError::PayloadTooLarge);
                        }
                        Ok(value)
                    })
                    .collect::<Vec<QueryResult>>()
            };

            let read = self.native.read_pinned_linearizable(group, deadline, query);
            let result = tokio::select! {
                biased;
                _ = closed.changed() => Err(RuntimeError::Closed),
                _ = &mut all_cancelled => return,
                result = std::panic::AssertUnwindSafe(read).catch_unwind() => match result {
                    Ok(result) => result.map_err(|error| native(error, false)),
                    Err(_) => {
                        self.panicked.store(true, Ordering::Release);
                        self.begin_cleanup();
                        Err(RuntimeError::TaskPanicked)
                    }
                },
            };
            match result {
                Ok(results) => {
                    let hold = lane.hold.lock().unwrap().take();
                    if let Some(Hold { captured, release }) = hold {
                        if captured.send(results.clone()).is_ok() {
                            tokio::select! {
                                biased;
                                _ = closed.changed() => return,
                                _ = &mut all_cancelled => return,
                                _ = timeout_at(deadline, release) => {}
                            }
                        }
                    }
                    results
                }
                Err(error) => vec![Err(error); items.len()],
            }
        };
        tracing::debug!(
            target: "multiraft::lab_group_version_batch",
            node_id = self.node_id,
            slot = slot.0,
            members = items.len(),
            succeeded = results.iter().filter(|result| result.is_ok()).count(),
            "completed GroupVersion batch"
        );
        for (item, result) in items.into_iter().zip(results) {
            let result = if Instant::now() >= item.deadline {
                Err(RuntimeError::Deadline)
            } else {
                result
            };
            // Reply bytes and queued query bytes are both covered while retained.
            drop(item.query);
            drop(item._guard);
            let _ = item.reply.send(result);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::limits::{Budget, SlotLimits};

    fn member(budget: &Arc<Budget>, deadline: Instant) -> (Item, oneshot::Receiver<QueryResult>) {
        let (reply, receiver) = oneshot::channel();
        let item = Item {
            query: vec![1],
            deadline,
            reply,
            _guard: budget.acquire(SlotId(0), 1).unwrap(),
        };
        (item, receiver)
    }

    #[test]
    fn collection_has_a_fixed_cutoff_and_rejects_short_budgets() {
        let budget = Budget::new(SlotLimits::default(), 1);
        let start = Instant::now();
        let window = Duration::from_millis(2);
        let deadline = start + Duration::from_secs(1);
        let (first, _first_waiter) = member(&budget, deadline);
        let mut batch = Collecting::new(start, window, first);
        let (later, _later_waiter) = member(&budget, deadline);
        let before_cutoff = start + Duration::from_millis(1);
        assert!(batch.push(later, before_cutoff).is_ok());
        assert_eq!(batch.cutoff, start + window);
        let (short, _short_waiter) = member(&budget, batch.cutoff);
        assert!(batch.push(short, before_cutoff).is_err());
        let (late, _late_waiter) = member(&budget, deadline);
        assert!(batch.push(late, batch.cutoff).is_err());
        assert_eq!(batch.items.len(), 2);
        assert_eq!(budget.snapshot().global.requests, 2);
    }

    #[test]
    fn cap_bypasses_and_cancelled_members_release_their_admission() {
        let budget = Budget::new(SlotLimits::default(), 1);
        let start = Instant::now();
        let deadline = start + Duration::from_secs(1);
        let (first, first_waiter) = member(&budget, deadline);
        let mut batch = Collecting::new(start, Duration::from_millis(2), first);
        let mut waiters = vec![first_waiter];
        for _ in 1..MAX_MEMBERS {
            let (item, waiter) = member(&budget, deadline);
            assert!(batch.push(item, start).is_ok());
            waiters.push(waiter);
        }
        let (overflow, _overflow_waiter) = member(&budget, deadline);
        assert!(batch.push(overflow, start).is_err());
        assert_eq!(budget.snapshot().global.requests, MAX_MEMBERS);

        drop(waiters.pop());
        let (replacement, _replacement_waiter) = member(&budget, deadline);
        assert!(batch.push(replacement, start).is_ok());
        assert_eq!(budget.snapshot().global.requests, MAX_MEMBERS);
        drop(batch);
        assert_eq!(budget.snapshot().global.requests, 0);
    }

    #[test]
    fn per_slot_permits_allow_two_independent_batches() {
        let lanes = BatchReads::new(2, 1);
        let first = &lanes.slot(SlotId(0)).unwrap().permits;
        let second = &lanes.slot(SlotId(1)).unwrap().permits;
        let inflight = first.try_acquire().unwrap();
        let collecting = first.try_acquire().unwrap();
        assert!(first.try_acquire().is_err());
        assert!(second.try_acquire().is_ok());
        drop(inflight);
        assert!(first.try_acquire().is_ok());
        drop(collecting);
    }
}
