//! Local counters for the Lab group-version read batch experiment.
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

static APPEND_EMPTY: AtomicU64 = AtomicU64::new(0);
static APPEND_ENTRIES: AtomicU64 = AtomicU64::new(0);
static SLOT_BUSY: AtomicU64 = AtomicU64::new(0);
static BATCHES: Mutex<BTreeMap<u32, BatchCounts>> = Mutex::new(BTreeMap::new());

#[derive(Default)]
struct BatchCounts {
    batches: u64,
    queries: u64,
    full: u64,
    queue_micros: u64,
}

pub(crate) fn record_append(empty: bool) {
    let counter = if empty {
        &APPEND_EMPTY
    } else {
        &APPEND_ENTRIES
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

pub fn record_slot_busy() {
    SLOT_BUSY.fetch_add(1, Ordering::Relaxed);
}

pub fn record_group_version_batch(slot: u32, queries: u64, queue_micros: u64) {
    let mut batches = BATCHES.lock().unwrap();
    let slot = batches.entry(slot).or_default();
    slot.batches += 1;
    slot.queries += queries;
    slot.full += u64::from(queries == 8);
    slot.queue_micros += queue_micros;
}

pub fn snapshot() -> BTreeMap<String, u64> {
    let mut out = BTreeMap::from([
        (
            "lab.append_empty".to_owned(),
            APPEND_EMPTY.load(Ordering::Relaxed),
        ),
        (
            "lab.append_entries".to_owned(),
            APPEND_ENTRIES.load(Ordering::Relaxed),
        ),
        (
            "lab.slot_busy".to_owned(),
            SLOT_BUSY.load(Ordering::Relaxed),
        ),
    ]);
    for (slot, counts) in BATCHES.lock().unwrap().iter() {
        let prefix = format!("lab.group_version_batch.slot.{slot}");
        out.insert(format!("{prefix}.batches"), counts.batches);
        out.insert(format!("{prefix}.queries"), counts.queries);
        out.insert(format!("{prefix}.full"), counts.full);
        out.insert(format!("{prefix}.queue_micros"), counts.queue_micros);
    }
    out
}
