//! Lock-free cumulative latency histograms for storage I/O diagnostics.
//!
//! Bucket `k` holds `(upper[k-1], upper[k]]` nanoseconds (the first from 0);
//! index [`BOUND_COUNT`] is the overflow bucket. Upper bounds grow by
//! [`BUCKET_RATIO`] from [`FIRST_UPPER_NS`] (100 µs) up to the first bound at
//! or above [`LAST_UPPER_NS`] (20 s). The scheme is identical to the ech0
//! `transport::latency` buckets so snapshots can be merged with them.
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::LazyLock;
use std::time::Duration;

/// Growth ratio between consecutive bucket upper bounds.
pub const BUCKET_RATIO: f64 = 1.05;
/// Upper bound of the first bucket, in nanoseconds.
pub const FIRST_UPPER_NS: u64 = 100_000;
/// The final bound is the first one at or above this, in nanoseconds.
pub const LAST_UPPER_NS: u64 = 20_000_000_000;
/// Number of finite upper bounds; there are `BOUND_COUNT + 1` buckets.
pub const BOUND_COUNT: usize = 252;

static UPPER_BOUNDS: LazyLock<Vec<u64>> = LazyLock::new(|| {
    (0..BOUND_COUNT)
        .map(|k| {
            let exponent = i32::try_from(k).unwrap_or(i32::MAX);
            // Exact enough: the f64 product stays far below 2^53.
            (FIRST_UPPER_NS as f64 * BUCKET_RATIO.powi(exponent)).ceil() as u64
        })
        .collect()
});

/// The [`BOUND_COUNT`] finite inclusive upper bounds, in nanoseconds.
pub fn upper_bounds_ns() -> &'static [u64] {
    &UPPER_BOUNDS
}

/// Index of the bucket holding `ns` (`BOUND_COUNT` is the overflow bucket).
pub fn bucket_index(ns: u64) -> usize {
    UPPER_BOUNDS.partition_point(|upper| ns > *upper)
}

/// Lock-free accumulator; counters saturate and are never reset.
pub struct LatencyHistogram {
    count: AtomicU64,
    sum_ns: AtomicU64,
    max_ns: AtomicU64,
    buckets: Box<[AtomicU64]>,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            count: AtomicU64::new(0),
            sum_ns: AtomicU64::new(0),
            max_ns: AtomicU64::new(0),
            buckets: (0..=BOUND_COUNT).map(|_| AtomicU64::new(0)).collect(),
        }
    }
}

impl std::fmt::Debug for LatencyHistogram {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LatencyHistogram")
            .field("count", &self.count.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl LatencyHistogram {
    /// Records one observation of `ns` nanoseconds.
    pub fn record_ns(&self, ns: u64) {
        saturating_add(&self.count, 1);
        saturating_add(&self.sum_ns, ns);
        self.max_ns.fetch_max(ns, Ordering::Relaxed);
        saturating_add(&self.buckets[bucket_index(ns)], 1);
    }

    /// Records one observation of `duration`.
    pub fn record(&self, duration: Duration) {
        self.record_ns(u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX));
    }

    /// Samples every counter; each load is individually atomic.
    pub fn snapshot(&self) -> LatencySnapshot {
        LatencySnapshot {
            count: self.count.load(Ordering::Relaxed),
            sum_ns: self.sum_ns.load(Ordering::Relaxed),
            max_ns: self.max_ns.load(Ordering::Relaxed),
            buckets: self
                .buckets
                .iter()
                .map(|bucket| bucket.load(Ordering::Relaxed))
                .collect(),
        }
    }
}

/// A cumulative sample of a [`LatencyHistogram`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LatencySnapshot {
    pub count: u64,
    /// Saturating sum of every observation, in nanoseconds.
    pub sum_ns: u64,
    /// Largest observation since creation, in nanoseconds.
    pub max_ns: u64,
    /// `BOUND_COUNT + 1` non-cumulative bucket counts.
    pub buckets: Vec<u64>,
}

impl Default for LatencySnapshot {
    fn default() -> Self {
        LatencyHistogram::default().snapshot()
    }
}

impl LatencySnapshot {
    /// Non-empty buckets as `(index, count)`.
    pub fn sparse_buckets(&self) -> impl Iterator<Item = (usize, u64)> + '_ {
        self.buckets
            .iter()
            .copied()
            .enumerate()
            .filter(|(_, count)| *count > 0)
    }
}

fn saturating_add(counter: &AtomicU64, amount: u64) {
    if amount == 0 {
        return;
    }
    let _ = counter.try_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
        Some(old.saturating_add(amount))
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounds_match_the_shared_scheme() {
        let bounds = upper_bounds_ns();
        assert_eq!(bounds.len(), 252);
        // Pinned values catch drift from the ech0 transport scheme.
        assert_eq!(bounds[0], 100_000);
        assert_eq!(bounds[1], 105_000);
        assert_eq!(bounds[2], 110_250);
        assert_eq!(bounds[100], 13_150_126);
        assert_eq!(bounds[BOUND_COUNT - 1], 20_821_598_442);
        assert!(bounds[BOUND_COUNT - 1] >= LAST_UPPER_NS);
        assert!(bounds[BOUND_COUNT - 2] < LAST_UPPER_NS);
        assert!(bounds.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(bucket_index(0), 0);
        assert_eq!(bucket_index(100_000), 0);
        assert_eq!(bucket_index(100_001), 1);
        assert_eq!(bucket_index(u64::MAX), BOUND_COUNT);
    }

    #[test]
    fn records_cumulative_snapshot_with_sparse_buckets() {
        let histogram = LatencyHistogram::default();
        assert_eq!(histogram.snapshot(), LatencySnapshot::default());
        histogram.record(Duration::from_millis(1));
        histogram.record(Duration::from_millis(1));
        histogram.record_ns(30_000_000_000);
        let first = histogram.snapshot();
        assert_eq!(first.count, 3);
        assert_eq!(first.sum_ns, 30_002_000_000);
        assert_eq!(first.max_ns, 30_000_000_000);
        assert_eq!(first.buckets.len(), BOUND_COUNT + 1);
        let sparse: Vec<_> = first.sparse_buckets().collect();
        assert_eq!(sparse, vec![(bucket_index(1_000_000), 2), (BOUND_COUNT, 1)]);
        histogram.record_ns(50);
        let second = histogram.snapshot();
        assert_eq!(second.count, 4, "snapshots never reset");
        assert_eq!(second.buckets[0], 1);
        histogram.record_ns(u64::MAX);
        assert_eq!(histogram.snapshot().sum_ns, u64::MAX);
    }
}
