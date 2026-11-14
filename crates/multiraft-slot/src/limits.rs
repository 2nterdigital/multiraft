//! Immediate, all-or-nothing admission; no waiting queue.
use crate::{RuntimeError, SlotId};
use std::sync::{Arc, Mutex};

pub const MAX_COMMAND_BYTES: usize = 128 * 1024;
pub const MAX_EFFECTS_BYTES: usize = 128 * 1024;
pub const MAX_QUERY_BYTES: usize = 128 * 1024;

#[derive(Debug, Clone)]
pub struct SlotLimits {
    pub per_slot_requests: usize,
    pub global_requests: usize,
    pub per_slot_bytes: usize,
    pub global_bytes: usize,
}
impl Default for SlotLimits {
    fn default() -> Self {
        Self {
            per_slot_requests: 64,
            global_requests: 1024,
            per_slot_bytes: 16 * 1024 * 1024,
            global_bytes: 128 * 1024 * 1024,
        }
    }
}
impl SlotLimits {
    pub(crate) fn validate(&self) -> Result<(), RuntimeError> {
        if self.per_slot_requests == 0
            || self.global_requests == 0
            || self.global_requests > 65_536
            || self.per_slot_bytes == 0
            || self.global_bytes == 0
        {
            return Err(RuntimeError::InvalidConfig);
        }
        Ok(())
    }
}
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Inflight {
    pub requests: usize,
    pub bytes: usize,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InflightStatus {
    pub global: Inflight,
    pub slots: Vec<Inflight>,
}
pub(crate) struct Budget {
    limits: SlotLimits,
    state: Mutex<InflightStatus>,
}
impl Budget {
    pub fn new(limits: SlotLimits, slots: u32) -> Arc<Self> {
        Arc::new(Self {
            limits,
            state: Mutex::new(InflightStatus {
                global: Inflight::default(),
                slots: vec![Inflight::default(); slots as usize],
            }),
        })
    }
    pub fn snapshot(&self) -> InflightStatus {
        self.state.lock().unwrap().clone()
    }
    pub fn acquire(self: &Arc<Self>, slot: SlotId, bytes: usize) -> Result<Guard, RuntimeError> {
        let mut state = self.state.lock().unwrap();
        let local = state
            .slots
            .get(slot.0 as usize)
            .ok_or(RuntimeError::InvalidRequest)?;
        if local.requests >= self.limits.per_slot_requests
            || state.global.requests >= self.limits.global_requests
            || bytes > self.limits.per_slot_bytes.saturating_sub(local.bytes)
            || bytes > self.limits.global_bytes.saturating_sub(state.global.bytes)
        {
            return Err(RuntimeError::Busy);
        }
        state.global.requests += 1;
        state.global.bytes += bytes;
        state.slots[slot.0 as usize].requests += 1;
        state.slots[slot.0 as usize].bytes += bytes;
        Ok(Guard {
            budget: self.clone(),
            slot,
            bytes,
        })
    }
}
pub(crate) struct Guard {
    budget: Arc<Budget>,
    slot: SlotId,
    bytes: usize,
}
impl Drop for Guard {
    fn drop(&mut self) {
        let mut s = self.budget.state.lock().unwrap();
        s.global.requests -= 1;
        s.global.bytes -= self.bytes;
        s.slots[self.slot.0 as usize].requests -= 1;
        s.slots[self.slot.0 as usize].bytes -= self.bytes;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn each_refusal_leaves_all_counters_unchanged() {
        for limits in [
            SlotLimits {
                per_slot_requests: 1,
                global_requests: 4,
                per_slot_bytes: 10,
                global_bytes: 100,
            },
            SlotLimits {
                per_slot_requests: 4,
                global_requests: 1,
                per_slot_bytes: 10,
                global_bytes: 100,
            },
            SlotLimits {
                per_slot_requests: 4,
                global_requests: 4,
                per_slot_bytes: 1,
                global_bytes: 100,
            },
            SlotLimits {
                per_slot_requests: 4,
                global_requests: 4,
                per_slot_bytes: 10,
                global_bytes: 1,
            },
        ] {
            let budget = Budget::new(limits, 2);
            let guard = budget.acquire(SlotId(0), 1).unwrap();
            let before = budget.snapshot();
            assert!(matches!(
                budget.acquire(SlotId(0), 1),
                Err(RuntimeError::Busy)
            ));
            assert_eq!(before, budget.snapshot());
            drop(guard);
            assert_eq!(budget.snapshot().global, Inflight::default());
        }
    }
}
