use crate::{ApplyOut, GroupId, StateMachine};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CounterError {
    #[error("decode: {0}")]
    Decode(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Cmd {
    idem: u64,
    delta: i64,
}

#[derive(Debug, Default)]
pub struct CounterFsm {
    values: HashMap<GroupId, i64>,
    seen: HashMap<GroupId, HashSet<u64>>,
}

impl CounterFsm {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn encode_add(delta: i64, idem: u64) -> Vec<u8> {
        bincode::serialize(&Cmd { idem, delta }).unwrap()
    }

    pub fn value(&self, group: GroupId) -> i64 {
        *self.values.get(&group).unwrap_or(&0)
    }
}

impl StateMachine for CounterFsm {
    type Error = CounterError;

    fn apply(&mut self, group: GroupId, _index: u64, data: &[u8]) -> Result<ApplyOut, Self::Error> {
        let cmd: Cmd =
            bincode::deserialize(data).map_err(|e| CounterError::Decode(e.to_string()))?;
        let seen = self.seen.entry(group).or_default();
        if seen.insert(cmd.idem) {
            *self.values.entry(group).or_default() += cmd.delta;
        }
        Ok(ApplyOut::default())
    }

    fn snapshot(&self, group: GroupId) -> Result<Vec<u8>, Self::Error> {
        let v = self.value(group);
        let seen: Vec<u64> = self
            .seen
            .get(&group)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default();
        Ok(serde_json::to_vec(&(v, seen)).unwrap())
    }

    fn freeze_bounded(
        &self,
        group: GroupId,
        max_bytes: usize,
    ) -> Result<Vec<u8>, crate::CaptureError<Self::Error>> {
        // Serialize the borrowed set directly: no unbounded intermediate Vec of IDs.
        struct Limited {
            bytes: Vec<u8>,
            max: usize,
            refused: bool,
        }
        impl std::io::Write for Limited {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if bytes.len() > self.max.saturating_sub(self.bytes.len()) {
                    self.refused = true;
                    return Err(std::io::Error::other("snapshot byte limit"));
                }
                self.bytes.extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let empty = HashSet::new();
        let seen = self.seen.get(&group).unwrap_or(&empty);
        let mut writer = Limited {
            bytes: Vec::new(),
            max: max_bytes,
            refused: false,
        };
        if let Err(error) = serde_json::to_writer(&mut writer, &(self.value(group), seen)) {
            if writer.refused {
                return Err(crate::CaptureError::Refused(
                    crate::CaptureRefusal::SizeLimit,
                ));
            }
            return Err(crate::CaptureError::Application(CounterError::Decode(
                error.to_string(),
            )));
        }
        Ok(writer.bytes)
    }

    fn restore(&mut self, group: GroupId, snapshot: &[u8]) -> Result<(), Self::Error> {
        let (v, seen): (i64, Vec<u64>) =
            serde_json::from_slice(snapshot).map_err(|e| CounterError::Decode(e.to_string()))?;
        self.values.insert(group, v);
        self.seen.insert(group, seen.into_iter().collect());
        Ok(())
    }
}
