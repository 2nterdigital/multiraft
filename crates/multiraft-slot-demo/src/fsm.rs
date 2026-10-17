//! Deliberately small acceptance FSM, not a production message implementation.
use multiraft_fsm::{ApplyOut, CaptureError, CaptureRefusal, StateMachine};
use multiraft_slot::{ClusterLayout, SlotStateMachine};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    io::{self, Write},
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Command {
    pub key: String,
    pub command_id: String,
    pub delta: i64,
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Effect {
    pub key: String,
    pub command_id: String,
    pub value: i64,
    pub accepted: bool,
}
#[derive(Clone, Default, Deserialize, Serialize)]
pub struct Image {
    pub group: u64,
    pub values: BTreeMap<String, i64>,
    pub commands: BTreeMap<String, (Command, Effect)>,
    pub apply_calls: u64,
}
#[derive(Default)]
pub struct GateState {
    pub command_id: Option<String>,
    pub entered: bool,
    pub released: bool,
}
#[derive(Default)]
pub struct Gate {
    pub state: Mutex<GateState>,
    pub changed: Condvar,
}
impl Gate {
    fn wait(&self, id: &str) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("gate lock poisoned"))?;
        if state.command_id.as_deref() != Some(id) {
            return Ok(());
        }
        state.entered = true;
        self.changed.notify_all();
        let (mut state, timeout) = self
            .changed
            .wait_timeout_while(state, Duration::from_secs(45), |s| !s.released)
            .map_err(|_| io::Error::other("gate wait poisoned"))?;
        state.command_id = None;
        if timeout.timed_out() && !state.released {
            return Err(io::Error::other("test apply gate safety timeout"));
        }
        Ok(())
    }
}
pub struct DemoFsm {
    pub image: Image,
    pub layout: ClusterLayout,
    pub gate: Arc<Gate>,
}
impl DemoFsm {
    fn check_key(&self, group: u64, key: &str) -> io::Result<()> {
        if group != self.image.group
            || self
                .layout
                .group_for_slot(
                    self.layout
                        .slot_for_key(key.as_bytes())
                        .map_err(io::Error::other)?,
                )
                .map_err(io::Error::other)?
                != group
        {
            return Err(io::Error::other("business key/group mismatch"));
        }
        Ok(())
    }
}
impl StateMachine for DemoFsm {
    type Error = io::Error;
    fn apply(&mut self, group: u64, _index: u64, data: &[u8]) -> io::Result<ApplyOut> {
        let command: Command = serde_json::from_slice(data)?;
        self.check_key(group, &command.key)?;
        self.gate.wait(&command.command_id)?;
        self.image.apply_calls += 1;
        let effect = if let Some((original, effect)) = self.image.commands.get(&command.command_id)
        {
            if original == &command {
                effect.clone()
            } else {
                Effect {
                    accepted: false,
                    ..effect.clone()
                }
            }
        } else {
            let value = self
                .image
                .values
                .get(&command.key)
                .copied()
                .unwrap_or(0)
                .checked_add(command.delta)
                .ok_or_else(|| io::Error::other("counter overflow"))?;
            self.image.values.insert(command.key.clone(), value);
            let effect = Effect {
                key: command.key.clone(),
                command_id: command.command_id.clone(),
                value,
                accepted: true,
            };
            self.image
                .commands
                .insert(command.command_id.clone(), (command, effect.clone()));
            effect
        };
        Ok(ApplyOut {
            effects: serde_json::to_vec(&effect)?,
        })
    }
    fn snapshot(&self, group: u64) -> io::Result<Vec<u8>> {
        if group != self.image.group {
            return Err(io::Error::other("snapshot group mismatch"));
        }
        Ok(serde_json::to_vec(&self.image)?)
    }
    fn restore(&mut self, group: u64, bytes: &[u8]) -> io::Result<()> {
        let image: Image = serde_json::from_slice(bytes)?;
        if image.group != group || group != self.image.group {
            return Err(io::Error::other("restore group mismatch"));
        }
        for key in image.values.keys() {
            self.check_key(group, key)?;
        }
        for (id, (command, effect)) in &image.commands {
            self.check_key(group, &command.key)?;
            if id != &command.command_id
                || effect.command_id != *id
                || effect.key != command.key
                || !effect.accepted
            {
                return Err(io::Error::other("invalid dedup snapshot"));
            }
        }
        self.image = image;
        Ok(())
    }
    fn freeze_bounded(&self, group: u64, max: usize) -> Result<Vec<u8>, CaptureError<io::Error>> {
        if group != self.image.group {
            return Err(CaptureError::Application(io::Error::other(
                "snapshot group mismatch",
            )));
        }
        struct Bounded {
            bytes: Vec<u8>,
            max: usize,
            exceeded: bool,
        }
        impl Write for Bounded {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                if bytes.len() > self.max.saturating_sub(self.bytes.len()) {
                    self.exceeded = true;
                    return Err(io::Error::other("snapshot size limit"));
                }
                self.bytes.extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let mut writer = Bounded {
            bytes: Vec::new(),
            max,
            exceeded: false,
        };
        if let Err(error) = serde_json::to_writer(&mut writer, &self.image) {
            return Err(if writer.exceeded {
                CaptureError::Refused(CaptureRefusal::SizeLimit)
            } else {
                CaptureError::Application(io::Error::other(error))
            });
        }
        Ok(writer.bytes)
    }
}
impl SlotStateMachine for DemoFsm {
    fn query(&self, group: u64, request: &[u8]) -> io::Result<Vec<u8>> {
        let key: String = serde_json::from_slice(request)?;
        self.check_key(group, &key)?;
        Ok(serde_json::to_vec(
            &serde_json::json!({"key":key,"value":self.image.values.get(&key).copied().unwrap_or(0),"apply_calls":self.image.apply_calls,"commands":self.image.commands}),
        )?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use multiraft_slot::{
        contiguous_bucket_table, LayoutConfig, NodeAddress, RouteVersion, SlotAssignment, SlotId,
        BUCKET_COUNT, FORMAT_VERSION,
    };
    fn fsm() -> DemoFsm {
        let layout = ClusterLayout::new(LayoutConfig {
            cluster_id: "test".into(),
            format_version: FORMAT_VERSION,
            route_version: RouteVersion::Sha256Bucket256V1,
            bucket_count: BUCKET_COUNT,
            slot_count: 1,
            group_id_base: 100,
            nodes: vec![NodeAddress {
                node_id: 1,
                raft: "127.0.0.1:1".parse().unwrap(),
                application_rpc: "127.0.0.1:2".parse().unwrap(),
            }],
            buckets: contiguous_bucket_table(1).unwrap(),
            slots: vec![SlotAssignment {
                slot: SlotId(0),
                voters: vec![1],
                preferred_initializer: 1,
                soft_preferred_leader: 1,
            }],
        })
        .unwrap();
        DemoFsm {
            image: Image {
                group: 100,
                ..Image::default()
            },
            layout,
            gate: Arc::new(Gate::default()),
        }
    }
    #[test]
    fn duplicate_intent_snapshot_and_identity() {
        let mut state = fsm();
        let mut command = Command {
            key: "a".into(),
            command_id: "id".into(),
            delta: 3,
        };
        let bytes = serde_json::to_vec(&command).unwrap();
        let first = state.apply(100, 1, &bytes).unwrap();
        assert_eq!(state.apply(100, 2, &bytes).unwrap().effects, first.effects);
        command.delta = 9;
        let rejection: Effect = serde_json::from_slice(
            &state
                .apply(100, 3, &serde_json::to_vec(&command).unwrap())
                .unwrap()
                .effects,
        )
        .unwrap();
        assert!(!rejection.accepted);
        assert_eq!(state.image.values["a"], 3);
        let snapshot = state.snapshot(100).unwrap();
        let mut recovered = fsm();
        recovered.restore(100, &snapshot).unwrap();
        assert_eq!(
            recovered.apply(100, 4, &bytes).unwrap().effects,
            first.effects
        );
        assert!(recovered.restore(101, &snapshot).is_err());
        assert!(recovered.apply(101, 5, &bytes).is_err());
        assert!(matches!(
            recovered.freeze_bounded(100, 1),
            Err(CaptureError::Refused(CaptureRefusal::SizeLimit))
        ));
    }
}
