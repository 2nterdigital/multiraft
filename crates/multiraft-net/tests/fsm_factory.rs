use multiraft_fsm::{ApplyOut, StateMachine};
use multiraft_net::{FsmFactoryContext, StateMachineFactory};

#[derive(Default)]
struct ProbeFsm {
    value: i64,
}

impl ProbeFsm {
    fn new() -> Self {
        Self::default()
    }

    #[allow(dead_code)]
    fn encode_add(delta: i64) -> Vec<u8> {
        delta.to_le_bytes().to_vec()
    }

    #[allow(dead_code)]
    fn value(&self) -> i64 {
        self.value
    }
}

impl StateMachine for ProbeFsm {
    type Error = std::io::Error;

    fn apply(&mut self, _group: u64, _index: u64, data: &[u8]) -> Result<ApplyOut, Self::Error> {
        let bytes: [u8; 8] = data
            .try_into()
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "probe delta"))?;
        self.value += i64::from_le_bytes(bytes);
        Ok(ApplyOut::default())
    }

    fn snapshot(&self, _group: u64) -> Result<Vec<u8>, Self::Error> {
        Ok(self.value.to_le_bytes().to_vec())
    }

    fn restore(&mut self, _group: u64, snapshot: &[u8]) -> Result<(), Self::Error> {
        let bytes: [u8; 8] = snapshot
            .try_into()
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "probe snapshot"))?;
        self.value = i64::from_le_bytes(bytes);
        Ok(())
    }
}

#[test]
fn closure_factory_is_public_and_receives_immutable_context() {
    let factory = |context: FsmFactoryContext| -> anyhow::Result<ProbeFsm> {
        let _node_id = context.node_id();
        let _group_id = context.group_id();
        Ok(ProbeFsm::new())
    };
    fn assert_factory<F: StateMachineFactory<ProbeFsm>>(_: &F) {}
    assert_factory(&factory);
}
