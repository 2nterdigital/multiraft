use multiraft_fsm::{ApplyOut, CaptureError, CaptureRefusal, CounterFsm, GroupId, StateMachine};

// Catch a fallback that allocates through the unbounded legacy snapshot method.
struct Legacy;
impl StateMachine for Legacy {
    type Error = std::io::Error;
    fn apply(&mut self, _: GroupId, _: u64, _: &[u8]) -> Result<ApplyOut, Self::Error> {
        Ok(ApplyOut::default())
    }
    fn snapshot(&self, _: GroupId) -> Result<Vec<u8>, Self::Error> {
        panic!("legacy unbounded capture must not run")
    }
    fn restore(&mut self, _: GroupId, _: &[u8]) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[test]
fn legacy_fsm_refuses_bounded_capture_without_allocating() {
    assert!(matches!(
        Legacy.freeze_bounded(1, 64),
        Err(CaptureError::Refused(CaptureRefusal::Unsupported))
    ));
}

#[test]
fn counter_capture_preserves_state_and_dedup_with_exact_bound() {
    let mut fsm = CounterFsm::new();
    fsm.apply(7, 1, &CounterFsm::encode_add(5, 1)).unwrap();
    let data = fsm.freeze_bounded(7, 7).unwrap();
    assert_eq!(data, b"[5,[1]]");
    assert!(matches!(
        fsm.freeze_bounded(7, 6),
        Err(CaptureError::Refused(CaptureRefusal::SizeLimit))
    ));
    let mut restored = CounterFsm::new();
    restored.restore(7, &data).unwrap();
    restored.apply(7, 2, &CounterFsm::encode_add(5, 1)).unwrap();
    assert_eq!(restored.value(7), 5);
}
