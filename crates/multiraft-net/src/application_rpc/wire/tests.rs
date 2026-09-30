use super::*;
#[test]
fn only_valid_expected_source_record_can_prove_remote_zero_dispatch() {
    let status = status_from_error(
        RpcError::new(
            RpcErrorKind::Closed,
            RpcPhase::Admission,
            RpcDispatch::NotDispatched,
            "closed",
        ),
        2,
    );
    let valid = error_from_status(&status, 2);
    assert_eq!(valid.source_node, Some(2));
    assert_eq!(valid.dispatch, RpcDispatch::NotDispatched);
    for target in [1, 3] {
        assert_eq!(
            error_from_status(&status, target).dispatch,
            RpcDispatch::MayHaveDispatched
        );
    }
    let legacy = Status::unavailable("known string cannot prove phase");
    assert_eq!(
        error_from_status(&legacy, 2).dispatch,
        RpcDispatch::MayHaveDispatched
    );
    for index in [0, 9, 10, 11] {
        let mut details = status.details().to_vec();
        details[index] = 255;
        let malformed = Status::with_details(Code::Unavailable, "malformed", details.into());
        let error = error_from_status(&malformed, 2);
        assert_eq!(error.source_node, None);
        assert_eq!(error.dispatch, RpcDispatch::MayHaveDispatched);
    }
    let conflict = Status::with_details(
        Code::Internal,
        "wrong status",
        status.details().to_vec().into(),
    );
    assert_eq!(
        error_from_status(&conflict, 2).dispatch,
        RpcDispatch::MayHaveDispatched
    );
    let wrong_stage = status_from_error(
        RpcError::new(
            RpcErrorKind::Closed,
            RpcPhase::Handler,
            RpcDispatch::NotDispatched,
            "invalid phase",
        ),
        2,
    );
    assert_eq!(
        error_from_status(&wrong_stage, 2).dispatch,
        RpcDispatch::MayHaveDispatched
    );
}
