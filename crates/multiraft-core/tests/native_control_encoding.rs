//! Typed control requests cannot collide with historical application bytes.
use multiraft_core::{NativeControl, OperationId, ReplacementSpec, Request};

#[test]
fn historical_bytes_stay_application_and_native_controls_are_explicit() {
    let historical = r#"{"data":[0,255,123,34,99,111,110,116,114,111,108,34,58,49,125]}"#;
    let request: Request = serde_json::from_str(historical).unwrap();
    assert!(request.control.is_none());
    assert_eq!(serde_json::to_string(&request).unwrap(), historical);
    let control = NativeControl::Claim {
        operation: OperationId {
            sequence: 7,
            nonce: [3; 16],
        },
        executor_epoch: 4,
    };
    let encoded = serde_json::to_vec(&Request::native_control(control.clone())).unwrap();
    let decoded: Request = serde_json::from_slice(&encoded).unwrap();
    assert!(decoded.data.is_empty());
    assert_eq!(decoded.control, Some(control));
    assert!(Request::new(encoded).control.is_none());
}

#[test]
fn begin_carries_exact_starting_configuration_and_target_generation() {
    let specification = ReplacementSpec {
        operation: OperationId {
            sequence: 1,
            nonce: [8; 16],
        },
        expected_start: Default::default(),
        target_voters: [1, 2, 4].into_iter().collect(),
        target_generation: 9,
    };
    let request = Request::native_control(NativeControl::Begin {
        specification,
        executor_epoch: 1,
    });
    let bytes = serde_json::to_vec(&request).unwrap();
    let decoded: Request = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(decoded.control, request.control);
}

#[test]
fn binary_legacy_application_layout_is_unchanged_and_cannot_carry_control() {
    let historical = [3, 0, 0, 0, 0, 0, 0, 0, 97, 98, 99];
    assert_eq!(
        bincode::serialize(&Request::new(b"abc")).unwrap(),
        historical
    );
    let decoded: Request = bincode::deserialize(&historical).unwrap();
    assert_eq!(decoded.data, b"abc");
    assert!(decoded.control.is_none());
    let control = Request::native_control(NativeControl::Claim {
        operation: OperationId {
            sequence: 1,
            nonce: [1; 16],
        },
        executor_epoch: 1,
    });
    assert!(
        bincode::serialize(&control).is_err(),
        "legacy binary transport must fail closed"
    );
}

#[test]
fn mixed_native_and_application_request_is_refused_before_encoding() {
    let request = Request {
        data: vec![1],
        control: Some(NativeControl::Claim {
            operation: OperationId {
                sequence: 1,
                nonce: [1; 16],
            },
            executor_epoch: 1,
        }),
    };
    assert!(
        serde_json::to_vec(&request).is_err(),
        "serializer must never persist a request its decoder refuses"
    );
}
