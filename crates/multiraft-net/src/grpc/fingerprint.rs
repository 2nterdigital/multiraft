//! Immutable replication-domain guard, not peer authentication or cohort proof.
use tonic::{metadata::MetadataMap, Status};

pub(crate) const HEADER: &str = "x-multiraft-fingerprint";

/// Exactly one canonical lowercase ASCII hex value is accepted when bound.
#[allow(clippy::result_large_err)]
pub(crate) fn validate(metadata: &MetadataMap, expected: Option<[u8; 32]>) -> Result<(), Status> {
    let values = metadata.get_all(HEADER);
    let mut values = values.iter();
    let value = values.next();
    if values.next().is_some() {
        return Err(Status::failed_precondition("raft fingerprint duplicate"));
    }
    let Some(value) = value else {
        return if expected.is_none() {
            Ok(())
        } else {
            Err(Status::failed_precondition("raft fingerprint missing"))
        };
    };
    let bytes = value.as_bytes();
    if bytes.len() != 64
        || !bytes
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    {
        return Err(Status::failed_precondition("raft fingerprint malformed"));
    }
    let Some(expected) = expected else {
        return Err(Status::failed_precondition("raft fingerprint unexpected"));
    };
    let nibble = |byte: u8| {
        if byte <= b'9' {
            byte - b'0'
        } else {
            byte - b'a' + 10
        }
    };
    let matches = bytes
        .as_chunks::<2>()
        .0
        .iter()
        .zip(expected)
        .all(|(pair, byte)| nibble(pair[0]) * 16 + nibble(pair[1]) == byte);
    if !matches {
        return Err(Status::failed_precondition("raft fingerprint mismatch"));
    }
    Ok(())
}

pub(crate) fn insert(metadata: &mut MetadataMap, fingerprint: Option<[u8; 32]>) {
    if let Some(fingerprint) = fingerprint {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut encoded = [0_u8; 64];
        for (pair, byte) in encoded.as_chunks_mut::<2>().0.iter_mut().zip(fingerprint) {
            pair[0] = HEX[usize::from(byte >> 4)];
            pair[1] = HEX[usize::from(byte & 15)];
        }
        // Construction above guarantees valid ASCII metadata.
        metadata.insert(
            HEADER,
            tonic::metadata::MetadataValue::try_from(encoded.as_slice()).expect("hex metadata"),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_domain_and_legacy_matrix() {
        for expected in [None, Some([0xab; 32]), Some([0xcd; 32])] {
            for supplied in [None, Some([0xab; 32]), Some([0xcd; 32])] {
                let mut metadata = MetadataMap::new();
                insert(&mut metadata, supplied);
                assert_eq!(validate(&metadata, expected).is_ok(), expected == supplied);
            }
        }
    }

    #[test]
    fn missing_duplicate_and_noncanonical_values_are_rejected() {
        assert!(validate(&MetadataMap::new(), Some([0; 32])).is_err());
        for value in ["", "00", &"g".repeat(64), &"A".repeat(64), &"0".repeat(65)] {
            let mut metadata = MetadataMap::new();
            metadata.insert(HEADER, value.parse().unwrap());
            assert_eq!(
                validate(&metadata, Some([0; 32])).unwrap_err().code(),
                tonic::Code::FailedPrecondition
            );
            assert!(validate(&metadata, None).is_err());
        }
        let mut metadata = MetadataMap::new();
        insert(&mut metadata, Some([0; 32]));
        metadata.append(HEADER, "0".repeat(64).parse().unwrap());
        assert_eq!(
            validate(&metadata, Some([0; 32])).unwrap_err().message(),
            "raft fingerprint duplicate"
        );
    }
}
