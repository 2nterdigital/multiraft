//! Raft protocol handlers for the in-process node dispatcher.
//!
//! Adapted from openraft `examples/multi-raft-kv/src/api.rs` at tag
//! `v0.10.0-alpha.30` (Raft paths only — app KV paths omitted).

use std::io::Cursor;

use openraft::raft::TransferLeaderRequest;
use openraft::raft::TransferLeaderResponse;

use crate::decode;
use crate::encode;
use multiraft_core::typ::*;
use multiraft_fsm::StateMachine;
use multiraft_store::Raft;

pub async fn vote<S: StateMachine>(raft: &Raft<S>, req: &[u8]) -> Vec<u8> {
    let res = raft.vote(decode(req)).await;
    encode(res)
}

pub async fn append<S: StateMachine>(raft: &Raft<S>, req: &[u8]) -> Vec<u8> {
    let res = raft.append_entries(decode(req)).await;
    encode(res)
}

pub async fn snapshot<S: StateMachine>(
    raft: &Raft<S>,
    req: &[u8],
    cap: usize,
) -> Result<Vec<u8>, tonic::Status> {
    let (vote, snapshot_meta, snapshot_data) =
        decode_snapshot_request(req, cap).map_err(|error| match error {
            SnapshotDecodeError::SizeLimit => {
                tonic::Status::resource_exhausted("native snapshot size limit")
            }
            SnapshotDecodeError::Encoding => {
                tonic::Status::invalid_argument("invalid native snapshot encoding")
            }
        })?;
    let snapshot = Snapshot {
        meta: snapshot_meta,
        snapshot: Cursor::new(snapshot_data),
    };
    let res = raft
        .install_full_snapshot(vote, snapshot)
        .await
        .map_err(RaftError::<Infallible>::Fatal);
    Ok(encode(res))
}

pub async fn transfer_leader<S: StateMachine>(raft: &Raft<S>, req: &[u8]) -> Vec<u8> {
    let transfer_req: TransferLeaderRequest<multiraft_core::TypeConfig> = decode(req);
    let res: Result<TransferLeaderResponse<multiraft_core::TypeConfig>, RaftError> = raft
        .handle_transfer_leader(transfer_req)
        .await
        .map_err(RaftError::Fatal);
    encode(res)
}

#[derive(Debug)]
enum SnapshotDecodeError {
    SizeLimit,
    Encoding,
}

fn decode_snapshot_request(
    req: &[u8],
    cap: usize,
) -> Result<(Vote, SnapshotMeta, Vec<u8>), SnapshotDecodeError> {
    use bincode::Options;
    const ENVELOPE: usize = 1024 * 1024;
    if cap == 0 || cap > 64 * 1024 * 1024 || req.len() > cap + ENVELOPE {
        return Err(SnapshotDecodeError::SizeLimit);
    }
    // Bound metadata independently of application bytes. Keep room in the
    // one-MiB envelope for the fixed payload length and protobuf framing.
    use serde::Deserialize;
    let mut input = Cursor::new(req);
    let (vote, meta) = {
        let options = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_limit((ENVELOPE - 1024) as u64);
        let mut decoder = bincode::Deserializer::with_reader(&mut input, options);
        let classify = |error: bincode::Error| match *error {
            bincode::ErrorKind::SizeLimit => SnapshotDecodeError::SizeLimit,
            _ => SnapshotDecodeError::Encoding,
        };
        let vote = Vote::deserialize(&mut decoder).map_err(classify)?;
        let meta = SnapshotMeta::deserialize(&mut decoder).map_err(classify)?;
        (vote, meta)
    };
    // The existing fixed-int bincode Vec<u8> is a u64 length followed by bytes.
    // Borrow those bytes until their declared and actual lengths both pass.
    let offset = input.position() as usize;
    let length = req
        .get(offset..offset + 8)
        .ok_or(SnapshotDecodeError::Encoding)?;
    let declared = u64::from_le_bytes(
        length
            .try_into()
            .map_err(|_| SnapshotDecodeError::Encoding)?,
    );
    if declared > cap as u64 || offset + 8 > ENVELOPE {
        return Err(SnapshotDecodeError::SizeLimit);
    }
    let data = &req[offset + 8..];
    if declared != data.len() as u64 {
        return Err(SnapshotDecodeError::Encoding);
    }
    Ok((vote, meta, data.to_vec()))
}

#[cfg(test)]
mod snapshot_bound_tests {
    use super::*;
    #[test]
    fn oversized_declared_metadata_is_classified_before_payload_completeness() {
        let meta = SnapshotMeta {
            snapshot_id: "x".repeat(2 * 1024 * 1024),
            ..Default::default()
        };
        let mut encoded = encode((Vote::new_committed(0, 0), meta, Vec::<u8>::new()));
        encoded.truncate(1024 * 1024);
        assert!(matches!(
            decode_snapshot_request(&encoded, 8 * 1024 * 1024),
            Err(SnapshotDecodeError::SizeLimit)
        ));
    }

    #[test]
    fn native_snapshot_decode_rejects_oversize_trailing_and_truncated_input() {
        let encoded = encode((
            Vote::new_committed(0, 0),
            SnapshotMeta::default(),
            vec![7_u8; 5],
        ));
        assert!(decode_snapshot_request(&encoded, 4).is_err());
        let (_, _, data) = decode_snapshot_request(&encoded, 5).unwrap();
        assert_eq!(data, vec![7; 5]);
        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(decode_snapshot_request(&trailing, 5).is_err());
        assert!(decode_snapshot_request(&encoded[..encoded.len() - 1], 5).is_err());
        assert!(decode_snapshot_request(&[255; 64], 5).is_err());
    }
}
