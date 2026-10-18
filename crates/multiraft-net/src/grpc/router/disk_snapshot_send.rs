//! Streaming send of a disk snapshot: lease the published checkpoint, send its
//! files as bounded `/raft/snapshot_chunk` calls, then submit the manifest as
//! the OpenRaft snapshot. Memory is one chunk, independent of snapshot size.
//!
//! Each call gets the native install timeout as its own deadline, so progress
//! rather than total size bounds the transfer. The owned job stops between
//! chunks once its waiter is gone (OpenRaft canceled or replaced the stream).
use super::*;
use multiraft_store::disk_snapshot::ChunkSource;
use tokio::time::Instant;

fn unreachable(reason: String) -> StreamingError<TypeConfig> {
    StreamingError::Unreachable(Unreachable::new(&std::io::Error::other(reason)))
}

pub(super) async fn stream(
    transport: &RpcTransport,
    target: NodeId,
    group_id: GroupId,
    vote: typ::Vote,
    snapshot: SnapshotOf<TypeConfig, typ::SnapshotData>,
    ttl: std::time::Duration,
    abandoned: impl Fn() -> bool,
) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
    let manifest = snapshot.snapshot.into_inner();
    let meta = snapshot.meta;
    let leased = manifest.clone();
    let mut source = tokio::task::spawn_blocking(move || ChunkSource::open(leased, group_id))
        .await
        .map_err(|error| unreachable(format!("disk snapshot lease task: {error}")))?
        .map_err(|error| unreachable(format!("disk snapshot source unavailable: {error}")))?;
    let (total, files) = (source.total_bytes(), source.file_count());
    let started = Instant::now();
    tracing::info!(target: "multiraft::transport", peer = target, group_id,
        snapshot_id = %meta.snapshot_id, snapshot_bytes = total, files,
        "disk snapshot send started");
    let deadline = || Instant::now().checked_add(ttl);
    loop {
        if abandoned() {
            return Err(unreachable("disk snapshot send abandoned".into()));
        }
        let (returned, chunk) = tokio::task::spawn_blocking(move || {
            let chunk = source.next_chunk();
            (source, chunk)
        })
        .await
        .map_err(|error| unreachable(format!("disk snapshot read task: {error}")))?;
        source = returned;
        let chunk = chunk.map_err(|error| unreachable(format!("disk snapshot read: {error}")))?;
        let Some(chunk) = chunk else {
            break;
        };
        transport
            .send::<_, ()>(target, group_id, "/raft/snapshot_chunk", chunk, deadline())
            .await
            .map_err(StreamingError::Unreachable)?;
    }
    // Release the hard-link lease off the async workers.
    tokio::task::spawn_blocking(move || drop(source));
    let response = transport
        .send(
            target,
            group_id,
            "/raft/snapshot",
            (vote, meta.clone(), manifest),
            deadline(),
        )
        .await
        .map_err(StreamingError::Unreachable)?;
    tracing::info!(target: "multiraft::transport", peer = target, group_id,
        snapshot_id = %meta.snapshot_id, snapshot_bytes = total, files,
        elapsed_ms = started.elapsed().as_millis() as u64, "disk snapshot send finished");
    Ok(response)
}
