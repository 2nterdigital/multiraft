//! The node-wide permit follows owned sends, never an abandoned RPC waiter.
use super::*;
use crate::multiraft::tasks::OwnedTasks;
use tokio::sync::{oneshot, Semaphore};
use tokio::time::Instant;

pub(super) struct SnapshotSender {
    slots: Arc<Semaphore>,
    jobs: OwnedTasks,
}
impl Default for SnapshotSender {
    fn default() -> Self {
        Self {
            slots: Arc::new(Semaphore::new(1)),
            jobs: OwnedTasks::default(),
        }
    }
}
fn refused(reason: &'static str) -> StreamingError<TypeConfig> {
    StreamingError::Unreachable(Unreachable::new(&std::io::Error::other(reason)))
}
impl SnapshotSender {
    pub(super) fn close(&self) {
        self.slots.close();
        self.jobs.close();
    }
    pub(super) fn abort(&self) {
        self.close();
        self.jobs.abort();
    }
    pub(super) async fn join(&self) -> anyhow::Result<()> {
        self.jobs.join().await
    }

    pub(super) async fn send(
        &self,
        transport: RpcTransport,
        target: NodeId,
        group_id: GroupId,
        vote: typ::Vote,
        snapshot: SnapshotOf<TypeConfig, typ::SnapshotData>,
        option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let deadline = Instant::now()
            .checked_add(option.hard_ttl())
            .ok_or_else(|| refused("native snapshot deadline outside supported bounds"))?;
        let permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::Closed => refused("native snapshot owner closed"),
                tokio::sync::TryAcquireError::NoPermits => refused("native snapshot send busy"),
            })?;
        if transport.snapshot_cap == 0
            || transport.snapshot_cap > 64 * 1024 * 1024
            || snapshot.snapshot.get_ref().len() > transport.snapshot_cap
        {
            return Err(refused("native snapshot size limit"));
        }
        let data = snapshot.snapshot.into_inner();
        let wire_size = bincode::serialized_size(&(vote, &snapshot.meta, &data))
            .map_err(|_| refused("invalid snapshot metadata"))?;
        if wire_size > (transport.snapshot_cap + 1024 * 1024) as u64
            || wire_size.saturating_sub(data.len() as u64 + 8) > 1024 * 1024 - 1024
        {
            return Err(refused("native snapshot wire limit"));
        }
        if Instant::now() >= deadline {
            return Err(refused("native snapshot deadline expired before dispatch"));
        }
        let (reply, receiver) = oneshot::channel();
        let registered = self.jobs.spawn(async move {
            let _permit = permit;
            if Instant::now() >= deadline {
                let _ = reply.send(Err(refused(
                    "native snapshot deadline expired before dispatch",
                )));
                return;
            }
            let result = transport
                .send(
                    target,
                    group_id,
                    "/raft/snapshot",
                    (vote, snapshot.meta, data),
                    Some(deadline),
                )
                .await
                .map_err(StreamingError::Unreachable);
            let _ = reply.send(result);
        });
        if !registered {
            return Err(refused("native snapshot owner closed"));
        }
        receiver.await.unwrap_or_else(|_| {
            Err(refused(
                "native snapshot send task stopped; outcome unconfirmed",
            ))
        })
    }
}
