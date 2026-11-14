//! The node-wide permit follows owned sends, never an abandoned RPC waiter.
use super::*;
use crate::multiraft::tasks::OwnedTasks;
use tokio::sync::{oneshot, Semaphore};
use tokio::time::Instant;

pub(super) struct SnapshotSender {
    slots: Arc<Semaphore>,
    groups: std::sync::Mutex<GroupState>,
}
#[derive(Default)]
struct GroupState {
    closed: bool,
    jobs: std::collections::BTreeMap<GroupId, Arc<OwnedTasks>>,
}
impl Default for SnapshotSender {
    fn default() -> Self {
        Self {
            slots: Arc::new(Semaphore::new(1)),
            groups: std::sync::Mutex::default(),
        }
    }
}
fn refused(reason: &'static str) -> StreamingError<TypeConfig> {
    StreamingError::Unreachable(Unreachable::new(&std::io::Error::other(reason)))
}
impl SnapshotSender {
    fn group_jobs(&self, group: GroupId) -> Result<Arc<OwnedTasks>, StreamingError<TypeConfig>> {
        let mut groups = self
            .groups
            .lock()
            .expect("snapshot group owner lock poisoned");
        if groups.closed {
            return Err(refused("native snapshot owner closed"));
        }
        Ok(groups.jobs.entry(group).or_default().clone())
    }
    pub(super) fn close(&self) {
        self.slots.close();
        let mut groups = self
            .groups
            .lock()
            .expect("snapshot group owner lock poisoned");
        groups.closed = true;
        for jobs in groups.jobs.values() {
            jobs.close();
        }
    }
    pub(super) fn abort(&self) {
        self.close();
        for jobs in self
            .groups
            .lock()
            .expect("snapshot group owner lock poisoned")
            .jobs
            .values()
        {
            jobs.abort();
        }
    }
    pub(super) async fn join(&self) -> anyhow::Result<()> {
        self.close();
        let jobs: Vec<_> = self
            .groups
            .lock()
            .expect("snapshot group owner lock poisoned")
            .jobs
            .values()
            .cloned()
            .collect();
        let mut first_error = None;
        for jobs in jobs {
            if let Err(error) = jobs.join().await {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }
    pub(super) async fn join_group(&self, group: GroupId) -> anyhow::Result<()> {
        let jobs = {
            let mut groups = self
                .groups
                .lock()
                .expect("snapshot group owner lock poisoned");
            let jobs = groups.jobs.entry(group).or_default().clone();
            jobs.close();
            jobs
        };
        jobs.join().await
    }
    pub(super) fn reopen_group(&self, group: GroupId) {
        let mut groups = self
            .groups
            .lock()
            .expect("snapshot group owner lock poisoned");
        if !groups.closed {
            groups.jobs.insert(group, Arc::new(OwnedTasks::default()));
        }
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
        if multiraft_store::is_disk_snapshot(&snapshot.meta, snapshot.snapshot.get_ref()) {
            return self
                .send_disk(transport, target, group_id, vote, snapshot, option, permit)
                .await;
        }
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
        let jobs = self.group_jobs(group_id)?;
        let registered = jobs.spawn(async move {
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

    /// Disk mode (`docs/specs/2026-10-19-disk-fsm-streaming-snapshot.md`): the
    /// whole-image byte caps do not apply; the manifest names a checkpoint
    /// directory that is streamed in bounded chunks.
    #[allow(clippy::too_many_arguments)]
    async fn send_disk(
        &self,
        transport: RpcTransport,
        target: NodeId,
        group_id: GroupId,
        vote: typ::Vote,
        snapshot: SnapshotOf<TypeConfig, typ::SnapshotData>,
        option: RPCOption,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        // The manifest travels twice: in `Begin` (bounded by MAX_MANIFEST_BYTES
        // at the receiver) and as the OpenRaft snapshot (bounded by the cap).
        let manifest = snapshot.snapshot.get_ref().len();
        if manifest > transport.snapshot_cap
            || manifest > multiraft_store::disk_snapshot::MAX_MANIFEST_BYTES
        {
            return Err(refused(
                "disk snapshot manifest exceeds the snapshot message cap",
            ));
        }
        let ttl = option.hard_ttl();
        let (reply, receiver) = oneshot::channel();
        let jobs = self.group_jobs(group_id)?;
        let registered = jobs.spawn(async move {
            let _permit = permit;
            let result = super::disk_snapshot_send::stream(
                &transport,
                target,
                group_id,
                vote,
                snapshot,
                ttl,
                || reply.is_closed(),
            )
            .await;
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
