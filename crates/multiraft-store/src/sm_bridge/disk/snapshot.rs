//! Disk snapshot provider: capture a checkpoint under the FSM lock, publish it
//! as the Group's current snapshot, and serve its manifest to OpenRaft.
use super::super::native::{Capture, NativeCaptureError, NativeRuntime};
use super::*;
use crate::disk_snapshot::TempCheckpoint;
use crate::NativeSnapshotInfo;
use multiraft_fsm::CaptureRefusal;

type DiskSnapshot = SnapshotOf<TypeConfig, Cursor<Vec<u8>>>;

impl<S: StateMachine> StateMachineStoreInner<S> {
    /// Checkpoint the live store at the current applied position (FSM lock held).
    pub(super) fn take_disk_checkpoint(
        &mut self,
    ) -> io::Result<(Workspace, TempCheckpoint, SnapshotMetaOf<TypeConfig>)> {
        self.load_disk_state()?;
        let workspace = self
            .disk_workspace
            .clone()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Unsupported, "no snapshot workspace"))?;
        if self.last_applied_log.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "nothing applied to snapshot",
            ));
        }
        let capture = workspace.temp_checkpoint();
        let started = std::time::Instant::now();
        self.fsm
            .disk_state()
            .ok_or_else(|| invalid("state machine left disk-backed mode"))?
            .checkpoint(capture.path())?;
        let meta = SnapshotMetaOf::<TypeConfig> {
            last_log_id: self.last_applied_log,
            last_membership: self.last_membership.clone(),
            snapshot_id: String::new(),
        };
        tracing::info!(group_id = self.group_id, last_log_index = ?meta.last_log_id.map(|id| id.index),
            checkpoint_us = started.elapsed().as_micros() as u64, "disk snapshot checkpoint");
        Ok((workspace, capture, meta))
    }
}

fn published_snapshot(published: crate::disk_snapshot::Published) -> DiskSnapshot {
    DiskSnapshot {
        meta: published.manifest.meta,
        snapshot: Cursor::new(published.bytes),
    }
}

impl<S: StateMachine> StateMachineStore<S> {
    /// A native capture for a disk-backed store: a checkpoint directory.
    pub(in crate::sm_bridge) async fn capture_disk(
        &self,
        native: &NativeRuntime,
    ) -> Result<Capture, NativeCaptureError> {
        let permits = native.admit().map_err(NativeCaptureError::Refused)?;
        let _transition = self.disk_rt.transition.lock().await;
        let mut inner = self.inner.lock().await;
        self.validation.readable().map_err(NativeCaptureError::Io)?;
        if self.native_is_closing() {
            return Err(NativeCaptureError::Io(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "native snapshot owner closed",
            )));
        }
        let (_, capture, meta) = inner.take_disk_checkpoint().map_err(|error| {
            if error.kind() == io::ErrorKind::Unsupported {
                NativeCaptureError::Refused(CaptureRefusal::Unsupported)
            } else {
                NativeCaptureError::Io(error)
            }
        })?;
        Ok(Capture::disk(meta, capture, permits))
    }

    /// Publish a captured checkpoint; a newer published snapshot wins.
    pub(in crate::sm_bridge) async fn publish_disk(
        &self,
        mut captured: Capture,
        capture: TempCheckpoint,
    ) -> io::Result<DiskSnapshot> {
        let _transition = self.disk_rt.transition.lock().await;
        self.validation.readable()?;
        if self.native_is_closing() {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "native snapshot owner closed",
            ));
        }
        let workspace = self.disk_workspace().await?;
        let group = self.group_id;
        let meta = captured.meta.clone();
        // Permits and the completion move into the blocking work and survive
        // cancellation of this waiter.
        let published = tokio::task::spawn_blocking(move || {
            let started = std::time::Instant::now();
            let published = workspace.publish(group, capture, meta)?;
            tracing::info!(target: "multiraft::recovery", operation = "native_snapshot_build",
                phase = "complete", group_id = group, snapshot_id = %published.manifest.meta.snapshot_id,
                last_log_index = ?published.manifest.meta.last_log_id.map(|id| id.index),
                snapshot_bytes = published.manifest.total_bytes().unwrap_or(0),
                files = published.manifest.files.len(), io_us = started.elapsed().as_micros() as u64,
                durability = "disk_checkpoint", "published disk snapshot");
            captured.complete();
            Ok::<_, io::Error>(published)
        })
        .await
        .map_err(io::Error::other)??;
        Ok(published_snapshot(published))
    }

    /// Whether the FSM implements the snapshot hooks (it has a workspace).
    pub(in crate::sm_bridge) async fn disk_snapshots_supported(&self) -> bool {
        let mut inner = self.inner.lock().await;
        inner.load_disk_state().is_ok() && inner.disk_workspace.is_some()
    }

    pub(super) async fn disk_workspace(&self) -> io::Result<Workspace> {
        let mut inner = self.inner.lock().await;
        inner.load_disk_state()?;
        inner
            .disk_workspace
            .clone()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Unsupported, "no snapshot workspace"))
    }

    async fn published_disk(&self) -> io::Result<Option<crate::disk_snapshot::Published>> {
        let workspace = match self.disk_workspace().await {
            Ok(workspace) => workspace,
            Err(error) if error.kind() == io::ErrorKind::Unsupported => return Ok(None),
            Err(error) => return Err(error),
        };
        let group = self.group_id;
        tokio::task::spawn_blocking(move || workspace.latest(group))
            .await
            .map_err(io::Error::other)?
    }

    /// The current published disk snapshot (its manifest), if any.
    pub(in crate::sm_bridge) async fn load_disk_snapshot(
        &self,
    ) -> io::Result<Option<DiskSnapshot>> {
        Ok(self.published_disk().await?.map(published_snapshot))
    }

    /// Published disk snapshot facts; `size` is the checkpoint's total file bytes.
    pub(in crate::sm_bridge) async fn disk_snapshot_info(
        &self,
    ) -> io::Result<Option<NativeSnapshotInfo>> {
        self.published_disk()
            .await?
            .map(|published| {
                Ok(NativeSnapshotInfo {
                    size: published.manifest.total_bytes()?,
                    meta: published.manifest.meta,
                })
            })
            .transpose()
    }

    /// Publish a checkpoint at the applied position unless one exists already.
    pub(super) async fn ensure_disk_snapshot_at_applied(&self) -> io::Result<()> {
        let _transition = self.disk_rt.transition.lock().await;
        let (workspace, capture, meta) = {
            let mut inner = self.inner.lock().await;
            let current = match &inner.disk_workspace {
                Some(workspace) => workspace.latest(self.group_id)?,
                None => None,
            };
            if current
                .is_some_and(|current| current.manifest.meta.last_log_id >= inner.last_applied_log)
            {
                return Ok(());
            }
            inner.take_disk_checkpoint()?
        };
        let group = self.group_id;
        tokio::task::spawn_blocking(move || workspace.publish(group, capture, meta))
            .await
            .map_err(io::Error::other)??;
        Ok(())
    }
}
