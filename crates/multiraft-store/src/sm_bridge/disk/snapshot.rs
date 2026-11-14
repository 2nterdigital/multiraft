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
        let transition = self.disk_rt.transition.clone().lock_owned().await;
        let mut inner = self.inner.clone().lock_owned().await;
        let validation = self.validation.clone();
        let owner = self.clone();
        // Acquire in the existing transition -> FSM order before dispatch. The
        // job owns both guards and permits even if its async waiter is dropped.
        let (capture, meta, permits, inner, transition, owner) =
            tokio::task::spawn_blocking(move || {
                validation.readable().map_err(NativeCaptureError::Io)?;
                if owner.native_is_closing() {
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
                // Tuple fields drop in order if the detached job's output is
                // discarded: delete the temporary checkpoint before returning
                // admission or unlocking either fence, and keep its owner last.
                Ok((capture, meta, permits, inner, transition, owner))
            })
            .await
            .map_err(|error| NativeCaptureError::Io(io::Error::other(error)))??;
        // No await between receiving the bundle and handing off checkpoint and
        // permits. Capture's own field drop order is irrelevant to job cleanup.
        let captured = Capture::disk(meta, capture, permits);
        drop(inner);
        drop(transition);
        drop(owner);
        Ok(captured)
    }

    /// Publish a captured checkpoint; a newer published snapshot wins.
    pub(in crate::sm_bridge) async fn publish_disk(
        &self,
        mut captured: Capture,
        capture: TempCheckpoint,
    ) -> io::Result<DiskSnapshot> {
        let transition = self.disk_rt.transition.clone().lock_owned().await;
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
        // The transition, permits and completion survive waiter cancellation;
        // quiescence cannot pass until publication's filesystem IO has finished.
        let published = tokio::task::spawn_blocking(move || {
            let _transition = transition;
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
        let transition = self.disk_rt.transition.clone().lock_owned().await;
        let mut inner = self.inner.clone().lock_owned().await;
        let group = self.group_id;
        // Recovery keeps the transition through publication, including when
        // its waiter is cancelled. The FSM fence only spans checkpoint capture.
        tokio::task::spawn_blocking(move || {
            let _transition = transition;
            let current = match &inner.disk_workspace {
                Some(workspace) => workspace.latest(group)?,
                None => None,
            };
            if current
                .is_some_and(|current| current.manifest.meta.last_log_id >= inner.last_applied_log)
            {
                return Ok(());
            }
            let (workspace, capture, meta) = inner.take_disk_checkpoint()?;
            drop(inner);
            workspace.publish(group, capture, meta)?;
            Ok::<_, io::Error>(())
        })
        .await
        .map_err(io::Error::other)?
    }
}
