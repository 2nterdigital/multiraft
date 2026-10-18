//! Receive a streamed checkpoint into staging, verify it at transport ingress,
//! then let OpenRaft install it by swapping it in as the live store.
//!
//! Every check that can fail on bad input (digests, layout, Group, applied
//! position) runs before OpenRaft sees the snapshot, so a failed transfer is a
//! transport refusal, never a fatal storage error. The install itself only
//! re-checks identity and performs the crash-safe swap.
use super::*;
use crate::disk_snapshot::{DiskChunk, DiskManifest};
use multiraft_fsm::ValidationKind;

impl<S: StateMachine> StateMachineStore<S> {
    fn require_disk(&self) -> io::Result<()> {
        if !self.disk {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "disk snapshot sent to a whole-image state machine",
            ));
        }
        if self.native_is_closing() {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "native snapshot owner closed",
            ));
        }
        Ok(())
    }

    /// Take the staged transfer out of its slot if `keep` accepts it.
    fn take_incoming(&self, keep: impl FnOnce(&Incoming) -> bool) -> Option<Incoming> {
        let mut slot = self.disk_rt.incoming.lock().unwrap();
        if slot.as_ref().is_some_and(keep) {
            slot.take()
        } else {
            None
        }
    }

    /// Put a transfer back unless a newer one started meanwhile.
    fn restore_incoming(&self, incoming: Incoming) {
        let mut slot = self.disk_rt.incoming.lock().unwrap();
        if slot.is_none() {
            *slot = Some(incoming);
        } else {
            drop(slot);
            discard(incoming);
        }
    }

    /// Transport ingress: write one chunk of a disk snapshot transfer. A new
    /// `Begin` replaces any unfinished transfer. Errors are refusals.
    pub async fn receive_disk_chunk(&self, chunk: DiskChunk) -> io::Result<()> {
        self.require_disk()?;
        match chunk {
            DiskChunk::Begin { transfer, manifest } => {
                let _receiving = self.disk_rt.receiving.lock().await;
                let workspace = self.disk_workspace().await?;
                let group = self.group_id;
                let incoming = tokio::task::spawn_blocking(move || {
                    Incoming::begin(&workspace, group, transfer, manifest)
                })
                .await
                .map_err(io::Error::other)??;
                let previous = self.disk_rt.incoming.lock().unwrap().replace(incoming);
                if let Some(previous) = previous {
                    discard(previous);
                }
                Ok(())
            }
            DiskChunk::Data {
                transfer,
                file,
                offset,
                bytes,
                sha256,
            } => {
                // Owned task: if the RPC is cancelled (sender deadline), the
                // write still finishes and puts the transfer back, so the
                // sender's retry of this chunk is recognized as a duplicate.
                let store = self.clone();
                tokio::spawn(async move {
                    let _receiving = store.disk_rt.receiving.clone().lock_owned().await;
                    let mut incoming = store
                        .take_incoming(|incoming| incoming.transfer() == transfer)
                        .ok_or_else(|| invalid("unknown disk snapshot transfer"))?;
                    let (incoming, result) = tokio::task::spawn_blocking(move || {
                        let result = incoming.write(file, offset, &bytes, sha256);
                        (incoming, result)
                    })
                    .await
                    .map_err(io::Error::other)?;
                    match result {
                        Ok(()) => {
                            store.restore_incoming(incoming);
                            Ok(())
                        }
                        Err(error) => {
                            discard(incoming);
                            Err(error)
                        }
                    }
                })
                .await
                .map_err(io::Error::other)?
            }
        }
    }

    /// Transport ingress, before OpenRaft dispatch: the staged transfer for
    /// this manifest is complete, every file matched its digest, the checkpoint
    /// has this store's layout, and its applied record is exactly `meta`'s cut.
    pub async fn verify_disk_snapshot(
        &self,
        meta: &SnapshotMetaOf<TypeConfig>,
        data: &[u8],
    ) -> io::Result<()> {
        self.require_disk()?;
        let manifest = DiskManifest::decode(data, self.group_id)?;
        if manifest.meta != *meta {
            return Err(invalid(
                "disk snapshot manifest disagrees with its metadata",
            ));
        }
        let _receiving = self.disk_rt.receiving.lock().await;
        let mut incoming = self
            .take_incoming(|incoming| incoming.manifest_bytes() == data)
            .ok_or_else(|| invalid("disk snapshot files were not received"))?;
        if !incoming.complete() {
            discard(incoming);
            return Err(invalid("disk snapshot transfer is incomplete"));
        }
        let record = {
            let mut inner = self.inner.lock().await;
            inner
                .fsm
                .disk_state()
                .ok_or_else(|| invalid("state machine left disk-backed mode"))
                .and_then(|disk| disk.inspect_checkpoint(incoming.dir()))
        };
        let checked = record.and_then(|record| {
            let record = AppliedRecord::decode(
                &record.ok_or_else(|| invalid("checkpoint has no applied position"))?,
            )?;
            if Some(record.last_applied) != meta.last_log_id
                || record.membership != meta.last_membership
            {
                return Err(invalid(
                    "checkpoint applied position disagrees with the snapshot metadata",
                ));
            }
            Ok(())
        });
        match checked {
            Ok(()) => {
                tracing::info!(target: "multiraft::native_install", phase = "verified",
                    group_id = self.group_id, snapshot_id = %manifest.meta.snapshot_id,
                    snapshot_bytes = incoming.manifest().total_bytes().unwrap_or(0),
                    "disk snapshot received and verified");
                incoming.mark_verified();
                self.restore_incoming(incoming);
                Ok(())
            }
            Err(error) => {
                discard(incoming);
                Err(error)
            }
        }
    }

    /// OpenRaft install of a verified disk snapshot: swap the staged checkpoint
    /// in, reload the applied position from it, and publish a local checkpoint
    /// at that position as the current snapshot.
    pub(in crate::sm_bridge) async fn install_disk_snapshot(
        &self,
        meta: &SnapshotMetaOf<TypeConfig>,
        data: Vec<u8>,
    ) -> io::Result<()> {
        let _validation = self.validation.transition.lock().await;
        let _transition = self.disk_rt.transition.lock().await;
        self.require_disk()?;
        self.validation.applicable()?;
        if self.validation.recovering() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "peer snapshot bypassed pending startup ingress validation",
            ));
        }
        let manifest = DiskManifest::decode(&data, self.group_id)?;
        let last = meta
            .last_log_id
            .filter(|_| manifest.meta == *meta)
            .ok_or_else(|| invalid("disk snapshot manifest disagrees with its metadata"))?;
        let incoming = self
            .take_incoming(|incoming| incoming.verified() && incoming.manifest_bytes() == data)
            .ok_or_else(|| invalid("disk snapshot was not verified at transport ingress"))?;
        {
            let inner = self.inner.lock().await;
            if inner.last_applied_log > meta.last_log_id {
                return Err(invalid("snapshot install would regress applied state"));
            }
        }
        self.validation.begin()?;
        let workspace = self.disk_workspace().await?;
        let staged = incoming.take_dir();
        let marker = workspace.clone();
        tokio::task::spawn_blocking(move || marker.write_installed(&last))
            .await
            .map_err(io::Error::other)??;
        let started = std::time::Instant::now();
        let (context, input) = {
            let mut inner = self.inner.lock().await;
            if self.native_is_closing() {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "native snapshot owner closed",
                ));
            }
            let disk = inner
                .fsm
                .disk_state()
                .ok_or_else(|| invalid("state machine left disk-backed mode"))?;
            if let Err(error) = disk.install_checkpoint(&staged) {
                let _ = std::fs::remove_dir_all(&staged);
                return Err(error);
            }
            self.validation.changed();
            inner.read_disk_applied()?;
            if inner.last_applied_log != meta.last_log_id
                || inner.last_membership != meta.last_membership
            {
                return Err(invalid(
                    "installed store disagrees with the snapshot metadata",
                ));
            }
            let context = self.validation.context(&inner, ValidationKind::PeerInstall);
            let input = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                inner.fsm.recovery_validation(context)
            }))
            .map_err(|_| io::Error::other("validation input capture panicked"))?;
            (context, input)
        };
        tracing::info!(target: "multiraft::native_install", phase = "swapped",
            group_id = self.group_id, last_log_index = last.index,
            swap_us = started.elapsed().as_micros() as u64, "disk snapshot installed");
        self.validation.validate(input).await?;
        let (workspace, capture, local_meta) = {
            let mut inner = self.inner.lock().await;
            self.validation.verify(context)?;
            inner
                .fsm
                .recovery_validated(context)
                .map_err(io::Error::other)?;
            self.validation.verify(context)?;
            inner.current_snapshot = None;
            inner.take_disk_checkpoint()?
        };
        let group = self.group_id;
        tokio::task::spawn_blocking(move || workspace.publish(group, capture, local_meta))
            .await
            .map_err(io::Error::other)??;
        self.validation.ready(false)?;
        Ok(())
    }
}

/// Drop a staged transfer (removing its directory) off the async workers.
fn discard(incoming: Incoming) {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            handle.spawn_blocking(move || drop(incoming));
        }
        Err(_) => drop(incoming),
    }
}
