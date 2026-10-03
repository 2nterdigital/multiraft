//! Restore gated candidates and activate only an exact, externally verified generation.
use super::*;
use multiraft_fsm::ValidationKind;

impl<S: StateMachine> StateMachineStore<S> {
    pub(in crate::sm_bridge) async fn install_native_snapshot(
        &self,
        meta: &SnapshotMetaOf<TypeConfig>,
        data: Vec<u8>,
    ) -> io::Result<()> {
        let native = self.native.as_ref().unwrap().clone();
        if data.len() > native.options.max_snapshot_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot install limit",
            ));
        }
        let _validation = self.validation.transition.lock().await;
        let _transition = native.transition.lock().await;
        if self.native_is_closing() {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "native snapshot owner closed",
            ));
        }
        let recovering = self.validation.recovering();
        // Constructor loading of an existing active generation is not activation
        // of a peer candidate. Validate only after its committed suffix is replayed.
        if recovering {
            if let Some(active) = self.load_native_snapshot().await? {
                if active.meta == *meta && active.snapshot.get_ref() == &data {
                    let mut inner = self.inner.lock().await;
                    self.validation.applicable()?;
                    inner
                        .fsm
                        .restore(self.group_id, &data)
                        .map_err(io::Error::other)?;
                    self.validation.changed();
                    inner.last_applied_log = meta.last_log_id;
                    inner.last_membership = meta.last_membership.clone();
                    return Ok(());
                }
            }
        }
        self.validation.applicable()?;
        {
            let inner = self.inner.lock().await;
            if inner.last_applied_log > meta.last_log_id {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "snapshot install would regress applied state",
                ));
            }
        }
        // Gate before any IO await. Cancellation leaves the owner fenced until
        // destruction; it cannot expose a partially restored or staged candidate.
        self.validation.begin()?;
        let permit = self
            .validation
            .work
            .clone()
            .acquire_owned()
            .await
            .map_err(io::Error::other)?;
        let options = native.options.clone();
        let owned_meta = meta.clone();
        let group = self.group_id;
        let stage_started = std::time::Instant::now();
        let (stage, data, permit) = tokio::task::spawn_blocking(move || {
            let stage = options.catalog.stage_native(
                group,
                &owned_meta,
                &data,
                options.max_snapshot_bytes,
            )?;
            Ok::<_, io::Error>((stage, data, permit))
        })
        .await
        .map_err(io::Error::other)??;
        tracing::info!(target: "multiraft::native_install", phase = "staged", group_id = group,
            snapshot_bytes = data.len(), stage_elapsed_us = stage_started.elapsed().as_micros() as u64,
            "native snapshot install");
        let (context, input) = {
            let mut inner = self.inner.lock().await;
            // Owner close racing staging cannot run restore or publish success.
            if self.native_is_closing() {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "native snapshot owner closed",
                ));
            }
            inner.fsm.restore(group, &data).map_err(io::Error::other)?;
            self.validation.changed();
            let mut context = self.validation.context(&inner, ValidationKind::PeerInstall);
            context.applied = meta
                .last_log_id
                .map(|id| (id.index, id.committed_leader_id().term));
            let input = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                inner.fsm.recovery_validation(context)
            }))
            .map_err(|_| io::Error::other("validation input capture panicked"))?;
            (context, input)
        };
        tracing::debug!(target: "multiraft::native_install", phase = "application_restored",
            group_id = group, "native snapshot install; candidate remains gated");
        self.validation.validate(input).await?;
        self.validation.verify(context)?;
        // Provider owns bytes after durable activation. Permit lives inside the
        // blocking child, so a canceled waiter must still join that resource.
        let catalog = native.options.catalog.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            catalog.activate_native(stage)
        })
        .await
        .map_err(io::Error::other)??;
        tracing::debug!(target: "multiraft::native_install", phase = "activated",
            group_id = group, "native snapshot install");
        let mut inner = self.inner.lock().await;
        self.validation.verify(context)?;
        if !recovering {
            inner
                .fsm
                .recovery_validated(context)
                .map_err(io::Error::other)?;
        }
        inner.last_applied_log = meta.last_log_id;
        inner.last_membership = meta.last_membership.clone();
        inner.current_snapshot = None;
        self.validation.ready(recovering)?;
        tracing::debug!(target: "multiraft::native_install", phase = "bridge_updated",
            group_id = group, "native snapshot install");
        tracing::debug!(target: "multiraft::native_install", phase = "validated_and_activated", group_id = group, generation = context.generation, "native snapshot install");
        Ok(())
    }
}
