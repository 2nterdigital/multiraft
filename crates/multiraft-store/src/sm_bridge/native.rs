//! Durable-native builder/install seams; application lock never spans filesystem IO.
use super::*;
use multiraft_fsm::{CaptureError, CaptureRefusal};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone)]
pub struct NativeSmOptions {
    pub catalog: Arc<SnapshotCatalog>,
    pub max_snapshot_bytes: usize,
    /// Shared by all Groups hosted by one node.
    pub build_budget: Arc<Semaphore>,
}

pub(super) struct NativeRuntime {
    options: NativeSmOptions,
    closing: std::sync::atomic::AtomicBool,
    group_budget: Arc<Semaphore>,
    transition: tokio::sync::Mutex<()>,
    refusal: std::sync::Mutex<Option<CaptureRefusal>>,
    pending: std::sync::Mutex<Option<Capture>>,
}

struct Capture {
    meta: SnapshotMetaOf<TypeConfig>,
    data: Vec<u8>,
    _permits: Option<(OwnedSemaphorePermit, OwnedSemaphorePermit)>,
    already_durable: bool,
    completion: Option<tokio::sync::oneshot::Sender<()>>,
}

/// Disposable observation of one reserved native build, never persisted intent.
pub struct NativeBuildReservation {
    pub target: Option<LogIdOf<TypeConfig>>,
    pub completion: tokio::sync::oneshot::Receiver<()>,
}

pub struct SnapshotBuilder<S: StateMachine> {
    store: StateMachineStore<S>,
    capture: Option<Result<Capture, io::Error>>,
}

fn refusal_io(refusal: CaptureRefusal) -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        format!("snapshot capture refused: {refusal:?}"),
    )
}

impl<S: StateMachine> StateMachineStore<S> {
    pub fn with_native_options(
        group: GroupId,
        fsm: S,
        options: NativeSmOptions,
    ) -> io::Result<Self> {
        if options.max_snapshot_bytes == 0 || options.max_snapshot_bytes > 64 * 1024 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid snapshot byte limit",
            ));
        }
        let mut store = Self::new(group, fsm);
        store.native = Some(Arc::new(NativeRuntime {
            options,
            closing: std::sync::atomic::AtomicBool::new(false),
            group_budget: Arc::new(Semaphore::new(1)),
            transition: tokio::sync::Mutex::new(()),
            refusal: std::sync::Mutex::new(None),
            pending: std::sync::Mutex::new(None),
        }));
        Ok(store)
    }

    /// Effective application-byte bound used before native transport/install copies.
    pub fn snapshot_byte_limit(&self) -> usize {
        self.native
            .as_ref()
            .map_or(64 * 1024 * 1024, |native| native.options.max_snapshot_bytes)
    }

    /// Close new native snapshot intake while allowing already-started transitions to finish.
    pub fn close_native_intake(&self) {
        if let Some(native) = &self.native {
            native
                .closing
                .store(true, std::sync::atomic::Ordering::SeqCst);
            native.pending.lock().unwrap().take();
        }
    }

    pub(super) fn native_is_closing(&self) -> bool {
        self.native
            .as_ref()
            .is_some_and(|native| native.closing.load(std::sync::atomic::Ordering::SeqCst))
    }

    /// Called after native core shutdown; wait for owned transition/capture work.
    pub async fn wait_native_quiescent(&self) {
        if let Some(native) = &self.native {
            let _transition = native.transition.lock().await;
            let _application = self.inner.lock().await;
        }
    }

    pub fn snapshot_capture_refusal(&self) -> Option<CaptureRefusal> {
        self.native
            .as_ref()
            .and_then(|native| *native.refusal.lock().unwrap())
    }

    pub(super) async fn prepare_builder(&self, force: bool) -> Option<SnapshotBuilder<S>> {
        let Some(native) = &self.native else {
            return Some(SnapshotBuilder {
                store: self.clone(),
                capture: None,
            });
        };
        if self.native_is_closing() {
            return if force {
                Some(SnapshotBuilder {
                    store: self.clone(),
                    capture: Some(Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "native snapshot owner closed",
                    ))),
                })
            } else {
                None
            };
        }
        if let Some(capture) = native.pending.lock().unwrap().take() {
            return Some(SnapshotBuilder {
                store: self.clone(),
                capture: Some(Ok(capture)),
            });
        }
        let mut captured = self.capture_native(native).await;
        let refusal = match &captured {
            Err(NativeCaptureError::Refused(reason)) => Some(*reason),
            _ => None,
        };
        *native.refusal.lock().unwrap() = refusal;
        if refusal.is_some() && !force {
            return None;
        }
        if refusal.is_some() && force {
            // A forced replication request may reuse a checkpoint covering the
            // entire current cut without competing for heavy build admission.
            let _transition = native.transition.lock().await;
            match self.load_native_snapshot().await {
                Ok(Some(snapshot)) => {
                    let inner = self.inner.lock().await;
                    if snapshot.meta.last_log_id >= inner.last_applied_log
                        && snapshot.meta.last_membership == inner.last_membership
                    {
                        captured = Ok(Capture {
                            meta: snapshot.meta,
                            data: snapshot.snapshot.into_inner(),
                            _permits: None,
                            already_durable: true,
                            completion: None,
                        });
                    }
                }
                Err(error) => captured = Err(NativeCaptureError::Io(error)),
                Ok(None) => (),
            }
        }
        let capture = captured.map_err(|error| match error {
            NativeCaptureError::Refused(reason) => refusal_io(reason),
            NativeCaptureError::Io(error) => error,
        });
        Some(SnapshotBuilder {
            store: self.clone(),
            capture: Some(capture),
        })
    }

    /// Reserve a bounded cut for the next native builder, without publishing it.
    pub async fn reserve_native_compaction(
        &self,
    ) -> Result<NativeBuildReservation, NativeCaptureError> {
        let native = self
            .native
            .as_ref()
            .ok_or(NativeCaptureError::Refused(CaptureRefusal::Unsupported))?;
        let mut capture = self.capture_native(native).await?;
        let (sender, completion) = tokio::sync::oneshot::channel();
        capture.completion = Some(sender);
        let target = capture.meta.last_log_id;
        let mut pending = native.pending.lock().unwrap();
        if self.native_is_closing() {
            return Err(NativeCaptureError::Io(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "native snapshot owner closed",
            )));
        }
        if pending.is_some() {
            return Err(NativeCaptureError::Refused(CaptureRefusal::Busy));
        }
        *pending = Some(capture);
        Ok(NativeBuildReservation { target, completion })
    }

    /// Used only when native trigger submission failed or native execution stopped.
    pub fn release_native_reservation(&self) {
        if let Some(native) = &self.native {
            native.pending.lock().unwrap().take();
        }
    }

    async fn capture_native(&self, native: &NativeRuntime) -> Result<Capture, NativeCaptureError> {
        let node_permit = native
            .options
            .build_budget
            .clone()
            .try_acquire_owned()
            .map_err(|_| NativeCaptureError::Refused(CaptureRefusal::Busy))?;
        let group_permit = native
            .group_budget
            .clone()
            .try_acquire_owned()
            .map_err(|_| NativeCaptureError::Refused(CaptureRefusal::Busy))?;
        let _transition = native.transition.lock().await;
        let inner = self.inner.lock().await;
        if self.native_is_closing() {
            return Err(NativeCaptureError::Io(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "native snapshot owner closed",
            )));
        }
        let started = std::time::Instant::now();
        let data = inner
            .fsm
            .freeze_bounded(self.group_id, native.options.max_snapshot_bytes)
            .map_err(|error| match error {
                CaptureError::Refused(reason) => NativeCaptureError::Refused(reason),
                CaptureError::Application(error) => NativeCaptureError::Io(io::Error::other(error)),
            })?;
        if data.len() > native.options.max_snapshot_bytes {
            return Err(NativeCaptureError::Refused(CaptureRefusal::SizeLimit));
        }
        let meta = SnapshotMetaOf::<TypeConfig> {
            last_log_id: inner.last_applied_log,
            last_membership: inner.last_membership.clone(),
            snapshot_id: match inner.last_applied_log {
                Some(last) => format!(
                    "native-{}-{}-{}-{}",
                    self.group_id,
                    last.committed_leader_id(),
                    last.index,
                    inner.next_snapshot_idx()
                ),
                None => format!(
                    "native-{}-empty-{}",
                    self.group_id,
                    inner.next_snapshot_idx()
                ),
            },
        };
        tracing::info!(
            group_id = self.group_id,
            snapshot_bytes = data.len(),
            freeze_us = started.elapsed().as_micros() as u64,
            "bounded snapshot capture"
        );
        Ok(Capture {
            meta,
            data,
            _permits: Some((node_permit, group_permit)),
            already_durable: false,
            completion: None,
        })
    }

    /// Read validated provider facts without allocating the full application image.
    pub async fn native_snapshot_info(&self) -> io::Result<Option<crate::NativeSnapshotInfo>> {
        let Some(native) = self.native.clone() else {
            return Ok(None);
        };
        let group = self.group_id;
        tokio::task::spawn_blocking(move || {
            native
                .options
                .catalog
                .describe_native(group, native.options.max_snapshot_bytes)
        })
        .await
        .map_err(io::Error::other)?
    }

    pub(super) async fn load_native_snapshot(
        &self,
    ) -> io::Result<Option<SnapshotOf<TypeConfig, Cursor<Vec<u8>>>>> {
        let native = self.native.as_ref().unwrap().clone();
        let group = self.group_id;
        tokio::task::spawn_blocking(move || {
            native
                .options
                .catalog
                .load_native(group, native.options.max_snapshot_bytes)
        })
        .await
        .map_err(io::Error::other)?
        .map(|snapshot| {
            snapshot.map(|snapshot| SnapshotOf::<TypeConfig, Cursor<Vec<u8>>> {
                meta: snapshot.meta,
                snapshot: Cursor::new(snapshot.data),
            })
        })
    }

    pub(super) async fn install_native_snapshot(
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
        let _transition = native.transition.lock().await;
        if self.native_is_closing() {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "native snapshot owner closed",
            ));
        }
        // Stage before restoring business state; no success/ack before durable activation.
        let options = native.options.clone();
        let owned_meta = meta.clone();
        let group = self.group_id;
        let stage_started = std::time::Instant::now();
        let (stage, data) = tokio::task::spawn_blocking(move || {
            let stage = options.catalog.stage_native(
                group,
                &owned_meta,
                &data,
                options.max_snapshot_bytes,
            )?;
            Ok::<_, io::Error>((stage, data))
        })
        .await
        .map_err(io::Error::other)??;
        tracing::info!(target: "multiraft::native_install", phase = "staged", group_id = group, snapshot_bytes = data.len(), stage_elapsed_us = stage_started.elapsed().as_micros() as u64, "native snapshot install");
        {
            let mut inner = self.inner.lock().await;
            if inner.last_applied_log > meta.last_log_id {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "snapshot install would regress applied state",
                ));
            }
            inner.fsm.restore(group, &data).map_err(io::Error::other)?;
        }
        tracing::debug!(target: "multiraft::native_install", phase = "application_restored", group_id = group, "native snapshot install");
        let catalog = native.options.catalog.clone();
        tokio::task::spawn_blocking(move || catalog.activate_native(stage))
            .await
            .map_err(io::Error::other)??;
        tracing::debug!(target: "multiraft::native_install", phase = "activated", group_id = group, "native snapshot install");
        let mut inner = self.inner.lock().await;
        inner.last_applied_log = meta.last_log_id;
        inner.last_membership = meta.last_membership.clone();
        // Durable bytes remain owned by the catalog, not a historical RAM snapshot.
        inner.current_snapshot = None;
        tracing::debug!(target: "multiraft::native_install", phase = "bridge_updated", group_id = group, "native snapshot install");
        Ok(())
    }
}

#[derive(Debug)]
pub enum NativeCaptureError {
    Refused(CaptureRefusal),
    Io(io::Error),
}

impl<S: StateMachine> RaftSnapshotBuilder<TypeConfig> for SnapshotBuilder<S> {
    type SnapshotData = Cursor<Vec<u8>>;
    async fn build_snapshot(&mut self) -> io::Result<SnapshotOf<TypeConfig, Self::SnapshotData>> {
        let Some(capture) = self.capture.take() else {
            if self.store.native.is_some() {
                return Err(io::Error::other("snapshot builder already consumed"));
            }
            return self.store.build_legacy_snapshot().await;
        };
        let mut captured = capture?;
        if captured.already_durable {
            if let Some(sender) = captured.completion.take() {
                let _ = sender.send(());
            }
            return Ok(SnapshotOf::<TypeConfig, Self::SnapshotData> {
                meta: captured.meta,
                snapshot: Cursor::new(captured.data),
            });
        }
        let native = self.store.native.as_ref().unwrap().clone();
        let _transition = native.transition.lock().await;
        if self.store.native_is_closing() {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "native snapshot owner closed",
            ));
        }
        let group = self.store.group_id;
        let options = native.options.clone();
        // Permits move into native work and survive cancellation of this waiter.
        let captured = tokio::task::spawn_blocking(move || {
            let mut captured = captured;
            if let Some(current) = options
                .catalog
                .load_native(group, options.max_snapshot_bytes)?
            {
                if current.meta.last_log_id > captured.meta.last_log_id {
                    // A newer native install won the serialized transition. Serve
                    // that valid checkpoint rather than turning an old build into
                    // a fatal storage error or regressing the active generation.
                    captured.meta = current.meta;
                    captured.data = current.data;
                    if let Some(sender) = captured.completion.take() {
                        let _ = sender.send(());
                    }
                    return Ok::<_, io::Error>(captured);
                }
            }
            let start = std::time::Instant::now();
            options.catalog.publish_native(
                group,
                &captured.meta,
                &captured.data,
                options.max_snapshot_bytes,
            )?;
            tracing::info!(
                target: "multiraft::recovery",
                operation = "native_snapshot_build",
                phase = "complete",
                group_id = group,
                snapshot_id = %captured.meta.snapshot_id,
                last_log_index = ?captured.meta.last_log_id.map(|id| id.index),
                snapshot_bytes = captured.data.len(),
                io_us = start.elapsed().as_micros() as u64,
                durability = "native_durable",
                "published native snapshot"
            );
            if let Some(sender) = captured.completion.take() {
                let _ = sender.send(());
            }
            Ok::<_, io::Error>(captured)
        })
        .await
        .map_err(io::Error::other)??;
        Ok(SnapshotOf::<TypeConfig, Self::SnapshotData> {
            meta: captured.meta,
            snapshot: Cursor::new(captured.data),
        })
    }
}
