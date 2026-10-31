//! Source-owned bounded export; descriptors hold neither checkpoint leases nor native stores.
use super::*;
use multiraft_store::disk_snapshot::{ChunkSource, DiskChunk};
use std::{future::Future, pin::Pin};
use tokio::sync::mpsc;

pub(super) type ChunkResult = Result<Option<DiskChunk>, String>;
pub(super) type ChunkReceiver = mpsc::Receiver<ChunkResult>;
type ExportFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ChunkReceiver, ControlledError>> + Send + 'a>>;
pub(super) trait Export: Send + Sync {
    fn run(&self, job: Pin<Box<dyn Future<Output = ()> + Send>>) -> Result<(), ControlledError>;
    fn open(&self, manifest: Vec<u8>, group: GroupId) -> ExportFuture<'_>;
}
struct LocalExport<S: StateMachine> {
    node: Weak<MultiRaft<S>>,
    group: GroupId,
}
impl<S: StateMachine> Export for LocalExport<S> {
    fn run(&self, job: Pin<Box<dyn Future<Output = ()> + Send>>) -> Result<(), ControlledError> {
        let node = self.node.upgrade().ok_or(ControlledError::Closed)?;
        let owner = node.group_owner(self.group);
        let mut stopping = owner.stopping();
        if *stopping.borrow()
            || !owner.watchers.spawn(async move {
                tokio::select! { _ = stopping.changed() => {}, _ = job => {} }
            })
        {
            return Err(ControlledError::Closed);
        }
        Ok(())
    }
    fn open(&self, manifest: Vec<u8>, group: GroupId) -> ExportFuture<'_> {
        Box::pin(async move {
            if group != self.group {
                return Err(ControlledError::Rejected("seed export group mismatch"));
            }
            let node = self.node.upgrade().ok_or(ControlledError::Closed)?;
            let owner = node.group_owner(group);
            let mut stopping = owner.stopping();
            if *stopping.borrow() {
                return Err(ControlledError::Closed);
            }
            let (sender, receiver) = mpsc::channel(1);
            if !owner.watchers.spawn(async move {
                let result =
                    tokio::task::spawn_blocking(move || ChunkSource::open(manifest, group)).await;
                let mut source = match result {
                    Ok(Ok(source)) => source,
                    Ok(Err(error)) => {
                        let _ = sender.try_send(Err(error.to_string()));
                        return;
                    }
                    Err(error) => {
                        let _ = sender.try_send(Err(error.to_string()));
                        return;
                    }
                };
                loop {
                    if *stopping.borrow_and_update() || sender.is_closed() {
                        break;
                    }
                    // Always join blocking children before releasing the source command owner.
                    let (returned, chunk) = match tokio::task::spawn_blocking(move || {
                        let chunk = source.next_chunk();
                        (source, chunk)
                    })
                    .await
                    {
                        Ok(result) => result,
                        Err(error) => {
                            let _ = sender.try_send(Err(error.to_string()));
                            return;
                        }
                    };
                    source = returned;
                    let done = !matches!(chunk, Ok(Some(_)));
                    let chunk = chunk.map_err(|error| error.to_string());
                    tokio::select! {
                        _ = stopping.changed() => break,
                        result = sender.send(chunk) => if result.is_err() { break; },
                    }
                    if done {
                        break;
                    }
                }
                let _ = tokio::task::spawn_blocking(move || drop(source)).await;
            }) {
                return Err(ControlledError::Closed);
            }
            Ok(receiver)
        })
    }
}

#[derive(Clone)]
pub(super) enum SeedInput {
    Local(Arc<dyn Export>),
    Received(Arc<Mutex<Option<ChunkReceiver>>>),
}
impl SeedInput {
    pub(super) fn local<S: StateMachine>(node: &Arc<MultiRaft<S>>, group: GroupId) -> Self {
        Self::Local(Arc::new(LocalExport {
            node: Arc::downgrade(node),
            group,
        }))
    }
    pub(super) fn received(receiver: ChunkReceiver) -> Self {
        Self::Received(Arc::new(Mutex::new(Some(receiver))))
    }
    pub(super) fn run(
        &self,
        job: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), ControlledError> {
        match self {
            Self::Local(export) => export.run(Box::pin(job)),
            Self::Received(_) => Err(ControlledError::Rejected(
                "received seeds cannot export source files",
            )),
        }
    }
    pub(super) async fn open(
        &self,
        manifest: Vec<u8>,
        group: GroupId,
    ) -> Result<ChunkReceiver, ControlledError> {
        match self {
            Self::Local(export) => export.open(manifest, group).await,
            Self::Received(receiver) => receiver
                .lock()
                .expect("seed receiver lock poisoned")
                .take()
                .ok_or(ControlledError::Rejected("seed stream already consumed")),
        }
    }
}
