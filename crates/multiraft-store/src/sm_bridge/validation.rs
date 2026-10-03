//! One owner for generation admission, external validation and failure fencing.
use super::*;
use multiraft_fsm::{ValidationContext, ValidationFuture, ValidationKind};
use std::sync::atomic::{AtomicBool, AtomicU8};
use tokio::sync::{watch, Semaphore};

const READY: u8 = 0;
const RECOVERING: u8 = 1;
const VALIDATING: u8 = 2;
const CLOSED: u8 = 4;

/// Narrow external validation admission; share `budget` across node Groups.
#[derive(Clone)]
pub struct ValidationOptions {
    pub deadline: Duration,
    pub budget: Arc<Semaphore>,
}
impl Default for ValidationOptions {
    fn default() -> Self {
        Self {
            deadline: Duration::from_secs(30),
            budget: Arc::new(Semaphore::new(1)),
        }
    }
}

pub(super) struct ValidationRuntime {
    state: AtomicU8,
    required: AtomicBool,
    generation: AtomicU64,
    pub(super) transition: tokio::sync::Mutex<()>,
    pub(super) work: Arc<Semaphore>,
    options: ValidationOptions,
    closed: watch::Sender<bool>,
}
impl ValidationRuntime {
    pub(super) fn new(required: bool, options: ValidationOptions) -> Self {
        Self {
            state: AtomicU8::new(READY),
            required: AtomicBool::new(required),
            generation: AtomicU64::new(0),
            transition: tokio::sync::Mutex::new(()),
            work: Arc::new(Semaphore::new(1)),
            options,
            closed: watch::channel(false).0,
        }
    }
    pub(super) fn readable(&self) -> io::Result<()> {
        if self.state.load(Ordering::Acquire) == READY {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "application generation is not validated",
            ))
        }
    }
    pub(super) fn applicable(&self) -> io::Result<()> {
        match self.state.load(Ordering::Acquire) {
            READY | RECOVERING => Ok(()),
            _ => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "application generation fenced",
            )),
        }
    }
    pub(super) fn recovering(&self) -> bool {
        self.state.load(Ordering::Acquire) == RECOVERING
    }
    pub(super) fn changed(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
    }
    pub(super) fn cancel_pending(&self) {
        self.closed.send_replace(true);
        for state in [VALIDATING, RECOVERING] {
            let _ = self
                .state
                .compare_exchange(state, CLOSED, Ordering::AcqRel, Ordering::Acquire);
        }
    }
    pub(super) fn close(&self) {
        self.state.store(CLOSED, Ordering::Release);
        self.closed.send_replace(true);
    }
    pub(super) fn begin(&self) -> io::Result<()> {
        if *self.closed.borrow() {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "validation owner closed",
            ));
        }
        let state = self.state.load(Ordering::Acquire);
        if state != READY && state != RECOVERING {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "application generation fenced",
            ));
        }
        self.state
            .compare_exchange(state, VALIDATING, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "validation owner changed"))?;
        Ok(())
    }
    pub(super) fn context<S: StateMachine>(
        &self,
        inner: &StateMachineStoreInner<S>,
        kind: ValidationKind,
    ) -> ValidationContext {
        ValidationContext {
            group_id: inner.group_id,
            generation: self.generation.load(Ordering::Acquire),
            applied: inner
                .last_applied_log
                .map(|id| (id.index, id.committed_leader_id().term)),
            kind,
        }
    }
    pub(super) async fn validate(&self, input: Option<ValidationFuture>) -> io::Result<()> {
        let mut closed = self.closed.subscribe();
        if *closed.borrow() {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "validation owner closed",
            ));
        }
        let Some(input) = input else {
            return Ok(());
        };
        let work = async {
            let _permit = self
                .options
                .budget
                .clone()
                .acquire_owned()
                .await
                .map_err(io::Error::other)?;
            // Catch callback future panics and drop all owned resources.
            use futures::FutureExt;
            std::panic::AssertUnwindSafe(input)
                .catch_unwind()
                .await
                .map_err(|_| io::Error::other("application validation panicked"))??;
            Ok(())
        };
        tokio::select! {
            biased;
            _ = closed.changed() => Err(io::Error::new(io::ErrorKind::BrokenPipe, "validation owner closed")),
            result = tokio::time::timeout(self.options.deadline, work) =>
                result.map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "application validation deadline"))?,
        }
    }
    pub(super) fn verify(&self, context: ValidationContext) -> io::Result<()> {
        if *self.closed.borrow() || self.state.load(Ordering::Acquire) != VALIDATING {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "validation owner closed or fenced",
            ));
        }
        if self.generation.load(Ordering::Acquire) != context.generation {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "stale validation generation",
            ));
        }
        Ok(())
    }
    pub(super) fn ready(&self, recovering: bool) -> io::Result<()> {
        self.state
            .compare_exchange(
                VALIDATING,
                if recovering { RECOVERING } else { READY },
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "validation owner changed"))?;
        Ok(())
    }
}

impl<S: StateMachine> StateMachineStore<S> {
    pub(super) async fn install_legacy_validated(
        &self,
        meta: &SnapshotMetaOf<TypeConfig>,
        data: Vec<u8>,
    ) -> io::Result<()> {
        let _transition = self.validation.transition.lock().await;
        self.validation.applicable()?;
        let recovering = self.validation.recovering();
        if recovering {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "peer snapshot bypassed pending startup ingress validation",
            ));
        }
        self.validation.begin()?;
        let (context, input) = {
            let mut inner = self.inner.lock().await;
            if inner.last_applied_log > meta.last_log_id {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "snapshot would regress applied state",
                ));
            }
            inner
                .fsm
                .restore(self.group_id, &data)
                .map_err(io::Error::other)?;
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
        self.validation.validate(input).await?;
        let mut inner = self.inner.lock().await;
        self.validation.verify(context)?;
        inner
            .fsm
            .recovery_validated(context)
            .map_err(io::Error::other)?;
        self.validation.verify(context)?;
        if let Some(catalog) = &self.catalog {
            let (index, term) = context.applied.unwrap_or((0, 0));
            catalog.write(self.group_id, index, term, meta.snapshot_id.clone(), &data)?;
        }
        inner.last_applied_log = meta.last_log_id;
        inner.last_membership = meta.last_membership.clone();
        inner.current_snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data,
        });
        self.validation.ready(false)?;
        Ok(())
    }

    /// Configure before cloning/registration. A zero deadline is rejected.
    pub fn with_validation_options(mut self, options: ValidationOptions) -> io::Result<Self> {
        if options.deadline.is_zero() || Arc::strong_count(&self.inner) != 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "zero validation deadline",
            ));
        }
        let required = self.validation.required.load(Ordering::Acquire);
        self.validation = Arc::new(ValidationRuntime::new(required, options));
        Ok(self)
    }
    /// Start native recovery without publishing the loaded image. Native replay
    /// is admitted, business reads and captures are not. Default consumers are unchanged.
    pub fn begin_recovery_validation(&self) {
        if self.validation.required.load(Ordering::Acquire) {
            let _ = self.validation.state.compare_exchange(
                READY,
                RECOVERING,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
    }
    /// Cancel recovery/installation proof while allowing already admitted normal
    /// business work on a ready generation to drain during owner shutdown.
    pub fn cancel_pending_validation(&self) {
        self.validation.cancel_pending();
    }
    /// Gate new business submissions; native recovery replay remains internal.
    pub fn application_ready(&self) -> io::Result<()> {
        self.validation.readable()
    }
    /// Fail-closed local read; never exposes an unvalidated candidate.
    pub async fn try_with_fsm<R>(&self, f: impl FnOnce(&S) -> R) -> io::Result<R> {
        let inner = self.inner.lock().await;
        self.validation.readable()?;
        Ok(f(&inner.fsm))
    }
    /// Validate the actual snapshot plus applied suffix, then execute the legacy
    /// local consumer check and publish the readiness marker, all on the same generation.
    pub async fn validate_recovery(
        &self,
        check: impl FnOnce(&S) -> io::Result<()>,
    ) -> io::Result<()> {
        let _transition = self.validation.transition.lock().await;
        self.validation.begin()?;
        let (context, input) = {
            let inner = self.inner.lock().await;
            let context = self.validation.context(&inner, ValidationKind::Startup);
            let input = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                inner.fsm.recovery_validation(context)
            }))
            .map_err(|_| io::Error::other("validation input capture panicked"))?;
            (context, input)
        };
        self.validation.validate(input).await?;
        let mut inner = self.inner.lock().await;
        self.validation.verify(context)?;
        check(&inner.fsm)?;
        self.validation.verify(context)?;
        inner
            .fsm
            .recovery_validated(context)
            .map_err(io::Error::other)?;
        self.validation.verify(context)?;
        self.validation.ready(false)?;
        Ok(())
    }
}
