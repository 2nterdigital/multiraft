//! Pluggable state machine for multiraft.

mod counter_fsm;
pub use counter_fsm::CounterFsm;

pub type GroupId = u64;
pub type NodeId = u64;

/// The actual application generation frozen for external recovery validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidationContext {
    pub group_id: GroupId,
    pub generation: u64,
    pub applied: Option<(u64, u64)>,
    pub kind: ValidationKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationKind {
    Startup,
    PeerInstall,
}

/// Own all inputs and remote resources. Dropping this future must cancel and
/// release its work; do not detach tasks or perform remote IO in `apply`.
pub type ValidationFuture =
    std::pin::Pin<Box<dyn std::future::Future<Output = std::io::Result<()>> + Send + 'static>>;

#[derive(Debug, Clone, Default)]
pub struct ApplyOut {
    pub effects: Vec<u8>,
}

/// Known refusal before snapshot work is admitted; never an IO/corruption error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaptureRefusal {
    Unsupported,
    SizeLimit,
    Busy,
}

#[derive(Debug)]
pub enum CaptureError<E> {
    Refused(CaptureRefusal),
    Application(E),
}

pub trait StateMachine: Send + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    fn apply(&mut self, group: GroupId, index: u64, data: &[u8]) -> Result<ApplyOut, Self::Error>;

    fn snapshot(&self, group: GroupId) -> Result<Vec<u8>, Self::Error>;

    fn restore(&mut self, group: GroupId, snapshot: &[u8]) -> Result<(), Self::Error>;

    /// Opt into gating reads/capture from construction until startup validation.
    fn requires_recovery_validation(&self) -> bool {
        false
    }

    /// Freeze bounded, owned validation inputs from this exact restored image.
    /// Called under the brief FSM lock; the returned future runs outside it.
    /// Startup includes the committed suffix; peer install includes its candidate.
    fn recovery_validation(&self, _context: ValidationContext) -> Option<ValidationFuture> {
        None
    }

    /// Publish a consumer readiness marker only after external proof succeeds.
    /// This synchronous local hook cannot perform external waits.
    fn recovery_validated(&mut self, _context: ValidationContext) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Capture within the supplied byte bound, checking while constructing bytes.
    /// The default refuses without calling the legacy unbounded serializer.
    /// Durable-native users implement this hook; legacy snapshot behavior is unchanged.
    fn freeze_bounded(
        &self,
        _group: GroupId,
        _max_bytes: usize,
    ) -> Result<Vec<u8>, CaptureError<Self::Error>> {
        Err(CaptureError::Refused(CaptureRefusal::Unsupported))
    }

    /// Consistent freeze for async snapshot. Default: [`Self::snapshot`].
    ///
    /// Standby holds the SM lock only for this call, then serializes/fsyncs
    /// off-lock via `spawn_blocking`.
    fn freeze_for_snapshot(&self, group: GroupId) -> Result<Vec<u8>, Self::Error> {
        self.snapshot(group)
    }
}
