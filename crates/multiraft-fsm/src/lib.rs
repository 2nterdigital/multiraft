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

/// Raft applied position for one entry, handed to a disk-backed state machine.
///
/// `record` is the bridge's versioned encoding of the applied log id and
/// membership. Persist it verbatim, in the same atomic write as the entry's
/// rows, and return it from [`DiskState::durable_applied`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedState {
    index: u64,
    term: u64,
    record: Vec<u8>,
}

impl AppliedState {
    /// Constructed by the storage bridge; applications only persist it.
    pub fn new(index: u64, term: u64, record: Vec<u8>) -> Self {
        Self {
            index,
            term,
            record,
        }
    }

    pub fn index(&self) -> u64 {
        self.index
    }

    pub fn term(&self) -> u64 {
        self.term
    }

    pub fn record(&self) -> &[u8] {
        &self.record
    }
}

/// Disk-backed mode: the state machine owns its durable store and commits each
/// entry's rows together with the applied position. Restart recovers both from
/// that store; no whole-image restore is involved.
///
/// In this mode the bridge never calls [`StateMachine::apply`],
/// [`StateMachine::snapshot`], [`StateMachine::restore`] or the capture hooks.
pub trait DiskState {
    /// The last record committed by [`Self::apply_durable`], or `None` for a
    /// new store. Called before the first entry is applied.
    fn durable_applied(&self) -> std::io::Result<Option<Vec<u8>>>;

    /// Apply one entry exactly once. `data` is `None` for entries without an
    /// application payload (blank, membership, internal). Before returning,
    /// make the entry's rows and `applied.record()` durable in ONE atomic
    /// write; an error stops the Group and the entry is replayed on restart.
    fn apply_durable(
        &mut self,
        group: GroupId,
        index: u64,
        data: Option<&[u8]>,
        applied: &AppliedState,
    ) -> std::io::Result<ApplyOut>;
}

pub trait StateMachine: Send + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    fn apply(&mut self, group: GroupId, index: u64, data: &[u8]) -> Result<ApplyOut, Self::Error>;

    fn snapshot(&self, group: GroupId) -> Result<Vec<u8>, Self::Error>;

    fn restore(&mut self, group: GroupId, snapshot: &[u8]) -> Result<(), Self::Error>;

    /// `Some` selects disk-backed mode ([`DiskState`]) for this state machine's
    /// whole lifetime; the default `None` keeps whole-image snapshots.
    fn disk_state(&mut self) -> Option<&mut dyn DiskState> {
        None
    }

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

    /// Prepare a consumer readiness marker after proof and local checks succeed.
    /// Rejection precedes new provider publication. The bridge keeps business
    /// access gated until publication completes. This hook cannot wait on IO.
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
