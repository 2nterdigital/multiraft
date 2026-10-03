//! Public factory contract for per-group state machines.

use multiraft_core::{GroupId, NodeId};
use multiraft_fsm::StateMachine;

/// Immutable identity supplied when constructing a local group's state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct FsmFactoryContext {
    pub(crate) node_id: NodeId,
    pub(crate) group_id: GroupId,
}

impl FsmFactoryContext {
    /// Returns the local node identifier for the state machine being created.
    pub const fn node_id(self) -> NodeId {
        self.node_id
    }

    /// Returns the Consensus Group identifier for the state machine being created.
    pub const fn group_id(self) -> GroupId {
        self.group_id
    }
}

/// Constructs independently owned state machines for local Consensus Groups.
///
/// Calls for the same key may be retried after a pre-publication construction
/// failure or process restart. Calls for different node/group keys may occur
/// concurrently, so factory state must be thread-safe. Each returned state
/// machine must own its per-group resources and release them safely when
/// dropped.
///
/// The owned [`crate::RuntimeHandle`] serializes Group construction. Legacy
/// [`crate::MultiRaft`] callers must serialize same-key `create_group` calls;
/// concurrent same-key creation through that low-level facade is unsupported.
pub trait StateMachineFactory<S>: Send + Sync + 'static
where
    S: StateMachine,
{
    /// Constructs one state machine for `context`.
    ///
    /// # Errors
    ///
    /// Returns an error when the state machine cannot be constructed.
    fn create(&self, context: FsmFactoryContext) -> anyhow::Result<S>;

    /// Deadline for the owned asynchronous FSM validation hook, including admission.
    /// Node Groups share one concurrent external validator slot by default.
    fn validation_timeout(&self, _context: FsmFactoryContext) -> std::time::Duration {
        std::time::Duration::from_secs(30)
    }

    /// Optional consumer-owned shared validator admission. Return the same
    /// semaphore for all Groups that share the resource; `None` uses one slot
    /// for this node. Acquisition is covered by `validation_timeout`.
    fn validation_budget(&self) -> Option<std::sync::Arc<tokio::sync::Semaphore>> {
        None
    }

    /// Validate the local application image after native recovery reaches the
    /// persisted commit point, before the owned runtime admits Group requests.
    ///
    /// The consumer owns all validation rules. This bounded synchronous callback
    /// runs under the FSM lock and receives no native Raft capability. It is a
    /// local recovery check, not leadership confirmation or a cluster-wide read.
    /// New empty Groups are validated as well. Returning an error (or panicking)
    /// fences the node; retained cleanup stops its Groups/listener and waits for
    /// actual FSM destruction before successful rollback completes. A later node
    /// start may retry the same key. Idempotently ensuring an already-ready Group
    /// does not repeat validation. The default accepts the recovered image.
    fn validate_recovered(&self, _context: FsmFactoryContext, _fsm: &S) -> anyhow::Result<()> {
        Ok(())
    }
}

impl<S, F> StateMachineFactory<S> for F
where
    S: StateMachine,
    F: Fn(FsmFactoryContext) -> anyhow::Result<S> + Send + Sync + 'static,
{
    fn create(&self, context: FsmFactoryContext) -> anyhow::Result<S> {
        self(context)
    }
}
