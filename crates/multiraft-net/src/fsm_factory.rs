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
/// Until group lifecycle serialization exists, callers must serialize
/// same-key `create_group` calls; concurrent same-key creation is unsupported.
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
