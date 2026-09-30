//! Owned, business-neutral node runtime. Request handles are weak capabilities.
//!
//! Applications supply static peers, Group declarations and an FSM factory;
//! the runtime constructs and recovers native Groups before publishing handles.
//! No operation exposes native Raft handles. Commands and effects stay opaque.

mod observation;
mod owner;
mod read;
mod requests;

use crate::multiraft::tasks::OwnedTasks;
use crate::{MultiRaft, SharedFabric};
use multiraft_core::{ClusterConfig, GroupId, MultiRaftError, NodeId};
use multiraft_fsm::StateMachine;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::sync::{watch, RwLock, Semaphore};
use tokio::time::Instant;

/// Static local Group declaration. Voters are bootstrap inputs, not current authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupConfig {
    pub group_id: GroupId,
    pub voters: Vec<NodeId>,
}

/// Transport used by the owned node runtime.
#[derive(Clone)]
pub enum RuntimeTransport {
    Grpc,
    /// Shared in-process transport for independent nodes and restart harnesses.
    InProcess(SharedFabric),
}

/// Inputs for one owned runtime. Timings retain the existing native configuration.
pub struct RuntimeConfig {
    pub cluster: ClusterConfig,
    pub groups: Vec<GroupConfig>,
    pub transport: RuntimeTransport,
    /// Requests use immediate bounded admission; no hidden unbounded queue.
    pub max_inflight: usize,
}

impl RuntimeConfig {
    pub fn new(cluster: ClusterConfig, groups: Vec<GroupConfig>) -> Self {
        Self {
            cluster,
            groups,
            transport: RuntimeTransport::Grpc,
            max_inflight: 1024,
        }
    }
}

/// Where a runtime operation stopped; expiration does not imply rollback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimePhase {
    Admission,
    GroupStart,
    Propose,
    Read,
    Shutdown,
}

/// Runtime failures preserve source errors and distinguish dispatched write uncertainty.
#[derive(Debug)]
pub enum RuntimeError {
    Closed,
    Busy,
    InvalidConfig(&'static str),
    Deadline {
        phase: RuntimePhase,
        outcome_unknown: bool,
    },
    Interrupted {
        phase: RuntimePhase,
        outcome_unknown: bool,
    },
    Source(MultiRaftError),
    ShutdownFailed(Arc<RuntimeError>),
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closed => f.write_str("runtime is closed"),
            Self::Busy => f.write_str("runtime request admission is full"),
            Self::InvalidConfig(reason) => write!(f, "invalid runtime configuration: {reason}"),
            Self::Deadline {
                phase,
                outcome_unknown,
            } => write!(
                f,
                "runtime deadline expired during {phase:?}; outcome_unknown={outcome_unknown}"
            ),
            Self::Interrupted {
                phase,
                outcome_unknown,
            } => write!(
                f,
                "runtime closed during {phase:?}; outcome_unknown={outcome_unknown}"
            ),
            Self::Source(source) => source.fmt(f),
            Self::ShutdownFailed(source) => write!(f, "runtime shutdown failed: {source}"),
        }
    }
}
impl std::error::Error for RuntimeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Source(source) => Some(source),
            Self::ShutdownFailed(source) => Some(source.as_ref()),
            _ => None,
        }
    }
}
impl From<MultiRaftError> for RuntimeError {
    fn from(source: MultiRaftError) -> Self {
        Self::Source(source)
    }
}

/// The single strong owner of native node work. Drop fences and schedules cleanup.
pub struct NodeOwner<S: StateMachine> {
    shared: Arc<RuntimeShared<S>>,
}

/// Cloneable request capability. Idle handles never keep a runtime alive.
pub struct RuntimeHandle<S: StateMachine> {
    pub(super) shared: Weak<RuntimeShared<S>>,
    node_id: NodeId,
}
impl<S: StateMachine> Clone for RuntimeHandle<S> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            node_id: self.node_id,
        }
    }
}

pub(super) struct RuntimeShared<S: StateMachine> {
    pub(super) node: MultiRaft<S>,
    accepting: AtomicBool,
    pub(super) admission: Arc<RwLock<()>>,
    pub(super) slots: Arc<Semaphore>,
    pub(super) ready: Mutex<BTreeSet<GroupId>>,
    pub(super) group_creation: tokio::sync::Mutex<()>,
    pub(super) startups: OwnedTasks,
    cleanup_started: AtomicBool,
    completed: watch::Sender<Option<Result<(), Arc<RuntimeError>>>>,
    abort_requests: watch::Sender<bool>,
    closed: watch::Sender<bool>,
    runtime: tokio::runtime::Handle,
    cleanup_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

const CLEANUP_TIMEOUT: Duration = Duration::from_secs(30);
