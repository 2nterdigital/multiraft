//! Transport facts distinguish pre-dispatch refusal from uncertain remote execution.
use multiraft_core::NodeId;
use std::fmt;
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RpcErrorKind {
    InvalidConfiguration = 1,
    Closed,
    UnknownPeer,
    DeadlineExceeded,
    ResourceExhausted,
    InvalidArgument,
    Unimplemented,
    Unavailable,
    Internal,
}
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RpcPhase {
    Admission = 1,
    RequestValidation,
    Connect,
    Dispatch,
    Handler,
    Response,
    Shutdown,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcDispatch {
    NotDispatched,
    MayHaveDispatched,
}
/// Source message is bounded display data; stage never comes from message/status heuristics.
#[derive(Clone)]
pub struct RpcError {
    pub kind: RpcErrorKind,
    pub phase: RpcPhase,
    pub dispatch: RpcDispatch,
    pub source_node: Option<NodeId>,
    pub status_code: Option<u8>,
    message: String,
}
impl RpcError {
    pub(crate) fn new(
        kind: RpcErrorKind,
        phase: RpcPhase,
        dispatch: RpcDispatch,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            phase,
            dispatch,
            message: bounded(message.into()),
            source_node: None,
            status_code: None,
        }
    }
    pub fn message(&self) -> &str {
        &self.message
    }
}
impl fmt::Debug for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RpcError")
            .field("kind", &self.kind)
            .field("phase", &self.phase)
            .field("dispatch", &self.dispatch)
            .field("source_node", &self.source_node)
            .field("status_code", &self.status_code)
            .field("message_bytes", &self.message.len())
            .finish()
    }
}
impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for RpcError {}
/// Application rejection conveys no claim that business work never ran.
#[derive(Clone)]
pub struct RpcHandlerError {
    pub kind: RpcErrorKind,
    message: String,
}
impl RpcHandlerError {
    pub fn new(kind: RpcErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: bounded(message.into()),
        }
    }
    pub fn message(&self) -> &str {
        &self.message
    }
}
impl fmt::Debug for RpcHandlerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RpcHandlerError")
            .field("kind", &self.kind)
            .field("message_bytes", &self.message.len())
            .finish()
    }
}
fn bounded(mut text: String) -> String {
    if text.len() > 256 {
        let mut end = 256;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
    }
    text
}
