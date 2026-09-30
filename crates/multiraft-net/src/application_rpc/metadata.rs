//! Finite opaque ASCII metadata. Configured names only, no business meaning.
use super::{RpcDispatch, RpcError, RpcErrorKind, RpcPhase};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Default, PartialEq, Eq)]
pub struct RpcMetadata {
    entries: BTreeMap<String, String>,
}
impl std::fmt::Debug for RpcMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcMetadata")
            .field("entry_count", &self.entries.len())
            .finish()
    }
}
impl RpcMetadata {
    pub fn insert(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<(), RpcError> {
        let key = key.into();
        let value = value.into();
        if !valid_key(&key) || value.len() > 256 || !value.bytes().all(|b| (32..=126).contains(&b))
        {
            return Err(invalid());
        }
        let previous = self.entries.insert(key.clone(), value);
        if self.entries.len() > 8
            || self
                .entries
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>()
                > 2048
        {
            if let Some(old) = previous {
                self.entries.insert(key, old);
            } else {
                self.entries.remove(&key);
            }
            return Err(invalid());
        }
        Ok(())
    }
    pub fn get(&self, key: &str) -> Option<&str> {
        self.entries.get(key).map(String::as_str)
    }
    pub(super) fn iter(&self) -> impl Iterator<Item = (&String, &String)> {
        self.entries.iter()
    }
    pub(super) fn validate(&self, allowed: &BTreeSet<String>) -> Result<(), RpcError> {
        if self.entries.keys().all(|k| allowed.contains(k)) {
            Ok(())
        } else {
            Err(invalid())
        }
    }
}
pub(super) fn valid_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= 64
        && !key.starts_with("grpc-")
        && !key.ends_with("-bin")
        && !["content-type", "te", "authorization", "host", "user-agent"].contains(&key)
        && key
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}
fn invalid() -> RpcError {
    RpcError::new(
        RpcErrorKind::InvalidArgument,
        RpcPhase::RequestValidation,
        RpcDispatch::NotDispatched,
        "application RPC metadata outside configured bounds",
    )
}
