//! Application transport carrier installation shares the owner admission and close fences.
use super::owner::{closed, expired};
use super::*;
use crate::{CarrierCatalog, CarrierError, CarrierInstall};
use std::sync::atomic::Ordering;
impl ApplicationRpcHandle {
    /// Install only this application transport component; no native or Slot-wide readiness is implied.
    pub async fn install_carrier_catalog(
        &self,
        catalog: CarrierCatalog,
        deadline: Instant,
    ) -> Result<CarrierInstall, CarrierError> {
        let shared = self.shared.upgrade().ok_or(CarrierError::Closed)?;
        let _permit = shared.core.admit()?;
        if Instant::now() >= deadline {
            return Err(expired(RpcPhase::Admission, RpcDispatch::NotDispatched).into());
        }
        if !catalog
            .peers
            .iter()
            .any(|peer| peer.node_id == shared.core.node)
        {
            return Err(CarrierError::InvalidCatalog("local node is missing"));
        }
        let receipt = shared.core.channels.install_carrier_catalog(&catalog)?;
        if !shared.core.accepting.load(Ordering::Acquire) {
            return Err(closed(RpcPhase::Admission).into());
        }
        if Instant::now() >= deadline {
            return Err(expired(RpcPhase::Admission, RpcDispatch::MayHaveDispatched).into());
        }
        Ok(receipt)
    }
    /// Actual application transport scope selected at owner startup.
    pub async fn carrier_scope(&self, deadline: Instant) -> Result<Option<[u8; 32]>, CarrierError> {
        let shared = self.shared.upgrade().ok_or(CarrierError::Closed)?;
        let _permit = shared.core.admit()?;
        if Instant::now() >= deadline {
            return Err(expired(RpcPhase::Admission, RpcDispatch::NotDispatched).into());
        }
        Ok(shared.core.channels.carrier_scope())
    }
    /// Observe this component only; None is the unverified configured version zero.
    pub async fn carrier_catalog(
        &self,
        deadline: Instant,
    ) -> Result<Option<CarrierCatalog>, CarrierError> {
        let shared = self.shared.upgrade().ok_or(CarrierError::Closed)?;
        let _permit = shared.core.admit()?;
        if Instant::now() >= deadline {
            return Err(expired(RpcPhase::Admission, RpcDispatch::NotDispatched).into());
        }
        shared.core.channels.carrier_catalog()
    }
}
