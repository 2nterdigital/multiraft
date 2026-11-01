//! Weak owned access to versioned native transport configuration, without Group side effects.
use super::*;
use crate::{CarrierCatalog, CarrierError, CarrierInstall};
impl<S: StateMachine> RuntimeHandle<S> {
    /// Install only the native carrier component; Slot acknowledges its other planes separately.
    pub async fn install_carrier_catalog(
        &self,
        catalog: CarrierCatalog,
        deadline: Instant,
    ) -> Result<CarrierInstall, CarrierError> {
        let admitted = self.admit(deadline).await?;
        let receipt = admitted.shared.node.install_carrier_catalog(&catalog)?;
        if !admitted.shared.accepting.load(Ordering::Acquire) {
            return Err(CarrierError::Closed);
        }
        if Instant::now() >= deadline {
            return Err(RuntimeError::Deadline {
                phase: RuntimePhase::LocalStatus,
                outcome_unknown: true,
            }
            .into());
        }
        Ok(receipt)
    }
    /// Actual configured native scope. A caller assertion never substitutes for this value.
    pub async fn carrier_scope(&self, deadline: Instant) -> Result<Option<[u8; 32]>, CarrierError> {
        let admitted = self.admit(deadline).await?;
        Ok(admitted.shared.node.carrier_scope())
    }
    /// Read the native component's installed verified catalog, or None for configured version zero.
    pub async fn carrier_catalog(
        &self,
        deadline: Instant,
    ) -> Result<Option<CarrierCatalog>, CarrierError> {
        let admitted = self.admit(deadline).await?;
        admitted.shared.node.carrier_catalog()
    }
}
