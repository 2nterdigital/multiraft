//! Composite carrier installation. Address and identity checks are data fences, not peer authentication.
use super::{Shared, SlotControlError, SlotHandle, SlotStateMachine};
use crate::{RuntimeError, SlotId};
use multiraft_net::{CarrierCatalog, CarrierError, CarrierInstall};
use std::{collections::BTreeSet, sync::atomic::Ordering};
use tokio::time::Instant;

/// Slot-wide installation outcomes; partial component changes never imply a full acknowledgement.
#[derive(Debug, thiserror::Error)]
pub enum SlotCarrierError {
    #[error(transparent)]
    Admission(#[from] SlotControlError),
    #[error(transparent)]
    Slot(#[from] RuntimeError),
    #[error(transparent)]
    Native(#[from] CarrierError),
    #[error("carrier installation {version} is unresolved; retry the same full catalog")]
    Pending { version: u64 },
    #[error("carrier components differ: native={native:?}, application_rpc={application_rpc:?}, discovery={discovery:?}")]
    Incomplete {
        native: Option<CarrierInstall>,
        application_rpc: Option<CarrierInstall>,
        discovery: Option<CarrierInstall>,
    },
}
pub(super) struct CarrierDirectory {
    initial: BTreeSet<u64>,
    installed: Option<CarrierCatalog>,
    pending: Option<CarrierCatalog>,
}
impl CarrierDirectory {
    pub(super) fn new(initial: BTreeSet<u64>) -> Self {
        Self {
            initial,
            installed: None,
            pending: None,
        }
    }
    pub(super) fn contains(&self, node: &u64) -> bool {
        self.installed.as_ref().map_or_else(
            || self.initial.contains(node),
            |catalog| catalog.peers.iter().any(|peer| peer.node_id == *node),
        )
    }
    pub(super) fn identity(&self, node: u64) -> Option<Vec<u8>> {
        self.installed
            .as_ref()?
            .peers
            .iter()
            .find(|peer| peer.node_id == node)
            .map(|peer| peer.identity.clone())
    }
}
fn installation(catalog: Option<CarrierCatalog>) -> Result<Option<CarrierInstall>, CarrierError> {
    catalog
        .map(|catalog| catalog.validated_install())
        .transpose()
}
impl<S: SlotStateMachine> Shared<S> {
    pub(super) async fn install_carriers(
        &self,
        mut catalog: CarrierCatalog,
        deadline: Instant,
    ) -> Result<CarrierInstall, SlotCarrierError> {
        self.check(deadline)?;
        let controlled = self.controlled.as_ref().ok_or(RuntimeError::Unavailable)?;
        let _serial = tokio::time::timeout_at(deadline, controlled.carrier_install.lock())
            .await
            .map_err(|_| RuntimeError::Deadline)?;
        self.check(deadline)?;
        if catalog.scope != Some(self.transport_digest.0) {
            return Err(CarrierError::ScopeMismatch.into());
        }
        let expected = catalog.validated_install()?;
        if !catalog
            .peers
            .iter()
            .any(|peer| peer.node_id == self.node_id)
        {
            return Err(CarrierError::InvalidCatalog("local node is missing").into());
        }
        catalog.peers.sort_by_key(|peer| peer.node_id);
        let rpc = self.rpc()?;
        if self.native.carrier_scope(deadline).await? != catalog.scope
            || rpc.carrier_scope(deadline).await? != catalog.scope
        {
            return Err(CarrierError::ScopeMismatch.into());
        }
        {
            let mut directory = controlled.directory.lock().unwrap();
            if let Some(pending) = &directory.pending {
                if pending != &catalog {
                    return Err(SlotCarrierError::Pending {
                        version: pending.version,
                    });
                }
            }
            directory.pending = Some(catalog.clone());
        }
        let native = match self
            .native
            .install_carrier_catalog(catalog.clone(), deadline)
            .await
        {
            Ok(receipt) => receipt,
            Err(error) => {
                if matches!(
                    error,
                    CarrierError::Stale { .. }
                        | CarrierError::Conflict { .. }
                        | CarrierError::IdentityMismatch { .. }
                        | CarrierError::ScopeMismatch
                        | CarrierError::InvalidCatalog(_)
                        | CarrierError::UnsupportedTransport
                ) {
                    controlled.directory.lock().unwrap().pending = None;
                }
                return Err(error.into());
            }
        };
        let application_rpc = rpc
            .install_carrier_catalog(catalog.clone(), deadline)
            .await?;
        if native != expected || application_rpc != expected {
            return Err(SlotCarrierError::Incomplete {
                native: Some(native),
                application_rpc: Some(application_rpc),
                discovery: None,
            });
        }
        // This is the same fence lock used by owner cleanup; no discovery publication
        // can race past shutdown, even after both transport components installed.
        let _fence = self.tasks.lock().unwrap();
        self.check(deadline)?;
        let mut directory = controlled.directory.lock().unwrap();
        directory.installed = Some(catalog);
        directory.pending = None;
        Ok(expected)
    }
    pub(super) async fn installed_carriers(
        &self,
        deadline: Instant,
    ) -> Result<Option<CarrierInstall>, SlotCarrierError> {
        self.check(deadline)?;
        let controlled = self.controlled.as_ref().ok_or(RuntimeError::Unavailable)?;
        let _serial = tokio::time::timeout_at(deadline, controlled.carrier_install.lock())
            .await
            .map_err(|_| RuntimeError::Deadline)?;
        let native = installation(self.native.carrier_catalog(deadline).await?)?;
        let application_rpc = installation(self.rpc()?.carrier_catalog(deadline).await?)?;
        let discovery = installation(controlled.directory.lock().unwrap().installed.clone())?;
        self.check(deadline)?;
        if native == application_rpc && native == discovery {
            Ok(native)
        } else {
            Err(SlotCarrierError::Incomplete {
                native,
                application_rpc,
                discovery,
            })
        }
    }
    pub(crate) fn carrier_identity(&self) -> Result<crate::rpc::CarrierIdentity, RuntimeError> {
        let controlled = self.controlled.as_ref().ok_or(RuntimeError::Unavailable)?;
        let directory = controlled.directory.lock().unwrap();
        Ok(crate::rpc::CarrierIdentity {
            node_id: self.node_id,
            identity: directory.identity(self.node_id),
        })
    }
    pub(crate) fn check_carrier_identity(
        &self,
        peer: &crate::rpc::CarrierIdentity,
    ) -> Result<(), RuntimeError> {
        let controlled = self.controlled.as_ref().ok_or(RuntimeError::Unavailable)?;
        let directory = controlled.directory.lock().unwrap();
        if !directory.contains(&peer.node_id) || directory.identity(peer.node_id) != peer.identity {
            return Err(RuntimeError::LayoutMismatch);
        }
        Ok(())
    }
    pub(crate) fn controlled_profile(&self) -> bool {
        self.controlled.is_some()
    }
}
impl<S: SlotStateMachine> SlotHandle<S> {
    /// Actual common transport domain of this owner, independent from directory revisions.
    pub fn transport_domain(&self) -> Result<[u8; 32], RuntimeError> {
        Ok(self.shared()?.transport_digest.0)
    }
    /// Install App-verified persistent passport/address facts on both pools and Slot discovery.
    ///
    /// This does not authenticate the DTO or peer, grant membership/admission/intake,
    /// construct Groups, or rewrite frozen Genesis/native membership metadata.
    /// Cancellation abandons waiting; owned installation continues. An uncertain partial
    /// install must be reconciled using identical version and content.
    pub async fn install_carrier_catalog(
        &self,
        catalog: CarrierCatalog,
        deadline: Instant,
    ) -> Result<CarrierInstall, SlotCarrierError> {
        let bytes = serde_json::to_vec(&catalog)
            .map_err(|_| RuntimeError::InvalidRequest)?
            .len();
        self.control_call(SlotId(0), bytes, deadline, move |shared| async move {
            Ok(shared.install_carriers(catalog, deadline).await)
        })
        .await?
    }
    /// Acknowledge only matching actual native pool, application pool and discovery installations.
    /// None means configured version zero, regardless of the startup directory revision.
    pub async fn carrier_catalog(
        &self,
        deadline: Instant,
    ) -> Result<Option<CarrierInstall>, SlotCarrierError> {
        let shared = self.shared()?;
        let _guard = shared.budget.acquire(SlotId(0), 0)?;
        let result = shared.installed_carriers(deadline).await;
        if !shared.accepting.load(Ordering::Acquire) {
            return Err(RuntimeError::Closed.into());
        }
        result
    }
}
