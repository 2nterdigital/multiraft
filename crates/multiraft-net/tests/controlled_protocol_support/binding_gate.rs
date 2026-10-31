//! Delay an actual persistent authority transaction at the public async seam.
use super::{authority::*, catalog::FileAuthority};
use std::sync::Arc;
use tokio::sync::{Notify, Semaphore};

pub struct BindingGate {
    pub entered: Notify,
    release: Semaphore,
    authority: Arc<FileAuthority>,
}

impl BindingGate {
    pub fn new(authority: Arc<FileAuthority>) -> Self {
        Self {
            entered: Notify::new(),
            release: Semaphore::new(0),
            authority,
        }
    }
    pub fn release(&self) {
        self.release.add_permits(1);
    }
}

impl StartupAuthority for BindingGate {
    fn admit(
        &self,
        request: StartupRequest,
    ) -> StartupAuthorityFuture<
        '_,
        Result<StartupAuthorityResult<StartupAdmission>, StartupAuthorityError>,
    > {
        self.authority.admit(request)
    }
    fn bind_active(
        &self,
        binding: StartupBinding,
    ) -> StartupAuthorityFuture<
        '_,
        Result<StartupAuthorityResult<StartupBinding>, StartupAuthorityError>,
    > {
        Box::pin(async move {
            self.entered.notify_one();
            self.release
                .acquire()
                .await
                .expect("binding gate remains open")
                .forget();
            self.authority.bind_active(binding).await
        })
    }
    fn terminal_handoff(
        &self,
        proof: StartupTerminalProof,
    ) -> StartupAuthorityFuture<
        '_,
        Result<StartupAuthorityResult<StartupTerminalProof>, StartupAuthorityError>,
    > {
        self.authority.terminal_handoff(proof)
    }
    fn read(
        &self,
        group: u64,
    ) -> StartupAuthorityFuture<'_, Result<StartupCatalog, StartupAuthorityError>> {
        self.authority.read(group)
    }
}
