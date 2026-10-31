//! Crash after durable native Complete and before the real external handoff transaction.
use super::{authority::*, catalog::FileAuthority, process};
use std::sync::Arc;

pub struct TerminalCrashAuthority(pub Arc<FileAuthority>);
impl StartupAuthority for TerminalCrashAuthority {
    fn admit(
        &self,
        request: StartupRequest,
    ) -> StartupAuthorityFuture<
        '_,
        Result<StartupAuthorityResult<StartupAdmission>, StartupAuthorityError>,
    > {
        self.0.admit(request)
    }
    fn bind_active(
        &self,
        binding: StartupBinding,
    ) -> StartupAuthorityFuture<
        '_,
        Result<StartupAuthorityResult<StartupBinding>, StartupAuthorityError>,
    > {
        self.0.bind_active(binding)
    }
    fn terminal_handoff(
        &self,
        proof: StartupTerminalProof,
    ) -> StartupAuthorityFuture<
        '_,
        Result<StartupAuthorityResult<StartupTerminalProof>, StartupAuthorityError>,
    > {
        Box::pin(async move {
            process::persisted_phase("terminal_before_handoff");
            self.0.terminal_handoff(proof).await
        })
    }
    fn read(
        &self,
        group: u64,
    ) -> StartupAuthorityFuture<'_, Result<StartupCatalog, StartupAuthorityError>> {
        self.0.read(group)
    }
}
