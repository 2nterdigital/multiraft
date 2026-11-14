//! Crash-safe replacement of a closed live store directory by a verified one.
//!
//! Two renames through the workspace: `live -> retired-N`, then `staged -> live`,
//! with both parents synced after each. A crash between them leaves `live`
//! missing and `retired-N` present; [`recover_swap`] renames it back, so the old
//! state wins. Once `live` is the new directory the swap is complete and the
//! retired copy is garbage.
use super::nonce;
use super::workspace::remove_entry;
use crate::durability::sync_dir;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Test-only fault points inside [`install_dir`]. The environment variable
/// `MULTIRAFT_TEST_DISK_INSTALL_FAULT` (same names) applies to a whole process.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstallFault {
    /// Fail after the live store was closed, before any rename.
    ErrorBeforeSwap,
    /// Fail the second rename, exercising rollback.
    ErrorAfterRetire,
    /// Abort the process between the two renames.
    AbortAfterRetire,
    /// Fail the directory sync right after the first rename.
    SyncFailAfterRetire,
    /// Fail the second rename and then the rename back (live stays missing).
    RollbackRenameFail,
}

static FAULT: Mutex<Option<InstallFault>> = Mutex::new(None);

#[doc(hidden)]
pub fn inject_install_fault(fault: Option<InstallFault>) {
    *FAULT.lock().unwrap() = fault;
}

fn fault() -> Option<InstallFault> {
    if let Some(fault) = *FAULT.lock().unwrap() {
        return Some(fault);
    }
    match std::env::var("MULTIRAFT_TEST_DISK_INSTALL_FAULT").as_deref() {
        Ok("error_before_swap") => Some(InstallFault::ErrorBeforeSwap),
        Ok("error_after_retire") => Some(InstallFault::ErrorAfterRetire),
        Ok("abort_after_retire") => Some(InstallFault::AbortAfterRetire),
        Ok("sync_fail_after_retire") => Some(InstallFault::SyncFailAfterRetire),
        Ok("rollback_rename_fail") => Some(InstallFault::RollbackRenameFail),
        _ => None,
    }
}

fn injected(point: &str) -> io::Error {
    io::Error::other(format!("injected disk install fault: {point}"))
}

fn parent(path: &Path) -> &Path {
    path.parent().unwrap_or_else(|| Path::new("."))
}

fn sync_both(live: &Path, workspace: &Path) -> io::Result<()> {
    sync_dir(parent(live))?;
    sync_dir(workspace)
}

/// A completed swap whose old directory still exists.
#[must_use]
pub struct Swapped {
    live: PathBuf,
    workspace: PathBuf,
    retired: PathBuf,
}

impl Swapped {
    /// The new store opened; discard the old one.
    pub fn commit(self) -> io::Result<()> {
        remove_entry(&self.retired)?;
        sync_dir(&self.workspace)
    }

    /// The new store failed to open; put the old one back.
    pub fn rollback(self) -> io::Result<()> {
        let failed = self.workspace.join(format!("failed-{}", nonce()));
        fs::rename(&self.live, &failed)?;
        fs::rename(&self.retired, &self.live)?;
        sync_both(&self.live, &self.workspace)?;
        remove_entry(&failed)
    }
}

/// Replace the closed store at `live` with `staged` (a verified, synced
/// directory inside `workspace`, same filesystem). On error the old store is
/// put back at `live` when possible; otherwise `live` may be missing (never
/// replaced by anything else) and [`recover_swap`] restores the retired store.
/// Callers must run [`recover_swap`] after an error before reopening.
pub fn install_dir(live: &Path, workspace: &Path, staged: &Path) -> io::Result<Swapped> {
    let fault = fault();
    if fault == Some(InstallFault::ErrorBeforeSwap) {
        return Err(injected("before swap"));
    }
    let retired = workspace.join(format!("retired-{}", nonce()));
    fs::rename(live, &retired)?;
    let put_back = |error: io::Error| -> io::Error {
        if fault == Some(InstallFault::RollbackRenameFail) {
            return error; // simulated failed rename back: live stays missing
        }
        if fs::rename(&retired, live).is_ok() {
            let _ = sync_both(live, workspace);
        }
        error
    };
    let synced = if fault == Some(InstallFault::SyncFailAfterRetire) {
        Err(injected("sync after retire"))
    } else {
        sync_both(live, workspace)
    };
    if let Err(error) = synced {
        return Err(put_back(error));
    }
    if fault == Some(InstallFault::AbortAfterRetire) {
        eprintln!("injected disk install fault: aborting between swap renames");
        std::process::abort();
    }
    let second = if matches!(
        fault,
        Some(InstallFault::ErrorAfterRetire | InstallFault::RollbackRenameFail)
    ) {
        Err(injected("after retire"))
    } else {
        fs::rename(staged, live)
    };
    if let Err(error) = second {
        return Err(put_back(error));
    }
    let swapped = Swapped {
        live: live.to_owned(),
        workspace: workspace.to_owned(),
        retired,
    };
    if let Err(error) = sync_both(live, workspace) {
        // The new store's rename may not be durable; go back to the old one.
        let _ = swapped.rollback();
        return Err(error);
    }
    Ok(swapped)
}

/// Finish or undo an interrupted swap before the store at `live` is opened.
pub fn recover_swap(live: &Path, workspace: &Path) -> io::Result<()> {
    let entries = match fs::read_dir(workspace) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    let mut retired = Vec::new();
    let mut failed = Vec::new();
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with("retired-") {
            retired.push(entry.path());
        } else if name.starts_with("failed-") {
            failed.push(entry.path());
        }
    }
    if retired.is_empty() && failed.is_empty() {
        return Ok(());
    }
    retired.sort();
    // A swap only ever moves a complete store (with CURRENT) into `live`. A
    // `live` without CURRENT next to a retired store is not a swapped-in store
    // (e.g. an empty directory left by a failed reopen); the retired one wins.
    if !retired.is_empty() && live.try_exists()? && !live.join("CURRENT").try_exists()? {
        let stray = workspace.join(format!("failed-{}", nonce()));
        fs::rename(live, &stray)?;
        failed.push(stray);
    }
    if !live.try_exists()? {
        if let Some(newest) = retired.pop() {
            fs::rename(&newest, live)?;
            sync_both(live, workspace)?;
        }
    }
    for path in retired.iter().chain(&failed) {
        remove_entry(path)?;
    }
    sync_dir(workspace)
}
