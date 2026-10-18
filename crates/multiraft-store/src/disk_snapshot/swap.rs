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
/// back at `live`.
pub fn install_dir(live: &Path, workspace: &Path, staged: &Path) -> io::Result<Swapped> {
    let fault = fault();
    if fault == Some(InstallFault::ErrorBeforeSwap) {
        return Err(injected("before swap"));
    }
    let retired = workspace.join(format!("retired-{}", nonce()));
    fs::rename(live, &retired)?;
    sync_both(live, workspace)?;
    if fault == Some(InstallFault::AbortAfterRetire) {
        eprintln!("injected disk install fault: aborting between swap renames");
        std::process::abort();
    }
    let second = if fault == Some(InstallFault::ErrorAfterRetire) {
        Err(injected("after retire"))
    } else {
        fs::rename(staged, live)
    };
    if let Err(error) = second {
        fs::rename(&retired, live)?;
        sync_both(live, workspace)?;
        return Err(error);
    }
    sync_both(live, workspace)?;
    Ok(Swapped {
        live: live.to_owned(),
        workspace: workspace.to_owned(),
        retired,
    })
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
