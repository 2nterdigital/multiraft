//! Chunked transfer of one checkpoint directory: a sender-side lease and chunk
//! reader, and a receiver-side staging writer. Both hold one chunk at a time.
use super::workspace::remove_entry;
use super::{invalid, nonce, DiskManifest, Workspace};
use crate::durability::sync_dir;
use multiraft_fsm::GroupId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

/// Largest data chunk on the wire; far below every whole-image message cap.
pub const MAX_CHUNK_BYTES: usize = 1024 * 1024;

/// One message of a disk snapshot transfer. A transfer is `Begin`, then every
/// file's bytes in manifest order, each file ending with its SHA-256; after the
/// last file the sender submits the manifest as the OpenRaft snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DiskChunk {
    Begin {
        transfer: u64,
        manifest: Vec<u8>,
    },
    Data {
        transfer: u64,
        file: u32,
        offset: u64,
        bytes: Vec<u8>,
        /// Present on the last chunk of a file: SHA-256 of the whole file.
        sha256: Option<[u8; 32]>,
    },
}

/// Sender side: a hard-link lease of a published checkpoint, read in chunks.
/// The lease keeps the files alive if a newer compaction removes the checkpoint.
pub struct ChunkSource {
    transfer: u64,
    manifest_bytes: Vec<u8>,
    manifest: DiskManifest,
    lease: PathBuf,
    begun: bool,
    file: usize,
    offset: u64,
    reader: Option<fs::File>,
    hasher: Sha256,
}

impl ChunkSource {
    /// Lease the checkpoint named by `manifest_bytes` (blocking IO).
    pub fn open(manifest_bytes: Vec<u8>, group: GroupId) -> io::Result<Self> {
        let manifest = DiskManifest::decode(&manifest_bytes, group)?;
        let source = manifest.source.clone();
        let name = source
            .file_name()
            .ok_or_else(|| invalid("invalid disk snapshot source"))?
            .to_string_lossy()
            .into_owned();
        let lease = source.with_file_name(format!("{name}.send-{}", nonce()));
        fs::create_dir(&lease)?;
        let source_ = Self {
            transfer: rand_transfer(),
            manifest_bytes,
            manifest,
            lease,
            begun: false,
            file: 0,
            offset: 0,
            reader: None,
            hasher: Sha256::new(),
        };
        for file in &source_.manifest.files {
            let from = source.join(&file.name);
            let to = source_.lease.join(&file.name);
            fs::hard_link(&from, &to)?;
            if fs::metadata(&to)?.len() != file.size {
                return Err(invalid("published checkpoint file changed size"));
            }
        }
        Ok(source_)
    }

    pub fn total_bytes(&self) -> u64 {
        self.manifest.total_bytes().unwrap_or(0)
    }

    pub fn file_count(&self) -> usize {
        self.manifest.files.len()
    }

    /// The next message, or `None` after the last file ended (blocking IO).
    pub fn next_chunk(&mut self) -> io::Result<Option<DiskChunk>> {
        if !self.begun {
            self.begun = true;
            return Ok(Some(DiskChunk::Begin {
                transfer: self.transfer,
                manifest: self.manifest_bytes.clone(),
            }));
        }
        let Some(entry) = self.manifest.files.get(self.file) else {
            return Ok(None);
        };
        if self.reader.is_none() {
            self.reader = Some(fs::File::open(self.lease.join(&entry.name))?);
            self.hasher = Sha256::new();
            self.offset = 0;
        }
        let remaining = entry.size - self.offset;
        let want = remaining.min(MAX_CHUNK_BYTES as u64) as usize;
        let mut bytes = vec![0; want];
        self.reader
            .as_mut()
            .expect("open chunk reader")
            .read_exact(&mut bytes)?;
        self.hasher.update(&bytes);
        let offset = self.offset;
        self.offset += want as u64;
        if self.offset == entry.size {
            let mut probe = [0_u8; 1];
            if self
                .reader
                .as_mut()
                .expect("open reader")
                .read(&mut probe)?
                != 0
            {
                return Err(invalid("published checkpoint file grew"));
            }
            self.reader = None;
            let file = self.file;
            self.file += 1;
            let digest = std::mem::take(&mut self.hasher).finalize().into();
            return Ok(Some(DiskChunk::Data {
                transfer: self.transfer,
                file: file as u32,
                offset,
                bytes,
                sha256: Some(digest),
            }));
        }
        Ok(Some(DiskChunk::Data {
            transfer: self.transfer,
            file: self.file as u32,
            offset,
            bytes,
            sha256: None,
        }))
    }
}

impl Drop for ChunkSource {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.lease);
    }
}

fn rand_transfer() -> u64 {
    let digest = Sha256::digest(nonce().as_bytes());
    u64::from_le_bytes(digest[..8].try_into().expect("8 bytes"))
}

/// Receiver side: one transfer being written into a staging directory.
pub(crate) struct Incoming {
    transfer: u64,
    manifest_bytes: Vec<u8>,
    manifest: DiskManifest,
    dir: Option<PathBuf>,
    file: usize,
    offset: u64,
    writer: Option<fs::File>,
    hasher: Sha256,
    verified: Option<std::time::Instant>,
    /// (file, offset, length) of the last chunk written.
    last: Option<(u32, u64, usize)>,
}

impl Incoming {
    /// Start staging a transfer (blocking IO).
    pub(crate) fn begin(
        workspace: &Workspace,
        group: GroupId,
        transfer: u64,
        manifest_bytes: Vec<u8>,
    ) -> io::Result<Self> {
        let manifest = DiskManifest::decode(&manifest_bytes, group)?;
        let dir = workspace.staging_dir();
        fs::create_dir(&dir)?;
        Ok(Self {
            transfer,
            manifest_bytes,
            manifest,
            dir: Some(dir),
            file: 0,
            offset: 0,
            writer: None,
            hasher: Sha256::new(),
            verified: None,
            last: None,
        })
    }

    pub(crate) fn transfer(&self) -> u64 {
        self.transfer
    }

    pub(crate) fn manifest(&self) -> &DiskManifest {
        &self.manifest
    }

    pub(crate) fn manifest_bytes(&self) -> &[u8] {
        &self.manifest_bytes
    }

    pub(crate) fn dir(&self) -> &Path {
        self.dir.as_deref().expect("live staging directory")
    }

    pub(crate) fn complete(&self) -> bool {
        self.file == self.manifest.files.len()
    }

    pub(crate) fn verified(&self) -> bool {
        self.verified.is_some()
    }

    /// Verified within `hold`: it may still be waiting for OpenRaft's install.
    pub(crate) fn awaiting_install(&self, hold: std::time::Duration) -> bool {
        self.verified.is_some_and(|at| at.elapsed() < hold)
    }

    pub(crate) fn mark_verified(&mut self) {
        self.verified = Some(std::time::Instant::now());
    }

    /// Append one chunk in order; verify size and digest at each file's end (blocking IO).
    pub(crate) fn write(
        &mut self,
        file: u32,
        offset: u64,
        bytes: &[u8],
        sha256: Option<[u8; 32]>,
    ) -> io::Result<()> {
        if self.last == Some((file, offset, bytes.len())) {
            // A retry of a chunk whose first delivery was written; the file
            // digest still covers the bytes actually written.
            return Ok(());
        }
        let Some(entry) = self.manifest.files.get(self.file) else {
            return Err(invalid("disk snapshot chunk after the last file"));
        };
        if file as usize != self.file || offset != self.offset || bytes.len() > MAX_CHUNK_BYTES {
            return Err(invalid("disk snapshot chunk out of order"));
        }
        let end = offset
            .checked_add(bytes.len() as u64)
            .filter(|end| *end <= entry.size)
            .ok_or_else(|| invalid("disk snapshot chunk beyond file size"))?;
        if self.writer.is_none() {
            let path = self.dir().join(&entry.name);
            self.writer = Some(
                fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(path)?,
            );
            self.hasher = Sha256::new();
        }
        let writer = self.writer.as_mut().expect("open staging writer");
        writer.write_all(bytes)?;
        self.hasher.update(bytes);
        self.offset = end;
        self.last = Some((file, offset, bytes.len()));
        match sha256 {
            None => Ok(()),
            Some(expected) => {
                if end != entry.size {
                    return Err(invalid("disk snapshot file ended early"));
                }
                let actual: [u8; 32] = std::mem::take(&mut self.hasher).finalize().into();
                if actual != expected {
                    return Err(invalid("disk snapshot file digest mismatch"));
                }
                writer.sync_all()?;
                self.writer = None;
                self.file += 1;
                self.offset = 0;
                if self.complete() {
                    sync_dir(self.dir())?;
                }
                Ok(())
            }
        }
    }

    /// Hand the verified staging directory to an install; it is no longer removed on drop.
    pub(crate) fn take_dir(mut self) -> PathBuf {
        self.dir.take().expect("live staging directory")
    }
}

impl Drop for Incoming {
    fn drop(&mut self) {
        if let Some(dir) = self.dir.take() {
            let _ = remove_entry(&dir);
        }
    }
}
