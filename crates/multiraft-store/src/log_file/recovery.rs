//! Native log parsing, compatibility migration and pristine provenance.
use super::*;
use openraft::alias::EntryOf;
use std::io::{BufRead, BufReader};

pub(super) fn load_hard_state<C>(dir: &Path) -> io::Result<HardState<C>>
where
    C: RaftTypeConfig,
    LogIdOf<C>: Serialize + DeserializeOwned,
    VoteOf<C>: Serialize + DeserializeOwned,
{
    let path = dir.join(HARD_STATE_FILE);
    if !path.exists() {
        return Ok(HardState::default());
    }
    let bytes = fs::read(&path)?;
    serde_json::from_slice(&bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Strict opt-in evidence parsing; legacy open/load remains compatible.
fn provenance_hard_state<C>(path: &Path) -> io::Result<HardState<C>>
where
    C: RaftTypeConfig,
    LogIdOf<C>: Serialize + DeserializeOwned,
    VoteOf<C>: Serialize + DeserializeOwned,
{
    let invalid = || io::Error::new(io::ErrorKind::InvalidData, "unsupported native hard state");
    let bytes = fs::read(path)?;
    // Serde structs also accept sequences. Only the native JSON object is
    // recognized provenance; optional known fields keep their legacy defaults.
    let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    if !value.is_object() {
        return Err(invalid());
    }
    let mut ignored = false;
    let mut decoder = serde_json::Deserializer::from_slice(&bytes);
    // Deserialize the original bytes, never the Value: this retains duplicate
    // field detection in HardState and in the native nested vote/log-id structs.
    let hard =
        serde_ignored::deserialize(&mut decoder, |_| ignored = true).map_err(|_| invalid())?;
    decoder.end().map_err(|_| invalid())?;
    if ignored {
        return Err(invalid());
    }
    Ok(hard)
}

pub(super) fn load_log<C>(dir: &Path) -> io::Result<BTreeMap<u64, C::Entry>>
where
    C: RaftTypeConfig,
    C::Entry: Clone + DeserializeOwned + Serialize,
{
    let bin = dir.join(LOG_BIN);
    if bin.exists() {
        return load_bin::<C>(&bin);
    }

    // Migrate older formats once into log.bin.
    let mut map = BTreeMap::new();
    let ndjson = dir.join(LOG_NDJSON);
    if ndjson.exists() {
        map = load_ndjson::<C>(&ndjson)?;
    } else {
        let legacy = dir.join(LOG_FILE_LEGACY);
        if legacy.exists() {
            let bytes = fs::read(&legacy)?;
            let entries: Vec<EntryOf<C>> = serde_json::from_slice(&bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            for ent in entries {
                map.insert(ent.index(), ent);
            }
        }
    }
    if !map.is_empty() {
        let mut tmp_store = FileLogInner::<C> {
            dir: dir.to_path_buf(),
            last_purged_log_id: None,
            log: map.clone(),
            committed: None,
            vote: None,
            log_writer: None,
            writer_cap: DEFAULT_WRITER_CAP,
            pending_buf: Vec::new(),
            pending_callbacks: Vec::new(),
            sync_level: FileLogSyncLevel::Os,
            hard_state_dirty: false,
        };
        tmp_store.rewrite_log()?;
    }
    Ok(map)
}

fn load_bin<C>(path: &Path) -> io::Result<BTreeMap<u64, C::Entry>>
where
    C: RaftTypeConfig,
    C::Entry: DeserializeOwned,
{
    let bytes = fs::read(path)?;
    let mut map = BTreeMap::new();
    let mut off = 0usize;
    while off + 4 <= bytes.len() {
        let len = u32::from_le_bytes(bytes[off..off + 4].try_into().unwrap()) as usize;
        off += 4;
        if off + len > bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{}: truncated frame at {}", path.display(), off),
            ));
        }
        let ent: EntryOf<C> = bincode::deserialize(&bytes[off..off + len])
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        off += len;
        map.insert(ent.index(), ent);
    }
    if off != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{}: trailing {} bytes", path.display(), bytes.len() - off),
        ));
    }
    Ok(map)
}

fn load_ndjson<C>(path: &Path) -> io::Result<BTreeMap<u64, C::Entry>>
where
    C: RaftTypeConfig,
    C::Entry: DeserializeOwned,
{
    let f = fs::File::open(path)?;
    let reader = BufReader::new(f);
    let mut map = BTreeMap::new();
    for (lineno, line) in reader.lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let ent: EntryOf<C> = serde_json::from_str(&line).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{}:{}: {e}", path.display(), lineno + 1),
            )
        })?;
        map.insert(ent.index(), ent);
    }
    Ok(map)
}

/// Inspect every owned log record before migration/construction can write it.
impl<C> FileLogStore<C>
where
    C: RaftTypeConfig,
    C::Entry: Clone + Serialize + DeserializeOwned,
    LogIdOf<C>: Serialize + DeserializeOwned,
    VoteOf<C>: Serialize + DeserializeOwned,
{
    pub fn startup_provenance(
        dir: impl AsRef<Path>,
    ) -> io::Result<multiraft_core::StartupProvenance> {
        use multiraft_core::StartupProvenance;
        let dir = dir.as_ref();
        match fs::symlink_metadata(dir) {
            Ok(meta) if !meta.file_type().is_dir() => {
                return Err(io::Error::other("native log root is not a directory"))
            }
            Ok(_) => (),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return Ok(StartupProvenance::Pristine)
            }
            Err(e) => return Err(e),
        }
        let entries = match fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return Ok(StartupProvenance::Pristine)
            }
            Err(e) => return Err(e),
        };
        let mut persisted = false;
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                return Err(io::Error::other("unrecognized native log namespace"));
            }
            let name = entry.file_name();
            match name.to_str() {
                Some(HARD_STATE_FILE) => {
                    let hard = provenance_hard_state::<C>(&entry.path())?;
                    persisted |= hard.vote.is_some()
                        || hard.committed.is_some()
                        || hard.last_purged_log_id.is_some();
                }
                Some(LOG_BIN) => persisted |= !load_bin::<C>(&entry.path())?.is_empty(),
                Some(LOG_NDJSON) => persisted |= !load_ndjson::<C>(&entry.path())?.is_empty(),
                Some(LOG_FILE_LEGACY) => {
                    let entries: Vec<EntryOf<C>> = serde_json::from_slice(&fs::read(entry.path())?)
                        .map_err(io::Error::other)?;
                    persisted |= !entries.is_empty();
                }
                _ => return Err(io::Error::other("unrecognized native log namespace")),
            }
        }
        Ok(if persisted {
            StartupProvenance::Persisted
        } else {
            StartupProvenance::Pristine
        })
    }
}
