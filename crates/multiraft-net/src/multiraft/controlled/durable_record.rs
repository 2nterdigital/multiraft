//! Atomically publish write-once control identities outside deletable replica payloads.
use std::{
    fs,
    io::{self, Write},
    path::Path,
};

pub(super) fn publish(root: &Path, path: &Path, bytes: &[u8]) -> io::Result<bool> {
    use sha2::Digest;
    match fs::read(path) {
        Ok(existing) if existing == bytes => return Ok(false),
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "immutable control record identity conflict",
            ))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let directory = path
        .parent()
        .filter(|directory| directory.starts_with(root))
        .ok_or_else(|| io::Error::other("control record directory is outside its owner"))?;
    fs::create_dir_all(directory)?;
    let suffix = format!("{:x}", sha2::Sha256::digest(bytes));
    let pending = directory.join(format!(".{suffix}.pending"));
    // Never truncate an orphan that might still be hard-linked to a published record.
    match fs::remove_file(&pending) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&pending)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    let created = match fs::hard_link(&pending, path) {
        Ok(()) => true,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists && fs::read(path)? == bytes => {
            false
        }
        Err(error) => return Err(error),
    };
    fs::remove_file(pending)?;
    for ancestor in directory
        .ancestors()
        .take_while(|ancestor| ancestor.starts_with(root))
    {
        fs::File::open(ancestor)?.sync_all()?;
    }
    Ok(created)
}
