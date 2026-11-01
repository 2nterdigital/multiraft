//! Revalidate original logical logs while the exclusive writable engine is held.
use super::*;
use crate::adoption::{canonical_log_value, LegacyReplica};
use multiraft_core::TypeConfig;
use sha2::{Digest, Sha256};

impl RocksLogDb<TypeConfig> {
    /// Verify exact original log state before enrollment or same-authorization crash resume.
    /// This neither creates a namespace nor changes any original entry or commit fact.
    ///
    /// # Errors
    /// Any generation, receipt, raw entry, vote or metadata change rejects conversion.
    pub fn verify_adoption_log(&self, original: &LegacyReplica, identity: &[u8]) -> io::Result<()> {
        let group = original.group;
        let gate = self.engine.gate(group)?;
        let _guard = lock(&gate)?;
        let generation = match self.engine.receipt(group)? {
            Some(receipt)
                if receipt.generation == 1
                    && receipt.identity == identity
                    && receipt.stage == ControlledGroupStage::Opened =>
            {
                Some(1)
            }
            Some(_) => {
                return Err(invalid(
                    "adoption log generation conflicts with authorization",
                ))
            }
            None => None,
        };
        let store = RocksLogStore {
            owner: self.clone(),
            group,
            gate: gate.clone(),
            generation,
        };
        let meta = store
            .validate()?
            .ok_or_else(|| invalid("adoption requires an existing namespace"))?;
        if !meta.ever_written
            || meta.vote != original.vote
            || meta.committed != original.committed
            || meta.tail != original.tail
            || meta.purged != original.purged
        {
            return Err(invalid("original adoption log facts changed"));
        }
        let start = prefix(group);
        let mut hash = Sha256::new();
        for row in self
            .engine
            .db
            .iterator(IteratorMode::From(&start, Direction::Forward))
        {
            let (key, value) = row.map_err(db_error)?;
            if !key.starts_with(&start) {
                break;
            }
            let value = canonical_log_value(key.get(9).copied(), &value)?;
            hash.update((key.len() as u64).to_be_bytes());
            hash.update(&key);
            hash.update((value.len() as u64).to_be_bytes());
            hash.update(value);
        }
        let actual: [u8; 32] = hash.finalize().into();
        if actual != original.log_digest {
            return Err(invalid("actual original log inventory changed"));
        }
        Ok(())
    }
}
