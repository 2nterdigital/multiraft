//! Persistent controlled-group receipts and generation fencing under the log gate.
use std::io;

use multiraft_core::StartupProvenance;
use openraft::alias::{LogIdOf, VoteOf};
use openraft::RaftTypeConfig;
use rocksdb::{Direction, IteratorMode, WriteBatch};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use super::{
    db_error, decode, encode, invalid, key, lock, prefix, Engine, Metadata, RocksLogDb,
    RocksLogStore, CONTROLLED_SCHEMA, SCHEMA, SCHEMA_KEY,
};

/// Durable target admission and cleanup stage; native actors authorize transitions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlledGroupStage {
    /// Admission is recorded but no core may yet be constructed.
    Reserved,
    /// The native actor has authorized construction for the same operation.
    Opening,
    /// The native actor durably acknowledged the constructed core.
    Opened,
    /// A validated terminal proof fenced all log handles.
    Retired,
    /// The log namespace is removed; native FSM and snapshot cleanup is pending.
    LogsCleaned,
    /// Native owners confirmed every local storage namespace has been removed.
    Cleaned,
}

/// Opaque operation and terminal identities persisted outside deletable log data.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlledGroupReceipt {
    /// Monotonically increasing local replica generation.
    pub generation: u64,
    /// Native cutover or admission operation identity; never validated by this store.
    pub identity: Vec<u8>,
    /// Last durable lifecycle stage.
    pub stage: ControlledGroupStage,
    /// Native terminal configuration proof, compared byte-for-byte during cleanup.
    pub terminal_proof: Option<Vec<u8>>,
}

fn registry_key(group: u64, tag: u8) -> Vec<u8> {
    let mut key = vec![tag];
    key.extend_from_slice(&group.to_be_bytes());
    key
}

impl Engine {
    pub(super) fn receipt(&self, group: u64) -> io::Result<Option<ControlledGroupReceipt>> {
        let fence = self.db.get(registry_key(group, b'c')).map_err(db_error)?;
        let raw = self.db.get(registry_key(group, b'x')).map_err(db_error)?;
        match (fence, raw) {
            (None, None) => Ok(None),
            (Some(fence), Some(raw)) if fence == SCHEMA => decode(&raw).map(Some),
            _ => Err(invalid("missing or corrupt controlled group registry")),
        }
    }
    fn put_receipt(
        &self,
        batch: &mut WriteBatch,
        group: u64,
        receipt: &ControlledGroupReceipt,
    ) -> io::Result<()> {
        batch.put(SCHEMA_KEY, CONTROLLED_SCHEMA);
        batch.put(registry_key(group, b'c'), SCHEMA);
        batch.put(registry_key(group, b'x'), encode(receipt)?);
        Ok(())
    }
}

impl<C> RocksLogStore<C>
where
    C: RaftTypeConfig,
    C::Entry: Clone + Serialize + DeserializeOwned,
    LogIdOf<C>: Serialize + DeserializeOwned,
    VoteOf<C>: Serialize + DeserializeOwned,
{
    pub(super) fn check_generation(&self) -> io::Result<()> {
        match (self.generation, self.owner.engine.receipt(self.group)?) {
            (None, None) => Ok(()),
            (Some(generation), Some(receipt))
                if generation == receipt.generation
                    && matches!(
                        receipt.stage,
                        ControlledGroupStage::Opening | ControlledGroupStage::Opened
                    ) =>
            {
                Ok(())
            }
            _ => Err(invalid("group generation is not active")),
        }
    }
}

impl<C> RocksLogDb<C>
where
    C: RaftTypeConfig,
    C::Entry: Clone + Serialize + DeserializeOwned,
    LogIdOf<C>: Serialize + DeserializeOwned,
    VoteOf<C>: Serialize + DeserializeOwned,
{
    /// Read the durable receipt without creating or starting a native core.
    pub fn lifecycle_receipt(&self, group: u64) -> io::Result<Option<ControlledGroupReceipt>> {
        let gate = self.engine.gate(group)?;
        let _guard = lock(&gate)?;
        self.engine.receipt(group)
    }

    /// Enroll an existing replica after native cutover authorization.
    /// The metadata generation field makes older strict metadata decoders refuse it.
    pub fn enroll_controlled(
        &self,
        group: u64,
        generation: u64,
        identity: &[u8],
    ) -> io::Result<()> {
        self.create_controlled(group, generation, identity, ControlledGroupStage::Opened)
            .map(|_| ())
    }

    /// Reserve a new replica; receipt replay never authorizes core reconstruction.
    pub fn reserve_controlled(
        &self,
        group: u64,
        generation: u64,
        identity: &[u8],
    ) -> io::Result<ControlledGroupReceipt> {
        self.create_controlled(group, generation, identity, ControlledGroupStage::Reserved)
    }

    fn create_controlled(
        &self,
        group: u64,
        generation: u64,
        identity: &[u8],
        stage: ControlledGroupStage,
    ) -> io::Result<ControlledGroupReceipt> {
        if generation == 0 || identity.is_empty() {
            return Err(invalid(
                "controlled generation and identity must be nonempty",
            ));
        }
        let store = RocksLogStore {
            owner: self.clone(),
            group,
            gate: self.engine.gate(group)?,
            generation: None,
        };
        let _guard = lock(&store.gate)?;
        if let Some(old) = self.engine.receipt(group)? {
            return if old.generation == generation
                && old.identity == identity
                && (stage == ControlledGroupStage::Reserved || old.stage == stage)
            {
                Ok(old)
            } else {
                Err(invalid("controlled group already enrolled"))
            };
        }
        let meta = store.validate()?;
        if stage == ControlledGroupStage::Reserved && meta.is_some() {
            return Err(invalid(
                "reservation cannot replace an existing log namespace",
            ));
        }
        self.write_controlled(group, generation, identity, stage, meta)
    }

    fn write_controlled(
        &self,
        group: u64,
        generation: u64,
        identity: &[u8],
        stage: ControlledGroupStage,
        metadata: Option<Metadata<C>>,
    ) -> io::Result<ControlledGroupReceipt> {
        let mut meta = metadata.unwrap_or(Metadata {
            controlled_generation: None,
            ever_written: false,
            vote: None,
            committed: None,
            purged: None,
            tail: None,
            bytes: 0,
        });
        meta.controlled_generation = Some(generation);
        let receipt = ControlledGroupReceipt {
            generation,
            identity: identity.to_vec(),
            stage,
            terminal_proof: None,
        };
        let mut batch = WriteBatch::default();
        batch.put(key(group, b'm'), encode(&meta)?);
        batch.put(key(group, b'r'), SCHEMA);
        self.engine.put_receipt(&mut batch, group, &receipt)?;
        self.engine.write(batch)?;
        Ok(receipt)
    }

    /// Advance admission after native actor revalidation; opaque bytes are not authority.
    pub fn advance_controlled(
        &self,
        group: u64,
        generation: u64,
        identity: &[u8],
        expected: ControlledGroupStage,
        next: ControlledGroupStage,
    ) -> io::Result<ControlledGroupReceipt> {
        if !matches!(
            (expected, next),
            (
                ControlledGroupStage::Reserved,
                ControlledGroupStage::Opening
            ) | (ControlledGroupStage::Opening, ControlledGroupStage::Opened)
        ) {
            return Err(invalid("invalid controlled admission transition"));
        }
        let gate = self.engine.gate(group)?;
        let _guard = lock(&gate)?;
        let mut receipt = self.exact_receipt(group, generation)?;
        if receipt.identity != identity {
            return Err(invalid("controlled operation identity mismatch"));
        }
        if receipt.stage == next {
            return Ok(receipt);
        }
        if receipt.stage != expected {
            return Err(invalid("controlled admission stage mismatch"));
        }
        receipt.stage = next;
        let mut batch = WriteBatch::default();
        self.engine.put_receipt(&mut batch, group, &receipt)?;
        self.engine.write(batch)?;
        Ok(receipt)
    }

    fn exact_receipt(&self, group: u64, generation: u64) -> io::Result<ControlledGroupReceipt> {
        self.engine
            .receipt(group)?
            .filter(|receipt| receipt.generation == generation)
            .ok_or_else(|| invalid("controlled generation mismatch or missing receipt"))
    }

    /// Open an exact active generation. Native actors own authorization to start cores.
    pub fn group_generation(&self, group: u64, generation: u64) -> io::Result<RocksLogStore<C>> {
        let store = RocksLogStore {
            owner: self.clone(),
            group,
            gate: self.engine.gate(group)?,
            generation: Some(generation),
        };
        {
            let _guard = lock(&store.gate)?;
            store.check_generation()?;
            store
                .validate()?
                .ok_or_else(|| invalid("controlled group metadata missing"))?;
        }
        Ok(store)
    }

    /// Read exact-generation provenance without changing admission or creating a core.
    pub fn startup_provenance_generation(
        &self,
        group: u64,
        generation: u64,
    ) -> io::Result<StartupProvenance> {
        let store = self.group_generation(group, generation)?;
        let _guard = lock(&store.gate)?;
        store.check_generation()?;
        Ok(if store.required_metadata()?.ever_written {
            StartupProvenance::Persisted
        } else {
            StartupProvenance::Pristine
        })
    }

    /// Read retained logical bytes for an exact active generation.
    pub fn retained_log_bytes_generation(&self, group: u64, generation: u64) -> io::Result<u64> {
        let store = self.group_generation(group, generation)?;
        let _guard = lock(&store.gate)?;
        store.check_generation()?;
        Ok(store.required_metadata()?.bytes)
    }

    /// Observe logical log-value bytes for an exact generation, including retired replicas.
    /// This does not open a log handle or grant permission to read or mutate retired logs.
    pub fn observe_retained_log_bytes_generation(
        &self,
        group: u64,
        generation: u64,
    ) -> io::Result<u64> {
        let gate = self.engine.gate(group)?;
        let _guard = lock(&gate)?;
        let receipt = self.exact_receipt(group, generation)?;
        if matches!(
            receipt.stage,
            ControlledGroupStage::LogsCleaned | ControlledGroupStage::Cleaned
        ) {
            return Ok(0);
        }
        let store = RocksLogStore {
            owner: self.clone(),
            group,
            gate: gate.clone(),
            generation: Some(generation),
        };
        let meta = store.required_metadata()?;
        if meta.controlled_generation != Some(generation) {
            return Err(invalid("log observation generation mismatch"));
        }
        Ok(meta.bytes)
    }

    /// Count actual log-namespace keys for an existing exact generation, including retired replicas.
    ///
    /// Registry keys live outside this namespace. Every retained namespace key, including
    /// metadata, is counted even when the receipt reports Cleaned. This observation neither
    /// creates a namespace nor permits reading or mutating its log entries.
    ///
    /// # Errors
    /// Rejects absent or stale generation receipts and propagates database read failures.
    pub fn group_namespace_key_count_generation(
        &self,
        group: u64,
        generation: u64,
    ) -> io::Result<u64> {
        let gate = self.engine.gate(group)?;
        let _guard = lock(&gate)?;
        self.exact_receipt(group, generation)?;
        let start = prefix(group);
        let mut count = 0_u64;
        for row in self
            .engine
            .db
            .iterator(IteratorMode::From(&start, Direction::Forward))
        {
            let (key, _) = row.map_err(db_error)?;
            if !key.starts_with(&start) {
                break;
            }
            count = count
                .checked_add(1)
                .ok_or_else(|| invalid("namespace key count overflow"))?;
        }
        Ok(count)
    }

    /// Persist retirement before any deletion; native validates terminal proof authority.
    pub fn retire(&self, group: u64, generation: u64, terminal_proof: &[u8]) -> io::Result<()> {
        if terminal_proof.is_empty() {
            return Err(invalid("empty terminal proof"));
        }
        let gate = self.engine.gate(group)?;
        let _guard = lock(&gate)?;
        let mut receipt = self.exact_receipt(group, generation)?;
        if matches!(
            receipt.stage,
            ControlledGroupStage::Retired
                | ControlledGroupStage::LogsCleaned
                | ControlledGroupStage::Cleaned
        ) && receipt.terminal_proof.as_deref() == Some(terminal_proof)
        {
            return Ok(());
        }
        if receipt.stage != ControlledGroupStage::Opened {
            return Err(invalid("only opened generations may retire"));
        }
        receipt.stage = ControlledGroupStage::Retired;
        receipt.terminal_proof = Some(terminal_proof.to_vec());
        let mut batch = WriteBatch::default();
        self.engine.put_receipt(&mut batch, group, &receipt)?;
        self.engine.write(batch)
    }

    /// Delete only the exact retired log namespace after native owners have joined.
    /// Proof bytes must match retirement; the caller validates their authorization.
    pub fn cleanup_group(
        &self,
        group: u64,
        generation: u64,
        terminal_proof: &[u8],
    ) -> io::Result<()> {
        let gate = self.engine.gate(group)?;
        let _guard = lock(&gate)?;
        let mut receipt = self.exact_receipt(group, generation)?;
        if receipt.terminal_proof.as_deref() != Some(terminal_proof)
            || !matches!(
                receipt.stage,
                ControlledGroupStage::Retired
                    | ControlledGroupStage::LogsCleaned
                    | ControlledGroupStage::Cleaned
            )
        {
            return Err(invalid("cleanup retirement identity mismatch"));
        }
        if receipt.stage == ControlledGroupStage::Cleaned {
            return Ok(());
        }
        let mut batch = WriteBatch::default();
        let start = prefix(group);
        for row in self
            .engine
            .db
            .iterator(IteratorMode::From(&start, Direction::Forward))
        {
            let (key, _) = row.map_err(db_error)?;
            if !key.starts_with(&start) {
                break;
            }
            batch.delete(key);
        }
        receipt.stage = ControlledGroupStage::LogsCleaned;
        self.engine.put_receipt(&mut batch, group, &receipt)?;
        self.engine.write(batch)
    }

    /// Confirm native task joins and FSM/snapshot deletion after log cleanup.
    /// The native caller owns the completion barrier; proof bytes are only compared.
    pub fn finish_cleanup(
        &self,
        group: u64,
        generation: u64,
        terminal_proof: &[u8],
    ) -> io::Result<()> {
        let gate = self.engine.gate(group)?;
        let _guard = lock(&gate)?;
        let mut receipt = self.exact_receipt(group, generation)?;
        if receipt.terminal_proof.as_deref() != Some(terminal_proof)
            || !matches!(
                receipt.stage,
                ControlledGroupStage::LogsCleaned | ControlledGroupStage::Cleaned
            )
        {
            return Err(invalid("native cleanup completion identity mismatch"));
        }
        if receipt.stage == ControlledGroupStage::Cleaned {
            return Ok(());
        }
        receipt.stage = ControlledGroupStage::Cleaned;
        let mut batch = WriteBatch::default();
        self.engine.put_receipt(&mut batch, group, &receipt)?;
        self.engine.write(batch)
    }

    /// Reserve the next generation only after exact prior-generation cleanup.
    pub fn begin_rejoin(
        &self,
        group: u64,
        old_generation: u64,
        new_generation: u64,
        identity: &[u8],
    ) -> io::Result<()> {
        if old_generation.checked_add(1) != Some(new_generation) || identity.is_empty() {
            return Err(invalid("rejoin requires next generation and new identity"));
        }
        let gate = self.engine.gate(group)?;
        let _guard = lock(&gate)?;
        let receipt = self
            .engine
            .receipt(group)?
            .ok_or_else(|| invalid("missing rejoin receipt"))?;
        if receipt.generation == new_generation
            && receipt.identity == identity
            && receipt.stage == ControlledGroupStage::Reserved
        {
            return Ok(());
        }
        if receipt.generation != old_generation
            || receipt.stage != ControlledGroupStage::Cleaned
            || receipt.identity == identity
        {
            return Err(invalid(
                "rejoin requires cleaned generation and new operation",
            ));
        }
        let start = prefix(group);
        if let Some(row) = self
            .engine
            .db
            .iterator(IteratorMode::From(&start, Direction::Forward))
            .next()
        {
            if row.map_err(db_error)?.0.starts_with(&start) {
                return Err(invalid("cleaned group still contains log data"));
            }
        }
        self.write_controlled(
            group,
            new_generation,
            identity,
            ControlledGroupStage::Reserved,
            None,
        )
        .map(|_| ())
    }
}

#[cfg(test)]
mod namespace_observation_tests {
    use super::*;
    use multiraft_core::TypeConfig;

    #[test]
    fn exact_namespace_observation_detects_residue_and_rejects_stale_generations() {
        let parent =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/namespace-reader");
        std::fs::create_dir_all(&parent).unwrap();
        let root = tempfile::tempdir_in(parent).unwrap();
        let db = RocksLogDb::<TypeConfig>::open(root.path().join("logs")).unwrap();
        assert!(
            db.group_namespace_key_count_generation(7, 1).is_err(),
            "missing receipt never proves an empty namespace"
        );
        db.enroll_controlled(7, 1, b"original").unwrap();
        assert!(db.group_namespace_key_count_generation(7, 1).unwrap() > 0);
        db.retire(7, 1, b"proof").unwrap();
        assert!(db.group_namespace_key_count_generation(7, 1).unwrap() > 0);
        db.cleanup_group(7, 1, b"proof").unwrap();
        assert_eq!(db.group_namespace_key_count_generation(7, 1).unwrap(), 0);
        db.finish_cleanup(7, 1, b"proof").unwrap();
        assert_eq!(db.group_namespace_key_count_generation(7, 1).unwrap(), 0);
        // Deliberate internal corruption checks that Cleaned never masks actual keys.
        let mut batch = WriteBatch::default();
        batch.put(key(7, b'm'), b"deliberate residual canonical metadata");
        db.engine.write(batch).unwrap();
        assert_eq!(db.group_namespace_key_count_generation(7, 1).unwrap(), 1);
        assert!(db.begin_rejoin(7, 1, 2, b"next").is_err());
        let mut batch = WriteBatch::default();
        batch.delete(key(7, b'm'));
        db.engine.write(batch).unwrap();
        db.begin_rejoin(7, 1, 2, b"next").unwrap();
        assert!(db.group_namespace_key_count_generation(7, 1).is_err());
        assert!(db.group_namespace_key_count_generation(7, 2).unwrap() > 0);
    }
}
