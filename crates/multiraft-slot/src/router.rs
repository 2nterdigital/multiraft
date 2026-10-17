//! Two-level key routing: key -> fixed hash bucket -> Raft slot.
//!
//! This module is the single routing implementation. Every caller (slot
//! runtime, RPC handler, applications) routes through [`KeyRouter`].

use sha2::{Digest, Sha256};

use crate::config::{GroupId, SlotId};
use crate::SlotError;

/// Fixed number of hash buckets in route version `Sha256Bucket256V1`.
pub const BUCKET_COUNT: u32 = 256;
/// Inclusive key limit, fixed by route version; empty keys are allowed.
pub const MAX_KEY_LEN: usize = 65_536;
const KEY_DOMAIN: &[u8] = b"multiraft-slot/key/sha256-v1\0";

/// Hash bucket index in `0..BUCKET_COUNT`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HashBucketId(u16);

impl HashBucketId {
    /// # Errors
    /// Returns [`SlotError::UnknownBucket`] when `value >= BUCKET_COUNT`.
    pub fn new(value: u16) -> Result<Self, SlotError> {
        if u32::from(value) < BUCKET_COUNT {
            Ok(Self(value))
        } else {
            Err(SlotError::UnknownBucket(u32::from(value)))
        }
    }

    pub fn get(self) -> u16 {
        self.0
    }

    fn index(self) -> usize {
        usize::from(self.0)
    }
}

/// Validated, copyable routing table for one layout.
///
/// Table values are slot ordinals; `slot_count <= 256` keeps them within `u8`.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct KeyRouter {
    table: [u8; BUCKET_COUNT as usize],
    slot_count: u32,
    group_id_base: GroupId,
}

impl std::fmt::Debug for KeyRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyRouter")
            .field("slot_count", &self.slot_count)
            .field("group_id_base", &self.group_id_base)
            .finish_non_exhaustive()
    }
}

impl KeyRouter {
    /// Build a router from the persisted `bucket -> slot` table.
    ///
    /// # Errors
    /// Rejects a table whose length is not exactly [`BUCKET_COUNT`], a
    /// `slot_count` outside `1..=256`, entries `>= slot_count`, slots that own
    /// no bucket, and a group id range that overflows `u64`.
    pub fn new(
        buckets: &[u32],
        slot_count: u32,
        group_id_base: GroupId,
    ) -> Result<Self, SlotError> {
        let invalid = |message: &str| SlotError::Config(message.to_owned());
        if slot_count == 0 || slot_count > BUCKET_COUNT {
            return Err(invalid("slot_count must be in 1..=256"));
        }
        if buckets.len() != BUCKET_COUNT as usize {
            return Err(invalid("buckets must contain exactly 256 entries"));
        }
        group_id_base
            .checked_add(u64::from(slot_count - 1))
            .ok_or_else(|| invalid("group id range overflows u64"))?;
        let mut table = [0_u8; BUCKET_COUNT as usize];
        let mut covered = [false; BUCKET_COUNT as usize];
        for (entry, &slot) in table.iter_mut().zip(buckets) {
            if slot >= slot_count {
                return Err(invalid("bucket entry references a slot >= slot_count"));
            }
            // slot < slot_count <= 256, so the conversion cannot fail.
            *entry = u8::try_from(slot).map_err(|_| invalid("bucket entry exceeds u8"))?;
            covered[slot as usize] = true;
        }
        if covered[..slot_count as usize].iter().any(|seen| !seen) {
            return Err(invalid("every slot must own at least one bucket"));
        }
        Ok(Self {
            table,
            slot_count,
            group_id_base,
        })
    }

    pub fn slot_count(&self) -> u32 {
        self.slot_count
    }

    pub fn group_id_base(&self) -> GroupId {
        self.group_id_base
    }

    /// The table as persisted: index is the bucket, value the slot ordinal.
    pub fn buckets(&self) -> impl ExactSizeIterator<Item = u32> + '_ {
        self.table.iter().map(|&slot| u32::from(slot))
    }

    /// `u64_be(SHA256(domain || u64_be(len) || key)[0..8]) % 256`.
    ///
    /// # Errors
    /// Returns [`SlotError::KeyTooLong`] for keys longer than [`MAX_KEY_LEN`].
    pub fn bucket_for_key(&self, key: &[u8]) -> Result<HashBucketId, SlotError> {
        bucket_for_key(key)
    }

    pub fn slot_for_bucket(&self, bucket: HashBucketId) -> SlotId {
        SlotId(u32::from(self.table[bucket.index()]))
    }

    /// # Errors
    /// Returns [`SlotError::KeyTooLong`] for keys longer than [`MAX_KEY_LEN`].
    pub fn slot_for_key(&self, key: &[u8]) -> Result<SlotId, SlotError> {
        Ok(self.slot_for_bucket(self.bucket_for_key(key)?))
    }

    /// # Errors
    /// Returns [`SlotError::KeyTooLong`] for keys longer than [`MAX_KEY_LEN`].
    pub fn group_for_key(&self, key: &[u8]) -> Result<GroupId, SlotError> {
        // Construction checked the whole group range for overflow.
        Ok(self.group_id_base + u64::from(self.slot_for_key(key)?.0))
    }
}

fn bucket_for_key(key: &[u8]) -> Result<HashBucketId, SlotError> {
    if key.len() > MAX_KEY_LEN {
        return Err(SlotError::KeyTooLong {
            actual: key.len(),
            max: MAX_KEY_LEN,
        });
    }
    let mut hash = Sha256::new();
    hash.update(KEY_DOMAIN);
    hash.update((key.len() as u64).to_be_bytes());
    hash.update(key);
    let result = hash.finalize();
    let mut prefix = [0_u8; 8];
    prefix.copy_from_slice(&result[..8]);
    let bucket = u64::from_be_bytes(prefix) % u64::from(BUCKET_COUNT);
    // bucket < 256.
    Ok(HashBucketId(bucket as u16))
}
