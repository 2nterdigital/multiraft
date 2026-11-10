//! Fixed, read-only storage memory observations; unsupported facts remain absent.
/// One database sample. Sizes are estimates in bytes, not a process memory cap.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageMemoryStats {
    pub db_id: String,
    pub captured_at_unix_ms: u64,
    pub column_families: Vec<ColumnFamilyMemoryStats>,
    pub num_snapshots: Option<u64>,
    pub oldest_snapshot_time: Option<u64>,
    pub num_running_flushes: Option<u64>,
    pub num_running_compactions: Option<u64>,
    pub shared: Option<SharedStorageMemoryStats>,
    pub errors: Vec<String>,
}
/// Column-family scoped RocksDB estimates, including obsolete pinned memtables.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnFamilyMemoryStats {
    pub name: String,
    pub active_memtable_bytes: Option<u64>,
    pub all_memtable_bytes: Option<u64>,
    pub pinned_memtable_bytes: Option<u64>,
    pub table_readers_bytes: Option<u64>,
    pub live_versions: Option<u64>,
    pub live_sst_bytes: Option<u64>,
    pub total_sst_bytes: Option<u64>,
}
/// Direct observations of shared resources; IDs permit consumers to deduplicate.
/// Limits are soft budgets and must never be summed with their charged contents.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SharedStorageMemoryStats {
    pub cache_id: String,
    pub cache_capacity_bytes: u64,
    pub cache_usage_bytes: u64,
    pub cache_pinned_bytes: u64,
    pub write_buffer_manager_id: String,
    pub write_buffer_manager_limit_bytes: u64,
    pub write_buffer_manager_usage_bytes: u64,
}
