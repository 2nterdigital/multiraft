use super::SnapshotReadyCb;
use multiraft_core::{ClusterConfig, SnapshotAdvertisement, SnapshotMode};
use multiraft_store::SnapshotCatalog;
use std::{
    fs,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

/// Shared snapshot catalog / ads for one MultiRaft node.
pub(super) struct SnapshotRuntime {
    pub(super) stopping: std::sync::atomic::AtomicBool,
    pub(super) operations: Mutex<
        std::collections::HashMap<multiraft_core::GroupId, Arc<super::maintenance::Operation>>,
    >,
    pub(super) build_budget: Arc<tokio::sync::Semaphore>,
    pub(super) catalog: Option<Arc<SnapshotCatalog>>,
    pub(super) ads: Mutex<Vec<SnapshotAdvertisement>>,
    pub(super) serialize_delay: Mutex<Option<Duration>>,
    pub(super) data_dir: PathBuf,
    pub(super) admin_advertise_addr: Option<std::net::SocketAddr>,
    pub(super) on_snapshot_ready: Mutex<Option<SnapshotReadyCb>>,
}

impl SnapshotRuntime {
    pub(super) fn new(config: &ClusterConfig) -> Arc<Self> {
        let catalog = if matches!(
            config.snapshot_mode,
            SnapshotMode::StandbyOffload | SnapshotMode::NativeDurable
        ) && !config.data_dir.as_os_str().is_empty()
        {
            let root = config.data_dir.join("snapshots");
            let _ = fs::create_dir_all(&root);
            Some(Arc::new(SnapshotCatalog::new(root, config.snapshot_keep)))
        } else {
            None
        };
        Arc::new(Self {
            stopping: std::sync::atomic::AtomicBool::new(false),
            operations: Mutex::new(Default::default()),
            build_budget: Arc::new(tokio::sync::Semaphore::new(1)),
            catalog,
            ads: Mutex::new(Self::load_ads(&config.data_dir)),
            serialize_delay: Mutex::new(None),
            data_dir: config.data_dir.clone(),
            admin_advertise_addr: config.admin_advertise_addr,
            on_snapshot_ready: Mutex::new(None),
        })
    }

    pub(super) fn load_ads(data_dir: &std::path::Path) -> Vec<SnapshotAdvertisement> {
        if data_dir.as_os_str().is_empty() {
            return Vec::new();
        }
        let path = data_dir.join("snapshot_ads.json");
        match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
            Err(_) => Vec::new(),
        }
    }

    pub(super) fn persist_ads(&self) {
        if self.data_dir.as_os_str().is_empty() {
            return;
        }
        let ads = self.ads.lock().unwrap().clone();
        if let Ok(bytes) = serde_json::to_vec_pretty(&ads) {
            let _ = fs::write(self.data_dir.join("snapshot_ads.json"), bytes);
        }
    }

    pub(super) fn record_ad(&self, ad: SnapshotAdvertisement) {
        {
            let mut ads = self.ads.lock().unwrap();
            ads.retain(|a| !(a.group == ad.group && a.snapshot_id == ad.snapshot_id));
            ads.push(ad.clone());
        }
        self.persist_ads();
        if let Some(cb) = self.on_snapshot_ready.lock().unwrap().clone() {
            cb(ad);
        }
    }
}
