//! Retained legacy Standby snapshot work; no new snapshot or recovery algorithm.
use super::{snapshot_runtime::SnapshotRuntime, TriggerCb};
use multiraft_core::{NodeId, SnapshotAdvertisement};
use multiraft_fsm::StateMachine;
use multiraft_store::{SnapshotCatalog, WeakStateMachineStore};
use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};

pub(super) fn trigger<S: StateMachine>(
    runtime: &Arc<SnapshotRuntime>,
    holder: Arc<OnceLock<WeakStateMachineStore<S>>>,
    catalog: Arc<SnapshotCatalog>,
    node_id: NodeId,
) -> TriggerCb {
    // The FSM owns this callback. It must not keep its runtime/registry alive.
    let runtime = Arc::downgrade(runtime);
    Arc::new(move |group, index, term| {
        let Some(owner) = runtime.upgrade() else {
            return;
        };
        if owner.stopping.load(Ordering::Acquire) {
            return;
        }
        let delay = *owner.serialize_delay.lock().unwrap();
        let holder = holder.clone();
        let catalog = catalog.clone();
        let runtime = runtime.clone();
        let stopping = owner.stopping.clone();
        // Atomic register/close fences late callbacks. The job captures no owning
        // runtime Arc, avoiding registry -> job -> registry cycles. The owner
        // retains the job until its blocking catalog write has actually joined.
        owner.maintenance_tasks.spawn_result(async move {
            if stopping.load(Ordering::Acquire) {
                return Ok(());
            }
            let Some(sm) = holder.get().and_then(WeakStateMachineStore::upgrade) else {
                tracing::warn!(target: "multiraft::recovery", operation="standby_snapshot",
                    phase="rejected", reason_code="state_machine_unavailable", node_id,
                    group_id=group,index,term,"standby snapshot capture unavailable");
                return Ok(());
            };
            let result = sm
                .build_standby_snapshot_async(&catalog, group, index, term, delay)
                .await;
            match result {
                Ok(entry) => {
                    if let Some(owner) = runtime.upgrade() {
                        let fetch_url = owner
                            .admin_advertise_addr
                            .map(|addr| format!("http://{addr}/snapshots/{group}/latest"))
                            .unwrap_or_default();
                        owner.record_ad(SnapshotAdvertisement {
                            group,
                            last_index: entry.last_index,
                            last_term: entry.last_term,
                            snapshot_id: entry.snapshot_id,
                            size: entry.size,
                            sha256_hex: entry.sha256_hex,
                            fetch_url,
                        });
                    }
                    Ok(())
                }
                Err(source) => {
                    tracing::error!(target: "multiraft::recovery", operation="standby_snapshot",
                        phase="error",reason_code="capture_or_write_failed",node_id,
                        group_id=group,index,term,"standby snapshot capture or storage failed");
                    // Callers of shutdown can inspect the original opaque chain;
                    // application-derived IO/Debug text is never added to logs.
                    Err(anyhow::Error::new(source)
                        .context("standby snapshot capture or storage failed"))
                }
            }
        });
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use multiraft_core::{ClusterConfig, SnapshotMode};
    use multiraft_fsm::CounterFsm;
    use multiraft_store::{SmOptions, StateMachineStore};

    #[tokio::test]
    async fn closed_registry_rejects_late_callback_and_callback_retains_no_runtime() {
        let root = tempfile::tempdir().unwrap();
        let mut config = ClusterConfig::for_test(2, &[1, 2]);
        config.data_dir = root.path().to_owned();
        config.snapshot_mode = SnapshotMode::StandbyOffload;
        let runtime = SnapshotRuntime::new(&config);
        let catalog = runtime.catalog.clone().unwrap();
        let sm = StateMachineStore::with_options(
            9,
            CounterFsm::new(),
            SmOptions {
                allow_hot_build: false,
                catalog: Some(catalog.clone()),
                on_standby_trigger: None,
            },
        );
        let holder = Arc::new(OnceLock::new());
        assert!(holder.set(sm.downgrade()).is_ok());
        let callback = trigger(&runtime, holder, catalog.clone(), 2);
        runtime.maintenance_tasks.close();
        callback(9, 1, 1);
        runtime.maintenance_tasks.join().await.unwrap();
        assert!(
            catalog.latest(9).unwrap().is_none(),
            "late callback must not start catalog work"
        );
        let weak = Arc::downgrade(&runtime);
        drop(runtime);
        assert!(
            weak.upgrade().is_none(),
            "FSM callback cannot retain its runtime/registry"
        );
        callback(9, 2, 1);
        assert!(catalog.latest(9).unwrap().is_none());
    }
}
