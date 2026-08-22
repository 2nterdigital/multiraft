use std::sync::Arc;

use multiraft_fsm::CounterFsm;
use multiraft_store::{SmOptions, SnapshotCatalog, StateMachineStore};
use openraft::storage::RaftStateMachine;
use openraft::RaftSnapshotBuilder;

#[tokio::test]
async fn catalog_only_get_current_snapshot_returns_none() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::TempDir::new()?;
    let catalog = Arc::new(SnapshotCatalog::new(dir.path().join("catalog"), 2));
    catalog.write(7, 10, 3, "10-3", b"catalog-only")?;
    catalog.write(7, 11, 3, "11-3", b"unreadable-latest")?;
    std::fs::remove_file(catalog.root().join("7").join("11-3").join("data.bin"))?;
    let mut sm = StateMachineStore::with_options(
        7,
        CounterFsm::new(),
        SmOptions {
            allow_hot_build: false,
            catalog: Some(catalog),
            on_standby_trigger: None,
        },
    );
    assert!(RaftStateMachine::get_current_snapshot(&mut sm)
        .await?
        .is_none());
    Ok(())
}

#[tokio::test]
async fn offload_build_snapshot_without_current_snapshot_errors(
) -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::TempDir::new()?;
    let catalog = Arc::new(SnapshotCatalog::new(dir.path().join("catalog"), 2));
    catalog.write(7, 10, 3, "10-3", b"catalog-only")?;
    let mut sm = StateMachineStore::with_options(
        7,
        CounterFsm::new(),
        SmOptions {
            allow_hot_build: false,
            catalog: Some(catalog),
            on_standby_trigger: None,
        },
    );
    assert!(RaftSnapshotBuilder::build_snapshot(&mut sm).await.is_err());
    Ok(())
}

#[tokio::test]
async fn standby_build_writes_catalog_checksum_but_never_current_snapshot(
) -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::TempDir::new()?;
    let catalog = Arc::new(SnapshotCatalog::new(dir.path().join("catalog"), 2));
    let mut sm = StateMachineStore::with_options(
        7,
        CounterFsm::new(),
        SmOptions {
            allow_hot_build: false,
            catalog: Some(catalog.clone()),
            on_standby_trigger: None,
        },
    );
    let entry = sm
        .build_standby_snapshot_async(catalog.as_ref(), 7, 10, 3, None)
        .await?;
    assert!(catalog.read(7, &entry.snapshot_id)?.is_some());
    let artifact_dir = catalog.root().join("7").join(&entry.snapshot_id);
    assert!(artifact_dir.join("data.bin").is_file());
    assert!(artifact_dir.join("meta.json").is_file());
    assert!(artifact_dir.join("sha256").is_file());
    assert!(RaftStateMachine::get_current_snapshot(&mut sm)
        .await?
        .is_none());
    Ok(())
}
