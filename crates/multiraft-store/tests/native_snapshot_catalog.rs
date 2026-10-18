//! Catch loss of native leader/membership identity and unsafe activation/regression.
use multiraft_core::TypeConfig;
use multiraft_store::SnapshotCatalog;
use openraft::alias::{LogIdOf, SnapshotMetaOf, StoredMembershipOf};
use openraft::vote::RaftLeaderIdExt;
use openraft::{BasicNode, Membership};
use std::collections::{BTreeMap, BTreeSet};

fn meta(index: u64) -> SnapshotMetaOf<TypeConfig> {
    let members = BTreeMap::from([
        (11, BasicNode::new("one")),
        (22, BasicNode::new("two")),
        (33, BasicNode::new("three")),
    ]);
    SnapshotMetaOf::<TypeConfig> {
        last_log_id: Some(LogIdOf::<TypeConfig>::new(
            openraft::alias::LeaderIdOf::<TypeConfig>::new_committed(7, 22),
            index,
        )),
        last_membership: StoredMembershipOf::<TypeConfig>::new(
            Some(LogIdOf::<TypeConfig>::new(
                openraft::alias::LeaderIdOf::<TypeConfig>::new_committed(6, 11),
                3,
            )),
            Membership::new(vec![BTreeSet::from([11, 22, 33])], members).unwrap(),
        ),
        snapshot_id: format!("snapshot-{index}"),
    }
}

#[test]
fn native_roundtrip_reopens_full_metadata_and_opaque_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = SnapshotCatalog::new(dir.path(), 1);
    let expected = meta(19);
    catalog
        .publish_native(42, &expected, b"opaque\0image", 1024)
        .unwrap();
    let reopened = SnapshotCatalog::new(dir.path(), 1);
    let got = reopened.load_native(42, 1024).unwrap().unwrap();
    assert_eq!(got.meta, expected);
    assert_eq!(got.data, b"opaque\0image");
    assert!(reopened.load_native(43, 1024).unwrap().is_none());
}

#[test]
fn older_publication_cannot_replace_newer_install() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = SnapshotCatalog::new(dir.path(), 1);
    catalog.publish_native(42, &meta(25), b"new", 1024).unwrap();
    assert!(catalog.publish_native(42, &meta(19), b"old", 1024).is_err());
    assert_eq!(catalog.load_native(42, 1024).unwrap().unwrap().data, b"new");
}

#[test]
fn oversized_publication_preserves_existing_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = SnapshotCatalog::new(dir.path(), 1);
    catalog.publish_native(42, &meta(19), b"old", 3).unwrap();
    assert!(catalog.publish_native(42, &meta(25), b"large", 3).is_err());
    assert_eq!(catalog.load_native(42, 3).unwrap().unwrap().meta, meta(19));
    assert!(catalog.load_native(42, 2).is_err());
}

fn active_generation(root: &std::path::Path) -> std::path::PathBuf {
    let native = root.join("42/native-v1");
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(native.join("active.json")).unwrap()).unwrap();
    native.join(manifest["generation"].as_str().unwrap())
}

#[test]
fn corrupt_active_metadata_or_data_rejects_without_fallback() {
    for component in ["native-meta.json", "data.bin"] {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SnapshotCatalog::new(dir.path(), 1);
        catalog.publish_native(42, &meta(19), b"old", 1024).unwrap();
        catalog.publish_native(42, &meta(25), b"new", 1024).unwrap();
        std::fs::write(active_generation(dir.path()).join(component), b"corrupt").unwrap();
        assert!(SnapshotCatalog::new(dir.path(), 1)
            .load_native(42, 1024)
            .is_err());
    }
}

#[test]
fn staged_candidate_is_not_current_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = SnapshotCatalog::new(dir.path(), 1);
    catalog.publish_native(42, &meta(19), b"old", 1024).unwrap();
    let staged = catalog.stage_native(42, &meta(25), b"new", 1024).unwrap();
    assert_eq!(
        SnapshotCatalog::new(dir.path(), 1)
            .load_native(42, 1024)
            .unwrap()
            .unwrap()
            .data,
        b"old"
    );
    catalog.activate_native(staged).unwrap();
    assert_eq!(
        SnapshotCatalog::new(dir.path(), 1)
            .load_native(42, 1024)
            .unwrap()
            .unwrap()
            .data,
        b"new"
    );
}

#[test]
fn foreign_or_incompatible_manifest_rejects() {
    for field in ["group", "version", "generation"] {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SnapshotCatalog::new(dir.path(), 1);
        catalog.publish_native(42, &meta(19), b"old", 1024).unwrap();
        let path = dir.path().join("42/native-v1/active.json");
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        manifest[field] = if field == "generation" {
            serde_json::json!("../../foreign")
        } else {
            serde_json::json!(999)
        };
        std::fs::write(path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        assert!(catalog.load_native(42, 1024).is_err());
    }
}

#[test]
fn activation_revalidates_candidate_before_replacing_current() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = SnapshotCatalog::new(dir.path(), 1);
    catalog.publish_native(42, &meta(19), b"old", 1024).unwrap();
    let old = active_generation(dir.path());
    let staged = catalog.stage_native(42, &meta(25), b"new", 1024).unwrap();
    for entry in std::fs::read_dir(dir.path().join("42/native-v1")).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() && path != old {
            std::fs::write(path.join("data.bin"), b"bad").unwrap();
        }
    }
    assert!(catalog.activate_native(staged).is_err());
    assert_eq!(catalog.load_native(42, 1024).unwrap().unwrap().data, b"old");
}

#[test]
fn successful_replacement_cleans_only_obsolete_active_generation() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = SnapshotCatalog::new(dir.path(), 1);
    catalog.publish_native(42, &meta(19), b"old", 1024).unwrap();
    let old = active_generation(dir.path());
    let pending = catalog
        .stage_native(42, &meta(30), b"future", 1024)
        .unwrap();
    catalog.publish_native(42, &meta(25), b"new", 1024).unwrap();
    assert!(!old.exists(), "obsolete active generation retained");
    catalog.activate_native(pending).unwrap();
    assert_eq!(
        catalog.load_native(42, 1024).unwrap().unwrap().data,
        b"future"
    );
}

#[cfg(unix)]
#[test]
fn symlinked_group_cannot_redirect_native_storage() {
    let dir = tempfile::tempdir().unwrap();
    let foreign = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(foreign.path(), dir.path().join("42")).unwrap();
    let catalog = SnapshotCatalog::new(dir.path(), 1);
    assert!(catalog
        .publish_native(42, &meta(19), b"foreign", 1024)
        .is_err());
    assert_eq!(std::fs::read_dir(foreign.path()).unwrap().count(), 0);
}

#[cfg(unix)]
#[test]
fn publication_crash_cuts_keep_valid_old_or_new_checkpoint() {
    use std::os::unix::process::ExitStatusExt;
    for cut in [
        "data_synced",
        "metadata_synced",
        "generation_synced",
        "generation_published",
        "manifest_synced",
        "manifest_renamed",
        "active_synced",
        "obsolete_removed",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SnapshotCatalog::new(dir.path(), 1);
        catalog.publish_native(42, &meta(19), b"old", 1024).unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "catalog_crash_writer",
                "--nocapture",
            ])
            .env("NATIVE_CATALOG_CRASH_ROOT", dir.path())
            .env("NATIVE_CATALOG_CRASH_CUT", cut)
            .output()
            .unwrap();
        assert_eq!(
            output.status.signal(),
            Some(9),
            "cut {cut} was not reached: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let current = SnapshotCatalog::new(dir.path(), 1)
            .load_native(42, 1024)
            .unwrap()
            .unwrap();
        assert!(
            current.data == b"old" || current.data == b"new",
            "cut {cut}"
        );
        assert_eq!(
            current.meta,
            if current.data == b"old" {
                meta(19)
            } else {
                meta(25)
            }
        );
        let reopened = SnapshotCatalog::new(dir.path(), 1);
        if matches!(cut, "data_synced" | "metadata_synced" | "generation_synced") {
            // Partial construction is diagnostic evidence, not authority. Keep
            // the original fail-closed rule for unknown/incomplete namespaces.
            assert!(reopened.startup_provenance(42, 1024).is_err());
            assert_eq!(generation_count(dir.path()), 2);
        } else {
            assert_eq!(
                reopened.startup_provenance(42, 1024).unwrap(),
                multiraft_core::StartupProvenance::Persisted
            );
            assert_eq!(generation_count(dir.path()), 1, "cut {cut}");
            assert!(!dir.path().join("42/native-v1/active.pending").exists());
        }
    }
}

#[cfg(unix)]
#[test]
#[ignore = "child process used only by publication_crash_cuts_keep_valid_old_or_new_checkpoint"]
fn catalog_crash_writer() {
    use tracing_subscriber::prelude::*;
    struct KillAt(String);
    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for KillAt {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if event.metadata().target() != "multiraft::native_catalog" {
                return;
            }
            struct Phase(Option<String>);
            impl tracing::field::Visit for Phase {
                fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
                fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                    if field.name() == "phase" {
                        self.0 = Some(value.to_owned());
                    }
                }
            }
            let mut phase = Phase(None);
            event.record(&mut phase);
            if phase.0.as_deref() == Some(self.0.as_str()) {
                std::process::Command::new("/bin/kill")
                    .args(["-KILL", &std::process::id().to_string()])
                    .status()
                    .unwrap();
                panic!("SIGKILL did not terminate writer");
            }
        }
    }
    let root = std::env::var_os("NATIVE_CATALOG_CRASH_ROOT").unwrap();
    let cut = std::env::var("NATIVE_CATALOG_CRASH_CUT").unwrap();
    let subscriber = tracing_subscriber::registry().with(KillAt(cut));
    tracing::subscriber::with_default(subscriber, || {
        SnapshotCatalog::new(std::path::PathBuf::from(root), 1)
            .publish_native(42, &meta(25), b"new", 1024)
            .unwrap();
    });
}

#[test]
fn same_cut_accepts_equivalent_application_encodings_without_inventing_canonicalization() {
    use multiraft_fsm::{CounterFsm, StateMachine};
    let dir = tempfile::tempdir().unwrap();
    let catalog = SnapshotCatalog::new(dir.path(), 1);
    let first = b"[5,[1,2]]";
    let second = b"[5,[2,1]]";
    let mut a = CounterFsm::new();
    let mut b = CounterFsm::new();
    a.restore(42, first).unwrap();
    b.restore(42, second).unwrap();
    a.apply(42, 20, &CounterFsm::encode_add(99, 1)).unwrap();
    b.apply(42, 20, &CounterFsm::encode_add(99, 1)).unwrap();
    assert_eq!(a.value(42), 5);
    assert_eq!(b.value(42), 5);
    catalog.publish_native(42, &meta(19), first, 1024).unwrap();
    catalog.publish_native(42, &meta(19), second, 1024).unwrap();
    assert_eq!(catalog.load_native(42, 1024).unwrap().unwrap().data, second);
}

#[test]
fn native_description_validates_metadata_and_data_without_returning_an_image() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = SnapshotCatalog::new(dir.path(), 1);
    catalog
        .publish_native(42, &meta(19), b"opaque", 1024)
        .unwrap();
    let info = catalog.describe_native(42, 1024).unwrap().unwrap();
    assert_eq!(info.meta, meta(19));
    assert_eq!(info.size, 6);
    std::fs::write(active_generation(dir.path()).join("data.bin"), b"broken").unwrap();
    assert!(catalog.describe_native(42, 1024).is_err());
}

fn generation_count(root: &std::path::Path) -> usize {
    std::fs::read_dir(root.join("42/native-v1"))
        .unwrap()
        .map(Result::unwrap)
        .filter(|entry| entry.file_type().unwrap().is_dir())
        .count()
}

#[test]
fn rejected_and_discarded_candidates_keep_disk_use_constant() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = SnapshotCatalog::new(dir.path(), 1);
    catalog
        .publish_native(42, &meta(25), b"active", 1024)
        .unwrap();
    for index in 3..25 {
        assert!(catalog
            .publish_native(42, &meta(index), b"older", 1024)
            .is_err());
        assert_eq!(generation_count(dir.path()), 1);
        let candidate = catalog
            .stage_native(42, &meta(30 + index), b"reject", 1024)
            .unwrap();
        assert_eq!(generation_count(dir.path()), 2);
        candidate.discard().unwrap();
        assert_eq!(generation_count(dir.path()), 1);
    }
    let candidate = catalog
        .stage_native(42, &meta(100), b"cancel", 1024)
        .unwrap();
    drop(candidate);
    assert_eq!(generation_count(dir.path()), 1);
    assert_eq!(
        catalog.load_native(42, 1024).unwrap().unwrap().data,
        b"active"
    );
}

#[test]
fn duplicate_candidates_and_reopened_catalogs_share_generation_ownership() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = SnapshotCatalog::new(dir.path(), 1);
    let reopened = SnapshotCatalog::new(dir.path().join("."), 1);
    let first = catalog.stage_native(42, &meta(25), b"same", 1024).unwrap();
    let second = reopened.stage_native(42, &meta(25), b"same", 1024).unwrap();
    drop(first);
    assert_eq!(generation_count(dir.path()), 1);
    assert_eq!(
        catalog.startup_provenance(42, 1024).unwrap(),
        multiraft_core::StartupProvenance::Pristine
    );
    reopened.activate_native(second).unwrap();
    assert_eq!(generation_count(dir.path()), 1);
    let active_candidate = catalog.stage_native(42, &meta(25), b"same", 1024).unwrap();
    drop(active_candidate);
    assert_eq!(
        catalog.load_native(42, 1024).unwrap().unwrap().data,
        b"same"
    );
}

#[test]
fn replacing_active_generation_preserves_candidate_owners_then_reclaims_it() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = SnapshotCatalog::new(dir.path(), 1);
    catalog.publish_native(42, &meta(19), b"old", 1024).unwrap();
    let active_owner = catalog.stage_native(42, &meta(19), b"old", 1024).unwrap();
    catalog.publish_native(42, &meta(25), b"new", 1024).unwrap();
    assert_eq!(generation_count(dir.path()), 2);
    drop(active_owner);
    assert_eq!(generation_count(dir.path()), 1);
    assert_eq!(catalog.load_native(42, 1024).unwrap().unwrap().data, b"new");
}

#[test]
fn stage_admission_is_bounded_and_released_by_drop() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = SnapshotCatalog::new(dir.path(), 1);
    let mut candidates: Vec<_> = (0..16)
        .map(|index| {
            catalog
                .stage_native(42, &meta(index + 10), b"candidate", 1024)
                .unwrap()
        })
        .collect();
    let error = catalog
        .stage_native(42, &meta(40), b"excess", 1024)
        .err()
        .unwrap();
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    assert_eq!(generation_count(dir.path()), 16);
    drop(candidates.pop());
    let admitted = catalog
        .stage_native(42, &meta(40), b"excess", 1024)
        .unwrap();
    drop(admitted);
    drop(candidates);
    assert_eq!(generation_count(dir.path()), 0);
}

#[test]
fn failed_stage_construction_cleans_temporary_directory_and_preserves_active_diagnostics() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = SnapshotCatalog::new(dir.path(), 1);
    catalog
        .publish_native(42, &meta(25), b"original", 1024)
        .unwrap();
    let data = active_generation(dir.path()).join("data.bin");
    std::fs::write(&data, b"corrupt").unwrap();
    for _ in 0..20 {
        assert!(catalog
            .stage_native(42, &meta(25), b"original", 1024)
            .is_err());
        assert_eq!(generation_count(dir.path()), 1);
    }
    assert_eq!(std::fs::read(data).unwrap(), b"corrupt");
    assert!(catalog.startup_provenance(42, 1024).is_err());
}

#[test]
fn malformed_manifests_block_new_staging_without_accumulating_candidates() {
    for name in ["active.json", "active.pending"] {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SnapshotCatalog::new(dir.path(), 1);
        catalog
            .publish_native(42, &meta(19), b"active", 1024)
            .unwrap();
        let path = dir.path().join("42/native-v1").join(name);
        std::fs::write(&path, b"corrupt").unwrap();
        for index in 20..40 {
            assert!(catalog
                .stage_native(42, &meta(index), b"candidate", 1024)
                .is_err());
            assert_eq!(generation_count(dir.path()), 1);
        }
        assert!(catalog.startup_provenance(42, 1024).is_err());
        assert_eq!(std::fs::read(path).unwrap(), b"corrupt");
    }
}

#[test]
fn startup_entry_budget_refuses_without_destroying_diagnostic_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = SnapshotCatalog::new(dir.path(), 1);
    catalog
        .publish_native(42, &meta(19), b"active", 1024)
        .unwrap();
    // Construct every inactive generation through the public catalog API, then
    // relocate it to represent historical ownerless on-disk candidates.
    for index in 20..84 {
        let source = tempfile::tempdir().unwrap();
        let staging = SnapshotCatalog::new(source.path(), 1);
        let candidate = staging
            .stage_native(42, &meta(index), b"candidate", 1024)
            .unwrap();
        let generation = std::fs::read_dir(source.path().join("42/native-v1"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        std::fs::rename(
            generation.path(),
            dir.path().join("42/native-v1").join(generation.file_name()),
        )
        .unwrap();
        drop(candidate);
    }
    let manifest = std::fs::read(dir.path().join("42/native-v1/active.json")).unwrap();
    let error = catalog.startup_provenance(42, 1024).err().unwrap();
    assert!(error.to_string().contains("entry budget"));
    assert_eq!(generation_count(dir.path()), 65);
    assert_eq!(
        std::fs::read(dir.path().join("42/native-v1/active.json")).unwrap(),
        manifest
    );
    assert_eq!(
        catalog.load_native(42, 1024).unwrap().unwrap().data,
        b"active"
    );
}

#[test]
fn failed_cleanup_fences_new_candidates_until_same_group_provenance_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = SnapshotCatalog::new(dir.path(), 1);
    catalog
        .publish_native(42, &meta(19), b"active", 1024)
        .unwrap();
    let candidate = catalog
        .stage_native(42, &meta(25), b"candidate", 1024)
        .unwrap();
    let path = dir.path().join("42/native-v1/active.json");
    let valid_manifest = std::fs::read(&path).unwrap();
    std::fs::write(&path, b"broken").unwrap();
    assert!(candidate.discard().is_err());
    assert_eq!(generation_count(dir.path()), 2);
    std::fs::write(&path, valid_manifest).unwrap();
    // Repairing only the authority file does not erase the cleanup obligation.
    assert!(catalog.stage_native(42, &meta(30), b"new", 1024).is_err());
    assert_eq!(
        catalog.startup_provenance(43, 1024).unwrap(),
        multiraft_core::StartupProvenance::Pristine
    );
    assert!(catalog.stage_native(42, &meta(30), b"new", 1024).is_err());
    assert_eq!(
        catalog.startup_provenance(42, 1024).unwrap(),
        multiraft_core::StartupProvenance::Persisted
    );
    assert_eq!(generation_count(dir.path()), 1);
    let recovered = catalog.stage_native(42, &meta(30), b"new", 1024).unwrap();
    recovered.discard().unwrap();
    assert_eq!(generation_count(dir.path()), 1);
}

#[test]
fn concurrent_same_content_discard_cannot_remove_published_generation() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = SnapshotCatalog::new(dir.path(), 1);
    for index in 20..28 {
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|threads| {
            threads.spawn(|| {
                let candidate = catalog
                    .stage_native(42, &meta(index), b"same", 1024)
                    .unwrap();
                barrier.wait();
                candidate.discard().unwrap();
            });
            threads.spawn(|| {
                let candidate = catalog
                    .stage_native(42, &meta(index), b"same", 1024)
                    .unwrap();
                barrier.wait();
                catalog.activate_native(candidate).unwrap();
            });
        });
        assert_eq!(
            catalog.load_native(42, 1024).unwrap().unwrap().meta,
            meta(index)
        );
        assert_eq!(generation_count(dir.path()), 1);
    }
}
