//! Acceptance uses a disposable full extraction of the sealed OLD node archive.
use multiraft_slot::adoption::{inspect_existing, read_completed_receipt};

#[test]
fn refuses_missing_legacy_directory_without_creating_it() {
    let root =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/absent-legacy-adoption");
    assert!(!root.exists());
    assert!(inspect_existing(&root).is_err());
    assert!(!root.exists());
}

#[test]
#[ignore = "requires disposable full OLD node-1 extraction via NATIVE_LEGACY_COPY"]
fn reads_actual_old_membership_and_preserves_every_file() {
    let root = std::path::PathBuf::from(std::env::var_os("NATIVE_LEGACY_COPY").unwrap())
        .canonicalize()
        .unwrap();
    assert!(
        root.to_string_lossy().contains(".tmp/"),
        "only disposable copies"
    );
    let evidence = inspect_existing(&root).unwrap();
    assert_eq!(evidence.node_id(), 1);
    assert_eq!(evidence.data_space_id(), 230001);
    assert_eq!(evidence.legacy_space_identity().format_version(), 5);
    assert_eq!(evidence.legacy_space_identity().max_skew_ms(), 300000);
    assert_eq!(
        evidence.legacy_space_identity().created_at_ms(),
        1793546410106
    );
    assert_eq!(
        evidence.legacy_passport_sha256(),
        evidence.legacy_space_identity().raw_sha256()
    );
    use sha2::Digest;
    let actual_passport = std::fs::read(root.parent().unwrap().join("data-space.json")).unwrap();
    let expected_passport: [u8; 32] = sha2::Sha256::digest(actual_passport).into();
    assert_eq!(evidence.legacy_passport_sha256(), expected_passport);
    let actual_manifest = std::fs::read(root.join("slot-manifest.json")).unwrap();
    let expected_manifest: [u8; 32] = sha2::Sha256::digest(actual_manifest).into();
    assert_eq!(evidence.old_slot_manifest_sha256(), expected_manifest);
    assert_eq!(evidence.replicas().len(), 12);
    let original = evidence.replicas().iter().find(|r| r.group == 110).unwrap();
    assert_eq!(original.applied.index, 34);
    assert_eq!(original.membership.log_id().unwrap().index, 0);
    assert_eq!(original.membership.log_id().unwrap().leader_id.node_id, 1);
    assert_eq!(
        original
            .membership
            .membership()
            .voter_ids()
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(original.committed, Some(original.applied));
    assert_eq!(original.tail, Some(original.applied));
    assert_eq!(
        inspect_existing(&root).unwrap().inventory_digest(),
        evidence.inventory_digest()
    );
}

#[test]
#[ignore = "requires actual OLD disposable archive via NATIVE_LEGACY_ARCHIVE"]
fn interrupted_enrollment_rejects_recover_then_same_authorization_resumes_without_changing_payloads(
) {
    use multiraft_slot::adoption::{adopt_existing, UpgradeAuthorization};
    use multiraft_slot::{
        SlotLocalIdentity, SlotStartupDisposition, StrictSlotStartup, TrustedSlotDirectory,
    };
    use multiraft_store::adoption::load_adoption_anchor;
    let base =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/adoption-acceptance");
    std::fs::create_dir_all(&base).unwrap();
    let copy = base.join(format!("node-1-{}", std::process::id()));
    assert!(
        !copy.exists(),
        "acceptance needs a new disposable extraction"
    );
    std::fs::create_dir(&copy).unwrap();
    assert!(std::process::Command::new("tar")
        .args(["-xf"])
        .arg(std::env::var_os("NATIVE_LEGACY_ARCHIVE").unwrap())
        .arg("-C")
        .arg(&copy)
        .status()
        .unwrap()
        .success());
    let root = copy.join("state/slot").canonicalize().unwrap();
    let evidence = inspect_existing(&root).unwrap();
    let config = evidence.layout().config();
    let identity = SlotLocalIdentity {
        data_space_id: 230001,
        data_dir_uuid: [23; 16],
        protocol_version: 3,
        compatibility_digest: [42; 32],
    };
    let startup = StrictSlotStartup {
        disposition: SlotStartupDisposition::RecoverExisting,
        identity: identity.clone(),
        directory: TrustedSlotDirectory {
            version: 1,
            data_space_id: 230001,
            genesis_digest: evidence.layout().digest(),
            nodes: config.nodes.clone(),
        },
    };
    let authorization = UpgradeAuthorization {
        session_id: [1; 16],
        authorization_id: [2; 16],
        cold_manifest_sha256: [3; 32],
        physical_domain: [4; 32],
        root: root.clone(),
        node_id: 1,
        data_space_id: 230001,
        groups: evidence.replicas().iter().map(|r| r.group).collect(),
        old_layout_digest: evidence.layout().digest().0,
        inventory_digest: evidence.inventory_digest(),
        per_directory_identity_digest: evidence.per_directory_identity_digest(&identity).unwrap(),
        startup,
    };
    let mut wrong = authorization.clone();
    wrong.node_id = 2;
    assert!(adopt_existing(&evidence, &wrong).is_err());
    wrong = authorization.clone();
    wrong.data_space_id += 1;
    assert!(adopt_existing(&evidence, &wrong).is_err());
    wrong = authorization.clone();
    wrong.root = copy.clone();
    assert!(adopt_existing(&evidence, &wrong).is_err());
    wrong = authorization.clone();
    wrong.inventory_digest = [99; 32];
    assert!(adopt_existing(&evidence, &wrong).is_err());
    std::fs::write(
        root.join("unexpected-inventory"),
        b"changed after inspection",
    )
    .unwrap();
    assert!(adopt_existing(&evidence, &authorization).is_err());
    std::fs::remove_file(root.join("unexpected-inventory")).unwrap();
    let before_read = snapshot_files(&root);
    assert!(read_completed_receipt(&root, &authorization)
        .unwrap()
        .is_none());
    assert_eq!(
        snapshot_files(&root),
        before_read,
        "read-only absent receipt lookup must not create anything"
    );
    let first = &evidence.replicas()[0];
    let blocker = root.join(format!(
        "controlled-adoption/group-{}.anchor.json",
        first.group
    ));
    std::fs::create_dir_all(&blocker).unwrap();
    assert!(
        adopt_existing(&evidence, &authorization).is_err(),
        "real filesystem failure after enrollment must interrupt conversion"
    );
    {
        let db = multiraft_store::RocksLogDb::<multiraft_store::TypeConfig>::open(
            root.join("raft-rocksdb"),
        )
        .unwrap();
        assert_eq!(
            db.lifecycle_receipt(first.group)
                .unwrap()
                .unwrap()
                .generation,
            1
        );
    }
    let group_identity = evidence.replica_identity(first.group, &identity).unwrap();
    let fingerprint = evidence
        .transport_fingerprint(&authorization.startup)
        .unwrap();
    assert!(
        load_adoption_anchor(&root, 1, first.group, 1, &group_identity, Some(fingerprint)).is_err(),
        "half conversion never grants normal recovery"
    );
    let before_read = snapshot_files(&root);
    assert!(
        read_completed_receipt(&root, &authorization)
            .unwrap()
            .is_none(),
        "half conversion has no completed receipt"
    );
    let mut wrong = authorization.clone();
    wrong.authorization_id = [99; 16];
    assert!(read_completed_receipt(&root, &wrong).is_err());
    assert_eq!(
        snapshot_files(&root),
        before_read,
        "partial and rejected lookup must remain read-only"
    );
    assert!(
        adopt_existing(&evidence, &wrong).is_err(),
        "different authorization cannot take over a partial conversion"
    );
    std::fs::remove_dir(&blocker).unwrap();
    let resumed = inspect_existing(&root).unwrap();
    let receipt = adopt_existing(&resumed, &authorization).unwrap();
    assert_eq!(receipt.binding.session_id, authorization.session_id);
    assert_eq!(
        receipt.binding.authorization_id,
        authorization.authorization_id
    );
    assert_eq!(
        receipt.binding.authorized_physical_domain,
        authorization.physical_domain
    );
    assert_eq!(
        receipt.binding.old_layout_digest,
        evidence.layout().digest().0
    );
    assert_eq!(
        receipt.binding.old_slot_manifest_sha256,
        evidence.old_slot_manifest_sha256()
    );
    assert_eq!(
        receipt.binding.old_passport_sha256,
        evidence.legacy_passport_sha256()
    );
    assert_eq!(receipt.binding.target_identity, identity);
    assert_eq!(receipt.original_replicas, evidence.replicas());
    assert_eq!(
        receipt.legacy_space_identity,
        evidence.legacy_space_identity().clone()
    );
    let before_read = snapshot_files(&root);
    assert_eq!(
        read_completed_receipt(&root, &authorization).unwrap(),
        Some(receipt.clone())
    );
    assert!(read_completed_receipt(&root, &wrong).is_err());
    assert_eq!(
        snapshot_files(&root),
        before_read,
        "completed lookup and wrong binding perform no writes"
    );
    assert_eq!(
        receipt,
        adopt_existing(&resumed, &authorization).unwrap(),
        "same authorization replay has no effect"
    );
    for original in evidence.replicas() {
        let anchor = load_adoption_anchor(
            &root,
            1,
            original.group,
            1,
            &receipt.identities[&original.group],
            Some(fingerprint),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            &anchor.original, original,
            "actual original facts must be retained exactly"
        );
    }
    // inspect_resume independently rechecks every original payload file and the adjacent passport.
    assert_eq!(
        inspect_existing(&root).unwrap().inventory_digest(),
        evidence.inventory_digest()
    );
}

fn snapshot_files(
    root: &std::path::Path,
) -> std::collections::BTreeMap<std::path::PathBuf, Vec<u8>> {
    fn visit(
        root: &std::path::Path,
        path: &std::path::Path,
        out: &mut std::collections::BTreeMap<std::path::PathBuf, Vec<u8>>,
    ) {
        for row in std::fs::read_dir(path).unwrap() {
            let row = row.unwrap();
            if row.file_type().unwrap().is_dir() {
                visit(root, &row.path(), out);
            } else {
                out.insert(
                    row.path().strip_prefix(root).unwrap().to_owned(),
                    std::fs::read(row.path()).unwrap(),
                );
            }
        }
    }
    let mut out = std::collections::BTreeMap::new();
    visit(root, root, &mut out);
    out
}
