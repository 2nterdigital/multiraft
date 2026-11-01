//! Acceptance uses a disposable full extraction of the sealed OLD node archive.
use multiraft_slot::adoption::inspect_existing;

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
        old_manifest_digest: evidence.layout().digest().0,
        inventory_digest: evidence.inventory_digest(),
        new_identity_digest: evidence.new_identity_digest(&identity).unwrap(),
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
    let mut wrong = authorization.clone();
    wrong.authorization_id = [99; 16];
    assert!(
        adopt_existing(&evidence, &wrong).is_err(),
        "different authorization cannot take over a partial conversion"
    );
    std::fs::remove_dir(&blocker).unwrap();
    let resumed = inspect_existing(&root).unwrap();
    let receipt = adopt_existing(&resumed, &authorization).unwrap();
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
