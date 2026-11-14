//! Public startup refusals preserve native authority and concrete storage ownership.
use super::{admitted, authority::*, catalog::FileAuthority, cluster, session::Session};
use cluster::{deadline, DiskImage, GROUP, VOTERS};
use multiraft_core::{typ::StoredMembership, MultiRaftError};
use multiraft_net::{ControlledOpen, ControlledOpenMode, FsmFactoryContext, MultiRaft};
use multiraft_store::{ControlledGroupStage, DiskFsmDb};
use openraft::{BasicNode, Membership};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

pub async fn initialized_catalog_denies_empty_bootstrap() {
    let root = cluster::scratch("catalog-empty-bootstrap-");
    let session = Session::fresh(&root).await;
    assert!(
        session
            .authority
            .read(GROUP)
            .await
            .expect("initialized catalog")
            .membership
            .log_id()
            .is_some(),
        "catalog comes from actual initialized three-voter native group"
    );
    let authority = session.authority.clone();
    let request = session.open_request(
        0,
        ControlledOpenMode::BootstrapNew {
            voters: VOTERS.into(),
        },
    );
    let admission = admitted(
        authority
            .admit(session.request(0, 3))
            .await
            .expect("admit same node/generation"),
    );
    let mut config = cluster::configurations(&root).remove(0);
    session.stop().await;
    config.data_dir = root.join("empty-node-1");
    assert!(
        !config.data_dir.exists(),
        "replacement data directory is actually empty"
    );
    let creations = Arc::new(AtomicUsize::new(0));
    let node = cluster::start(config, creations.clone()).await;
    let result = node
        .open_controlled_group(request, authority, admission, deadline())
        .await;
    assert!(matches!(result, Err(multiraft_net::ControlledError::Rejected(
        "bootstrap requires a pristine exact-three admitted group"))),
        "initialized external membership cannot authorize new singleton/bootstrap storage: {result:?}");
    assert_eq!(
        creations.load(Ordering::SeqCst),
        0,
        "refusal precedes FSM/Core construction"
    );
    assert!(matches!(
        node.observe_group(GROUP),
        Err(MultiRaftError::UnknownGroup(GROUP))
    ));
    node.shutdown()
        .await
        .expect("close refused bootstrap runtime");
}

pub async fn missing_compacted_storage_denies_recovery() {
    let root = cluster::scratch("bootstrap-missing-storage-");
    let session = Session::fresh(&root).await;
    let leader = session.leader().await;
    session.nodes[leader]
        .propose(GROUP, b"must-not-be-lost".to_vec())
        .await
        .expect("commit actual application data before purge");
    let cut = cluster::compact(&session.nodes[leader]).await;
    assert!(cut > 0, "real checkpoint and log purge completed");
    let authority = session.authority.clone();
    let request = session.open_request(leader, ControlledOpenMode::Recover);
    let admission = admitted(
        authority
            .admit(session.request(leader, 3))
            .await
            .expect("fresh recovery admission"),
    );
    let config = cluster::configurations(&root).remove(leader);
    let data_root = config.data_dir.clone();
    let live = DiskFsmDb::group_path(&data_root, GROUP);
    let workspace = DiskFsmDb::workspace_path(&live).expect("concrete checkpoint workspace");
    let anchor = data_root.join(format!("controlled-bootstrap/group-{GROUP}.anchor.json"));
    let before = std::fs::read(&anchor).expect("real durable bootstrap anchor");
    session.stop().await;
    assert!(
        live.join("CURRENT").exists(),
        "remove only an actual live RocksDB"
    );
    assert!(
        workspace.exists(),
        "remove actual compacted checkpoint workspace"
    );
    std::fs::remove_dir_all(&live).expect("simulate lost live application DB");
    std::fs::remove_dir_all(&workspace).expect("simulate lost necessary application checkpoint");
    let node = cluster::start(config, Arc::new(AtomicUsize::new(0))).await;
    let result = node
        .open_controlled_group(request, authority, admission, deadline())
        .await;
    assert!(
        result.is_err(),
        "retained registry/anchor cannot replace lost native application state: {result:?}"
    );
    assert!(
        matches!(
            node.observe_group(GROUP),
            Err(MultiRaftError::UnknownGroup(GROUP))
        ),
        "failed disk recovery must never publish ordinary native routes"
    );
    assert_eq!(
        node.with_fsm(GROUP, |_| ()).await,
        None,
        "empty FSM cannot become application truth"
    );
    assert_eq!(
        std::fs::read(anchor).expect("retained anchor after refusal"),
        before
    );
    node.shutdown()
        .await
        .expect("close failed recovery runtime");
}

pub async fn noncanonical_storage_is_not_owned() {
    let root = cluster::scratch("noncanonical-ownership-");
    let config = cluster::configurations(&root).remove(0);
    let canonical = DiskFsmDb::group_path(&config.data_dir, GROUP);
    let workspace = DiskFsmDb::workspace_path(&canonical).expect("canonical snapshot workspace");
    std::fs::create_dir_all(&canonical).expect("create misleading canonical directory");
    let misleading = b"this CURRENT is not the actual open RocksDB";
    std::fs::write(canonical.join("CURRENT"), misleading)
        .expect("write misleading canonical CURRENT");
    let actual_root = root.join("actual-custom-disk");
    let actual = DiskFsmDb::group_path(&actual_root, GROUP);
    let image = DiskImage::open(&actual_root, GROUP).expect("create actual noncanonical RocksDB");
    drop(image);
    std::fs::write(
        actual.join("preserved-marker"),
        b"actual custom application data",
    )
    .expect("mark actual custom namespace");
    let authority = Arc::new(FileAuthority::provision(
        &root.join("catalog.json"),
        StartupCatalog {
            group_id: GROUP,
            revision: 0,
            membership: StoredMembership::new(
                None,
                Membership::new(
                    vec![VOTERS.into()],
                    VOTERS
                        .into_iter()
                        .map(|id| (id, BasicNode::default()))
                        .collect::<BTreeMap<_, _>>(),
                )
                .expect("exact pristine voters"),
            ),
            authorized: [(1, 10), (2, 20), (3, 30)].into(),
            active_operation: None,
            admissions: vec![],
            completed: vec![],
        },
    ));
    let admission = admitted(
        authority
            .admit(StartupRequest {
                group_id: GROUP,
                node_id: 1,
                generation: 10,
                request_id: [71; 16],
            })
            .await
            .expect("admit pristine controlled bootstrap"),
    );
    // Recover enters the real factory only with an existing generation; bootstrap's
    // pristine namespace check correctly rejects our misleading canonical path first.
    let phantom = canonical.with_file_name("misleading-canonical-current");
    std::fs::rename(&canonical, &phantom).expect("park misleading path until actual factory runs");
    let count = Arc::new(AtomicUsize::new(0));
    let creations = count.clone();
    let canonical_factory = canonical.clone();
    let node = Arc::new(
        MultiRaft::start_grpc_with_factory(config, move |context: FsmFactoryContext| {
            std::fs::rename(&phantom, &canonical_factory)?;
            let image = DiskImage::open(&actual_root, context.group_id())?
                .claim_snapshot_workspace(workspace.clone());
            creations.fetch_add(1, Ordering::SeqCst);
            Ok(image)
        })
        .await
        .expect("start real custom storage factory"),
    );
    let result = node
        .open_controlled_group(
            ControlledOpen {
                group_id: GROUP,
                generation: 10,
                identity: vec![1],
                mode: ControlledOpenMode::BootstrapNew {
                    voters: VOTERS.into(),
                },
            },
            authority.clone(),
            admission,
            deadline(),
        )
        .await;
    let error =
        result.expect_err("actual noncanonical DiskState must not be inferred from CURRENT");
    assert!(
        error
            .to_string()
            .contains("canonical owned live and snapshot namespaces"),
        "refusal must identify actual declared disk paths: {error}"
    );
    assert_eq!(
        count.load(Ordering::SeqCst),
        1,
        "real factory ran before ownership refusal"
    );
    assert!(matches!(
        node.observe_group(GROUP),
        Err(MultiRaftError::UnknownGroup(GROUP))
    ));
    let receipt = node
        .inspect_controlled_group(GROUP, 10, vec![1], authority)
        .await
        .expect("inspect refused generation without constructing another Core")
        .receipt()
        .expect("retained opening receipt");
    assert_eq!(
        receipt.stage,
        ControlledGroupStage::Opening,
        "refused ownership cannot certify Cleaned"
    );
    assert_eq!(
        std::fs::read(canonical.join("CURRENT")).expect("canonical marker retained"),
        misleading
    );
    assert!(
        actual.join("CURRENT").exists(),
        "actual custom DB survives refusal"
    );
    assert_eq!(
        std::fs::read(actual.join("preserved-marker")).expect("actual custom data retained"),
        b"actual custom application data"
    );
    node.shutdown()
        .await
        .expect("close refused ownership runtime");
}

pub async fn wrong_transport_domain_preserves_seed_and_retired_storage() {
    let root = cluster::scratch("wrong-domain-seed-cleanup-");
    let mut session = Session::fresh(&root).await;
    let (leader, specification, target) = session.reserve(false).await;
    let executor = session
        .group(leader)
        .begin(specification.clone(), 1, &target, deadline())
        .await
        .expect("bind real replacement before wrong-domain seed delivery");
    let seed = session
        .group(leader)
        .prepare_seed(&executor, deadline())
        .await
        .expect("capture sealed actual default-domain checkpoint");
    let admission = admitted(
        session
            .authority
            .admit(session.request(3, 1))
            .await
            .expect("real target startup admission"),
    );
    let identity = target
        .receipt()
        .expect("reserved actual generation")
        .identity;
    session.nodes[3]
        .shutdown()
        .await
        .expect("stop original target listener before cold wrong-domain boot");
    let mut target_config = cluster::configurations(&root).remove(3);
    target_config.raft_transport_fingerprint = Some([73; 32]);
    let creations = Arc::new(AtomicUsize::new(0));
    let wrong_target = cluster::start(target_config, creations.clone()).await;
    let wrong_handle = wrong_target
        .inspect_controlled_group(GROUP, 40, identity.clone(), session.authority.clone())
        .await
        .expect("inspect original reserved generation under changed transport config");
    let result = wrong_handle
        .open_target(seed.clone(), admission.clone(), deadline())
        .await;
    assert!(
        matches!(
            result,
            Err(multiraft_net::ControlledError::Rejected(
                "seed transport domain does not match the target"
            ))
        ),
        "sealed foreign-domain seed fails before install: {result:?}"
    );
    assert_eq!(
        creations.load(Ordering::SeqCst),
        0,
        "wrong domain cannot construct target Core/FSM"
    );
    assert_eq!(
        wrong_handle.receipt().expect("retained reservation").stage,
        ControlledGroupStage::Reserved
    );
    assert!(matches!(
        wrong_target.observe_group(GROUP),
        Err(MultiRaftError::UnknownGroup(GROUP))
    ));
    wrong_target
        .shutdown()
        .await
        .expect("close wrong-domain target");
    session.nodes[3] = cluster::start(
        cluster::configurations(&root).remove(3),
        session.creations[3].clone(),
    )
    .await;
    let restored = session.nodes[3]
        .inspect_controlled_group(GROUP, 40, identity, session.authority.clone())
        .await
        .expect("restore exact original reserved target");
    session.groups[3] = Some(
        restored
            .open_target(seed, admission, deadline())
            .await
            .expect("same untouched seed and admission work in original domain"),
    );
    session
        .group(leader)
        .add_learner(&executor, session.group(3), deadline())
        .await
        .expect("real target learner catches up");
    let source = *VOTERS
        .iter()
        .find(|id| !specification.target_voters.contains(id))
        .expect("exact excluded source");
    let source_index = (source - 1) as usize;
    cluster::compact(&session.nodes[source_index]).await;
    session
        .group(leader)
        .enter_joint(&executor, session.group(3), deadline())
        .await
        .expect("real joint");
    session
        .group(leader)
        .finalize_uniform(&executor, deadline())
        .await
        .expect("real uniform");
    let terminal = session
        .group(leader)
        .complete(&executor, session.group(3), deadline())
        .await
        .expect("actual sealed native terminal in original domain");
    session
        .group(source_index)
        .retire(&terminal, deadline())
        .await
        .expect("physically retire source and retain real application/checkpoint files");
    let mut config = cluster::configurations(&root).remove(source_index);
    let live = DiskFsmDb::group_path(&config.data_dir, GROUP);
    let workspace = DiskFsmDb::workspace_path(&live).expect("retired source workspace");
    assert!(
        live.join("CURRENT").exists(),
        "retirement retains actual source DB"
    );
    assert!(
        workspace.exists(),
        "retirement retains actual source checkpoint workspace"
    );
    let before = directory_image(&live);
    let checkpoint_before = directory_image(&workspace);
    let authority = session.authority.clone();
    session.stop().await;
    config.raft_transport_fingerprint = Some([74; 32]);
    let node = cluster::start(config, Arc::new(AtomicUsize::new(0))).await;
    let retired = node
        .inspect_controlled_group(GROUP, source * 10, vec![source as u8], authority)
        .await
        .expect("cold inspect of actual retired source without constructing Core");
    let result = retired.cleanup(&terminal, deadline()).await;
    assert!(
        matches!(
            result,
            Err(multiraft_net::ControlledError::Rejected(
                "terminal does not exclude this local replica"
            ))
        ),
        "original sealed terminal cannot authorize cleanup in changed transport domain: {result:?}"
    );
    assert_eq!(
        retired.receipt().expect("retained source receipt").stage,
        ControlledGroupStage::Retired,
        "wrong-domain cleanup cannot advance to Cleaned"
    );
    assert_eq!(
        directory_image(&live),
        before,
        "all retired live files survive failed cleanup byte-for-byte"
    );
    assert_eq!(
        directory_image(&workspace),
        checkpoint_before,
        "all retired checkpoint files survive failed cleanup byte-for-byte"
    );
    node.shutdown()
        .await
        .expect("close wrong-domain cold cleanup runtime");
}

fn directory_image(root: &std::path::Path) -> BTreeMap<std::path::PathBuf, Vec<u8>> {
    fn visit(
        root: &std::path::Path,
        path: &std::path::Path,
        files: &mut BTreeMap<std::path::PathBuf, Vec<u8>>,
    ) {
        for entry in std::fs::read_dir(path).expect("read preserved directory") {
            let path = entry.expect("preserved directory entry").path();
            if path.is_dir() {
                visit(root, &path, files);
            } else {
                files.insert(
                    path.strip_prefix(root)
                        .expect("relative preserved path")
                        .to_path_buf(),
                    std::fs::read(path).expect("preserved file bytes"),
                );
            }
        }
    }
    let mut files = BTreeMap::new();
    visit(root, root, &mut files);
    files
}
