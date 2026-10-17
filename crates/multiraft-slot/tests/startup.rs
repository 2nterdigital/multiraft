//! Native preference startup through `SlotNodeOwner` and the pre-start probe,
//! on a generated RF3 / 12-slot layout over real gRPC.
use multiraft_fsm::{ApplyOut, CaptureError, StateMachine};
use multiraft_slot::*;
use std::{
    collections::BTreeMap,
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime},
};
use tokio::time::Instant;

const SLOTS: u32 = 12;

fn address() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}
static DIR: AtomicU64 = AtomicU64::new(0);
fn directory() -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/slot-foundation/tmp");
    std::fs::create_dir_all(&root).unwrap();
    root.join(format!(
        "slot-startup-{}-{}",
        std::process::id(),
        DIR.fetch_add(1, Ordering::Relaxed)
    ))
}
/// The deterministic RF3 / 12-slot plan (as `layouts/rf3-12slot.json`) on free ports.
fn layout() -> ClusterLayout {
    plan_layout(&PlacementInput {
        cluster_id: "slot-startup-test".into(),
        slot_count: SLOTS,
        group_id_base: 100,
        nodes: (1..=3)
            .map(|node_id| PlacementNode {
                node_id,
                raft: address(),
                application_rpc: address(),
                weight: 1,
            })
            .collect(),
    })
    .unwrap()
}
fn key(layout: &ClusterLayout, slot: u32) -> Vec<u8> {
    (0u64..)
        .map(|n| n.to_be_bytes().to_vec())
        .find(|k| layout.slot_for_key(k).unwrap() == SlotId(slot))
        .unwrap()
}

struct Counter(u64);
impl StateMachine for Counter {
    type Error = std::io::Error;
    fn apply(&mut self, _: u64, _: u64, _: &[u8]) -> Result<ApplyOut, Self::Error> {
        self.0 += 1;
        Ok(ApplyOut {
            effects: self.0.to_be_bytes().to_vec(),
        })
    }
    fn snapshot(&self, _: u64) -> Result<Vec<u8>, Self::Error> {
        Ok(self.0.to_be_bytes().to_vec())
    }
    fn restore(&mut self, _: u64, bytes: &[u8]) -> Result<(), Self::Error> {
        self.0 = u64::from_be_bytes(
            bytes
                .try_into()
                .map_err(|_| std::io::Error::other("invalid snapshot"))?,
        );
        Ok(())
    }
    fn freeze_bounded(&self, group: u64, max: usize) -> Result<Vec<u8>, CaptureError<Self::Error>> {
        if max < 8 {
            return Err(CaptureError::Refused(
                multiraft_fsm::CaptureRefusal::SizeLimit,
            ));
        }
        self.snapshot(group).map_err(CaptureError::Application)
    }
}
impl SlotStateMachine for Counter {
    fn query(&self, _: u64, _: &[u8]) -> Result<Vec<u8>, Self::Error> {
        Ok(self.0.to_be_bytes().to_vec())
    }
}

fn config(layout: &ClusterLayout, node: u64, dir: &Path, plan: SlotStartupPlan) -> SlotNodeConfig {
    let mut config = SlotNodeConfig::new(layout.clone(), node, dir);
    config.native.heartbeat_interval_ms = 50;
    config.native.election_timeout_min_ms = 200;
    config.native.election_timeout_max_ms = 400;
    config.startup = plan;
    config
}
async fn try_start(
    layout: &ClusterLayout,
    node: u64,
    dir: &Path,
    plan: SlotStartupPlan,
) -> Result<SlotNodeOwner<Counter>, RuntimeError> {
    SlotNodeOwner::start(
        config(layout, node, dir, plan),
        |_| Ok(Counter(0)),
        Instant::now() + Duration::from_secs(30),
    )
    .await
}
fn grace(ms: u64) -> SlotStartupPlan {
    SlotStartupPlan {
        bootstrap_grace: Duration::from_millis(ms),
        ..SlotStartupPlan::default()
    }
}
fn forbidden_everywhere(plan: SlotStartupPlan) -> SlotStartupPlan {
    (0..SLOTS).fold(plan, |plan, slot| {
        plan.with_policy(SlotId(slot), InitializePolicy::Forbidden)
    })
}

/// Start the given nodes concurrently; node `order[i]` begins `i * stagger` late.
async fn start_staggered(
    layout: &ClusterLayout,
    dirs: &BTreeMap<u64, PathBuf>,
    order: &[u64],
    stagger: Duration,
    plan: SlotStartupPlan,
) -> BTreeMap<u64, SlotNodeOwner<Counter>> {
    let starts = order.iter().enumerate().map(|(i, &node)| {
        let plan = plan.clone();
        let dir = dirs[&node].clone();
        async move {
            tokio::time::sleep(stagger * i as u32).await;
            let owner = try_start(layout, node, &dir, plan).await;
            (node, owner.unwrap_or_else(|e| panic!("node {node}: {e}")))
        }
    });
    futures::future::join_all(starts)
        .await
        .into_iter()
        .collect()
}

/// Write once to every slot through `owner`; retry only proven-undispatched refusals.
async fn write_every_slot(layout: &ClusterLayout, owner: &SlotNodeOwner<Counter>) {
    let handle = owner.handle();
    let until = Instant::now() + Duration::from_secs(20);
    for slot in 0..SLOTS {
        let k = key(layout, slot);
        loop {
            match handle.propose(&k, vec![1], until).await {
                Ok(applied) => {
                    assert_eq!(applied.slot, SlotId(slot));
                    break;
                }
                Err(e @ (RuntimeError::NotLeader { .. } | RuntimeError::Unavailable)) => {
                    assert!(Instant::now() < until, "slot {slot} never served: {e}");
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(e) => panic!("slot {slot}: {e}"),
            }
        }
    }
}
async fn read_every_slot(layout: &ClusterLayout, owner: &SlotNodeOwner<Counter>, expected: u64) {
    let handle = owner.handle();
    let until = Instant::now() + Duration::from_secs(20);
    for slot in 0..SLOTS {
        let k = key(layout, slot);
        loop {
            match handle.read_linearizable(&k, &[], until).await {
                Ok(value) => {
                    assert_eq!(value, expected.to_be_bytes(), "slot {slot}");
                    break;
                }
                Err(e @ (RuntimeError::NotLeader { .. } | RuntimeError::Unavailable)) => {
                    assert!(Instant::now() < until, "slot {slot} never served: {e}");
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(e) => panic!("slot {slot}: {e}"),
            }
        }
    }
}
async fn shutdown_all(owners: BTreeMap<u64, SlotNodeOwner<Counter>>) {
    for (node, owner) in owners {
        let report = owner
            .shutdown(Instant::now() + Duration::from_secs(30))
            .await;
        assert!(report.complete, "node {node}: {report:?}");
    }
}
fn report_of(
    owner: &SlotNodeOwner<Counter>,
    layout: &ClusterLayout,
) -> BTreeMap<SlotId, GroupStartupReport> {
    owner
        .startup_report()
        .groups
        .iter()
        .map(|g| (layout.slot_for_group(g.group_id).unwrap(), g.clone()))
        .collect()
}

type Reports = BTreeMap<u64, BTreeMap<SlotId, GroupStartupReport>>;
fn reports(owners: &BTreeMap<u64, SlotNodeOwner<Counter>>, layout: &ClusterLayout) -> Reports {
    owners
        .iter()
        .map(|(node, owner)| (*node, report_of(owner, layout)))
        .collect()
}
/// What native preference startup guarantees for a fresh cohort (`operation.rs`):
/// the preference orders initialization but each voter's grace is anchored at
/// its own registration, so a slow preferred node may find its group already
/// initialized by another voter's fallback.
fn assert_fresh_initialization(layout: &ClusterLayout, reports: &Reports) {
    for slot in (0..SLOTS).map(SlotId) {
        let preferred = layout.preferred_initializer(slot).unwrap();
        let groups: Vec<_> = reports.iter().map(|(node, r)| (*node, &r[&slot])).collect();
        for (node, group) in &groups {
            assert_eq!(group.provenance, Some(StartupProvenance::Pristine));
            assert_eq!(group.phase, StartupPhase::Ready);
            assert_eq!(group.preferred_initializer, Some(preferred));
            if *node == preferred {
                assert!(
                    !group.fallback,
                    "the preferred initializer never waits: {slot:?}"
                );
            } else if group.initialization != InitializeDisposition::NotDispatched {
                assert!(
                    group.fallback,
                    "non-preferred initializes only after grace: {slot:?} node {node}"
                );
            }
        }
        assert!(
            groups
                .iter()
                .any(|(_, g)| g.initialization == InitializeDisposition::InitOk),
            "{slot:?} was initialized by somebody: {groups:?}"
        );
        let preferred_dispatched = groups.iter().any(|(node, g)| {
            *node == preferred && g.initialization != InitializeDisposition::NotDispatched
        });
        if !preferred_dispatched {
            assert!(
                groups.iter().any(|(_, g)| g.fallback),
                "{slot:?} formed without its preferred initializer only via fallback: {groups:?}"
            );
        }
    }
}

/// Every path with type, length, mtime and bytes: equal means untouched.
type Tree = Vec<(PathBuf, bool, u64, SystemTime, Vec<u8>)>;
fn tree(root: &Path) -> Tree {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries {
            let entry = entry.unwrap();
            let meta = entry.metadata().unwrap();
            let bytes = if meta.is_dir() {
                stack.push(entry.path());
                Vec::new()
            } else {
                std::fs::read(entry.path()).unwrap()
            };
            out.push((
                entry.path(),
                meta.is_dir(),
                meta.len(),
                meta.modified().unwrap(),
                bytes,
            ));
        }
    }
    out.sort();
    out
}
fn probe_untouched(config: &SlotNodeConfig) -> BTreeMap<SlotId, StartupProvenance> {
    let before = tree(&config.data_dir);
    let existed = config.data_dir.exists();
    let probed = config.probe_local_state().unwrap();
    assert_eq!(existed, config.data_dir.exists(), "probe created data_dir");
    assert_eq!(before, tree(&config.data_dir), "probe modified local state");
    assert_eq!(probed.len(), SLOTS as usize, "every voter slot is probed");
    probed
}
fn all(probed: &BTreeMap<SlotId, StartupProvenance>, expected: StartupProvenance) -> bool {
    probed.values().all(|p| *p == expected)
}

#[test]
fn probe_treats_missing_and_manifest_only_dirs_as_pristine_without_touching_them() {
    let layout = layout();
    let dir = directory();
    let config = config(&layout, 1, &dir, SlotStartupPlan::default());
    assert!(all(&probe_untouched(&config), StartupProvenance::Pristine));
    assert!(!dir.exists());
    // A crash between manifest creation and any Raft write leaves only the manifest.
    SlotManifest::open_or_create(&dir, &layout, 1).unwrap();
    assert!(dir.join(MANIFEST_FILE_NAME).is_file());
    assert!(all(&probe_untouched(&config), StartupProvenance::Pristine));
    // Unknown log backend state is an error, never Pristine.
    std::fs::write(dir.join("raft-log-backend"), b"rocksdb-v1\n").unwrap();
    assert!(matches!(
        config.probe_local_state(),
        Err(ProbeError::Storage { slot: None, .. })
    ));
    std::fs::write(dir.join("raft-log-backend"), b"other\n").unwrap();
    assert!(config.probe_local_state().is_err());
}

#[test]
fn plan_validation_rejects_bad_grace_budget_and_nonlocal_slots() {
    let layout = layout();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    for plan in [
        grace(99),
        grace(2001),
        SlotStartupPlan {
            recovery_timeout: Duration::ZERO,
            ..SlotStartupPlan::default()
        },
        SlotStartupPlan {
            recovery_timeout: Duration::from_secs(31),
            ..SlotStartupPlan::default()
        },
        SlotStartupPlan::default().with_policy(SlotId(SLOTS), InitializePolicy::Forbidden),
    ] {
        let dir = directory();
        let config = config(&layout, 1, &dir, plan.clone());
        assert!(matches!(
            config.probe_local_state(),
            Err(ProbeError::InvalidConfig)
        ));
        let result = runtime.block_on(SlotNodeOwner::start(
            config,
            |_| Ok(Counter(0)),
            Instant::now() + Duration::from_secs(5),
        ));
        assert!(
            matches!(result, Err(RuntimeError::InvalidConfig)),
            "{plan:?}"
        );
        assert!(!dir.exists(), "rejected before creating local state");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn fresh_start_in_any_node_order_forms_all_slots_from_preferred_initializers() {
    let layout = layout();
    for (order, plan) in [
        (vec![1, 2, 3], SlotStartupPlan::default()),
        (vec![3, 1, 2], grace(200)),
        (vec![2, 3, 1], grace(200)),
    ] {
        let dirs: BTreeMap<_, _> = (1..=3).map(|n| (n, directory())).collect();
        let owners = start_staggered(&layout, &dirs, &order, Duration::from_millis(30), plan).await;
        for (node, owner) in &owners {
            let report = owner.startup_report();
            assert_eq!(report.node_id, *node);
            assert_eq!(report.input_digest, Some(layout.digest().0));
            assert!(!report.configuration_verified);
            assert_eq!(report.groups.len(), SLOTS as usize);
        }
        assert_fresh_initialization(&layout, &reports(&owners, &layout));
        write_every_slot(&layout, &owners[&order[2]]).await;
        read_every_slot(&layout, &owners[&order[1]], 1).await;
        shutdown_all(owners).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn absent_preferred_initializer_falls_back_after_grace_and_forms_every_slot() {
    let layout = layout();
    let dirs: BTreeMap<_, _> = (1..=3).map(|n| (n, directory())).collect();
    let mut owners = start_staggered(&layout, &dirs, &[1, 2], Duration::ZERO, grace(200)).await;
    let absent: Vec<_> = (0..SLOTS)
        .map(SlotId)
        .filter(|s| layout.preferred_initializer(*s).unwrap() == 3)
        .collect();
    assert_eq!(absent.len(), 4, "node 3 prefers a third of the slots");
    let reports = reports(&owners, &layout);
    assert_fresh_initialization(&layout, &reports);
    for slot in &absent {
        // Nobody can initialize before the grace expires, so a fallback occurs.
        assert!(
            reports.values().any(|r| r[slot].fallback),
            "{slot:?}: {reports:?}"
        );
    }
    // Two of three voters form and serve every slot, including node 3's.
    write_every_slot(&layout, &owners[&1]).await;
    // The late preferred initializer joins without disturbing formed groups.
    let late = try_start(&layout, 3, &dirs[&3], grace(200)).await.unwrap();
    owners.insert(3, late);
    read_every_slot(&layout, &owners[&3], 1).await;
    write_every_slot(&layout, &owners[&3]).await;
    read_every_slot(&layout, &owners[&2], 2).await;
    shutdown_all(owners).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cold_restart_recovers_persisted_slots_without_initialize() {
    let layout = layout();
    let dirs: BTreeMap<_, _> = (1..=3).map(|n| (n, directory())).collect();
    let owners = start_staggered(&layout, &dirs, &[1, 2, 3], Duration::ZERO, grace(200)).await;
    write_every_slot(&layout, &owners[&1]).await;
    shutdown_all(owners).await;
    for (node, dir) in &dirs {
        let probed = probe_untouched(&config(&layout, *node, dir, SlotStartupPlan::default()));
        assert!(
            all(&probed, StartupProvenance::Persisted),
            "node {node}: {probed:?}"
        );
    }
    let owners = start_staggered(&layout, &dirs, &[3, 2, 1], Duration::ZERO, grace(200)).await;
    for (node, owner) in &owners {
        for (slot, group) in report_of(owner, &layout) {
            assert_eq!(
                group.provenance,
                Some(StartupProvenance::Persisted),
                "{node} {slot:?}"
            );
            assert_eq!(group.initialization, InitializeDisposition::NotDispatched);
            assert!(!group.fallback);
            assert_eq!(group.phase, StartupPhase::Ready);
        }
    }
    read_every_slot(&layout, &owners[&2], 1).await;
    shutdown_all(owners).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn wiped_node_rejoins_forbidden_without_initialize_and_serves_after_catch_up() {
    let layout = layout();
    let dirs: BTreeMap<_, _> = (1..=3).map(|n| (n, directory())).collect();
    let mut owners = start_staggered(&layout, &dirs, &[1, 2, 3], Duration::ZERO, grace(200)).await;
    write_every_slot(&layout, &owners[&1]).await;
    let wiped = owners.remove(&3).unwrap();
    assert!(
        wiped
            .shutdown(Instant::now() + Duration::from_secs(30))
            .await
            .complete
    );
    // Progress the wiped node never saw.
    write_every_slot(&layout, &owners[&2]).await;
    std::fs::remove_dir_all(&dirs[&3]).unwrap();
    let rejoin = forbidden_everywhere(grace(200));
    let probed = probe_untouched(&config(&layout, 3, &dirs[&3], rejoin.clone()));
    assert!(all(&probed, StartupProvenance::Pristine));
    let owner = try_start(&layout, 3, &dirs[&3], rejoin).await.unwrap();
    for (slot, group) in report_of(&owner, &layout) {
        assert_eq!(
            group.provenance,
            Some(StartupProvenance::Pristine),
            "{slot:?}"
        );
        assert_eq!(group.initialization, InitializeDisposition::NotDispatched);
        assert!(!group.fallback);
        assert_eq!(group.phase, StartupPhase::Ready);
    }
    // Ready implies caught up: the local applied state already has both writes.
    let handle = owner.handle();
    for slot in 0..SLOTS {
        let view = handle
            .read_applied(SlotId(slot), &[], Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(view.value, 2u64.to_be_bytes(), "slot {slot}");
    }
    owners.insert(3, owner);
    // With node 1 down, every slot needs node 3's vote and replication.
    let one = owners.remove(&1).unwrap();
    assert!(
        one.shutdown(Instant::now() + Duration::from_secs(30))
            .await
            .complete
    );
    write_every_slot(&layout, &owners[&3]).await;
    read_every_slot(&layout, &owners[&2], 3).await;
    shutdown_all(owners).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_startup_reports_the_native_cause_and_releases_the_node() {
    // A lone pristine voter forbidden to initialize can never be caught up.
    let layout = layout();
    let dir = directory();
    let plan = SlotStartupPlan {
        recovery_timeout: Duration::from_millis(500),
        ..forbidden_everywhere(grace(100))
    };
    let started = Instant::now();
    let error = try_start(&layout, 1, &dir, plan).await.err().unwrap();
    assert!(started.elapsed() < Duration::from_secs(20));
    match &error {
        RuntimeError::StartupFailed {
            slot,
            phase,
            outcome_unknown,
            cause,
        } => {
            assert!(slot.is_some(), "{error}");
            // The per-group budget runs from each group's own registration, so
            // it expires either while later groups still register or in the wait.
            assert!(phase == "native_wait" || phase == "registered", "{error}");
            assert!(*outcome_unknown, "groups were registered: {error}");
            assert!(cause.contains("deadline expired"), "{error}");
        }
        other => panic!("unexpected startup error: {other}"),
    }
    // Listener, database lock and data directory are released: the same node
    // starts again on the same ports (alone it cannot form, so stay Forbidden
    // for a short budget once more, proving the second start also cleans up).
    let again = try_start(
        &layout,
        1,
        &dir,
        SlotStartupPlan {
            recovery_timeout: Duration::from_millis(300),
            ..forbidden_everywhere(grace(100))
        },
    )
    .await;
    assert!(matches!(again, Err(RuntimeError::StartupFailed { .. })));
    let probed = probe_untouched(&config(&layout, 1, &dir, SlotStartupPlan::default()));
    assert!(all(&probed, StartupProvenance::Pristine), "{probed:?}");
}

/// A legacy FileLog root whose hard state holds a vote (persisted history).
fn legacy_vote(dir: &Path, group: u64) {
    let root = dir.join(format!("group-{group}"));
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(
        root.join("hard_state.json"),
        br#"{"last_purged_log_id":null,"committed":null,"vote":{"leader_id":{"term":4,"node_id":1},"committed":false}}"#,
    )
    .unwrap();
    assert_eq!(
        multiraft_store::FileLogStoreOf::startup_provenance(&root).unwrap(),
        StartupProvenance::Persisted
    );
}
fn assert_node_wide_refusal(config: &SlotNodeConfig, case: &str) {
    let before = tree(&config.data_dir);
    match config.probe_local_state() {
        Err(ProbeError::Storage { slot: None, .. }) => {}
        other => panic!("{case}: expected a node-wide refusal, got {other:?}"),
    }
    assert_eq!(
        before,
        tree(&config.data_dir),
        "{case}: probe modified state"
    );
}

/// Native start on `dir` (node 1, alone): `Err(cause)` when refused.
fn native_start(layout: &ClusterLayout, dir: &Path) -> Result<(), String> {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        match try_start(layout, 1, dir, grace(100)).await {
            Ok(owner) => {
                let report = owner
                    .shutdown(Instant::now() + Duration::from_secs(30))
                    .await;
                assert!(report.complete, "{report:?}");
                Ok(())
            }
            Err(RuntimeError::StartupFailed { cause, .. }) => Err(cause),
            Err(other) => panic!("unexpected native refusal before log selection: {other}"),
        }
    })
}
fn assert_both_refuse(layout: &ClusterLayout, dir: &Path, case: &str, cause: &str) {
    assert_node_wide_refusal(&config(layout, 1, dir, SlotStartupPlan::default()), case);
    match native_start(layout, dir) {
        Err(native) => assert!(native.contains(cause), "{case}: {native}"),
        Ok(()) => panic!("{case}: native start accepted a directory the probe refused"),
    }
}
/// A directory a previous start created: manifest present (without it the
/// manifest layer refuses any existing data before the log is selected).
fn started_dir(layout: &ClusterLayout) -> PathBuf {
    let dir = directory();
    SlotManifest::open_or_create(&dir, layout, 1).unwrap();
    dir
}
/// RocksDB log engine plus marker; `written` groups get a stored vote, which
/// sets their `ever_written` basis.
fn rocks(dir: &Path, written: &[u64]) {
    use openraft::storage::RaftLogStorage;
    let db =
        multiraft_store::RocksLogDb::<multiraft_core::TypeConfig>::open(dir.join("raft-rocksdb"))
            .unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    for &group in written {
        let mut store = db.group(group).unwrap();
        runtime
            .block_on(store.save_vote(&multiraft_core::typ::Vote::new(4, 1)))
            .unwrap();
        assert_eq!(
            db.startup_provenance(group).unwrap(),
            StartupProvenance::Persisted
        );
    }
    drop(db);
    std::fs::write(dir.join("raft-log-backend"), b"rocksdb-v1\n").unwrap();
}
/// A valid, active native snapshot for `group`.
fn persisted_snapshot(dir: &Path, group: u64) {
    use openraft::{
        alias::{LeaderIdOf, LogIdOf, SnapshotMetaOf, StoredMembershipOf},
        vote::RaftLeaderIdExt,
        BasicNode, Membership,
    };
    type C = multiraft_core::TypeConfig;
    let members = std::collections::BTreeSet::from([1, 2, 3]);
    let nodes: BTreeMap<_, _> = members
        .iter()
        .map(|id| (*id, BasicNode::new("n")))
        .collect();
    let meta = SnapshotMetaOf::<C> {
        last_log_id: Some(LogIdOf::<C>::new(LeaderIdOf::<C>::new_committed(2, 1), 5)),
        last_membership: StoredMembershipOf::<C>::new(
            Some(LogIdOf::<C>::new(LeaderIdOf::<C>::new_committed(1, 1), 1)),
            Membership::new(vec![members], nodes).unwrap(),
        ),
        snapshot_id: "persisted".into(),
    };
    let catalog = multiraft_store::SnapshotCatalog::new(dir.join("snapshots"), 1);
    catalog
        .publish_native(group, &meta, b"image", 1024)
        .unwrap();
    assert_eq!(
        catalog.probe_provenance(group, 1024).unwrap(),
        StartupProvenance::Persisted
    );
}

const NO_BASIS: &str = "persisted FileLog or snapshot history";

#[test]
fn probe_refuses_history_the_native_opener_refuses_including_non_voter_groups() {
    let layout = layout();
    let voter_group = layout.group_for_slot(SlotId(0)).unwrap();
    let non_voter = 999;
    assert!(layout.slot_for_group(non_voter).is_err());
    for (case, with_rocks, group) in [
        ("voter FileLog vote, no RocksDB", false, voter_group),
        ("voter FileLog vote, RocksDB pristine", true, voter_group),
        ("non-voter FileLog vote, no RocksDB", false, non_voter),
        ("non-voter FileLog vote, RocksDB pristine", true, non_voter),
    ] {
        let dir = started_dir(&layout);
        if with_rocks {
            rocks(&dir, &[]);
        }
        legacy_vote(&dir, group);
        assert_both_refuse(&layout, &dir, case, NO_BASIS);
    }
    for (case, with_rocks) in [
        ("non-voter valid snapshot, no RocksDB", false),
        ("non-voter valid snapshot, RocksDB pristine", true),
    ] {
        let dir = started_dir(&layout);
        if with_rocks {
            rocks(&dir, &[]);
        }
        persisted_snapshot(&dir, non_voter);
        assert_both_refuse(&layout, &dir, case, NO_BASIS);
    }
    // Malformed non-voter namespaces fail closed in both.
    let dir = started_dir(&layout);
    std::fs::create_dir_all(dir.join(format!("group-{non_voter}"))).unwrap();
    std::fs::write(dir.join(format!("group-{non_voter}/mystery")), b"?").unwrap();
    assert_both_refuse(&layout, &dir, "malformed non-voter FileLog", "namespace");
    let dir = started_dir(&layout);
    std::fs::create_dir_all(dir.join(format!("snapshots/{non_voter}"))).unwrap();
    std::fs::write(dir.join(format!("snapshots/{non_voter}/mystery")), b"?").unwrap();
    assert_both_refuse(
        &layout,
        &dir,
        "malformed non-voter snapshot namespace",
        "namespace",
    );
}

#[test]
fn probe_accepts_exactly_what_the_native_opener_accepts() {
    let layout = layout();
    let non_voter = 999;
    // Native short-circuits on a persisted FileLog and never inspects the
    // snapshot namespace; with a RocksDB basis the directory starts.
    let dir = started_dir(&layout);
    rocks(&dir, &[non_voter]);
    legacy_vote(&dir, non_voter);
    std::fs::create_dir_all(dir.join(format!("snapshots/{non_voter}"))).unwrap();
    std::fs::write(dir.join(format!("snapshots/{non_voter}/mystery")), b"?").unwrap();
    let probed = probe_untouched(&config(&layout, 1, &dir, SlotStartupPlan::default()));
    assert!(all(&probed, StartupProvenance::Pristine), "{probed:?}");
    native_start(&layout, &dir).expect("native accepts persisted FileLog with a RocksDB basis");
    // Recognized empty non-voter history is accepted by both.
    let dir = started_dir(&layout);
    std::fs::create_dir_all(dir.join(format!("group-{non_voter}"))).unwrap();
    let probed = probe_untouched(&config(&layout, 1, &dir, SlotStartupPlan::default()));
    assert!(all(&probed, StartupProvenance::Pristine));
    native_start(&layout, &dir).expect("native accepts empty history");
}
