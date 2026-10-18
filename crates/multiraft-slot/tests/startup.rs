//! Native preference startup through `SlotNodeOwner` on a generated
//! RF3 / 12-slot layout over real gRPC.
use multiraft_fsm::{ApplyOut, CaptureError, StateMachine};
use multiraft_slot::*;
use std::{
    collections::BTreeMap,
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
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

/// Write once to every slot through `owner`, moving each counter to `expected`.
///
/// `Unavailable` is a slot-level refusal before dispatch and is retried. A
/// native `NotLeader` (OpenRaft `ForwardToLeader`) leaves the write outcome
/// unknown: the entry may already be in the log (leader step-down, or log
/// purge after a snapshot install). The counter is additive, so the write is
/// first settled by a linearizable read, which a leader serves only after
/// committing an entry of its own term; it is retried only if it did not land.
/// `Unknown` is never retried.
async fn write_every_slot(layout: &ClusterLayout, owner: &SlotNodeOwner<Counter>, expected: u64) {
    let handle = owner.handle();
    let until = Instant::now() + Duration::from_secs(20);
    for slot in 0..SLOTS {
        let k = key(layout, slot);
        loop {
            match handle.propose(&k, vec![1], until).await {
                Ok(applied) => {
                    assert_eq!(applied.slot, SlotId(slot));
                    assert_eq!(applied.effects, expected.to_be_bytes(), "slot {slot}");
                    break;
                }
                Err(e @ RuntimeError::Unavailable) => {
                    assert!(Instant::now() < until, "slot {slot} never served: {e}");
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(e @ RuntimeError::NotLeader { .. }) => {
                    assert!(Instant::now() < until, "slot {slot} never served: {e}");
                    match read_slot(&handle, &k, slot, until).await {
                        v if v == expected => break,
                        v if v + 1 == expected => {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                        v => panic!("slot {slot}: counter {v}, expected {expected}"),
                    }
                }
                Err(e) => panic!("slot {slot}: {e}"),
            }
        }
    }
}
/// Linearizable counter value of one slot, retrying OpenRaft's normal
/// transient read errors (no leader yet, stale leader, quorum not reached).
async fn read_slot(handle: &SlotHandle<Counter>, k: &[u8], slot: u32, until: Instant) -> u64 {
    loop {
        match handle.read_linearizable(k, &[], until).await {
            Ok(value) => {
                return u64::from_be_bytes(value.as_slice().try_into().expect("counter bytes"))
            }
            Err(e @ (RuntimeError::NotLeader { .. } | RuntimeError::Unavailable)) => {
                assert!(Instant::now() < until, "slot {slot} never served: {e}");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(e) => panic!("slot {slot}: {e}"),
        }
    }
}
async fn read_every_slot(layout: &ClusterLayout, owner: &SlotNodeOwner<Counter>, expected: u64) {
    let handle = owner.handle();
    let until = Instant::now() + Duration::from_secs(20);
    for slot in 0..SLOTS {
        let k = key(layout, slot);
        assert_eq!(
            read_slot(&handle, &k, slot, until).await,
            expected,
            "slot {slot}"
        );
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

#[test]
fn plan_validation_rejects_bad_grace_and_budget_before_creating_state() {
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
    ] {
        let dir = directory();
        let result = runtime.block_on(SlotNodeOwner::start(
            config(&layout, 1, &dir, plan.clone()),
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
        write_every_slot(&layout, &owners[&order[2]], 1).await;
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
    write_every_slot(&layout, &owners[&1], 1).await;
    // The late preferred initializer joins without disturbing formed groups.
    let late = try_start(&layout, 3, &dirs[&3], grace(200)).await.unwrap();
    owners.insert(3, late);
    read_every_slot(&layout, &owners[&3], 1).await;
    write_every_slot(&layout, &owners[&3], 2).await;
    read_every_slot(&layout, &owners[&2], 2).await;
    shutdown_all(owners).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cold_restart_recovers_persisted_slots_without_initialize() {
    let layout = layout();
    let dirs: BTreeMap<_, _> = (1..=3).map(|n| (n, directory())).collect();
    let owners = start_staggered(&layout, &dirs, &[1, 2, 3], Duration::ZERO, grace(200)).await;
    write_every_slot(&layout, &owners[&1], 1).await;
    shutdown_all(owners).await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_startup_reports_the_native_cause_and_releases_the_node() {
    // Alone, node 1 waits out the native grace for the slots it does not
    // prefer; a recovery budget shorter than the grace expires first.
    let layout = layout();
    let dir = directory();
    let short = |budget_ms| SlotStartupPlan {
        recovery_timeout: Duration::from_millis(budget_ms),
        ..grace(2000)
    };
    let started = Instant::now();
    let error = try_start(&layout, 1, &dir, short(300)).await.err().unwrap();
    assert!(started.elapsed() < Duration::from_secs(20));
    match &error {
        RuntimeError::StartupFailed {
            slot, phase, cause, ..
        } => {
            assert!(slot.is_some(), "{error}");
            assert!(!phase.is_empty(), "{error}");
            assert!(cause.contains("deadline expired"), "{error}");
        }
        other => panic!("unexpected startup error: {other}"),
    }
    // The first failed start released its listener and database lock: a
    // leaked resource would make this second start fail inside
    // `NodeOwner::start` instead of with the same native deadline failure.
    let again = try_start(&layout, 1, &dir, short(300)).await;
    assert!(
        matches!(again, Err(RuntimeError::StartupFailed { .. })),
        "{:?}",
        again.err()
    );
}
