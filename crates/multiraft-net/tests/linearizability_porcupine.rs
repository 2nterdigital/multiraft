//! In-process porcupine linearizability check under leader kill (Jepsen-adjacent).
//!
//! Run with:
//! `cargo test -p multiraft-net --test linearizability_porcupine -- --nocapture`
//!
//! Failure diagnostics: this test has no RNG seed to expose — nondeterminism
//! comes from real scheduling and timers — so on a non-linearizable verdict it
//! dumps the complete operation history, the swallowed-error timeline, the
//! leader-kill instant, and porcupine's longest partial linearization to a
//! JSONL file for offline analysis.
//!
//! Environment knobs (defaults preserve the historical scenario exactly):
//! - `PORCUPINE_WORKLOAD_MS`  total workload duration (default 3000)
//! - `PORCUPINE_CLIENTS`      concurrent client tasks (default 8)
//! - `PORCUPINE_KILL_MS`      leader-kill instant from start (default 1500)
//! - `PORCUPINE_DUMP_DIR`     dump directory (default: std::env::temp_dir())
//! - `PORCUPINE_DUMP_ALWAYS`  set (any value) to dump even on a passing run

use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use multiraft_core::ClusterConfig;
use multiraft_fsm::CounterFsm;
use multiraft_net::wait_for_leader;
use multiraft_net::MultiRaft;
use porcupine_rs::CheckResult;
use porcupine_rs::Model;
use porcupine_rs::Operation;

#[derive(Clone, Debug)]
enum CounterOp {
    /// Successful propose(+delta).
    Inc { delta: i64 },
    /// Successful read_linearizable observed value.
    Read(i64),
}

#[derive(Clone, Debug)]
struct CounterModel;

impl Model for CounterModel {
    type State = i64;
    type Op = CounterOp;
    type Metadata = ();

    fn init() -> i64 {
        0
    }

    fn step(state: &i64, op: &CounterOp) -> (bool, i64) {
        match op {
            CounterOp::Inc { delta } => (true, state + delta),
            CounterOp::Read(v) => (*v == *state, *state),
        }
    }
}

fn now_ns(start: Instant) -> i64 {
    start.elapsed().as_nanos() as i64
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Bounded timeline of swallowed (retried) client errors. NotLeader and
/// UnknownGroup churn during failover is the interesting signal, so every
/// swallowed error is recorded with its history-relative timestamp; the cap
/// bounds memory and the drop counter keeps the loss visible.
const DIAG_CAP: usize = 20_000;

struct Diag {
    lines: Mutex<Vec<String>>,
    dropped: AtomicU64,
}

impl Diag {
    fn new() -> Self {
        Self {
            lines: Mutex::new(Vec::new()),
            dropped: AtomicU64::new(0),
        }
    }

    fn push(&self, line: String) {
        let mut lines = self.lines.lock().unwrap();
        if lines.len() < DIAG_CAP {
            lines.push(line);
        } else {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Propose +1, retrying across local nodes until success.
///
/// The recorded operation window opens at the FIRST attempt's start, not the
/// successful attempt's: an errored propose may still have committed (the
/// leader can die after commit+apply but before the ack) and the retry then
/// dedupes on `idem`, so the true effect can precede the last attempt.
/// Recording only the last attempt's window shifted the window past the real
/// commit point and produced false Illegal verdicts whenever another client
/// legitimately read the committed value in between.
async fn propose_inc_ok(
    nodes: &[MultiRaft],
    group: u64,
    idem: u64,
    client_id: u32,
    t0: Instant,
    history: &Mutex<Vec<Operation<CounterModel>>>,
    diag: &Diag,
) {
    let data = CounterFsm::encode_add(1, idem);
    let deadline = Instant::now() + Duration::from_secs(30);
    // Ambiguous-effect window: opened once, before the first attempt.
    let call = now_ns(t0);
    loop {
        // Prefer current leader, then try everyone (NotLeader race / failover).
        let mut order: Vec<&MultiRaft> = Vec::with_capacity(nodes.len());
        for n in nodes {
            if n.is_leader(group) {
                order.push(n);
            }
        }
        for n in nodes {
            if !n.is_leader(group) {
                order.push(n);
            }
        }

        for n in order {
            match n.propose(group, data.clone()).await {
                Ok(_) => {
                    let ret = now_ns(t0);
                    history.lock().unwrap().push(Operation {
                        client_id: Some(client_id),
                        call_time: call,
                        return_time: ret,
                        op: CounterOp::Inc { delta: 1 },
                        metadata: None,
                    });
                    return;
                }
                Err(err) => {
                    diag.push(format!(
                        "t_ns={} client={client_id} node={} op=propose err={err:?}",
                        now_ns(t0),
                        n.node_id(),
                    ));
                }
            }
        }

        if Instant::now() >= deadline {
            panic!("client {client_id}: propose timed out");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Linearizable read, retrying across local nodes until success.
async fn read_ok(
    nodes: &[MultiRaft],
    group: u64,
    client_id: u32,
    t0: Instant,
    history: &Mutex<Vec<Operation<CounterModel>>>,
    diag: &Diag,
) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut order: Vec<&MultiRaft> = Vec::with_capacity(nodes.len());
        for n in nodes {
            if n.is_leader(group) {
                order.push(n);
            }
        }
        for n in nodes {
            if !n.is_leader(group) {
                order.push(n);
            }
        }

        for n in order {
            let call = now_ns(t0);
            match n.read_linearizable(group, |fsm| fsm.value(group)).await {
                Ok(v) => {
                    let ret = now_ns(t0);
                    history.lock().unwrap().push(Operation {
                        client_id: Some(client_id),
                        call_time: call,
                        return_time: ret,
                        op: CounterOp::Read(v),
                        metadata: None,
                    });
                    return;
                }
                Err(err) => {
                    diag.push(format!(
                        "t_ns={} client={client_id} node={} op=read err={err:?}",
                        now_ns(t0),
                        n.node_id(),
                    ));
                }
            }
        }

        if Instant::now() >= deadline {
            panic!("client {client_id}: read_linearizable timed out");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Writes the complete diagnosis report as JSONL: one `meta` line, one `op`
/// line per recorded operation (`i` matches porcupine's `op_index`), the
/// longest partial linearization porcupine found, then the swallowed-error
/// timeline. Returns the dump path.
#[allow(clippy::too_many_arguments)]
fn dump_report(
    dir: &std::path::Path,
    verdict: &str,
    ops: &[Operation<CounterModel>],
    diag: &Diag,
    killed: Option<u64>,
    kill_t_ns: i64,
    workload_ms: u64,
    n_clients: u32,
    longest: Option<&Vec<usize>>,
) -> std::io::Result<PathBuf> {
    use std::io::Write as _;

    std::fs::create_dir_all(dir)?;
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let path = dir.join(format!(
        "porcupine-history-{}-{stamp}.jsonl",
        std::process::id()
    ));
    let mut file = std::fs::File::create(&path)?;

    let n_inc = ops
        .iter()
        .filter(|o| matches!(o.op, CounterOp::Inc { .. }))
        .count();
    let n_read = ops
        .iter()
        .filter(|o| matches!(o.op, CounterOp::Read(_)))
        .count();
    let diag_lines = diag.lines.lock().unwrap();
    writeln!(
        file,
        "{}",
        serde_json::json!({
            "kind": "meta",
            "verdict": verdict,
            "ops": ops.len(),
            "inc": n_inc,
            "read": n_read,
            "killed_leader": killed,
            "kill_t_ns": kill_t_ns,
            "workload_ms": workload_ms,
            "clients": n_clients,
            "diag_lines": diag_lines.len(),
            "diag_dropped": diag.dropped.load(Ordering::Relaxed),
        })
    )?;
    for (i, o) in ops.iter().enumerate() {
        let (kind, value) = match o.op {
            CounterOp::Inc { delta } => ("inc", delta),
            CounterOp::Read(v) => ("read", v),
        };
        writeln!(
            file,
            "{}",
            serde_json::json!({
                "kind": "op",
                "i": i,
                "client": o.client_id,
                "call_ns": o.call_time,
                "ret_ns": o.return_time,
                "op": kind,
                "value": value,
            })
        )?;
    }
    if let Some(seq) = longest {
        writeln!(
            file,
            "{}",
            serde_json::json!({
                "kind": "longest_partial_linearization",
                "len": seq.len(),
                "op_indexes": seq,
            })
        )?;
    }
    for line in diag_lines.iter() {
        writeln!(
            file,
            "{}",
            serde_json::json!({ "kind": "diag", "line": line })
        )?;
    }
    file.sync_all()?;
    Ok(path)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn porcupine_counter_under_leader_kill() {
    let workload_ms = env_u64("PORCUPINE_WORKLOAD_MS", 3000);
    let n_clients = env_u64("PORCUPINE_CLIENTS", 8) as u32;
    let kill_ms = env_u64("PORCUPINE_KILL_MS", 1500);
    let dump_always = std::env::var("PORCUPINE_DUMP_ALWAYS").is_ok();
    let dump_dir = std::env::var("PORCUPINE_DUMP_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir());

    let peer_ids = [1u64, 2, 3];
    let configs: Vec<_> = peer_ids
        .iter()
        .map(|&id| ClusterConfig::for_test(id, &peer_ids))
        .collect();
    let nodes = Arc::new(
        MultiRaft::start_cluster(configs)
            .await
            .expect("start_cluster"),
    );
    let group = 1u64;
    for n in nodes.iter() {
        n.create_group(group, &peer_ids)
            .await
            .expect("create_group");
    }
    let _ = wait_for_leader(&nodes, group, Duration::from_secs(10))
        .await
        .expect("initial leader");

    let history: Arc<Mutex<Vec<Operation<CounterModel>>>> = Arc::new(Mutex::new(Vec::new()));
    let diag = Arc::new(Diag::new());
    let idem = Arc::new(AtomicU64::new(1));
    let stop = Arc::new(AtomicBool::new(false));
    let t0 = Instant::now();
    let workload = Duration::from_millis(workload_ms);
    let kill_at = Duration::from_millis(kill_ms);

    let clients: Vec<_> = (0..n_clients)
        .map(|cid| {
            let nodes = Arc::clone(&nodes);
            let history = Arc::clone(&history);
            let diag = Arc::clone(&diag);
            let idem = Arc::clone(&idem);
            let stop = Arc::clone(&stop);
            tokio::spawn(async move {
                let mut next_write = cid % 2 == 0;
                while !stop.load(Ordering::Relaxed) {
                    if next_write {
                        let id = idem.fetch_add(1, Ordering::Relaxed);
                        propose_inc_ok(&nodes, group, id, cid, t0, &history, &diag).await;
                    } else {
                        read_ok(&nodes, group, cid, t0, &history, &diag).await;
                    }
                    next_write = !next_write;
                }
            })
        })
        .collect();

    // Mid-test chaos: shut down the current leader once.
    tokio::time::sleep(kill_at).await;
    let mut killed = None;
    let mut kill_t_ns = 0i64;
    for n in nodes.iter() {
        if n.is_leader(group) {
            let id = n.node_id();
            kill_t_ns = now_ns(t0);
            eprintln!("porcupine: shutting down leader node {id} at t_ns={kill_t_ns}");
            n.shutdown().await.expect("shutdown leader");
            killed = Some(id);
            break;
        }
    }
    assert!(killed.is_some(), "expected a leader to kill mid-test");

    tokio::time::sleep(workload.saturating_sub(kill_at)).await;
    stop.store(true, Ordering::Relaxed);

    for h in clients {
        h.await.expect("client join");
    }

    let ops = history.lock().unwrap().clone();
    let n_inc = ops
        .iter()
        .filter(|o| matches!(o.op, CounterOp::Inc { .. }))
        .count();
    let n_read = ops
        .iter()
        .filter(|o| matches!(o.op, CounterOp::Read(_)))
        .count();
    eprintln!(
        "porcupine: history ops={} (inc={n_inc} read={n_read}) killed_leader={killed:?}",
        ops.len()
    );
    assert!(n_inc > 0, "expected some successful Inc ops");
    assert!(n_read > 0, "expected some successful Read ops");

    let (verdict, info) = porcupine_rs::check_operations_info::<CounterModel>(&ops);
    let ok = verdict == CheckResult::Ok;
    let verdict_str = match verdict {
        CheckResult::Ok => "ok",
        CheckResult::Illegal => "illegal",
        CheckResult::Unknown => "unknown",
    };
    if !ok || dump_always {
        let longest = info
            .partial_linearizations
            .iter()
            .flatten()
            .max_by_key(|seq| seq.len());
        match dump_report(
            &dump_dir,
            verdict_str,
            &ops,
            &diag,
            killed,
            kill_t_ns,
            workload_ms,
            n_clients,
            longest,
        ) {
            Ok(path) => {
                eprintln!(
                    "porcupine: verdict={verdict_str}; history dumped to {}",
                    path.display()
                );
            }
            Err(err) => {
                eprintln!("porcupine: verdict={verdict_str}; FAILED to dump history: {err}");
            }
        }
    }
    assert!(
        ok,
        "history is not linearizable (verdict={verdict_str}, {} ops); \
         see the porcupine-history-*.jsonl dump under {}",
        ops.len(),
        dump_dir.display()
    );
}
