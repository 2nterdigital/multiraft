//! Local-only acceptance server for the fixed-slot foundation.
mod fsm;
use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use fsm::{Command, DemoFsm, DiskImage, Gate, GateState, Image};
use multiraft_slot::{
    ClusterLayout, LayoutConfig, SlotHandle, SlotId, SlotNodeConfig, SlotNodeOwner,
};
use multiraft_store::DiskFsmDb;
use serde::Deserialize;
use serde_json::{json, Value};
use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio::time::Instant;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    layout: PathBuf,
    #[arg(long)]
    node_id: u64,
    #[arg(long)]
    data_dir: PathBuf,
    #[arg(long)]
    admin: SocketAddr,
    #[arg(long)]
    test_controls: bool,
    #[arg(long, default_value_t = 1)]
    per_slot_requests: usize,
    #[arg(long, default_value_t = 16)]
    global_requests: usize,
    /// `image` keeps whole-image snapshots; `disk` keeps state in per-Slot RocksDB.
    #[arg(long, value_enum, default_value_t = FsmStorage::Image)]
    fsm_storage: FsmStorage,
}
#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum FsmStorage {
    Image,
    Disk,
}
#[derive(Clone)]
struct App {
    handle: SlotHandle<DemoFsm>,
    gate: Arc<Gate>,
    controls: bool,
    disk: bool,
}
#[derive(Deserialize)]
struct Write {
    key: String,
    command_id: String,
    delta: i64,
    #[serde(default = "default_timeout")]
    timeout_ms: u64,
}
fn default_timeout() -> u64 {
    5000
}
fn deadline(ms: u64) -> Instant {
    Instant::now() + Duration::from_millis(ms.min(30_000))
}
#[derive(Deserialize)]
struct Read {
    key: String,
    #[serde(default)]
    local: bool,
    #[serde(default = "default_timeout")]
    timeout_ms: u64,
}
#[derive(Deserialize)]
struct Slot {
    slot: u32,
}
#[derive(Deserialize)]
struct GateRequest {
    command_id: Option<String>,
    #[serde(default)]
    release: bool,
}
fn error(error: impl std::fmt::Debug) -> Value {
    json!({"ok":false,"error":format!("{error:?}")})
}
async fn write(State(app): State<App>, Json(request): Json<Write>) -> Json<Value> {
    let command = Command {
        key: request.key.clone(),
        command_id: request.command_id,
        delta: request.delta,
    };
    let result = match serde_json::to_vec(&command) {
        Ok(command) => match app
            .handle
            .propose(
                request.key.as_bytes(),
                command,
                deadline(request.timeout_ms),
            )
            .await
        {
            Ok(applied) => {
                json!({"ok":true,"slot":applied.slot.0,"index":applied.index,"term":applied.term,"effects":serde_json::from_slice::<Value>(&applied.effects).ok()})
            }
            Err(e) => error(e),
        },
        Err(e) => error(e),
    };
    Json(result)
}
async fn read(State(app): State<App>, Json(request): Json<Read>) -> Json<Value> {
    let query = serde_json::to_vec(&request.key).expect("string JSON");
    if request.local {
        let slot = match app.handle.layout().slot_for_key(request.key.as_bytes()) {
            Ok(slot) => slot,
            Err(_) => return Json(error("invalid key")),
        };
        return Json(
            match app
                .handle
                .read_applied(slot, &query, deadline(request.timeout_ms))
                .await
            {
                Ok(view) => {
                    json!({"ok":true,"result":serde_json::from_slice::<Value>(&view.value).ok(),"applied":view.applied.map(|id| json!({"term":id.term,"node_id":id.node_id,"index":id.index}))})
                }
                Err(e) => error(e),
            },
        );
    }
    Json(
        match app
            .handle
            .read_linearizable_with_applied(
                request.key.as_bytes(),
                &query,
                deadline(request.timeout_ms),
            )
            .await
        {
            Ok(view) => {
                json!({"ok":true,"result":serde_json::from_slice::<Value>(&view.value).ok(),"applied":{"term":view.applied.term,"node_id":view.applied.node_id,"index":view.applied.index}})
            }
            Err(e) => error(e),
        },
    )
}

async fn status(State(app): State<App>) -> Json<Value> {
    let mut slots = Vec::new();
    for slot in 0..app.handle.layout().slot_count() {
        let group = app
            .handle
            .local_group_status(SlotId(slot), deadline(1000))
            .await;
        let storage = app
            .handle
            .local_storage_status(SlotId(slot), deadline(1000))
            .await;
        slots.push(json!({"slot":slot,"group":group.as_ref().ok().map(|g| json!({"leader":g.leader_hint,"term":g.current_term,"last_log_index":g.last_log_index,"last_applied":format!("{:?}",g.last_applied)})),"storage":storage.as_ref().ok().map(|s| json!({"backend":format!("{:?}",s.log_storage_backend),"mode":format!("{:?}",s.mode),"snapshot":s.durable_snapshot.as_ref().map(|d|json!({"id":d.snapshot_id,"bytes":d.bytes,"last_log_id":format!("{:?}",d.last_log_id)})),"purged":format!("{:?}",s.purged),"progress":format!("{:?}",s.progress)}))}));
    }
    let inflight = app.handle.inflight();
    let gate = app.gate.state.lock().expect("gate mutex");
    Json(
        json!({"ok":true,"node_id":app.handle.node_id(),"fsm_storage":if app.disk {"disk"} else {"image"},"digest":app.handle.layout().digest().0,"route_digest":app.handle.layout().route_digest().0,"slots":slots,"inflight":{"global":inflight.global.requests,"slots":inflight.slots.iter().map(|s|s.requests).collect::<Vec<_>>()},"gate":{"entered":gate.entered,"released":gate.released,"command_id":gate.command_id}}),
    )
}
async fn compact(State(app): State<App>, Json(request): Json<Slot>) -> Json<Value> {
    Json(
        match app
            .handle
            .request_compaction(SlotId(request.slot), deadline(5000))
            .await
        {
            Ok(s) => json!({"ok":true,"submission":format!("{s:?}")}),
            Err(e) => error(e),
        },
    )
}
async fn gate(State(app): State<App>, Json(request): Json<GateRequest>) -> Json<Value> {
    if !app.controls {
        return Json(error("test controls disabled"));
    }
    let mut state = app.gate.state.lock().expect("gate mutex");
    if request.release {
        state.released = true;
        app.gate.changed.notify_all();
    } else if state.command_id.is_some() {
        return Json(error("gate already armed"));
    } else if let Some(id) = request.command_id {
        *state = GateState {
            command_id: Some(id),
            entered: false,
            released: false,
        };
    } else {
        return Json(error("command_id required"));
    }
    Json(json!({"ok":true}))
}
#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    anyhow::ensure!(
        args.admin.ip().is_loopback(),
        "demo admin must bind loopback"
    );
    let config = LayoutConfig::from_json(&std::fs::read(args.layout)?)?;
    let layout = ClusterLayout::new(config)?;
    let disk = args.fsm_storage == FsmStorage::Disk;
    let data_dir = args.data_dir.clone();
    let mut config = SlotNodeConfig::new(layout.clone(), args.node_id, args.data_dir);
    config.limits.per_slot_requests = args.per_slot_requests;
    config.limits.global_requests = args.global_requests;
    config.native.install_snapshot_timeout_ms = 5000;
    config.native.retain_log_entries = 0;
    let gate_control = Arc::new(Gate::default());
    let factory_gate = gate_control.clone();
    let owner = SlotNodeOwner::start(
        config,
        move |context: multiraft_net::FsmFactoryContext| {
            let group = context.group_id();
            Ok(DemoFsm {
                image: Image {
                    group,
                    ..Image::default()
                },
                layout: layout.clone(),
                gate: factory_gate.clone(),
                disk: if disk {
                    Some(DiskImage::open(
                        &DiskFsmDb::group_path(&data_dir, group),
                        group,
                    )?)
                } else {
                    None
                },
            })
        },
        deadline(30000),
    )
    .await?;
    let app = App {
        handle: owner.handle(),
        gate: gate_control.clone(),
        controls: args.test_controls,
        disk,
    };
    let router = Router::new()
        .route("/status", get(status))
        .route("/write", post(write))
        .route("/read", post(read))
        .route("/compact", post(compact))
        .route("/gate", post(gate))
        .with_state(app);
    let listener = tokio::net::TcpListener::bind(args.admin).await?;
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            #[cfg(unix)]
            {
                let mut term =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                        .expect("SIGTERM handler");
                tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
            }
            #[cfg(not(unix))]
            {
                let _ = tokio::signal::ctrl_c().await;
            }
            if let Ok(mut state) = gate_control.state.lock() {
                state.released = true;
                gate_control.changed.notify_all();
            }
        })
        .await?;
    let report = owner.shutdown(deadline(30000)).await;
    println!(
        "{}",
        json!({"shutdown":{"complete":report.complete,"remaining":report.remaining.global.requests,"unknown_writes":report.unknown_writes,"error":format!("{:?}",report.error)}})
    );
    anyhow::ensure!(
        report.complete && report.remaining.global.requests == 0,
        "incomplete shutdown: {report:?}"
    );
    Ok(())
}
