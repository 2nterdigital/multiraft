//! Public legacy Standby capture remains owned through stop/canceled wait and IO.
use multiraft_core::{ClusterConfig, FileLogSyncLevel, NodeRole, SnapshotMode};
use multiraft_fsm::{ApplyOut, CounterFsm, GroupId, StateMachine};
use multiraft_net::MultiRaft;
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::Notify;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::{layer::Context, prelude::*, Layer};
const GROUP: u64 = 9;
static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
#[derive(Clone, Default)]
struct Gate {
    entered: Arc<Notify>,
    released: Arc<(Mutex<bool>, Condvar)>,
    frozen: Arc<Notify>,
}
impl Gate {
    fn freeze(&self) {
        self.entered.notify_one();
        let (lock, changed) = &*self.released;
        let (open, timeout) = changed
            .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(5), |open| !*open)
            .unwrap();
        assert!(
            *open && !timeout.timed_out(),
            "capture fixture must be released"
        );
        self.frozen.notify_one();
    }
    fn release(&self) {
        *self.released.0.lock().unwrap() = true;
        self.released.1.notify_all();
    }
}
struct Consumer {
    counter: CounterFsm,
    lease: Option<PathBuf>,
    gate: Option<Gate>,
    failure: Option<String>,
}
impl StateMachine for Consumer {
    type Error = std::io::Error;
    fn apply(&mut self, group: GroupId, index: u64, data: &[u8]) -> Result<ApplyOut, Self::Error> {
        self.counter
            .apply(group, index, data)
            .map_err(std::io::Error::other)
    }
    fn snapshot(&self, group: GroupId) -> Result<Vec<u8>, Self::Error> {
        self.counter.snapshot(group).map_err(std::io::Error::other)
    }
    fn restore(&mut self, group: GroupId, data: &[u8]) -> Result<(), Self::Error> {
        self.counter
            .restore(group, data)
            .map_err(std::io::Error::other)
    }
    fn freeze_for_snapshot(&self, group: GroupId) -> Result<Vec<u8>, Self::Error> {
        if let Some(gate) = &self.gate {
            gate.freeze();
        }
        if let Some(failure) = &self.failure {
            return Err(std::io::Error::other(failure.clone()));
        }
        self.snapshot(group)
    }
}
impl Drop for Consumer {
    fn drop(&mut self) {
        if let Some(path) = &self.lease {
            fs::remove_file(path).unwrap();
        }
    }
}
fn address() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}
fn config(id: u64, addresses: [SocketAddr; 2], root: &Path, role: NodeRole) -> ClusterConfig {
    let mut config = ClusterConfig::for_test(id, &[1, 2]);
    config.peers = vec![(1, addresses[0]), (2, addresses[1])];
    config.data_dir = root.join(format!("node-{id}"));
    config.file_log_sync_level = FileLogSyncLevel::Data;
    config.role = role;
    config.snapshot_mode = SnapshotMode::StandbyOffload;
    config
}
async fn cluster(
    root: &Path,
    gate: Option<Gate>,
    failure: Option<String>,
) -> (
    Arc<MultiRaft<Consumer>>,
    Arc<MultiRaft<Consumer>>,
    [SocketAddr; 2],
    PathBuf,
) {
    let addresses = [address(), address()];
    let lease = root.join("standby.lease");
    let standby_lease = lease.clone();
    let voter = Arc::new(
        MultiRaft::start_grpc_with_factory(config(1, addresses, root, NodeRole::Voter), |_| {
            Ok(Consumer {
                counter: CounterFsm::new(),
                lease: None,
                gate: None,
                failure: None,
            })
        })
        .await
        .unwrap(),
    );
    let standby = Arc::new(
        MultiRaft::start_grpc_with_factory(
            config(2, addresses, root, NodeRole::Standby),
            move |_| {
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&standby_lease)?;
                Ok(Consumer {
                    counter: CounterFsm::new(),
                    lease: Some(standby_lease.clone()),
                    gate: gate.clone(),
                    failure: failure.clone(),
                })
            },
        )
        .await
        .unwrap(),
    );
    voter.create_group(GROUP, &[1]).await.unwrap();
    standby.create_group(GROUP, &[1]).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !voter.is_leader(GROUP) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    voter.add_standby(GROUP, 2).await.unwrap();
    (voter, standby, addresses, lease)
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn standby_capture_and_blocking_catalog_write_outlive_cancelled_shutdown_waiter() {
    let _lock = LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let gate = Gate::default();
    let (voter, standby, addresses, lease) = cluster(root.path(), Some(gate.clone()), None).await;
    standby.set_snapshot_serialize_delay(Some(Duration::from_millis(250)));
    voter.trigger_standby_snapshot(GROUP).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .unwrap();
    let stopping = standby.clone();
    let mut first = tokio::spawn(async move { stopping.shutdown().await });
    assert!(tokio::time::timeout(Duration::from_millis(30), &mut first)
        .await
        .is_err());
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let stopping = standby.clone();
    let mut second = tokio::spawn(async move { stopping.shutdown().await });
    assert!(tokio::time::timeout(Duration::from_millis(30), &mut second)
        .await
        .is_err());
    assert!(lease.exists());
    gate.release();
    tokio::time::timeout(Duration::from_secs(2), gate.frozen.notified())
        .await
        .unwrap();
    // The public existing serialize delay keeps the blocking writer active after
    // the consumer freeze returns; no private probe or production timing change.
    assert!(tokio::time::timeout(Duration::from_millis(40), &mut second)
        .await
        .is_err());
    assert!(lease.exists());
    tokio::time::timeout(Duration::from_secs(3), second)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(standby.latest_catalog_entry(GROUP).is_some());
    assert!(!standby.snapshot_ads().is_empty());
    assert!(!lease.exists());
    assert!(
        matches!(
            standby
                .read_group_control_sample(
                    GROUP,
                    &[1],
                    Duration::from_secs(1),
                    Duration::from_secs(1)
                )
                .await,
            Err(multiraft_net::GroupControlSampleError::Closed { group_id: GROUP })
        ),
        "closed control intake rejects later operations"
    );
    voter.shutdown().await.unwrap();
    for address in addresses {
        TcpListener::bind(address).expect("both original listener resources released");
    }
}
#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<BTreeMap<String, String>>>>);
#[derive(Default)]
struct Fields(BTreeMap<String, String>);
impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().into(), format!("{value:?}"));
    }
}
impl<S: Subscriber> Layer<S> for Capture {
    fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
        if event.metadata().target() != "multiraft::recovery" {
            return;
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        self.0.lock().unwrap().push(fields.0);
    }
}
fn capture() -> Capture {
    static SOURCE: OnceLock<Capture> = OnceLock::new();
    SOURCE
        .get_or_init(|| {
            let capture = Capture::default();
            tracing::subscriber::set_global_default(
                tracing_subscriber::registry().with(capture.clone()),
            )
            .unwrap();
            capture
        })
        .clone()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn standby_application_failure_keeps_original_chain_but_only_finite_source_log() {
    let _lock = LOCK.lock().await;
    let capture = capture();
    let root = tempfile::tempdir().unwrap();
    let secret = format!(
        "STANDBY_SECRET_SENTINEL authorization Bearer fake-token payload={}",
        "private-snapshot-body".repeat(512)
    );
    let (voter, standby, addresses, lease) = cluster(root.path(), None, Some(secret.clone())).await;
    voter.trigger_standby_snapshot(GROUP).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if capture.0.lock().unwrap().iter().any(|event| {
                event.get("operation").map(String::as_str) == Some("standby_snapshot")
                    && event.get("phase").map(String::as_str) == Some("error")
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let error = standby
        .shutdown()
        .await
        .expect_err("retained snapshot task failure remains observable");
    let multiraft_core::MultiRaftError::Other(source) = error else {
        panic!("opaque original application-derived IO chain retained")
    };
    assert!(source.root_cause().to_string().contains(&secret));
    {
        let events = capture.0.lock().unwrap();
        let failure = events
            .iter()
            .find(|event| {
                event.get("operation").map(String::as_str) == Some("standby_snapshot")
                    && event.get("phase").map(String::as_str) == Some("error")
            })
            .unwrap();
        assert_eq!(
            failure.get("reason_code").map(String::as_str),
            Some("capture_or_write_failed")
        );
        assert!(!failure.contains_key("error"));
        assert!(!failure.contains_key("error_debug"));
        assert!(events
            .iter()
            .flat_map(|event| event.values())
            .all(|value| !value.contains("STANDBY_SECRET_SENTINEL")
                && !value.contains("fake-token")
                && !value.contains("private-snapshot-body")));
    }
    assert!(!lease.exists());
    voter.shutdown().await.unwrap();
    for address in addresses {
        TcpListener::bind(address).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_owned_standby_reclaims_capture_fsm_lease_and_listener_after_actual_job() {
    use multiraft_net::{GroupConfig, NodeOwner, RuntimeConfig, RuntimeError};
    let _lock = LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let addresses = [address(), address()];
    let gate = Gate::default();
    let voter = MultiRaft::start_grpc_with_factory(
        config(1, addresses, root.path(), NodeRole::Voter),
        |_| {
            Ok(Consumer {
                counter: CounterFsm::new(),
                lease: None,
                gate: None,
                failure: None,
            })
        },
    )
    .await
    .unwrap();
    voter.create_group(GROUP, &[1]).await.unwrap();
    let lease = root.path().join("owned-standby.lease");
    let factory_lease = lease.clone();
    let factory_gate = gate.clone();
    let owner = NodeOwner::start(
        RuntimeConfig::new(
            config(2, addresses, root.path(), NodeRole::Standby),
            vec![GroupConfig {
                group_id: GROUP,
                voters: vec![1],
            }],
        ),
        move |_| {
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&factory_lease)?;
            Ok(Consumer {
                counter: CounterFsm::new(),
                lease: Some(factory_lease.clone()),
                gate: Some(factory_gate.clone()),
                failure: None,
            })
        },
        tokio::time::Instant::now() + Duration::from_secs(3),
    )
    .await
    .unwrap();
    let handle = owner.handle();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !voter.is_leader(GROUP) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    voter.add_standby(GROUP, 2).await.unwrap();
    voter.trigger_standby_snapshot(GROUP).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .unwrap();
    drop(owner);
    assert!(matches!(
        handle
            .local_group_status(GROUP, tokio::time::Instant::now() + Duration::from_secs(1))
            .await,
        Err(RuntimeError::Closed)
    ));
    assert!(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lease)
            .is_err(),
        "Drop cannot release a live consumer lease early"
    );
    gate.release();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if !lease.exists() && TcpListener::bind(addresses[1]).is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    voter.shutdown().await.unwrap();
    for address in addresses {
        TcpListener::bind(address).unwrap();
    }
}
