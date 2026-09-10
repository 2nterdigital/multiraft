//! Test-only process control. Production replication remains the existing native gRPC path.
#[path = "../native_lab_support/mod.rs"]
mod lab;
pub use lab::require_lab;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

pub static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[derive(Serialize, Deserialize)]
pub struct ChildConfig {
    pub id: u64,
    pub peers: Vec<(u64, String)>,
    pub root: PathBuf,
}

pub struct Process {
    child: Child,
    input: BufWriter<ChildStdin>,
    replies: Receiver<Value>,
    reader: Option<std::thread::JoinHandle<()>>,
    running: bool,
}
impl Process {
    fn start(config: ChildConfig, evidence: &Path, epoch: usize) -> Self {
        std::fs::create_dir_all(&config.root).unwrap();
        let prefix = format!("node-{}-epoch-{epoch}", config.id);
        let stderr = std::fs::File::create(evidence.join(format!("{prefix}.stderr.log"))).unwrap();
        let stdout = std::fs::File::create(evidence.join(format!("{prefix}.stdout.log"))).unwrap();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--ignored", "--exact", "rf3_process_node", "--nocapture"])
            .env(
                "NATIVE_RF3_NODE_CONFIG",
                serde_json::to_string(&config).unwrap(),
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(stderr))
            .spawn()
            .unwrap();
        let output = child.stdout.take().unwrap();
        let input = BufWriter::new(child.stdin.take().unwrap());
        let (sender, replies) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            let mut log = BufWriter::new(stdout);
            for line in BufReader::new(output).lines() {
                let line = line.unwrap();
                writeln!(log, "{line}").unwrap();
                log.flush().unwrap();
                if let Some(reply) = line.strip_prefix("NATIVE_REPLY ") {
                    sender.send(serde_json::from_str(reply).unwrap()).ok();
                }
            }
        });
        let process = Self {
            child,
            input,
            replies,
            reader: Some(reader),
            running: true,
        };
        let ready = process
            .replies
            .recv_timeout(Duration::from_secs(40))
            .expect("child ready reply");
        assert_eq!(ready["ready"], true, "child startup: {ready}");
        process
    }
    pub fn call(&mut self, command: Value) -> Value {
        serde_json::to_writer(&mut self.input, &command).unwrap();
        writeln!(&mut self.input).unwrap();
        self.input.flush().unwrap();
        let reply = self
            .replies
            .recv_timeout(Duration::from_secs(40))
            .expect("bounded child reply");
        assert!(
            reply.get("error").is_none(),
            "command={command}, reply={reply}"
        );
        reply
    }
    pub fn stop(&mut self) {
        if !self.running {
            return;
        }
        self.call(json!({"op":"stop"}));
        assert!(self.child.wait().unwrap().success());
        self.running = false;
        if let Some(reader) = self.reader.take() {
            reader.join().unwrap();
        }
    }
    pub fn kill(&mut self) {
        if !self.running {
            return;
        }
        self.child.kill().unwrap();
        self.child.wait().unwrap();
        self.running = false;
        if let Some(reader) = self.reader.take() {
            reader.join().unwrap();
        }
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        if self.running {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

pub struct Cluster {
    pub root: PathBuf,
    peers: Vec<(u64, String)>,
    pub nodes: BTreeMap<u64, Process>,
    epochs: BTreeMap<u64, usize>,
}
impl Cluster {
    pub fn new(case: &str) -> Self {
        let base = std::env::var_os("NATIVE_EVIDENCE_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let base = lab::require_lab(&base);
        let root = tempfile::Builder::new()
            .prefix(case)
            .tempdir_in(base)
            .unwrap()
            .keep();
        println!("NATIVE_EVIDENCE case={case} root={}", root.display());
        let listeners: Vec<_> = (0..3)
            .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let peers = listeners
            .iter()
            .enumerate()
            .map(|(index, listener)| {
                (
                    (index + 1) as u64,
                    listener.local_addr().unwrap().to_string(),
                )
            })
            .collect();
        drop(listeners);
        Self {
            root,
            peers,
            nodes: BTreeMap::new(),
            epochs: BTreeMap::new(),
        }
    }
    pub fn start(&mut self, id: u64) {
        assert!(!self.nodes.contains_key(&id));
        let epoch = self.epochs.entry(id).or_default();
        let process = Process::start(
            ChildConfig {
                id,
                peers: self.peers.clone(),
                root: self.root.join(format!("node-{id}")),
            },
            &self.root,
            *epoch,
        );
        *epoch += 1;
        self.nodes.insert(id, process);
    }
    pub fn start_all(&mut self) {
        for id in 1..=3 {
            self.start(id);
        }
    }
    pub fn stop(&mut self, id: u64) {
        self.nodes.remove(&id).unwrap().stop();
    }
    pub fn kill(&mut self, id: u64) {
        self.nodes.remove(&id).unwrap().kill();
    }
    pub fn stop_all(&mut self) {
        for id in self.nodes.keys().copied().collect::<Vec<_>>() {
            self.stop(id);
        }
    }
    pub fn status(&mut self, id: u64) -> Value {
        self.nodes
            .get_mut(&id)
            .unwrap()
            .call(json!({"op":"status"}))
    }
    pub fn leader(&mut self) -> u64 {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            for id in self.nodes.keys().copied().collect::<Vec<_>>() {
                if self.status(id)["is_leader"] == true {
                    return id;
                }
            }
            assert!(Instant::now() < deadline, "no actual leader");
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    pub fn add(&mut self, delta: i64, idem: u64) {
        let leader = self.leader();
        self.nodes
            .get_mut(&leader)
            .unwrap()
            .call(json!({"op":"add","delta":delta,"idem":idem}));
    }
    pub fn wait_value(&mut self, id: u64, value: i64) -> Value {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let state = self.status(id);
            if state["value"] == value {
                println!("NATIVE_ORACLE node={id} value={value} state={state}");
                return state;
            }
            assert!(
                Instant::now() < deadline,
                "victim {id} expected {value}: {state}"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    pub fn wait_all(&mut self, value: i64) {
        for id in self.nodes.keys().copied().collect::<Vec<_>>() {
            self.wait_value(id, value);
        }
    }
    pub fn compact(&mut self, id: u64) -> Value {
        let submitted = self
            .nodes
            .get_mut(&id)
            .unwrap()
            .call(json!({"op":"compact"}));
        let target = submitted["target"].as_u64().unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let state = self.status(id);
            if state["progress"] == "CompletedObserved"
                && state["purged"].as_u64().is_some_and(|p| p >= target)
            {
                assert!(state["durable_snapshot"]
                    .as_u64()
                    .is_some_and(|s| s >= target));
                println!("NATIVE_PURGE node={id} target={target} state={state}");
                return state;
            }
            assert!(Instant::now() < deadline, "compaction unfinished: {state}");
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}
