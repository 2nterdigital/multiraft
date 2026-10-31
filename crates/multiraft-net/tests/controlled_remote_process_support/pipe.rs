//! Private inherited pipes bind a preconfigured peer to the OS-created child process.
use super::source::DOMAIN;
use multiraft_core::ReplacementSpec;
use multiraft_net::{
    controlled_startup::StartupAuthorityFuture,
    portable::{
        NativeTargetCommand, NativeTargetIdentity, NativeTargetOutcome, NativeTargetRequest,
        NativeTargetTransport, NativeTargetTransportError,
    },
};
use serde::{Deserialize, Serialize};
use std::{
    io::{self, Read, Write},
    path::PathBuf,
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};
/// JSON expansion of a 1 MiB chunk plus bounded native header fields fits this frame cap.
pub const FRAME_CAP: usize = 192 * 1024 * 1024;
pub const MAGIC: &[u8] = b"REMOTE-TARGET-PIPE-V1\n";
#[derive(Serialize, Deserialize)]
pub struct Setup {
    pub root: PathBuf,
    pub peers: Vec<(u64, std::net::SocketAddr)>,
    pub catalog: PathBuf,
    pub specification: ReplacementSpec,
}
#[derive(Serialize, Deserialize)]
pub enum Request {
    Native(Vec<u8>),
    Diagnostics,
    ReadBarrier {
        term: u64,
        node: u64,
        index: u64,
        rows: Vec<Vec<u8>>,
    },
    Stop,
}
#[derive(Serialize, Deserialize, Debug)]
pub enum Reply {
    Identity(NativeTargetIdentity),
    Native(Vec<u8>),
    Diagnostics {
        rows: Vec<Vec<u8>>,
        creations: usize,
        pid: u32,
    },
    Barrier,
    Stopped,
}
pub fn write_frame(w: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    if bytes.len() > FRAME_CAP {
        return Err(io::Error::other("frame exceeds cap"));
    }
    w.write_all(&(bytes.len() as u32).to_be_bytes())?;
    w.write_all(bytes)?;
    w.flush()
}
pub fn read_frame(r: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut prefix = [0; 4];
    r.read_exact(&mut prefix)?;
    let len = u32::from_be_bytes(prefix) as usize;
    if len > FRAME_CAP {
        return Err(io::Error::other("frame exceeds cap"));
    }
    let mut bytes = vec![0; len];
    r.read_exact(&mut bytes)?;
    Ok(bytes)
}
struct Channel {
    child: Child,
    input: ChildStdin,
    output: ChildStdout,
}
pub struct Pipe {
    channel: Arc<Mutex<Channel>>,
    pid: u32,
    drop_finish: AtomicBool,
    drop_observe: AtomicBool,
}
impl Pipe {
    pub fn spawn(setup: Setup) -> (Arc<Self>, NativeTargetIdentity) {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "controlled_remote_target_child",
                "--nocapture",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn actual target process");
        let pid = child.id();
        assert_ne!(pid, std::process::id());
        let mut input = child.stdin.take().unwrap();
        let mut output = child.stdout.take().unwrap();
        // libtest owns a short stdout preamble; native framing starts at this fixed magic.
        let mut matched = 0;
        for _ in 0..4096 {
            let mut b = [0];
            output.read_exact(&mut b).unwrap();
            if b[0] == MAGIC[matched] {
                matched += 1;
            } else {
                matched = usize::from(b[0] == MAGIC[0]);
            }
            if matched == MAGIC.len() {
                break;
            }
        }
        assert_eq!(matched, MAGIC.len(), "child reached private framed channel");
        write_frame(&mut input, &serde_json::to_vec(&setup).unwrap()).unwrap();
        let Reply::Identity(identity) =
            serde_json::from_slice(&read_frame(&mut output).unwrap()).unwrap()
        else {
            panic!("target reservation identity")
        };
        // Peer authority comes from this exact Child's inherited handles and preset config,
        // never from the identity decoded above. Native validates the remaining scope.
        assert_eq!((identity.node, identity.domain), (4, Some(DOMAIN)));
        (
            Arc::new(Self {
                channel: Arc::new(Mutex::new(Channel {
                    child,
                    input,
                    output,
                })),
                pid,
                drop_finish: AtomicBool::new(true),
                drop_observe: AtomicBool::new(false),
            }),
            identity,
        )
    }
    fn call(channel: &Mutex<Channel>, request: Request) -> io::Result<Reply> {
        let mut c = channel
            .lock()
            .map_err(|_| io::Error::other("pipe poisoned"))?;
        if c.child.try_wait()?.is_some() {
            return Err(io::Error::other("spawned peer exited"));
        }
        write_frame(&mut c.input, &serde_json::to_vec(&request)?)?;
        serde_json::from_slice(&read_frame(&mut c.output)?).map_err(io::Error::other)
    }
    pub fn lose_next_observe(&self) {
        self.drop_observe.store(true, Ordering::SeqCst);
    }
    pub async fn diagnostics(&self) -> (Vec<Vec<u8>>, usize) {
        let channel = self.channel.clone();
        let reply = tokio::task::spawn_blocking(move || Self::call(&channel, Request::Diagnostics))
            .await
            .unwrap()
            .unwrap();
        let Reply::Diagnostics {
            rows,
            creations,
            pid,
        } = reply
        else {
            panic!("diagnostic reply")
        };
        assert_eq!(pid, self.pid);
        (rows, creations)
    }
    pub async fn read_barrier(&self, barrier: multiraft_net::ObservedLogId, rows: Vec<Vec<u8>>) {
        let channel = self.channel.clone();
        let reply = tokio::task::spawn_blocking(move || {
            Self::call(
                &channel,
                Request::ReadBarrier {
                    term: barrier.term,
                    node: barrier.node_id,
                    index: barrier.index,
                    rows,
                },
            )
        })
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(reply, Reply::Barrier));
    }
    pub async fn stop(&self) {
        let channel = self.channel.clone();
        tokio::task::spawn_blocking(move || {
            let reply = Self::call(&channel, Request::Stop).unwrap();
            assert!(matches!(reply, Reply::Stopped));
            let status = channel.lock().unwrap().child.wait().unwrap();
            assert!(status.success());
        })
        .await
        .unwrap();
    }
}
impl Drop for Pipe {
    fn drop(&mut self) {
        if let Ok(mut c) = self.channel.lock() {
            let _ = c.child.kill();
            let _ = c.child.wait();
        }
    }
}
impl NativeTargetTransport for Pipe {
    fn peer(&self) -> (u64, Option<[u8; 32]>) {
        (4, Some(DOMAIN))
    }
    fn exchange(
        &self,
        request: Vec<u8>,
    ) -> StartupAuthorityFuture<'_, Result<Vec<u8>, NativeTargetTransportError>> {
        let finish = serde_json::from_slice::<NativeTargetRequest>(&request)
            .is_ok_and(|r| matches!(r.command, NativeTargetCommand::FinishSeed));
        let observe = serde_json::from_slice::<NativeTargetRequest>(&request)
            .is_ok_and(|r| matches!(r.command, NativeTargetCommand::Observe { .. }));
        let lose = (finish && self.drop_finish.swap(false, Ordering::SeqCst))
            || (observe && self.drop_observe.swap(false, Ordering::SeqCst));
        let channel = self.channel.clone();
        Box::pin(async move {
            // This owned blocking exchange keeps reading after its async waiter departs.
            let reply =
                tokio::task::spawn_blocking(move || Self::call(&channel, Request::Native(request)))
                    .await
                    .map_err(|e| NativeTargetTransportError {
                        outcome: NativeTargetOutcome::Unknown,
                        message: e.to_string(),
                    })?
                    .map_err(|e| NativeTargetTransportError {
                        outcome: NativeTargetOutcome::Unknown,
                        message: e.to_string(),
                    })?;
            if lose {
                return Err(NativeTargetTransportError {
                    outcome: NativeTargetOutcome::Unknown,
                    message: "injected lost native reply after durable dispatch".into(),
                });
            }
            match reply {
                Reply::Native(bytes) => Ok(bytes),
                _ => Err(NativeTargetTransportError {
                    outcome: NativeTargetOutcome::Unknown,
                    message: "unexpected pipe response".into(),
                }),
            }
        })
    }
}
