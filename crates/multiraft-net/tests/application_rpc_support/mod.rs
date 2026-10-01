use multiraft_net::application_rpc::*;
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::Instant;

pub fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
pub fn address() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}
pub fn call(payload: &[u8]) -> RpcCall {
    RpcCall::new(7, 11, payload.to_vec())
}
pub fn config(node: u64, peers: Vec<(u64, SocketAddr)>) -> ApplicationRpcConfig {
    let mut config = ApplicationRpcConfig::new(node, "127.0.0.1:0".parse().unwrap(), peers);
    config.metadata_keys.insert("test-context".into());
    config
}
pub async fn start(
    node: u64,
    peers: Vec<(u64, SocketAddr)>,
    handler: Arc<dyn ApplicationRpcHandler>,
) -> (ApplicationRpcOwner, ApplicationRpcHandle) {
    ApplicationRpcOwner::start(config(node, peers), handler, deadline())
        .await
        .unwrap()
}
#[derive(Default)]
pub struct Echo {
    pub calls: AtomicUsize,
    pub observed: Mutex<Vec<(Instant, RpcMetadata)>>,
    pub handle: OnceLock<ApplicationRpcHandle>,
}
impl ApplicationRpcHandler for Echo {
    fn call(&self, context: RpcContext, call: RpcCall) -> RpcFuture<'_> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.observed
                .lock()
                .unwrap()
                .push((context.deadline, context.metadata.clone()));
            if call.payload == b"reject" {
                return Err(RpcHandlerError::new(
                    RpcErrorKind::InvalidArgument,
                    "application rejected",
                ));
            }
            if call.payload == b"nested" {
                return self
                    .handle
                    .get()
                    .unwrap()
                    .call(1, super::application_rpc_support::call(b"inner"), context)
                    .await
                    .map_err(|e| RpcHandlerError::new(e.kind, e.message()));
            }
            if call.payload == b"oversize" {
                return Ok(RpcReply::new(vec![0; APPLICATION_RPC_OUTER_BYTES]));
            }
            let mut payload = vec![
                u8::try_from(call.service_id).unwrap(),
                u8::try_from(call.method_id).unwrap(),
            ];
            payload.extend(call.payload);
            Ok(RpcReply::new(payload))
        })
    }
}
#[derive(Default)]
pub struct Blocking {
    pub started: Notify,
    pub release: Notify,
    pub dropped: Arc<AtomicUsize>,
    pub calls: AtomicUsize,
}
struct End(Arc<AtomicUsize>);
impl Drop for End {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
impl ApplicationRpcHandler for Blocking {
    fn call(&self, _context: RpcContext, _call: RpcCall) -> RpcFuture<'_> {
        Box::pin(async move {
            let _end = End(self.dropped.clone());
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.started.notify_one();
            self.release.notified().await;
            Ok(RpcReply::new(b"done".to_vec()))
        })
    }
}
pub async fn until_dropped(count: &AtomicUsize, expected: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while count.load(Ordering::SeqCst) < expected {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
pub async fn reusable(address: SocketAddr) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(listener) = TcpListener::bind(address) {
                drop(listener);
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
