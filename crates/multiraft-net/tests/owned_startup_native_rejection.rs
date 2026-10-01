//! Existing bounded source records expose a real eligibility/dispatch race.
mod startup_support;
use multiraft_core::StartupProvenance;
use multiraft_net::InitializeDisposition;
use startup_support::*;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{sync::Notify, time::timeout};
use tracing::{
    field::{Field, Visit},
    Event, Subscriber,
};
use tracing_subscriber::{layer::Context, prelude::*, Layer};
#[derive(Default)]
struct Fields {
    node: u64,
    phase: String,
    term: u64,
}
impl Visit for Fields {
    fn record_u64(&mut self, f: &Field, v: u64) {
        match f.name() {
            "node_id" | "local_node_id" => self.node = v,
            "vote_term" => self.term = v,
            _ => (),
        }
    }
    fn record_str(&mut self, f: &Field, v: &str) {
        if f.name() == "phase" {
            self.phase = v.to_owned();
        }
    }
    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
}
struct Sources {
    gate: Gate,
    seen: Arc<AtomicBool>,
    dirty: Arc<Notify>,
}
impl<S: Subscriber> Layer<S> for Sources {
    fn on_event(&self, e: &Event<'_>, _: Context<'_, S>) {
        let mut f = Fields::default();
        e.record(&mut f);
        if f.node != 2 {
            return;
        }
        if e.metadata().target() == "multiraft::startup"
            && f.phase == "initialize_dispatch"
            && !self.seen.swap(true, Ordering::AcqRel)
        {
            self.gate.block();
        }
        if e.metadata().target() == "multiraft::control" && f.term > 0 {
            self.dirty.notify_one();
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn peer_state_after_eligibility_keeps_raw_native_not_allowed_disposition() {
    let gate = Gate::default();
    let _release = Release(vec![gate.clone()]);
    let dirty = Arc::new(Notify::new());
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(Sources {
        gate: gate.clone(),
        seen: Arc::new(AtomicBool::new(false)),
        dirty: dirty.clone(),
    }))
    .unwrap();
    let roots = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
    let peers = vec![(1, addr()), (2, addr())];
    let o1 = start(
        roots[0].path(),
        1,
        &peers,
        Factory::new(roots[0].path()),
        true,
    )
    .await;
    let o2 = start(
        roots[1].path(),
        2,
        &peers,
        Factory::new(roots[1].path()),
        true,
    )
    .await;
    let h1 = o1.handle();
    let h2 = o2.handle();
    let worker = h2.clone();
    let startup = tokio::spawn(async move {
        worker
            .start_groups_with_preference(batch(&[7], &[1, 2], Some(2), Duration::from_secs(3)))
            .await
    });
    timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .unwrap();
    h1.start_groups_with_preference(batch(&[7], &[1, 2], Some(1), Duration::from_secs(3)))
        .await
        .unwrap();
    timeout(Duration::from_secs(2), dirty.notified())
        .await
        .unwrap();
    gate.release();
    let report = startup.await.unwrap().unwrap();
    assert_eq!(
        report.groups[0].provenance,
        Some(StartupProvenance::Pristine)
    );
    assert_eq!(
        report.groups[0].initialization,
        InitializeDisposition::NotAllowed
    );
    let handles = [h1, h2];
    leader(&handles, 7)
        .await
        .propose(7, vec![89], deadline())
        .await
        .unwrap();
    o1.shutdown(deadline()).await.unwrap();
    o2.shutdown(deadline()).await.unwrap();
    for (i, root) in roots.iter().enumerate() {
        reusable(root.path(), &peers[i..i + 1]);
    }
}
