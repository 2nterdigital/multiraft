//! Public consumer and actual subscriber: steady successful observations stay quiet at INFO.
use multiraft_core::ClusterConfig;
use multiraft_fsm::{ApplyOut, StateMachine};
use multiraft_net::{
    ControlContext, ControlInvocationId, GroupConfig, GroupControlLayoutObservation,
    GroupControlRequestEcho, GroupControlRequestResult, GroupControlSampleError, NodeOwner,
    RuntimeConfig,
};
use std::{
    io,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::Instant;
use tracing_subscriber::prelude::*;
#[derive(Default)]
struct Bytes;
impl StateMachine for Bytes {
    type Error = io::Error;
    fn apply(&mut self, _: u64, _: u64, data: &[u8]) -> Result<ApplyOut, io::Error> {
        Ok(ApplyOut {
            effects: data.to_vec(),
        })
    }
    fn snapshot(&self, _: u64) -> Result<Vec<u8>, io::Error> {
        Ok(vec![])
    }
    fn restore(&mut self, _: u64, _: &[u8]) -> Result<(), io::Error> {
        Ok(())
    }
}
#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);
impl io::Write for Logs {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Logs {
    type Writer = Self;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}
impl Logs {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}
fn context(id: u8) -> ControlContext {
    ControlContext::new(
        ControlInvocationId([id; 16]),
        Instant::now() + Duration::from_secs(3),
    )
}
fn filter(level: tracing::Level) -> tracing_subscriber::filter::Targets {
    tracing_subscriber::filter::Targets::new().with_target("multiraft::control", level)
}
#[tokio::test]
async fn public_observations_are_debug_while_refusal_transfer_cancel_and_changes_stay_info() {
    let logs = Logs::default();
    let (levels, reload) = tracing_subscriber::reload::Layer::new(filter(tracing::Level::INFO));
    let subscriber = tracing_subscriber::registry().with(
        tracing_subscriber::fmt::layer()
            .with_writer(logs.clone())
            .with_ansi(false)
            .without_time()
            .with_filter(levels),
    );
    tracing::subscriber::set_global_default(subscriber).unwrap();
    let addr = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let config = RuntimeConfig::new(
        ClusterConfig::new(1, vec![(1, addr)]),
        vec![GroupConfig {
            group_id: 7,
            voters: vec![1],
        }],
    );
    let owner = NodeOwner::start(config, |_| Ok(Bytes), context(1).deadline)
        .await
        .unwrap();
    let handle = owner.handle();
    let sample = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(sample) = handle.read_group_control_sample(7, &[1], context(2)).await {
                break sample;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let echo = GroupControlRequestEcho {
        group_id: 7,
        source: 1,
        target: 1,
    };
    for _ in 0..20 {
        handle
            .read_group_control_sample(7, &[1], context(55))
            .await
            .unwrap();
        assert!(matches!(
            handle
                .observe_group_control_layout(echo, &[1], context(55))
                .await,
            GroupControlLayoutObservation::TargetObserved { .. }
        ));
    }
    assert!(
        !logs
            .text()
            .contains(&ControlInvocationId([55; 16]).to_string()),
        "default INFO has no repetitive started/sampled/layout success"
    );
    assert!(matches!(
        handle
            .read_group_control_sample(999, &[1], context(56))
            .await,
        Err(GroupControlSampleError::UnknownGroup { .. })
    ));
    let result = handle
        .try_transfer_group_leader(&sample.observed_preconditions_for(1), &[1], context(66))
        .await;
    assert!(matches!(
        result.result,
        GroupControlRequestResult::PrecheckRejected { .. }
    ));
    let mut canceled = Box::pin(handle.read_group_control_sample(7, &[1], context(67)));
    assert!(futures::poll!(&mut canceled).is_pending());
    drop(canceled);
    let info = logs.text();
    for value in [
        "sample_unavailable",
        "unknown_group",
        "precheck_rejected",
        "cancelled",
        "initial_observation",
        "cause_unknown",
    ] {
        assert!(info.contains(value), "INFO omitted {value}: {info}");
    }
    for id in [56, 66, 67] {
        assert!(info.contains(&ControlInvocationId([id; 16]).to_string()));
    }
    reload.reload(filter(tracing::Level::DEBUG)).unwrap();
    handle
        .read_group_control_sample(7, &[1], context(77))
        .await
        .unwrap();
    handle
        .observe_group_control_layout(echo, &[1], context(77))
        .await;
    let debug = logs.text();
    let lines: Vec<_> = debug
        .lines()
        .filter(|line| line.contains(&ControlInvocationId([77; 16]).to_string()))
        .collect();
    for field in [
        "DEBUG",
        "invocation_id=",
        "group_id=7",
        "local_node_id=1",
        "source_node_id=1",
        "target_node_id=1",
        "stage=",
        "result=",
        "duration_ms=",
        "vote_term=",
    ] {
        assert!(
            lines.iter().any(|line| line.contains(field)),
            "DEBUG omitted {field}: {debug}"
        );
    }
    assert!(
        !debug.contains("payload=")
            && !debug.contains("credential=")
            && !debug.contains("command=")
    );
    owner.shutdown(context(9).deadline).await.unwrap();
}
