use super::*;
use openraft::async_runtime::WatchSender as _;
use openraft::type_config::TypeConfigExt as _;
use std::time::Duration;
fn metrics(candidate: Option<NodeId>) -> RaftMetrics<TypeConfig> {
    let mut metrics = RaftMetrics::new_initial(101);
    metrics.current_leader = candidate;
    metrics
}
#[tokio::test(start_paused = true)]
async fn subscribed_before_inspection_includes_prior_update_and_marks_seen() {
    let (tx, raw) = TypeConfig::watch_channel(metrics(None));
    let mut rx = LeaderHintReceiver::new(7, raw, None);
    tx.send(metrics(Some(102))).unwrap();
    let hint = rx.latest().unwrap();
    assert_eq!(
        (hint.group_id, hint.local_node_id, hint.candidate),
        (7, 101, Some(102))
    );
    assert_eq!(hint.source, HintSource::LocalRaft);
    assert!(futures::poll!(Box::pin(rx.changed())).is_pending());
}
#[tokio::test(start_paused = true)]
async fn unrelated_updates_and_excluded_hint_do_not_reset_deadline() {
    let (tx, raw) = TypeConfig::watch_channel(metrics(Some(102)));
    let mut rx = LeaderHintReceiver::new(7, raw, None);
    let started = Instant::now();
    let wait = rx.wait_until(started + Duration::from_secs(1), Some(102));
    tokio::pin!(wait);
    assert!(futures::poll!(&mut wait).is_pending());
    tokio::time::advance(Duration::from_millis(900)).await;
    let mut updated = metrics(Some(102));
    updated.current_term = 9;
    tx.send(updated).unwrap();
    assert!(futures::poll!(&mut wait).is_pending());
    assert_eq!(wait.await.unwrap(), None);
    assert_eq!(Instant::now() - started, Duration::from_secs(1));
}
#[tokio::test(start_paused = true)]
async fn different_hint_is_observed_but_exact_deadline_wins() {
    for delay in [900, 1000] {
        let (tx, raw) = TypeConfig::watch_channel(metrics(Some(102)));
        let mut rx = LeaderHintReceiver::new(7, raw, None);
        let wait = rx.wait_until(Instant::now() + Duration::from_secs(1), Some(102));
        tokio::pin!(wait);
        assert!(futures::poll!(&mut wait).is_pending());
        tokio::time::advance(Duration::from_millis(delay)).await;
        tx.send(metrics(Some(103))).unwrap();
        let hint = wait.await.unwrap();
        assert_eq!(
            hint.map(|h| h.candidate),
            if delay == 900 { Some(Some(103)) } else { None }
        );
    }
}
#[tokio::test]
async fn stopped_panicked_and_dropped_source_have_distinct_typed_results() {
    let mut sample = metrics(None);
    sample.running_state = Err(openraft::error::Fatal::Stopped);
    let (_tx, raw) = TypeConfig::watch_channel(sample);
    let mut rx = LeaderHintReceiver::new(7, raw, None);
    assert!(matches!(rx.latest(), Err(MultiRaftError::ObservationClosed(e)) if e.group_id()==7));
    let mut sample = metrics(None);
    sample.running_state = Err(openraft::error::Fatal::Panicked);
    let (_tx, raw) = TypeConfig::watch_channel(sample);
    let mut rx = LeaderHintReceiver::new(7, raw, None);
    assert!(matches!(
        rx.latest(),
        Err(MultiRaftError::ObservationFailed {
            group_id: 7,
            source: NativeFailure::Panicked
        })
    ));
    let (tx, raw) = TypeConfig::watch_channel(metrics(None));
    let mut rx = LeaderHintReceiver::new(7, raw, None);
    drop(tx);
    assert!(matches!(
        rx.changed().await,
        Err(MultiRaftError::ObservationClosed(_))
    ));
}
#[tokio::test]
async fn owner_close_racing_candidate_cannot_revive_subscription() {
    let (tx, raw) = TypeConfig::watch_channel(metrics(None));
    let (closed, closing) = watch::channel(false);
    let mut rx = LeaderHintReceiver::new(7, raw, Some(closing));
    let wait = rx.wait_until(Instant::now() + Duration::from_secs(2), None);
    tokio::pin!(wait);
    assert!(futures::poll!(&mut wait).is_pending());
    tx.send(metrics(Some(102))).unwrap();
    closed.send_replace(true);
    assert!(matches!(
        wait.await,
        Err(MultiRaftError::ObservationClosed(_))
    ));
}
#[tokio::test]
async fn cancelled_wait_releases_receiver_without_consuming_update() {
    let (tx, raw) = TypeConfig::watch_channel(metrics(None));
    let mut rx = LeaderHintReceiver::new(7, raw, None);
    let mut wait = Box::pin(rx.changed());
    assert!(futures::poll!(&mut wait).is_pending());
    drop(wait);
    tx.send(metrics(Some(102))).unwrap();
    assert_eq!(rx.changed().await.unwrap().candidate, Some(102));
    drop(rx);
    assert!(tx.send(metrics(None)).is_err());
}
