//! Test-only interruption at the durable bootstrap-anchor / lifecycle publication boundary.
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};
use tokio::sync::{watch, Notify};
struct Gate {
    root: PathBuf,
    entered: Notify,
}
static GATE: OnceLock<Mutex<Option<Arc<Gate>>>> = OnceLock::new();
pub(crate) async fn wait(root: &Path, mut stopping: watch::Receiver<bool>) -> bool {
    let gate = GATE
        .get_or_init(Mutex::default)
        .lock()
        .expect("bootstrap test gate lock poisoned")
        .clone();
    let Some(gate) = gate.filter(|gate| gate.root == root) else {
        return false;
    };
    gate.entered.notify_one();
    if !*stopping.borrow_and_update() {
        let _ = stopping.changed().await;
    }
    true
}
use crate::controlled_startup as authority;
#[path = "../tests/startup_authority_support/catalog.rs"]
#[allow(dead_code)]
mod catalog;
#[path = "../tests/controlled_protocol_support/disk.rs"]
#[allow(dead_code)]
mod disk;
#[path = "../tests/owned_controlled_support/fixture.rs"]
#[allow(dead_code)]
mod support;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_anchor_before_opened_interruption_recovers_same_original_generation() {
    use crate::{ControlledOpen, ControlledOpenMode};
    use authority::{StartupAuthority, StartupAuthorityResult, StartupRequest};
    use support::{deadline, Fixture, GROUP};
    let fixture = Fixture::unopened().await;
    let gate = Arc::new(Gate {
        root: fixture.configs[0].data_dir.clone(),
        entered: Notify::new(),
    });
    *GATE
        .get_or_init(Mutex::default)
        .lock()
        .expect("bootstrap test gate lock poisoned") = Some(gate.clone());
    let mut openings = Vec::new();
    for index in 0..3 {
        let node = (index + 1) as u64;
        let handle = fixture.owners[index].handle();
        let authority = fixture.authority.clone();
        let admission = match authority
            .admit(StartupRequest {
                group_id: GROUP,
                node_id: node,
                generation: node * 10,
                request_id: [node as u8; 16],
            })
            .await
            .unwrap()
        {
            StartupAuthorityResult::Applied(value) => value,
            other => panic!("{other:?}"),
        };
        openings.push(tokio::spawn(async move {
            handle
                .open_controlled_group(
                    ControlledOpen {
                        group_id: GROUP,
                        generation: node * 10,
                        identity: vec![node as u8],
                        mode: ControlledOpenMode::BootstrapNew {
                            voters: [1, 2, 3].into(),
                        },
                    },
                    authority,
                    admission,
                    deadline(),
                )
                .await
        }));
    }
    tokio::time::timeout(Duration::from_secs(10), gate.entered.notified())
        .await
        .expect("actual native durable bootstrap anchor reached");
    assert!(gate
        .root
        .join("controlled-bootstrap/group-7.anchor.json")
        .exists());
    let Fixture {
        root,
        authority,
        mut owners,
        configs,
        ..
    } = fixture;
    owners.remove(0).shutdown(deadline()).await.unwrap();
    assert!(
        openings.remove(0).await.unwrap().is_err(),
        "interrupted open never reports generic Opened success"
    );
    for opening in openings {
        opening.await.unwrap().unwrap();
    }
    for owner in owners {
        owner.shutdown(deadline()).await.unwrap();
    }
    *GATE
        .get()
        .unwrap()
        .lock()
        .expect("bootstrap test gate lock poisoned") = None;
    let owner = support::start_owned(configs[0].clone()).await;
    let inspected = owner
        .handle()
        .inspect_controlled_group(GROUP, 10, vec![1], authority.clone(), deadline())
        .await
        .unwrap();
    assert_eq!(
        inspected.receipt(deadline()).await.unwrap().stage,
        multiraft_store::ControlledGroupStage::Opening,
        "real anchor survived before lifecycle Opened publication"
    );
    let group = support::recover(&owner, authority.clone(), 1, 93)
        .await
        .expect("standalone same-generation recovery under provisional catalog");
    assert_eq!(
        group.genesis_basis(deadline()).await.unwrap().generation(),
        10
    );
    assert!(authority
        .read(GROUP)
        .await
        .unwrap()
        .membership
        .log_id()
        .is_none());
    owner.shutdown(deadline()).await.unwrap();
    drop(root);
}
