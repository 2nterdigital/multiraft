//! Native operation identity and executor fencing survive real durable-applied recovery.
use futures::stream;
use multiraft_core::{
    typ::{Entry, LogId, StoredMembership},
    NativeControl, OperationId, ReplacementSpec, Request, TargetOpenReceipt, TypeConfig,
};
use multiraft_fsm::{AppliedState, ApplyOut, DiskState, GroupId, StateMachine};
use multiraft_store::{rocksdb::WriteBatch, DiskFsmDb, DiskFsmOptions, StateMachineStore};
use openraft::{
    alias::{EntryPayloadOf, LeaderIdOf},
    storage::RaftStateMachine,
    vote::RaftLeaderIdExt,
    EntryPayload, Membership,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    path::Path,
};

struct Disk {
    db: DiskFsmDb,
}

#[tokio::test]
async fn completion_requires_exact_applied_uniform_and_survives_checkpoint_without_logs() {
    let scratch = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/223-ledger");
    std::fs::create_dir_all(&scratch).unwrap();
    let root = tempfile::tempdir_in(scratch).unwrap();
    let nodes = BTreeMap::from_iter([1, 2, 3, 4].map(|id| (id, openraft::BasicNode::default())));
    let initial = Membership::new(
        vec![BTreeSet::from([1, 2, 3])],
        BTreeMap::from_iter([1, 2, 3].map(|id| (id, openraft::BasicNode::default()))),
    )
    .unwrap();
    let specification = ReplacementSpec {
        operation: OperationId {
            sequence: 1,
            nonce: [9; 16],
        },
        expected_start: StoredMembership::new(Some(id(0)), initial.clone()),
        target_voters: BTreeSet::from([1, 2, 4]),
        target_generation: 5,
    };
    let target_open = TargetOpenReceipt {
        operation: specification.operation,
        target_node: 4,
        generation: 5,
        applied: id(6),
        receipt: vec![1, 2, 3],
    };
    let complete = NativeControl::Complete {
        operation: specification.operation,
        executor_epoch: 1,
        target_open,
    };
    let mut store = open(&root.path().join("live"));
    apply(&mut store, 0, EntryPayload::Membership(initial)).await;
    apply(
        &mut store,
        1,
        EntryPayload::Normal(Request::native_control(NativeControl::Begin {
            specification: specification.clone(),
            executor_epoch: 1,
        })),
    )
    .await;
    apply(
        &mut store,
        2,
        EntryPayload::Membership(
            Membership::new(vec![BTreeSet::from([1, 2, 3])], nodes.clone()).unwrap(),
        ),
    )
    .await;
    apply(
        &mut store,
        3,
        EntryPayload::Normal(Request::native_control(complete.clone())),
    )
    .await;
    assert!(
        store
            .controlled_ledger()
            .await
            .unwrap()
            .current
            .unwrap()
            .terminal
            .is_none(),
        "learner cannot complete"
    );
    apply(
        &mut store,
        4,
        EntryPayload::Membership(
            Membership::new(
                vec![BTreeSet::from([1, 2, 3]), BTreeSet::from([1, 2, 4])],
                nodes,
            )
            .unwrap(),
        ),
    )
    .await;
    apply(
        &mut store,
        5,
        EntryPayload::Normal(Request::native_control(complete.clone())),
    )
    .await;
    assert!(
        store
            .controlled_ledger()
            .await
            .unwrap()
            .current
            .unwrap()
            .terminal
            .is_none(),
        "joint cannot complete"
    );
    let final_membership = Membership::new(
        vec![BTreeSet::from([1, 2, 4])],
        BTreeMap::from_iter([1, 2, 4].map(|id| (id, openraft::BasicNode::default()))),
    )
    .unwrap();
    apply(
        &mut store,
        6,
        EntryPayload::Membership(final_membership.clone()),
    )
    .await;
    apply(
        &mut store,
        7,
        EntryPayload::Normal(Request::native_control(match complete.clone() {
            NativeControl::Complete {
                operation,
                executor_epoch,
                mut target_open,
            } => {
                target_open.applied = LogId::new(LeaderIdOf::<TypeConfig>::new_committed(2, 1), 5);
                NativeControl::Complete {
                    operation,
                    executor_epoch,
                    target_open,
                }
            }
            _ => unreachable!(),
        })),
    )
    .await;
    assert!(
        store
            .controlled_ledger()
            .await
            .unwrap()
            .current
            .unwrap()
            .terminal
            .is_none(),
        "a newer term cannot hide a target behind the final configuration index"
    );
    apply(
        &mut store,
        8,
        EntryPayload::Normal(Request::native_control(match complete.clone() {
            NativeControl::Complete {
                operation,
                executor_epoch,
                mut target_open,
            } => {
                target_open.applied = LogId::new(LeaderIdOf::<TypeConfig>::new_committed(2, 1), 7);
                NativeControl::Complete {
                    operation,
                    executor_epoch,
                    target_open,
                }
            }
            _ => unreachable!(),
        })),
    )
    .await;
    assert!(
        store
            .controlled_ledger()
            .await
            .unwrap()
            .current
            .unwrap()
            .terminal
            .is_none(),
        "target application cannot come from a term after the Complete barrier"
    );
    apply(
        &mut store,
        9,
        EntryPayload::Normal(Request::native_control(complete)),
    )
    .await;
    let proof = store
        .controlled_ledger()
        .await
        .unwrap()
        .current
        .unwrap()
        .terminal
        .unwrap();
    assert_eq!(
        proof.configuration,
        StoredMembership::new(Some(id(6)), final_membership)
    );
    assert_eq!(proof.barrier, id(9));
    let checkpoint = root.path().join("checkpoint");
    store
        .try_with_fsm(|disk| disk.db.checkpoint(&checkpoint))
        .await
        .unwrap()
        .unwrap();
    drop(store);
    let mut restored = open(&checkpoint);
    apply(
        &mut restored,
        10,
        EntryPayload::Normal(Request::native_control(NativeControl::Claim {
            operation: specification.operation,
            executor_epoch: 99,
        })),
    )
    .await;
    let record = restored.controlled_ledger().await.unwrap().current.unwrap();
    assert_eq!(
        record.terminal,
        Some(proof),
        "terminal proof retained without Raft logs"
    );
    assert_eq!(
        record.executor_epoch, 1,
        "terminal cannot acquire a new executor"
    );
    let mut next = specification.clone();
    next.operation = OperationId {
        sequence: 2,
        nonce: [11; 16],
    };
    next.expected_start = record.terminal.unwrap().configuration;
    next.target_voters = BTreeSet::from([1, 4, 5]);
    apply(
        &mut restored,
        11,
        EntryPayload::Normal(Request::native_control(NativeControl::Begin {
            specification: next.clone(),
            executor_epoch: 1,
        })),
    )
    .await;
    apply(
        &mut restored,
        12,
        EntryPayload::Normal(Request::native_control(NativeControl::Begin {
            specification,
            executor_epoch: 1,
        })),
    )
    .await;
    let ledger = restored.controlled_ledger().await.unwrap();
    assert_eq!(ledger.high_water, 2);
    assert_eq!(
        ledger.current.unwrap().specification,
        next,
        "terminal handoff permits next operation but rejects old operation resurrection"
    );
}
impl StateMachine for Disk {
    type Error = io::Error;
    fn apply(&mut self, _: GroupId, _: u64, _: &[u8]) -> io::Result<ApplyOut> {
        unreachable!()
    }
    fn snapshot(&self, _: GroupId) -> io::Result<Vec<u8>> {
        unreachable!()
    }
    fn restore(&mut self, _: GroupId, _: &[u8]) -> io::Result<()> {
        unreachable!()
    }
    fn disk_state(&mut self) -> Option<&mut dyn DiskState> {
        Some(self)
    }
}
impl DiskState for Disk {
    fn snapshot_workspace(&self) -> io::Result<std::path::PathBuf> {
        self.db.snapshot_workspace()
    }
    fn checkpoint(&self, dir: &Path) -> io::Result<()> {
        self.db.checkpoint(dir)
    }
    fn inspect_checkpoint(&self, dir: &Path) -> io::Result<Option<Vec<u8>>> {
        self.db.inspect_checkpoint(dir)
    }
    fn durable_applied(&self) -> io::Result<Option<Vec<u8>>> {
        self.db.applied_record()
    }
    fn apply_durable(
        &mut self,
        _: GroupId,
        _: u64,
        data: Option<&[u8]>,
        applied: &AppliedState,
    ) -> io::Result<ApplyOut> {
        let mut batch = WriteBatch::default();
        if let Some(data) = data {
            batch.put(b"application", data);
        }
        self.db.commit(batch, applied)?;
        Ok(ApplyOut::default())
    }
}

#[tokio::test]
async fn controlled_checkpoint_inspection_reads_the_published_database_not_live_ledger() {
    use openraft::RaftSnapshotBuilder;
    let scratch = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/223-ledger");
    std::fs::create_dir_all(&scratch).unwrap();
    let root = tempfile::tempdir_in(scratch).unwrap();
    let disk = Disk {
        db: DiskFsmDb::open(
            root.path().join("live"),
            7,
            DiskFsmOptions::new(b"controlled-test-v1".to_vec()),
        )
        .unwrap(),
    };
    let mut store = StateMachineStore::with_native_options(
        7,
        disk,
        multiraft_store::NativeSmOptions {
            catalog: std::sync::Arc::new(multiraft_store::SnapshotCatalog::new(
                root.path().join("catalog"),
                2,
            )),
            max_snapshot_bytes: 1024 * 1024,
            build_budget: std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
        },
    )
    .unwrap();
    let initial = Membership::new(
        vec![BTreeSet::from([1, 2, 3])],
        BTreeMap::from_iter([1, 2, 3].map(|id| (id, openraft::BasicNode::default()))),
    )
    .unwrap();
    let specification = ReplacementSpec {
        operation: OperationId {
            sequence: 1,
            nonce: [11; 16],
        },
        expected_start: StoredMembership::new(Some(id(0)), initial.clone()),
        target_voters: BTreeSet::from([1, 2, 4]),
        target_generation: 5,
    };
    apply(&mut store, 0, EntryPayload::Membership(initial)).await;
    apply(
        &mut store,
        1,
        EntryPayload::Normal(Request::native_control(NativeControl::Begin {
            specification: specification.clone(),
            executor_epoch: 1,
        })),
    )
    .await;
    let snapshot = store
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();
    apply(
        &mut store,
        2,
        EntryPayload::Normal(Request::native_control(NativeControl::Claim {
            operation: specification.operation,
            executor_epoch: 9,
        })),
    )
    .await;
    let state = store
        .inspect_controlled_checkpoint(&snapshot.meta, snapshot.snapshot.get_ref())
        .await
        .unwrap();
    assert_eq!(state.applied, id(1));
    assert_eq!(state.membership, specification.expected_start);
    assert_eq!(
        state.ledger.current.unwrap().executor_epoch,
        1,
        "published database was frozen before live Claim9"
    );
    assert_eq!(
        store
            .controlled_ledger()
            .await
            .unwrap()
            .current
            .unwrap()
            .executor_epoch,
        9
    );
    let actual_live = store.inspect_controlled_applied().await.unwrap();
    assert_eq!(actual_live.applied, id(2));
    assert_eq!(actual_live.ledger.current.unwrap().executor_epoch, 9);
    let mut descriptor =
        multiraft_store::disk_snapshot::DiskManifest::decode(snapshot.snapshot.get_ref(), 7)
            .unwrap();
    descriptor.source = root.path().join("live");
    assert!(
        store
            .inspect_controlled_checkpoint(&descriptor.meta, &descriptor.encode().unwrap())
            .await
            .is_err(),
        "caller-controlled path cannot substitute the live database"
    );
    let manifest =
        multiraft_store::disk_snapshot::DiskManifest::decode(snapshot.snapshot.get_ref(), 7)
            .unwrap();
    let writable = root.path().join("writable-copy");
    std::fs::create_dir(&writable).unwrap();
    let mut original = BTreeMap::new();
    for file in &manifest.files {
        original.insert(
            file.name.clone(),
            std::fs::read(manifest.source.join(&file.name)).unwrap(),
        );
        std::fs::copy(manifest.source.join(&file.name), writable.join(&file.name)).unwrap();
    }
    let writer = DiskFsmDb::open(
        &writable,
        7,
        DiskFsmOptions::new(b"controlled-test-v1".to_vec()),
    )
    .unwrap();
    let mut wal_record: serde_json::Value =
        serde_json::from_slice(&writer.applied_record().unwrap().unwrap()).unwrap();
    wal_record["control"]["current"]["executor_epoch"] = serde_json::json!(7);
    writer
        .commit(
            WriteBatch::default(),
            &AppliedState::new(1, 1, serde_json::to_vec(&wal_record).unwrap()),
        )
        .unwrap();
    let mut injected = Vec::new();
    for entry in std::fs::read_dir(&writable).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|extension| extension == "log") {
            let destination = manifest.source.join(path.file_name().unwrap());
            assert!(!original.contains_key(path.file_name().unwrap().to_str().unwrap()));
            std::fs::copy(&path, &destination).unwrap();
            injected.push(destination);
        }
    }
    assert!(
        !injected.is_empty(),
        "real writable database produced a WAL"
    );
    for (name, bytes) in original {
        assert_eq!(
            std::fs::read(manifest.source.join(name)).unwrap(),
            bytes,
            "all manifest-listed bytes remain unchanged"
        );
    }
    assert!(
        store
            .inspect_controlled_checkpoint(&snapshot.meta, snapshot.snapshot.get_ref())
            .await
            .is_err(),
        "undeclared WAL cannot change inspected facts relative to transferred files"
    );
    for path in injected {
        std::fs::remove_file(path).unwrap();
    }
    drop(writer);
    let changed = DiskFsmDb::open(
        &manifest.source,
        7,
        DiskFsmOptions::new(b"controlled-test-v1".to_vec()),
    )
    .unwrap();
    let mut record: serde_json::Value =
        serde_json::from_slice(&changed.applied_record().unwrap().unwrap()).unwrap();
    record["last_applied"] = serde_json::to_value(id(99)).unwrap();
    changed
        .commit(
            WriteBatch::default(),
            &AppliedState::new(99, 1, serde_json::to_vec(&record).unwrap()),
        )
        .unwrap();
    drop(changed);
    assert!(
        store
            .inspect_controlled_checkpoint(&snapshot.meta, snapshot.snapshot.get_ref())
            .await
            .is_err(),
        "actual checkpoint applied record must match metadata"
    );
}

#[tokio::test]
async fn actual_applied_inspection_accepts_bootstrap_v1_and_rejects_pristine_or_corrupt_disk() {
    let scratch = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/223-ledger");
    std::fs::create_dir_all(&scratch).unwrap();
    let root = tempfile::tempdir_in(scratch).unwrap();
    let mut store = open(root.path());
    assert!(
        store.inspect_controlled_applied().await.is_err(),
        "pristine store has no durable native identity"
    );
    let membership = Membership::new(
        vec![BTreeSet::from([1, 2, 3])],
        BTreeMap::from_iter([1, 2, 3].map(|id| (id, openraft::BasicNode::default()))),
    )
    .unwrap();
    apply(&mut store, 0, EntryPayload::Membership(membership.clone())).await;
    apply(
        &mut store,
        1,
        EntryPayload::Normal(Request::new(b"bootstrap application")),
    )
    .await;
    let facts = store.inspect_controlled_applied().await.unwrap();
    assert_eq!(facts.applied, id(1));
    assert_eq!(
        facts.membership,
        StoredMembership::new(Some(id(0)), membership)
    );
    assert_eq!(
        facts.ledger,
        multiraft_core::NativeLedger::default(),
        "v1 has no native operation ledger"
    );
    store
        .try_with_fsm(|disk| {
            let mut record: serde_json::Value =
                serde_json::from_slice(&disk.db.applied_record().unwrap().unwrap()).unwrap();
            record["version"] = serde_json::json!(999);
            disk.db
                .commit(
                    WriteBatch::default(),
                    &AppliedState::new(1, 1, serde_json::to_vec(&record).unwrap()),
                )
                .unwrap();
        })
        .await
        .unwrap();
    assert!(
        store.inspect_controlled_applied().await.is_err(),
        "fresh inspection cannot reuse the valid cached applied record after durable corruption"
    );
}
fn open(root: &Path) -> StateMachineStore<Disk> {
    StateMachineStore::new(
        7,
        Disk {
            db: DiskFsmDb::open(root, 7, DiskFsmOptions::new(b"controlled-test-v1".to_vec()))
                .unwrap(),
        },
    )
}
fn id(index: u64) -> LogId {
    LogId::new(LeaderIdOf::<TypeConfig>::new_committed(1, 1), index)
}
async fn apply(
    store: &mut StateMachineStore<Disk>,
    index: u64,
    payload: EntryPayloadOf<TypeConfig>,
) {
    store
        .apply(stream::iter([Ok((
            Entry {
                log_id: id(index),
                payload,
            },
            None,
        ))]))
        .await
        .unwrap();
}

#[tokio::test]
async fn operation_arguments_and_executor_epoch_survive_restart() {
    let scratch = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/223-ledger");
    std::fs::create_dir_all(&scratch).unwrap();
    let root = tempfile::tempdir_in(scratch).unwrap();
    let initial = Membership::new(
        vec![BTreeSet::from([1, 2, 3])],
        BTreeMap::from_iter([1, 2, 3].map(|id| (id, openraft::BasicNode::default()))),
    )
    .unwrap();
    let operation = ReplacementSpec {
        operation: OperationId {
            sequence: 1,
            nonce: [7; 16],
        },
        expected_start: StoredMembership::new(Some(id(0)), initial.clone()),
        target_voters: BTreeSet::from([1, 2, 4]),
        target_generation: 1,
    };
    let mut store = open(root.path());
    apply(&mut store, 0, EntryPayload::Membership(initial)).await;
    apply(
        &mut store,
        1,
        EntryPayload::Normal(Request::new(b"legacy application control bytes")),
    )
    .await;
    apply(
        &mut store,
        2,
        EntryPayload::Normal(Request::native_control(NativeControl::Begin {
            specification: operation.clone(),
            executor_epoch: 1,
        })),
    )
    .await;
    assert_eq!(
        store
            .controlled_ledger()
            .await
            .unwrap()
            .current
            .as_ref()
            .unwrap()
            .specification,
        operation
    );
    drop(store);
    let mut store = open(root.path());
    apply(
        &mut store,
        3,
        EntryPayload::Normal(Request::native_control(NativeControl::Claim {
            operation: operation.operation,
            executor_epoch: 2,
        })),
    )
    .await;
    let mut changed = operation.clone();
    changed.target_voters = BTreeSet::from([1, 2, 5]);
    apply(
        &mut store,
        4,
        EntryPayload::Normal(Request::native_control(NativeControl::Begin {
            specification: changed,
            executor_epoch: 3,
        })),
    )
    .await;
    apply(
        &mut store,
        5,
        EntryPayload::Normal(Request::native_control(NativeControl::Claim {
            operation: operation.operation,
            executor_epoch: 1,
        })),
    )
    .await;
    let record = store.controlled_ledger().await.unwrap().current.unwrap();
    assert_eq!(
        record.specification, operation,
        "same operation cannot change arguments"
    );
    assert_eq!(
        record.executor_epoch, 2,
        "old executor cannot reclaim operation"
    );
}

#[tokio::test]
async fn whole_image_state_machine_refuses_typed_native_control() {
    let mut store = StateMachineStore::new(7, multiraft_fsm::CounterFsm::default());
    let entry = Entry {
        log_id: id(0),
        payload: EntryPayload::Normal(Request::native_control(NativeControl::Claim {
            operation: OperationId {
                sequence: 1,
                nonce: [1; 16],
            },
            executor_epoch: 1,
        })),
    };
    let result = store.apply(stream::iter([Ok((entry, None))])).await;
    assert_eq!(
        result.unwrap_err().kind(),
        io::ErrorKind::Unsupported,
        "unsupported whole-image mode must reject before application dispatch"
    );
}

#[tokio::test]
async fn direct_apply_refuses_mixed_control_before_durable_mutation() {
    let scratch = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/223-ledger");
    std::fs::create_dir_all(&scratch).unwrap();
    let root = tempfile::tempdir_in(scratch).unwrap();
    let mut store = open(root.path());
    let request = Request {
        data: vec![1],
        control: Some(NativeControl::Claim {
            operation: OperationId {
                sequence: 1,
                nonce: [1; 16],
            },
            executor_epoch: 1,
        }),
    };
    let result = store
        .apply(stream::iter([Ok((
            Entry {
                log_id: id(0),
                payload: EntryPayload::Normal(request),
            },
            None,
        ))]))
        .await;
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
    assert!(
        store.applied_state().await.unwrap().0.is_none(),
        "invalid direct request must not advance durable applied state"
    );
}
