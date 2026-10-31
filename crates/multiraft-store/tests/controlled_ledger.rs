//! Native operation identity and executor fencing survive real durable-applied recovery.
use futures::stream;
use multiraft_core::{
    typ::{Entry, LogId, StoredMembership},
    NativeControl, OperationId, ReplacementSpec, Request, TypeConfig,
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
