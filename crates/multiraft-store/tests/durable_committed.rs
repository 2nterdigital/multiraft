//! A volatile FSM must recover the saved commit frontier without a later write or peer.
use multiraft_core::typ::{Entry, LogId};
use multiraft_core::{FileLogSyncLevel, TypeConfig};
use multiraft_store::FileLogStoreOf;
use openraft::alias::LeaderIdOf;
use openraft::entry::RaftEntry;
use openraft::storage::{RaftLogStorage, RaftLogStorageExt};
use openraft::vote::RaftLeaderIdExt;

#[tokio::test]
async fn data_and_all_persist_committed_without_a_following_storage_operation() {
    for level in [FileLogSyncLevel::Data, FileLogSyncLevel::All] {
        let root = tempfile::tempdir().unwrap();
        let mut store = FileLogStoreOf::open_with_options(root.path(), 0, level).unwrap();
        let id = |index| LogId::new(LeaderIdOf::<TypeConfig>::new_committed(1, 1), index);
        store
            .blocking_append(
                (0..=2)
                    .map(|index| Entry::new_blank(id(index)))
                    .collect::<Vec<_>>(),
            )
            .await
            .unwrap();
        store.save_committed(Some(id(2))).await.unwrap();
        drop(store);
        let mut reopened = FileLogStoreOf::open_with_options(root.path(), 0, level).unwrap();
        assert_eq!(
            reopened.read_committed().await.unwrap(),
            Some(id(2)),
            "saved commit was only volatile at {level:?}"
        );
    }
}
