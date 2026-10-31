//! Controlled Groups enter through owned handles from their first native startup.
#![allow(dead_code)]
#[path = "startup_authority_support/catalog.rs"]
mod catalog;
#[path = "controlled_protocol_support/disk.rs"]
mod disk;
mod owned_controlled_support;
use multiraft_net::controlled_startup as authority;
use owned_controlled_support::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owned_bootstrap_exports_actual_basis_and_retained_handles_close() {
    let fixture = Fixture::fresh().await;
    let mut membership = None;
    for group in &fixture.groups {
        let proof = group
            .genesis_basis(deadline())
            .await
            .expect("actual durable Genesis basis");
        assert_eq!(proof.group_id(), GROUP);
        assert_eq!(proof.generation(), group.generation());
        assert_eq!(
            proof
                .configuration()
                .membership()
                .voter_ids()
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        if let Some(expected) = &membership {
            assert_eq!(proof.configuration(), expected);
        }
        membership = Some(proof.configuration().clone());
    }
    let retained = fixture.groups[0].clone();
    fixture.stop().await;
    assert!(
        retained.receipt(deadline()).await.is_err(),
        "retained handle cannot bypass closed runtime"
    );
}
