//! Native replacement with three parent owners and a separately owned target process.
#[path = "startup_authority_support/catalog.rs"]
mod catalog;
#[path = "controlled_remote_process_support/child.rs"]
mod child;
#[allow(dead_code)]
#[path = "controlled_protocol_support/disk.rs"]
mod disk;
#[path = "controlled_remote_process_support/pipe.rs"]
mod pipe;
#[path = "controlled_remote_process_support/source.rs"]
mod source;
#[path = "controlled_remote_process_support/trajectory.rs"]
mod trajectory;

use multiraft_net::controlled_startup as authority;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independently_owned_target_installs_expired_source_seed_and_completes_native_replacement()
{
    trajectory::run().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "private self-spawned target process"]
async fn controlled_remote_target_child() {
    child::run().await;
}
