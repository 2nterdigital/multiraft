//! Deterministic layout artifact generator for the slot demo and acceptance harness.
//!
//! Reads a placement input JSON, writes the canonical v2 layout artifact, and
//! prints both digests as JSON on stdout.
use clap::Parser;
use multiraft_slot::{plan_layout, PlacementInput};
use serde_json::json;
use std::path::PathBuf;

#[derive(Parser)]
struct Args {
    /// Placement input: cluster_id, slot_count, group_id_base, nodes.
    #[arg(long)]
    input: PathBuf,
    /// Artifact destination; existing files are never overwritten.
    #[arg(long)]
    output: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let input: PlacementInput = serde_json::from_slice(&std::fs::read(&args.input)?)?;
    let layout = plan_layout(&input)?;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&args.output)?;
    std::io::Write::write_all(&mut file, &layout.to_canonical_json())?;
    file.sync_all()?;
    println!(
        "{}",
        json!({
            "layout": args.output,
            "route_digest": layout.route_digest().to_hex(),
            "layout_digest": layout.layout_digest().to_hex(),
        })
    );
    Ok(())
}
