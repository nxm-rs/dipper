//! dipper — a cast-like CLI for Ethereum Swarm.
//!
//! Phase 1: a vertical slice that talks to a `vertex` node over gRPC for node
//! status/topology and chunk download/upload, plus offline wallet/key loading.
//! Network/chain operations (phase 2) and mantaray/file ops (phase 3) come
//! later; the `--network` flag is accepted now but not yet used.

mod chunkops;
mod cli;
mod commands;
mod proto;
mod rpc;
mod wallet;

use anyhow::Result;
use clap::Parser;

use cli::{ChunkCommand, Cli, Command, NodeCommand, WalletCommand};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let endpoint = &cli.endpoint;

    match cli.command {
        Command::Node { command } => match command {
            NodeCommand::Status => commands::node::status(endpoint).await?,
            NodeCommand::Topology => commands::node::topology(endpoint).await?,
        },
        Command::Chunk { command } => match command {
            ChunkCommand::Download { addr, out, raw } => {
                commands::chunk::download(endpoint, &addr, out.as_deref(), raw).await?
            }
            ChunkCommand::Upload {
                file,
                stdin,
                batch_id,
                depth,
                bucket_depth,
                signer,
            } => {
                commands::chunk::upload(
                    endpoint,
                    file.as_deref(),
                    stdin,
                    &batch_id,
                    depth,
                    bucket_depth,
                    &signer,
                )
                .await?
            }
        },
        Command::Wallet { command } => match command {
            WalletCommand::Address { signer } => commands::wallet::address(&signer)?,
        },
    }

    Ok(())
}
