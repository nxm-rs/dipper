//! dipper - a cast-like CLI for Ethereum Swarm, talking to a vertex node over gRPC.

mod chain;
mod chunkops;
mod cli;
mod commands;
mod download;
mod manifest;
mod proto;
mod rpc;
mod store;
mod usage;
mod wallet;

use anyhow::{Context, Result};
use clap::Parser;

use cli::{BatchCommand, ChunkCommand, Cli, Command, NodeCommand, PeerCommand, WalletCommand};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let endpoint = &cli.endpoint;
    let network = cli.network;

    // RPC URL is only required by `batch` ops; resolve lazily there.
    let require_rpc_url = || -> Result<&str> {
        cli.rpc_url
            .as_deref()
            .context("--rpc-url is required for on-chain batch operations")
    };

    match cli.command {
        Command::Node { command } => match command {
            NodeCommand::Status => commands::node::status(endpoint).await?,
            NodeCommand::Topology => commands::node::topology(endpoint).await?,
        },
        Command::Peer { command } => match command {
            PeerCommand::Add { multiaddr } => commands::peer::add(endpoint, &multiaddr).await?,
            PeerCommand::Remove { overlay } => commands::peer::remove(endpoint, &overlay).await?,
            PeerCommand::List { long, all } => commands::peer::list(endpoint, long, all).await?,
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
        Command::Batch { command } => match command {
            BatchCommand::List { signer } => {
                chain::list(require_rpc_url()?, network, endpoint, &signer).await?
            }
            BatchCommand::Create {
                amount,
                depth,
                bucket_depth,
                immutable,
                owner,
                nonce,
                signer,
            } => {
                chain::create(
                    require_rpc_url()?,
                    network,
                    &amount,
                    depth,
                    bucket_depth,
                    immutable,
                    owner.as_deref(),
                    nonce.as_deref(),
                    &signer,
                )
                .await?
            }
            BatchCommand::Topup {
                batch_id,
                amount,
                signer,
            } => chain::topup(require_rpc_url()?, network, &batch_id, &amount, &signer).await?,
            BatchCommand::Dilute {
                batch_id,
                depth,
                signer,
            } => chain::dilute(require_rpc_url()?, network, &batch_id, depth, &signer).await?,
            BatchCommand::Info { batch_id } => {
                chain::info(require_rpc_url()?, network, &batch_id).await?
            }
        },
        Command::Upload {
            path,
            batch_id,
            depth,
            bucket_depth,
            immutable,
            index_document,
            error_document,
            signer,
        } => {
            manifest::upload(
                endpoint,
                network,
                cli.rpc_url.as_deref(),
                &path,
                batch_id.as_deref(),
                depth,
                bucket_depth,
                immutable,
                index_document.as_deref(),
                error_document.as_deref(),
                &signer,
            )
            .await?
        }
        Command::Download { root, path, out } => {
            manifest::download(endpoint, &root, path.as_deref(), out.as_deref()).await?
        }
        Command::Ls { root, long } => manifest::ls(endpoint, &root, long).await?,
    }

    Ok(())
}
