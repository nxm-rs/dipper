//! Command-line interface definition (clap derive).
//!
//! The tree mirrors `cast`: a small set of noun subcommands (`node`, `chunk`,
//! `wallet`) each with verbs. Global flags live on the top-level [`Cli`].

use clap::{Args, Parser, Subcommand, ValueEnum};

/// dipper — a cast-like CLI for Ethereum Swarm over a vertex node's gRPC API.
#[derive(Debug, Parser)]
#[command(name = "dipper", version, about, long_about = None)]
pub(crate) struct Cli {
    /// gRPC endpoint of the vertex node.
    #[arg(long, global = true, default_value = "http://127.0.0.1:1635")]
    pub(crate) endpoint: String,

    /// Target network. Accepted now, used in later phases (chain ops).
    #[arg(long, global = true, value_enum, default_value_t = Network::Gnosis)]
    pub(crate) network: Network,

    #[command(subcommand)]
    pub(crate) command: Command,
}

/// Swarm network selector. Not yet wired into behaviour (phase 2+).
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum Network {
    /// Gnosis Chain (mainnet Swarm).
    Gnosis,
    /// Sepolia testnet.
    Sepolia,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Inspect the local Swarm node.
    Node {
        #[command(subcommand)]
        command: NodeCommand,
    },
    /// Download, upload, and inspect chunks.
    Chunk {
        #[command(subcommand)]
        command: ChunkCommand,
    },
    /// Wallet / key utilities.
    Wallet {
        #[command(subcommand)]
        command: WalletCommand,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum NodeCommand {
    /// Show node status (overlay, depth, peer counts).
    Status,
    /// Show Kademlia topology, bin by bin.
    Topology,
}

#[derive(Debug, Subcommand)]
pub(crate) enum ChunkCommand {
    /// Retrieve a chunk by address from the Swarm network.
    Download {
        /// Chunk address (hex, with or without 0x prefix; 64 hex chars).
        addr: String,

        /// Write the chunk payload here instead of stdout.
        #[arg(long)]
        out: Option<String>,

        /// Write the raw wire body (8-byte span + payload) instead of just the
        /// payload.
        #[arg(long)]
        raw: bool,
    },
    /// Chunk + stamp a single content chunk locally and push it to the network.
    Upload {
        /// Input file. Omit and pass --stdin to read from standard input.
        file: Option<String>,

        /// Read the chunk data from standard input.
        #[arg(long)]
        stdin: bool,

        /// Postage batch id (hex, 32 bytes).
        #[arg(long)]
        batch_id: String,

        /// Batch depth.
        #[arg(long)]
        depth: u8,

        /// Bucket depth.
        #[arg(long)]
        bucket_depth: u8,

        #[command(flatten)]
        signer: SignerArgs,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum WalletCommand {
    /// Load a signer and print its Ethereum address.
    Address {
        #[command(flatten)]
        signer: SignerArgs,
    },
}

/// Key material for any command that needs a signer.
///
/// Exactly one of `--private-key` / `--keystore` is required (enforced by the
/// `ArgGroup`). The password sources for a keystore are, in order: `--password`,
/// the `DIPPER_KEYSTORE_PASSWORD` env var, or an interactive prompt.
#[derive(Debug, Args)]
#[command(group = clap::ArgGroup::new("key_source").required(true).multiple(false).args(["private_key", "keystore"]))]
pub(crate) struct SignerArgs {
    /// Raw private key (hex, 32 bytes, with or without 0x prefix).
    #[arg(long)]
    pub(crate) private_key: Option<String>,

    /// Path to a cast / EIP-2335 JSON keystore.
    #[arg(long)]
    pub(crate) keystore: Option<String>,

    /// Keystore password (otherwise taken from DIPPER_KEYSTORE_PASSWORD or
    /// prompted). Ignored with --private-key.
    #[arg(long)]
    pub(crate) password: Option<String>,
}
