//! Command-line interface definition (clap derive).
//!
//! The tree mirrors `cast`: a small set of noun subcommands (`node`, `chunk`,
//! `wallet`) each with verbs. Global flags live on the top-level [`Cli`].

use clap::{Args, Parser, Subcommand, ValueEnum};

/// dipper - a cast-like CLI for Ethereum Swarm over a vertex node's gRPC API.
#[derive(Debug, Parser)]
#[command(name = "dipper", version, about, long_about = None)]
pub(crate) struct Cli {
    /// gRPC endpoint of the vertex node.
    #[arg(long, global = true, default_value = "http://127.0.0.1:1635")]
    pub(crate) endpoint: String,

    /// Target network. Selects the contract address book and expected chain id
    /// for on-chain batch operations.
    #[arg(long, global = true, value_enum, default_value_t = Network::Gnosis)]
    pub(crate) network: Network,

    /// Ethereum RPC endpoint for on-chain operations (Gnosis / Sepolia).
    ///
    /// Required by the `batch` subcommands; ignored by node/chunk/upload ops.
    #[arg(long, global = true)]
    pub(crate) rpc_url: Option<String>,

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
    /// On-chain postage batch operations (create / topup / dilute / info).
    Batch {
        #[command(subcommand)]
        command: BatchCommand,
    },
    /// Upload a file, directory, or `.tar.gz` as a mantaray manifest.
    Upload {
        /// Input path: a single file, a directory tree, or a `.tar.gz` archive.
        path: String,

        /// Postage batch id (hex, 32 bytes); if omitted, discovered from chain (needs `--rpc-url`).
        #[arg(long)]
        batch_id: Option<String>,

        /// Batch depth; ignored when `--batch-id` is omitted.
        #[arg(long)]
        depth: Option<u8>,

        /// Bucket depth (must be 16 to match bee); ignored when `--batch-id` is omitted.
        #[arg(long, default_value_t = 16)]
        bucket_depth: u8,

        /// Mark the batch immutable; must match the on-chain batch. Ignored when `--batch-id` omitted.
        #[arg(long)]
        immutable: bool,

        /// Website index document served for `bzz://<root>/` (directory /
        /// archive uploads). Defaults to `index.html`.
        #[arg(long)]
        index_document: Option<String>,

        /// Website error document served for missing paths.
        #[arg(long)]
        error_document: Option<String>,

        #[command(flatten)]
        signer: SignerArgs,
    },
    /// Download a file or whole manifest tree by its root reference.
    Download {
        /// Manifest (or raw file) root reference (hex, `0x` optional).
        root: String,

        /// Manifest path to a single file. If omitted, the whole tree is
        /// reconstructed under `--out`.
        path: Option<String>,

        /// Output target: a file when `[path]` is given, otherwise a directory
        /// the tree is recreated beneath.
        #[arg(short, long)]
        out: Option<String>,
    },
    /// List the entries of a manifest by its root reference.
    Ls {
        /// Manifest root reference (hex, `0x` optional).
        root: String,

        /// Include a size column (one extra RPC per entry).
        #[arg(short, long)]
        long: bool,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum BatchCommand {
    /// Discover and list every postage batch the signer owns, with on-chain state and usage.
    List {
        #[command(flatten)]
        signer: SignerArgs,
    },
    /// Create a postage batch: BZZ.approve then PostageStamp.createBatch.
    Create {
        /// Initial balance per chunk, in BZZ (decimal string, 16-decimal).
        #[arg(long)]
        amount: String,

        /// Batch depth (number of chunks = 2^depth).
        #[arg(long)]
        depth: u8,

        /// Bucket depth (default 16 to match bee).
        #[arg(long, default_value_t = 16)]
        bucket_depth: u8,

        /// Immutable batch (cannot be diluted).
        #[arg(long)]
        immutable: bool,

        /// Batch owner (defaults to the signer's address).
        #[arg(long)]
        owner: Option<String>,

        /// 32-byte nonce (hex); random if omitted.
        #[arg(long)]
        nonce: Option<String>,

        #[command(flatten)]
        signer: SignerArgs,
    },
    /// Top up an existing batch: PostageStamp.topUp(batchId, amountPerChunk).
    Topup {
        /// Batch id (hex, 32 bytes).
        #[arg(long)]
        batch_id: String,

        /// Top-up balance per chunk, in BZZ (decimal string, 16-decimal).
        #[arg(long)]
        amount: String,

        #[command(flatten)]
        signer: SignerArgs,
    },
    /// Increase batch depth (a.k.a. dilute): PostageStamp.increaseDepth.
    Dilute {
        /// Batch id (hex, 32 bytes).
        #[arg(long)]
        batch_id: String,

        /// New depth; must exceed the current batch depth.
        #[arg(long)]
        depth: u8,

        #[command(flatten)]
        signer: SignerArgs,
    },
    /// Read batch state: owner / depth / bucketDepth / immutable / balance.
    Info {
        /// Batch id (hex, 32 bytes).
        #[arg(long)]
        batch_id: String,
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
