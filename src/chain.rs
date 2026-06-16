//! On-chain postage batch operations (phase 2).
//!
//! Drives the Gnosis/Sepolia `PostageStamp` contract through alloy: create a
//! batch (`approve` + `createBatch`), top it up, increase its depth (dilute),
//! and read its state. The provider is built with the recommended fillers and a
//! `PrivateKeySigner` wallet; reads need no signer.
//!
//! `nectar_contracts::IPostageStamp` is read-only, so dipper declares its own
//! write surface below and reuses `nectar_contracts::IERC20` for token approval.

use alloy_contract::CallBuilder;
use alloy_primitives::{
    Address, B256, U256, keccak256,
    utils::{ParseUnits, Unit},
};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_types_eth::Filter;
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::{SolEvent, SolValue, sol};
use anyhow::{Context, Result, bail};

use crate::cli::{Network, SignerArgs};
use crate::wallet;

// Reuse nectar's complete ERC-20 interface (approve/allowance/balanceOf/
// decimals) rather than redeclaring it.
pub(crate) use nectar_contracts::IERC20;

sol! {
    /// PostageStamp write + lookup surface dipper drives directly.
    ///
    /// Signatures and event field order are taken verbatim from the
    /// authoritative `storage-incentives/src/PostageStamp.sol`. Read methods
    /// that `nectar_contracts::IPostageStamp` already exposes are duplicated
    /// here only where dipper needs them on one interface object.
    #[derive(Debug)]
    #[allow(missing_docs)]
    interface IPostageStampWrite {
        function createBatch(
            address _owner,
            uint256 _initialBalancePerChunk,
            uint8 _depth,
            uint8 _bucketDepth,
            bytes32 _nonce,
            bool _immutable
        ) external returns (bytes32);

        function topUp(bytes32 _batchId, uint256 _topupAmountPerChunk) external;
        function increaseDepth(bytes32 _batchId, uint8 _newDepth) external;

        // reads
        function bzzToken() external view returns (address);
        function remainingBalance(bytes32 _batchId) external view returns (uint256);
        function minimumInitialBalancePerChunk() external view returns (uint256);
        function batches(bytes32 _batchId) external view returns (
            address owner,
            uint8 depth,
            uint8 bucketDepth,
            bool immutableFlag,
            uint256 normalisedBalance,
            uint256 lastUpdatedBlockNumber
        );

        // events - field order MUST match the contract.
        event BatchCreated(
            bytes32 indexed batchId,
            uint256 totalAmount,
            uint256 normalisedBalance,
            address owner,
            uint8 depth,
            uint8 bucketDepth,
            bool immutableFlag
        );
        event BatchTopUp(bytes32 indexed batchId, uint256 topupAmount, uint256 normalisedBalance);
        event BatchDepthIncrease(bytes32 indexed batchId, uint8 newDepth, uint256 normalisedBalance);
    }
}

/// BZZ uses 16 decimals (not 18); never use `parse_ether` for batch amounts.
pub(crate) const BZZ_DECIMALS: u8 = 16;

/// Resolved contract addresses and expected chain id for a network.
pub(crate) struct ChainAddrs {
    /// PostageStamp contract address.
    pub(crate) postage: Address,
    /// BZZ ERC-20 token address.
    pub(crate) bzz: Address,
    /// Expected EVM chain id (100 Gnosis, 11155111 Sepolia).
    pub(crate) chain_id: u64,
    /// PostageStamp deploy block: the floor for a `BatchCreated` log scan.
    pub(crate) postage_deploy_block: u64,
}

/// Address book for the supported networks.
pub(crate) const fn addrs_for(net: Network) -> ChainAddrs {
    use nectar_contracts::{mainnet, testnet};
    match net {
        Network::Gnosis => ChainAddrs {
            postage: mainnet::POSTAGE_STAMP.address,
            bzz: mainnet::BZZ_TOKEN.address,
            chain_id: 100,
            postage_deploy_block: mainnet::POSTAGE_STAMP.block,
        },
        Network::Sepolia => ChainAddrs {
            postage: testnet::POSTAGE_STAMP.address,
            bzz: testnet::BZZ_TOKEN.address,
            chain_id: 11155111,
            postage_deploy_block: testnet::POSTAGE_STAMP.block,
        },
    }
}

/// The BZZ unit (16 decimals) used for parsing/formatting human amounts.
const fn bzz_unit() -> Unit {
    // 16 is well within the valid 0..=77 range, so this never fails.
    Unit::new(BZZ_DECIMALS).expect("BZZ_DECIMALS (16) is a valid unit")
}

/// Parse a human BZZ amount (decimal string, 16-decimal) into base units.
fn parse_bzz(amount: &str) -> Result<U256> {
    Ok(ParseUnits::parse_units(amount, bzz_unit())
        .with_context(|| format!("invalid BZZ amount: {amount}"))?
        .into())
}

/// Format a base-unit BZZ value as a human decimal string.
fn format_bzz(value: U256) -> String {
    ParseUnits::from(value).format_units(bzz_unit())
}

/// Parse a 32-byte hex value (0x-prefix optional) into a [`B256`].
fn parse_b256(s: &str, what: &str) -> Result<B256> {
    let raw = s.strip_prefix("0x").unwrap_or(s);
    let bytes = hex::decode(raw).with_context(|| format!("{what} is not valid hex"))?;
    if bytes.len() != 32 {
        bail!(
            "{what} must be 32 bytes (64 hex chars), got {}",
            bytes.len()
        );
    }
    Ok(B256::from_slice(&bytes))
}

/// Total BZZ a batch costs at creation / top-up: `perChunk * 2^depth`.
///
/// The contract computes `_initialBalancePerChunk * (1 << _depth)`; the
/// `bucketDepth` never enters the cost, it only constrains validity.
fn total_amount(per_chunk: U256, depth: u8) -> U256 {
    per_chunk * (U256::from(1u8) << depth)
}

/// Build a signed provider from `rpc_url` + the wallet, and verify the chain id
/// matches the selected network.
async fn signed_provider(
    rpc_url: &str,
    addrs: &ChainAddrs,
    signer: PrivateKeySigner,
) -> Result<impl Provider> {
    let provider = ProviderBuilder::new()
        .wallet(signer)
        .connect(rpc_url)
        .await
        .with_context(|| format!("failed to connect to RPC at {rpc_url}"))?;
    verify_chain_id(&provider, addrs).await?;
    Ok(provider)
}

/// Build a read-only provider (no wallet) and verify the chain id.
pub(crate) async fn read_provider(rpc_url: &str, addrs: &ChainAddrs) -> Result<impl Provider> {
    let provider = ProviderBuilder::new()
        .connect(rpc_url)
        .await
        .with_context(|| format!("failed to connect to RPC at {rpc_url}"))?;
    verify_chain_id(&provider, addrs).await?;
    Ok(provider)
}

/// Bail if the connected chain id is not the one the `--network` flag selected.
async fn verify_chain_id<P: Provider>(provider: &P, addrs: &ChainAddrs) -> Result<()> {
    let connected = provider
        .get_chain_id()
        .await
        .context("failed to query RPC chain id")?;
    if connected != addrs.chain_id {
        bail!(
            "RPC chain id {connected} does not match the selected network (expected {})",
            addrs.chain_id
        );
    }
    Ok(())
}

/// Ensure `owner` has approved at least `amount` BZZ to the PostageStamp
/// contract, sending an `approve` only when the current allowance is short.
async fn ensure_allowance<P: Provider>(
    provider: &P,
    addrs: &ChainAddrs,
    owner: Address,
    amount: U256,
) -> Result<()> {
    let current = CallBuilder::new_sol(
        provider,
        &addrs.bzz,
        &IERC20::allowanceCall {
            owner,
            spender: addrs.postage,
        },
    )
    .call()
    .await
    .context("failed to read BZZ allowance")?;

    if current >= amount {
        println!("Allowance already sufficient ({} BZZ)", format_bzz(current));
        return Ok(());
    }

    println!("Approving {} BZZ to PostageStamp...", format_bzz(amount));
    let receipt = CallBuilder::new_sol(
        provider,
        &addrs.bzz,
        &IERC20::approveCall {
            spender: addrs.postage,
            amount,
        },
    )
    .send()
    .await
    .context("approve transaction failed to send")?
    .get_receipt()
    .await
    .context("approve transaction failed to confirm")?;

    if !receipt.status() {
        bail!(
            "approve transaction reverted (tx {:#x})",
            receipt.transaction_hash
        );
    }
    println!("  approve tx: {:#x}", receipt.transaction_hash);
    Ok(())
}

/// `dipper batch create` - BZZ.approve then PostageStamp.createBatch.
///
/// Reads the authoritative batch id from the `BatchCreated` receipt log and
/// prints it (0x-prefixed) on success, alongside the locally computed id for
/// cross-check.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn create(
    rpc_url: &str,
    network: Network,
    amount: &str,
    depth: u8,
    bucket_depth: u8,
    immutable: bool,
    owner: Option<&str>,
    nonce: Option<&str>,
    signer_args: &SignerArgs,
) -> Result<()> {
    let addrs = addrs_for(network);
    let signer = wallet::load_signer(signer_args)?;
    let sender = signer.address();

    let owner = match owner {
        Some(s) => s.parse::<Address>().context("invalid owner address")?,
        None => sender,
    };
    let nonce = match nonce {
        Some(s) => parse_b256(s, "nonce")?,
        None => B256::random(),
    };

    let per_chunk = parse_bzz(amount)?;
    let total = total_amount(per_chunk, depth);

    let provider = signed_provider(rpc_url, &addrs, signer).await?;

    // BZZ.transferFrom inside createBatch needs an allowance for `total`.
    ensure_allowance(&provider, &addrs, sender, total).await?;

    println!(
        "Creating batch: {} BZZ/chunk, depth {depth}, bucket depth {bucket_depth}, total {} BZZ",
        format_bzz(per_chunk),
        format_bzz(total),
    );
    let receipt = CallBuilder::new_sol(
        &provider,
        &addrs.postage,
        &IPostageStampWrite::createBatchCall {
            _owner: owner,
            _initialBalancePerChunk: per_chunk,
            _depth: depth,
            _bucketDepth: bucket_depth,
            _nonce: nonce,
            _immutable: immutable,
        },
    )
    .send()
    .await
    .context("createBatch transaction failed to send")?
    .get_receipt()
    .await
    .context("createBatch transaction failed to confirm")?;

    if !receipt.status() {
        bail!(
            "createBatch transaction reverted (tx {:#x})",
            receipt.transaction_hash
        );
    }

    // batchId = keccak256(abi.encode(msg.sender, nonce)); read the authoritative
    // value from the BatchCreated log and verify it against the local compute.
    let local_id = keccak256((sender, nonce).abi_encode());
    let batch_id = receipt
        .logs()
        .iter()
        .find_map(|log| log.log_decode::<IPostageStampWrite::BatchCreated>().ok())
        .map(|log| log.inner.data.batchId)
        .context("BatchCreated event not found in transaction receipt")?;

    println!("Batch created");
    println!("  tx hash:  {:#x}", receipt.transaction_hash);
    println!("  batch id: {batch_id:#x}");
    if batch_id != local_id {
        println!("  warning: locally computed id {local_id:#x} differs from on-chain id");
    }
    Ok(())
}

/// `dipper batch topup` - PostageStamp.topUp(batchId, amountPerChunk).
///
/// The BZZ cost is `amountPerChunk * 2^depth`, using the batch's *stored* depth
/// (read on-chain), not any CLI value.
pub(crate) async fn topup(
    rpc_url: &str,
    network: Network,
    batch_id: &str,
    amount: &str,
    signer_args: &SignerArgs,
) -> Result<()> {
    let addrs = addrs_for(network);
    let signer = wallet::load_signer(signer_args)?;
    let sender = signer.address();
    let batch_id = parse_b256(batch_id, "batch id")?;
    let per_chunk = parse_bzz(amount)?;

    let provider = signed_provider(rpc_url, &addrs, signer).await?;

    // The cost multiplier is the batch's stored depth, not the bucket depth.
    let batch = CallBuilder::new_sol(
        &provider,
        &addrs.postage,
        &IPostageStampWrite::batchesCall { _batchId: batch_id },
    )
    .call()
    .await
    .context("failed to read batch state")?;
    if batch.owner == Address::ZERO {
        bail!("batch {batch_id:#x} does not exist");
    }
    let total = total_amount(per_chunk, batch.depth);

    ensure_allowance(&provider, &addrs, sender, total).await?;

    println!(
        "Topping up batch {batch_id:#x}: {} BZZ/chunk, depth {}, total {} BZZ",
        format_bzz(per_chunk),
        batch.depth,
        format_bzz(total),
    );
    let receipt = CallBuilder::new_sol(
        &provider,
        &addrs.postage,
        &IPostageStampWrite::topUpCall {
            _batchId: batch_id,
            _topupAmountPerChunk: per_chunk,
        },
    )
    .send()
    .await
    .context("topUp transaction failed to send")?
    .get_receipt()
    .await
    .context("topUp transaction failed to confirm")?;

    if !receipt.status() {
        bail!(
            "topUp transaction reverted (tx {:#x})",
            receipt.transaction_hash
        );
    }
    println!("Batch topped up");
    println!("  tx hash: {:#x}", receipt.transaction_hash);
    Ok(())
}

/// `dipper batch dilute` - PostageStamp.increaseDepth(batchId, newDepth).
///
/// No BZZ is transferred (no approve), only the depth is raised.
pub(crate) async fn dilute(
    rpc_url: &str,
    network: Network,
    batch_id: &str,
    depth: u8,
    signer_args: &SignerArgs,
) -> Result<()> {
    let addrs = addrs_for(network);
    let signer = wallet::load_signer(signer_args)?;
    let batch_id = parse_b256(batch_id, "batch id")?;

    let provider = signed_provider(rpc_url, &addrs, signer).await?;

    println!("Increasing depth of batch {batch_id:#x} to {depth}...");
    let receipt = CallBuilder::new_sol(
        &provider,
        &addrs.postage,
        &IPostageStampWrite::increaseDepthCall {
            _batchId: batch_id,
            _newDepth: depth,
        },
    )
    .send()
    .await
    .context("increaseDepth transaction failed to send")?
    .get_receipt()
    .await
    .context("increaseDepth transaction failed to confirm")?;

    if !receipt.status() {
        bail!(
            "increaseDepth transaction reverted (tx {:#x})",
            receipt.transaction_hash
        );
    }
    println!("Batch diluted");
    println!("  tx hash: {:#x}", receipt.transaction_hash);
    Ok(())
}

/// Block window per `eth_getLogs` page.
const LOG_PAGE_BLOCKS: u64 = 50_000;

/// A postage batch discovered on-chain, reconciled against the live `batches` view.
#[derive(Debug, Clone)]
pub(crate) struct DiscoveredBatch {
    /// The batch id.
    pub(crate) batch_id: B256,
    /// Current owner.
    pub(crate) owner: Address,
    /// Current depth (reflects any dilution).
    pub(crate) depth: u8,
    /// Bucket depth.
    pub(crate) bucket_depth: u8,
    /// Whether the batch is immutable.
    pub(crate) immutable: bool,
    /// Remaining balance per chunk, in base BZZ units.
    pub(crate) remaining_per_chunk: U256,
    /// Whether the live view still reports the batch as existing.
    pub(crate) live: bool,
}

/// Discover every postage batch owned by `owner` by paging `BatchCreated` logs.
pub(crate) async fn discover_batches<P: Provider>(
    provider: &P,
    addrs: &ChainAddrs,
    owner: Address,
) -> Result<Vec<DiscoveredBatch>> {
    let head = provider
        .get_block_number()
        .await
        .context("failed to read chain head block")?;

    let created_sig = IPostageStampWrite::BatchCreated::SIGNATURE_HASH;

    // Collect candidate batch ids whose creation owner matches, in creation
    // order, de-duplicated (a batch is created once, but guard anyway).
    let mut seen: std::collections::BTreeSet<B256> = std::collections::BTreeSet::new();
    let mut ordered: Vec<B256> = Vec::new();

    let mut from = addrs.postage_deploy_block;
    while from <= head {
        let to = (from + LOG_PAGE_BLOCKS - 1).min(head);

        let filter = Filter::new()
            .address(addrs.postage)
            .event_signature(created_sig)
            .from_block(from)
            .to_block(to);

        let logs = provider.get_logs(&filter).await.with_context(|| {
            format!("failed to fetch BatchCreated logs for blocks {from}..={to}")
        })?;

        for log in logs {
            let decoded = match log.log_decode::<IPostageStampWrite::BatchCreated>() {
                Ok(decoded) => decoded,
                // A non-decodable log under this signature is not ours; skip it.
                Err(_) => continue,
            };
            let event = decoded.inner.data;
            if event.owner == owner && seen.insert(event.batchId) {
                ordered.push(event.batchId);
            }
        }

        from = to + 1;
    }

    // Reconcile each candidate against the live view to pick up dilution and
    // balance changes the creation log cannot reflect.
    let mut batches = Vec::with_capacity(ordered.len());
    for batch_id in ordered {
        let view = CallBuilder::new_sol(
            provider,
            &addrs.postage,
            &IPostageStampWrite::batchesCall { _batchId: batch_id },
        )
        .call()
        .await
        .with_context(|| format!("failed to read live state for batch {batch_id:#x}"))?;

        let live = view.owner != Address::ZERO;
        let remaining = if live {
            CallBuilder::new_sol(
                provider,
                &addrs.postage,
                &IPostageStampWrite::remainingBalanceCall { _batchId: batch_id },
            )
            .call()
            .await
            .with_context(|| format!("failed to read remaining balance for batch {batch_id:#x}"))?
        } else {
            U256::ZERO
        };

        batches.push(DiscoveredBatch {
            batch_id,
            owner: if live { view.owner } else { owner },
            depth: view.depth,
            bucket_depth: view.bucketDepth,
            immutable: view.immutableFlag,
            remaining_per_chunk: remaining,
            live,
        });
    }

    Ok(batches)
}

/// `dipper batch list`: discover and print every batch the signer owns, with usage if available.
pub(crate) async fn list(
    rpc_url: &str,
    network: Network,
    endpoint: &str,
    signer_args: &SignerArgs,
) -> Result<()> {
    let addrs = addrs_for(network);
    let signer = wallet::load_signer(signer_args)?;
    let owner = signer.address();

    let provider = read_provider(rpc_url, &addrs).await?;
    let batches = discover_batches(&provider, &addrs, owner).await?;

    if batches.is_empty() {
        println!("No batches found for owner {owner:#x}");
        return Ok(());
    }

    // Best-effort: open each batch's usage snapshot over the node to report
    // utilization. A node that is unreachable simply omits the usage column.
    let usage_source = crate::rpc::chunk_client(endpoint)
        .await
        .ok()
        .map(crate::usage::ChunkClientSource::new);

    println!("Batches owned by {owner:#x}:");
    for b in &batches {
        let status = if b.live { "live" } else { "expired" };
        println!(
            "  {:#x}  depth {:<3} bucket {:<3} {:<9} {:<7} remaining/chunk {} BZZ",
            b.batch_id,
            b.depth,
            b.bucket_depth,
            if b.immutable { "immutable" } else { "mutable" },
            status,
            format_bzz(b.remaining_per_chunk),
        );

        if let Some(source) = &usage_source {
            let batch = nectar_postage::Batch::new(
                b.batch_id,
                0,
                0,
                b.owner,
                b.depth,
                b.bucket_depth,
                b.immutable,
            );
            match crate::usage::open_snapshot(source, &batch).await {
                Ok(snapshot) => {
                    let used = snapshot.table().max_count();
                    println!(
                        "      usage: sequence {}, peak bucket fill {}",
                        snapshot.sequence(),
                        used,
                    );
                }
                Err(_) => {
                    println!("      usage: (no published snapshot)");
                }
            }
        }
    }

    Ok(())
}

/// `dipper batch info` - read-only batch state (no signer required).
pub(crate) async fn info(rpc_url: &str, network: Network, batch_id: &str) -> Result<()> {
    let addrs = addrs_for(network);
    let batch_id = parse_b256(batch_id, "batch id")?;

    let provider = read_provider(rpc_url, &addrs).await?;

    let batch = CallBuilder::new_sol(
        &provider,
        &addrs.postage,
        &IPostageStampWrite::batchesCall { _batchId: batch_id },
    )
    .call()
    .await
    .context("failed to read batch state")?;

    if batch.owner == Address::ZERO {
        bail!("batch {batch_id:#x} does not exist");
    }

    let remaining = CallBuilder::new_sol(
        &provider,
        &addrs.postage,
        &IPostageStampWrite::remainingBalanceCall { _batchId: batch_id },
    )
    .call()
    .await
    .context("failed to read remaining balance")?;

    println!("Batch {batch_id:#x}");
    println!("  owner:             {:#x}", batch.owner);
    println!("  depth:             {}", batch.depth);
    println!("  bucket depth:      {}", batch.bucketDepth);
    println!("  immutable:         {}", batch.immutableFlag);
    println!("  remaining/chunk:   {} BZZ", format_bzz(remaining));
    println!("  last updated block: {}", batch.lastUpdatedBlockNumber);
    Ok(())
}
