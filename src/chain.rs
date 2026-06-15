//! On-chain postage batch operations (phase 2).
//!
//! Drives the Gnosis/Sepolia `PostageStamp` contract through alloy: create a
//! batch (`approve` + `createBatch`), top it up, increase its depth (dilute),
//! and read its state. The provider is built with the recommended fillers and a
//! `PrivateKeySigner` wallet; reads need no signer.
//!
//! `nectar_contracts::IPostageStamp` is read-only, so dipper declares its own
//! write surface below and reuses `nectar_contracts::IERC20` for token approval.
//!
//! Scaffold note: the `sol!` interface, address book, and amount constant are
//! integration plumbing the batch-ops impl consumes. They are allowed-dead here
//! so the stub compiles clippy-clean; the impl phase wires them and this
//! module-level allow is removed.
#![allow(dead_code)]

use alloy_sol_types::sol;
use anyhow::{Result, bail};

use crate::cli::{Network, SignerArgs};

// Reuse nectar's complete ERC-20 interface (approve/allowance/balanceOf/
// decimals) rather than redeclaring it. Re-exported as plumbing for the impl
// agent; allowed-unused until the bodies below drive it.
#[allow(unused_imports)]
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

        // events — field order MUST match the contract.
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
    pub(crate) postage: alloy_primitives::Address,
    /// BZZ ERC-20 token address.
    pub(crate) bzz: alloy_primitives::Address,
    /// Expected EVM chain id (100 Gnosis, 11155111 Sepolia).
    pub(crate) chain_id: u64,
}

/// Address book for the supported networks.
pub(crate) const fn addrs_for(net: Network) -> ChainAddrs {
    use nectar_contracts::{mainnet, testnet};
    match net {
        Network::Gnosis => ChainAddrs {
            postage: mainnet::POSTAGE_STAMP.address,
            bzz: mainnet::BZZ_TOKEN.address,
            chain_id: 100,
        },
        Network::Sepolia => ChainAddrs {
            postage: testnet::POSTAGE_STAMP.address,
            bzz: testnet::BZZ_TOKEN.address,
            chain_id: 11155111,
        },
    }
}

/// `dipper batch create` — BZZ.approve then PostageStamp.createBatch.
///
/// Reads the authoritative batch id from the `BatchCreated` receipt log and
/// prints it (0x-prefixed) on success.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn create(
    _rpc_url: &str,
    _network: Network,
    _amount: &str,
    _depth: u8,
    _bucket_depth: u8,
    _immutable: bool,
    _owner: Option<&str>,
    _nonce: Option<&str>,
    _signer: &SignerArgs,
) -> Result<()> {
    bail!("batch create: not implemented")
}

/// `dipper batch topup` — PostageStamp.topUp(batchId, amountPerChunk).
pub(crate) async fn topup(
    _rpc_url: &str,
    _network: Network,
    _batch_id: &str,
    _amount: &str,
    _signer: &SignerArgs,
) -> Result<()> {
    bail!("batch topup: not implemented")
}

/// `dipper batch dilute` — PostageStamp.increaseDepth(batchId, newDepth).
pub(crate) async fn dilute(
    _rpc_url: &str,
    _network: Network,
    _batch_id: &str,
    _depth: u8,
    _signer: &SignerArgs,
) -> Result<()> {
    bail!("batch dilute: not implemented")
}

/// `dipper batch info` — read-only batch state (no signer required).
pub(crate) async fn info(_rpc_url: &str, _network: Network, _batch_id: &str) -> Result<()> {
    bail!("batch info: not implemented")
}
