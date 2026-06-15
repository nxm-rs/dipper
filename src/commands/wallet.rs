//! `dipper wallet ...` command handlers.

use anyhow::Result;

use crate::cli::SignerArgs;
use crate::wallet::load_signer;

/// `dipper wallet address` — load a signer and print its Ethereum address.
///
/// This is a fully offline command: it proves the private-key and keystore
/// loading paths work without touching the network.
pub(crate) fn address(signer_args: &SignerArgs) -> Result<()> {
    let signer = load_signer(signer_args)?;
    println!("{}", signer.address());
    Ok(())
}
