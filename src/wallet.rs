//! Key / keystore loading.
//!
//! Produces an [`alloy_signer_local::PrivateKeySigner`] from either a raw
//! private key or a cast / EIP-2335 JSON keystore. The signer implements
//! `SignerSync`, which is what the postage stamper needs, and exposes
//! `.address()` for the `wallet address` command.

use alloy_primitives::B256;
use alloy_signer_local::PrivateKeySigner;
use anyhow::{Context, Result, bail};

use crate::cli::SignerArgs;

/// Environment variable consulted for a keystore password when `--password`
/// is not given.
const PASSWORD_ENV: &str = "DIPPER_KEYSTORE_PASSWORD";

/// Load a [`PrivateKeySigner`] from the parsed [`SignerArgs`].
///
/// Exactly one of `--private-key` / `--keystore` is present (clap's `ArgGroup`
/// guarantees it), but we still handle the empty case defensively.
pub(crate) fn load_signer(args: &SignerArgs) -> Result<PrivateKeySigner> {
    if let Some(pk) = &args.private_key {
        return signer_from_private_key(pk);
    }
    if let Some(path) = &args.keystore {
        let password = resolve_keystore_password(args)?;
        return PrivateKeySigner::decrypt_keystore(path, password)
            .with_context(|| format!("failed to decrypt keystore at {path}"));
    }
    bail!("no key source provided: pass --private-key or --keystore")
}

/// Build a signer from a hex private key (0x-prefix optional).
fn signer_from_private_key(pk: &str) -> Result<PrivateKeySigner> {
    let raw = pk.strip_prefix("0x").unwrap_or(pk);
    let bytes = hex::decode(raw).context("private key is not valid hex")?;
    if bytes.len() != 32 {
        bail!("private key must be 32 bytes, got {}", bytes.len());
    }
    PrivateKeySigner::from_bytes(&B256::from_slice(&bytes)).context("invalid secp256k1 private key")
}

/// Determine the keystore password: explicit flag, env var, or interactive
/// prompt (in that order).
fn resolve_keystore_password(args: &SignerArgs) -> Result<String> {
    if let Some(p) = &args.password {
        return Ok(p.clone());
    }
    if let Ok(p) = std::env::var(PASSWORD_ENV) {
        return Ok(p);
    }
    rpassword::prompt_password("Keystore password: ")
        .context("failed to read keystore password from terminal")
}
