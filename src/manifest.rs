//! Mantaray manifest pipeline: upload, download, and list (phase 3).
//!
//! All three route through the [`crate::store::GrpcStore`], which implements
//! nectar's `SyncChunkGet`/`SyncChunkPut`. `upload` splits each input file into
//! the store (stamp+upload per chunk) and builds a `PlainManifest` whose nodes
//! upload through the same path; `download`/`ls` open the manifest read-only and
//! reconstruct files via `sync_join`.

use anyhow::{Result, bail};

use crate::cli::SignerArgs;

/// `dipper upload <path>` — split + manifest a file, directory, or `.tar.gz`,
/// then print the manifest root reference (0x-prefixed hex).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn upload(
    _endpoint: &str,
    _path: &str,
    _batch_id: &str,
    _depth: u8,
    _bucket_depth: u8,
    _index_document: Option<&str>,
    _error_document: Option<&str>,
    _signer: &SignerArgs,
) -> Result<()> {
    bail!("upload: not implemented")
}

/// `dipper download <root> [path]` — reconstruct a single file (when `path` is
/// given) or the whole manifest tree under `out`.
pub(crate) async fn download(
    _endpoint: &str,
    _root: &str,
    _path: Option<&str>,
    _out: Option<&str>,
) -> Result<()> {
    bail!("download: not implemented")
}

/// `dipper ls <root>` — open the manifest and print each entry (path, address,
/// content-type; size when `long`).
pub(crate) async fn ls(_endpoint: &str, _root: &str, _long: bool) -> Result<()> {
    bail!("ls: not implemented")
}
