//! Mantaray manifest pipeline: upload, download, and list, over a [`crate::store::GrpcStore`].

use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};

use nectar_mantaray::{MantarayError, PlainManifest, metadata};
use nectar_primitives::file::SyncChunkPutExt;

use crate::chunkops;
use crate::cli::SignerArgs;
use crate::rpc;
use crate::store::GrpcStore;
use crate::wallet;

/// One file to be added to a manifest.
struct InputFile {
    path: String,
    content_type: String,
    data: Vec<u8>,
}

/// `dipper upload <path>`: split + manifest a file/dir/`.tar.gz`, printing the root reference.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn upload(
    endpoint: &str,
    path: &str,
    batch_id: &str,
    depth: u8,
    bucket_depth: u8,
    index_document: Option<&str>,
    error_document: Option<&str>,
    signer: &SignerArgs,
) -> Result<()> {
    let batch = parse_batch_id(batch_id)?;
    let signer = wallet::load_signer(signer)?;

    // Collect the input set and decide whether it is a website (dir/archive)
    // that should carry an index document.
    let (files, is_collection) = collect_inputs(path)?;
    if files.is_empty() {
        bail!("no files to upload at {path}");
    }

    let handle = tokio::runtime::Handle::current();
    let client = rpc::chunk_client(endpoint).await?;
    let store = GrpcStore::new(client, batch, depth, bucket_depth, signer, handle, false);
    // Keep a handle to the store so the upload stream can be closed and awaited
    // after the (blocking) split/manifest work has fed every chunk through it.
    let upload_store = store.clone();

    // A directory / archive defaults to serving `index.html`; an explicit flag
    // always wins. A single file gets no index document unless asked.
    let index_doc: Option<String> = match index_document {
        Some(name) => Some(name.to_owned()),
        None if is_collection => Some("index.html".to_owned()),
        None => None,
    };
    let error_doc = error_document.map(str::to_owned);

    // Split, manifest, and save off the async workers: the splitter and trie
    // serialization are CPU-bound and call blocking gRPC under the hood.
    let root = tokio::task::spawn_blocking(move || -> Result<nectar_primitives::ChunkAddress> {
        let mut manifest: PlainManifest<GrpcStore> = PlainManifest::new(store.clone());

        for file in &files {
            // Split this file into the store: every leaf and intermediate node
            // is stamped + uploaded as it is produced.
            let file_root = store
                .write_file(&file.data)
                .with_context(|| format!("failed to split {}", file.path))?;

            let meta: BTreeMap<String, String> =
                BTreeMap::from([(metadata::CONTENT_TYPE.to_owned(), file.content_type.clone())]);
            manifest
                .add_with_metadata(&file.path, file_root, meta)
                .with_context(|| format!("failed to add {} to manifest", file.path))?;
        }

        // Website index/error documents live on the root-path node metadata and
        // must be set before the final save.
        if let Some(name) = &index_doc {
            manifest
                .set_index_document(name)
                .context("failed to set index document")?;
        }
        if let Some(name) = &error_doc {
            manifest
                .set_error_document(name)
                .context("failed to set error document")?;
        }

        manifest.save().context("failed to save manifest")
    })
    .await
    .context("upload task panicked")??;

    // Close the upload stream and await the drain task: every chunk has now been
    // stamped and enqueued, so this flushes the stream and surfaces any
    // server-side upload failure (a bad stamp, a missing storer) before the
    // root is reported as durable.
    upload_store
        .finish()
        .await
        .context("upload stream failed")?;

    println!("0x{}", hex::encode(root.as_bytes()));
    Ok(())
}

/// One file to reconstruct: where to fetch it and where to write it.
struct DownloadItem {
    file_ref: nectar_primitives::ChunkAddress,
    dest: PathBuf,
}

/// `dipper download <root> [path]`: reconstruct a single file or the whole tree under `out`.
pub(crate) async fn download(
    endpoint: &str,
    root: &str,
    path: Option<&str>,
    out: Option<&str>,
) -> Result<()> {
    let root_addr = chunkops::parse_address(root)?;
    let path = path.map(str::to_owned);
    let out = out.map(str::to_owned);

    let handle = tokio::runtime::Handle::current();
    // One shared channel: the sync store resolves the manifest over it, and the
    // streaming reconstruction multiplexes its long-lived RPC over the same
    // HTTP/2 connection.
    let channel = rpc::connect(endpoint).await?;
    let client = rpc::chunk_client_on(channel.clone());
    // Reads do not stamp, but the store still needs a signer-shaped stamper;
    // synthesize a throwaway key (never used on the read path).
    let signer = alloy_signer_local::PrivateKeySigner::random();
    let store = GrpcStore::connect_read_only(client, signer, handle);

    let root_hex = hex::encode(root_addr.as_bytes());

    // Step 1: resolve the manifest on the sync store (small) to a plan of files.
    let plan_root_hex = root_hex.clone();
    let items = tokio::task::spawn_blocking(move || -> Result<Vec<DownloadItem>> {
        path.as_deref().map_or_else(
            || resolve_tree(&store, root_addr, &plan_root_hex, out.as_deref()),
            |p| resolve_one(&store, root_addr, p, out.as_deref()),
        )
    })
    .await
    .context("manifest resolution task panicked")??;

    // Step 2: reconstruct each file's bytes over the streaming RPC and write it.
    let mut count = 0usize;
    for item in items {
        if let Some(parent) = item.dest.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let bytes = crate::download::download_file(channel.clone(), item.file_ref)
            .await
            .with_context(|| format!("failed to reconstruct {}", item.dest.display()))?;
        std::fs::write(&item.dest, &bytes)
            .with_context(|| format!("failed to write {}", item.dest.display()))?;
        eprintln!("Wrote {} bytes to {}", bytes.len(), item.dest.display());
        count += 1;
    }
    if count > 1 {
        eprintln!("Wrote {count} file(s)");
    }

    Ok(())
}

/// Resolve a single manifest path to a one-item download plan.
fn resolve_one(
    store: &GrpcStore,
    root: nectar_primitives::ChunkAddress,
    path: &str,
    out: Option<&str>,
) -> Result<Vec<DownloadItem>> {
    let mut manifest: PlainManifest<GrpcStore> = PlainManifest::open(root, store.clone());

    let entry = manifest.lookup(path).map_err(|e| match e {
        MantarayError::NoForkFound { .. } => anyhow!("path not found: {path}"),
        MantarayError::NotValueType => {
            anyhow!("{path} is a directory; omit <path> to download the tree")
        }
        other => anyhow!("manifest lookup failed: {other}"),
    })?;
    let file_ref = entry
        .address()
        .ok_or_else(|| anyhow!("path {path} has no file reference"))?;

    // --out is a file target; default to the path's basename in cwd.
    let dest: PathBuf = out.map_or_else(|| PathBuf::from(basename(path)), PathBuf::from);
    Ok(vec![DownloadItem {
        file_ref: *file_ref,
        dest,
    }])
}

/// Resolve the whole manifest tree under `out` to a plan, or `root` as a raw file.
fn resolve_tree(
    store: &GrpcStore,
    root: nectar_primitives::ChunkAddress,
    root_hex: &str,
    out: Option<&str>,
) -> Result<Vec<DownloadItem>> {
    let mut manifest: PlainManifest<GrpcStore> = PlainManifest::open(root, store.clone());

    // A mantaray decode error here means `root` is a raw file, not a manifest.
    let entries = match manifest.entries() {
        Ok(entries) => entries,
        Err(_) => {
            return resolve_raw_file(root, root_hex, out);
        }
    };

    let out_dir: PathBuf = out.map_or_else(|| PathBuf::from("."), PathBuf::from);

    let mut items = Vec::new();
    for entry in entries {
        let Some(addr) = entry.address() else {
            // Metadata-only node (e.g. the root-path index marker): skip.
            continue;
        };
        let rel = entry
            .path_str()
            .ok_or_else(|| anyhow!("manifest entry has a non-utf8 path"))?;

        items.push(DownloadItem {
            file_ref: *addr,
            dest: out_dir.join(rel),
        });
    }
    Ok(items)
}

/// Treat `root` as a single uploaded file (not a manifest) and plan to write it.
fn resolve_raw_file(
    root: nectar_primitives::ChunkAddress,
    root_hex: &str,
    out: Option<&str>,
) -> Result<Vec<DownloadItem>> {
    let dest: PathBuf = out.map_or_else(
        || PathBuf::from(root_hex),
        |o| {
            let p = PathBuf::from(o);
            // If --out names an existing directory, write the root hex inside it.
            if p.is_dir() { p.join(root_hex) } else { p }
        },
    );
    Ok(vec![DownloadItem {
        file_ref: root,
        dest,
    }])
}

/// `dipper ls <root>`: print each manifest entry (size too when `long`).
pub(crate) async fn ls(endpoint: &str, root: &str, long: bool) -> Result<()> {
    let root_addr = chunkops::parse_address(root)?;

    let handle = tokio::runtime::Handle::current();
    let client = rpc::chunk_client(endpoint).await?;
    let signer = alloy_signer_local::PrivateKeySigner::random();
    let store = GrpcStore::connect_read_only(client, signer, handle);

    tokio::task::spawn_blocking(move || -> Result<()> {
        let mut manifest: PlainManifest<GrpcStore> = PlainManifest::open(root_addr, store.clone());

        for entry in manifest.iter() {
            let entry = entry.context("failed to read manifest entry")?;
            let path = entry.path_str().unwrap_or("<non-utf8>");
            let ctype = entry.content_type().unwrap_or("");
            let addr_hex = entry
                .address()
                .map_or_else(String::new, |a| hex::encode(a.as_bytes()));

            if long {
                // The manifest stores no size; pay one extra RPC to read the
                // file root chunk's span (total file length in bytes).
                use nectar_primitives::store::SyncChunkGet;
                let size = entry
                    .address()
                    .map_or(0, |addr| store.get(addr).map(|c| c.span()).unwrap_or(0));
                println!("{size:>12}  0x{addr_hex}  {ctype:<24}  {path}");
            } else {
                println!("0x{addr_hex}  {ctype:<24}  {path}");
            }
        }
        Ok(())
    })
    .await
    .context("ls task panicked")??;

    Ok(())
}

/// Gather the upload input set from `path`; second tuple field is true for a collection.
fn collect_inputs(path: &str) -> Result<(Vec<InputFile>, bool)> {
    let lower = path.to_ascii_lowercase();
    if lower.ends_with(".tar.gz") || lower.ends_with(".tgz") {
        return Ok((collect_tar_gz(path)?, true));
    }

    let meta = std::fs::metadata(path).with_context(|| format!("failed to stat {path}"))?;
    if meta.is_dir() {
        Ok((collect_dir(path)?, true))
    } else {
        Ok((vec![collect_single_file(path)?], false))
    }
}

/// One single file: manifest path is its file name.
fn collect_single_file(path: &str) -> Result<InputFile> {
    let data = std::fs::read(path).with_context(|| format!("failed to read {path}"))?;
    let name = Path::new(path)
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| anyhow!("could not derive a file name from {path}"))?
        .to_owned();
    let content_type = guess_content_type(&name);
    Ok(InputFile {
        path: name,
        content_type,
        data,
    })
}

/// Recurse a directory tree, using each file's path relative to `root` (with
/// `/` separators) as the manifest path.
fn collect_dir(root: &str) -> Result<Vec<InputFile>> {
    let root_path = Path::new(root);
    let mut files = Vec::new();
    for entry in walkdir::WalkDir::new(root) {
        let entry = entry.with_context(|| format!("failed to walk {root}"))?;
        if !entry.file_type().is_file() {
            continue;
        }
        let abs = entry.path();
        let rel = abs
            .strip_prefix(root_path)
            .with_context(|| format!("{} is not under {root}", abs.display()))?;
        let manifest_path = normalize_path(rel);
        let data =
            std::fs::read(abs).with_context(|| format!("failed to read {}", abs.display()))?;
        let content_type = guess_content_type(&manifest_path);
        files.push(InputFile {
            path: manifest_path,
            content_type,
            data,
        });
    }
    Ok(files)
}

/// Read every regular file from a gzip-compressed tar archive.
fn collect_tar_gz(path: &str) -> Result<Vec<InputFile>> {
    let file = std::fs::File::open(path).with_context(|| format!("failed to open {path}"))?;
    let gz = flate2::read::GzDecoder::new(file);
    let mut archive = tar::Archive::new(gz);

    let mut files = Vec::new();
    for entry in archive.entries().context("failed to read tar archive")? {
        let mut entry = entry.context("failed to read tar entry")?;
        if !entry.header().entry_type().is_file() {
            continue;
        }
        let entry_path = entry
            .path()
            .context("tar entry has an invalid path")?
            .into_owned();
        let manifest_path = normalize_path(&entry_path);
        let mut data = Vec::new();
        entry
            .read_to_end(&mut data)
            .with_context(|| format!("failed to read tar entry {manifest_path}"))?;
        let content_type = guess_content_type(&manifest_path);
        files.push(InputFile {
            path: manifest_path,
            content_type,
            data,
        });
    }
    Ok(files)
}

/// Normalize an OS path to a manifest path: `/` separators, no leading slash.
fn normalize_path(p: &Path) -> String {
    let s = p.to_string_lossy().replace(std::path::MAIN_SEPARATOR, "/");
    s.trim_start_matches('/').to_owned()
}

/// The basename of a `/`-separated manifest path.
fn basename(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

/// Guess a content type from a manifest path, defaulting to octet-stream.
fn guess_content_type(path: &str) -> String {
    mime_guess::from_path(path)
        .first_or_octet_stream()
        .essence_str()
        .to_owned()
}

/// Parse a 32-byte hex batch id (0x-prefix optional).
fn parse_batch_id(s: &str) -> Result<alloy_primitives::B256> {
    let raw = s.strip_prefix("0x").unwrap_or(s);
    let bytes = hex::decode(raw).context("batch id is not valid hex")?;
    if bytes.len() != 32 {
        bail!(
            "batch id must be 32 bytes (64 hex chars), got {}",
            bytes.len()
        );
    }
    Ok(alloy_primitives::B256::from_slice(&bytes))
}
