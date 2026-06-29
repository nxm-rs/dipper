//! `dipper chunk ...` command handlers.

use std::io::{Read, Write};

use alloy_primitives::B256;
use anyhow::{Context, Result, bail};

use crate::chunkops::{self, prepare_content_chunk};
use crate::cli::SignerArgs;
use crate::proto::chunk::{
    ChunkType, RetrieveChunkRequest, UploadChunkRequest, retrieve_chunk_response,
    upload_chunk_response,
};
use crate::rpc;
use crate::wallet;

/// `dipper chunk download <addr>` - retrieve a chunk and write it out.
///
/// By default the payload (wire body minus the 8-byte span) is written; with
/// `--raw` the verbatim wire body is written instead.
pub(crate) async fn download(
    endpoint: &str,
    addr: &str,
    out: Option<&str>,
    raw: bool,
) -> Result<()> {
    let address = chunkops::parse_address(addr)?;
    let mut client = rpc::chunk_client(endpoint).await?;

    let resp = client
        .retrieve_chunk(RetrieveChunkRequest {
            address: address.as_bytes().to_vec(),
        })
        .await?
        .into_inner();

    let data = match resp.result {
        Some(retrieve_chunk_response::Result::Chunk(c)) => c.data,
        Some(retrieve_chunk_response::Result::Error(e)) => {
            bail!("chunk retrieval failed: {}", e.message);
        }
        None => bail!("chunk retrieval returned no result"),
    };

    let bytes: &[u8] = if raw {
        &data
    } else {
        chunkops::payload_of(&data)
    };

    match out {
        Some(path) => {
            std::fs::write(path, bytes)
                .with_context(|| format!("failed to write chunk to {path}"))?;
            eprintln!("Wrote {} bytes to {path}", bytes.len());
        }
        None => {
            std::io::stdout()
                .write_all(bytes)
                .context("failed to write chunk to stdout")?;
        }
    }
    Ok(())
}

/// `dipper chunk upload [<file>|--stdin] ...` - build, stamp, and push a chunk.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn upload(
    endpoint: &str,
    file: Option<&str>,
    stdin: bool,
    batch_id: &str,
    depth: u8,
    bucket_depth: u8,
    signer_args: &SignerArgs,
) -> Result<()> {
    let data = read_input(file, stdin)?;
    let batch = parse_batch_id(batch_id)?;
    let signer = wallet::load_signer(signer_args)?;

    let prepared = prepare_content_chunk(data, batch, depth, bucket_depth, signer)?;
    let address_hex = chunkops::address_hex(&prepared.address);

    let mut client = rpc::chunk_client(endpoint).await?;
    let resp = client
        .upload_chunk(UploadChunkRequest {
            data: prepared.body,
            stamp: prepared.stamp,
            address: prepared.address.as_bytes().to_vec(),
            chunk_type: ChunkType::Content as i32,
            validate: false,
        })
        .await?
        .into_inner();

    let receipt = match resp.result {
        Some(upload_chunk_response::Result::Receipt(r)) => r,
        Some(upload_chunk_response::Result::Error(e)) => {
            bail!("chunk upload failed: {}", e.message);
        }
        None => bail!("chunk upload returned no result"),
    };

    println!("Uploaded chunk 0x{address_hex}");
    println!("Accepted by storer: 0x{}", hex::encode(&receipt.storer));
    println!("Storage radius:     {}", receipt.storage_radius);
    Ok(())
}

/// Read the chunk data from a file or stdin.
fn read_input(file: Option<&str>, stdin: bool) -> Result<Vec<u8>> {
    match (file, stdin) {
        (Some(_), true) => bail!("pass either a file or --stdin, not both"),
        (Some(path), false) => {
            std::fs::read(path).with_context(|| format!("failed to read {path}"))
        }
        (None, true) => {
            let mut buf = Vec::new();
            std::io::stdin()
                .read_to_end(&mut buf)
                .context("failed to read from stdin")?;
            Ok(buf)
        }
        (None, false) => bail!("no input: pass a file path or --stdin"),
    }
}

/// Parse a 32-byte hex batch id (0x-prefix optional).
fn parse_batch_id(s: &str) -> Result<B256> {
    let raw = s.strip_prefix("0x").unwrap_or(s);
    let bytes = hex::decode(raw).context("batch id is not valid hex")?;
    if bytes.len() != 32 {
        bail!(
            "batch id must be 32 bytes (64 hex chars), got {}",
            bytes.len()
        );
    }
    Ok(B256::from_slice(&bytes))
}
