//! Content-chunk construction, stamping, and address/hex helpers.
//!
//! This is the local half of an upload: take raw bytes, build a nectar
//! [`ContentChunk`](nectar_primitives::ContentChunk), derive its address and BMT
//! wire body (span + data), and
//! produce a correctly-bucketed 113-byte postage stamp via the postage issuer.

use alloy_primitives::B256;
use alloy_signer_local::PrivateKeySigner;
use anyhow::{Context, Result, bail};

use nectar_postage_issuer::{BatchStamper, MemoryIssuer, Stamper};
use nectar_primitives::{Chunk, DefaultContentChunk, SwarmAddress};

/// A locally-built, stamped chunk ready to push over gRPC.
pub(crate) struct PreparedChunk {
    /// Chunk address (`SwarmAddress`, 32 bytes).
    pub(crate) address: SwarmAddress,
    /// BMT wire body: 8-byte little-endian span followed by the payload.
    pub(crate) body: Vec<u8>,
    /// The serialized 113-byte postage stamp.
    pub(crate) stamp: Vec<u8>,
}

/// Build a content chunk from `data`, then stamp it for the given batch.
///
/// The stamp's bucket is derived from the chunk address (top `bucket_depth`
/// bits) and the index is the next free slot in that bucket — handled by
/// [`MemoryIssuer`] inside [`BatchStamper`], so we never hardcode `(0, 0)`.
pub(crate) fn prepare_content_chunk(
    data: Vec<u8>,
    batch_id: B256,
    depth: u8,
    bucket_depth: u8,
    signer: PrivateKeySigner,
) -> Result<PreparedChunk> {
    // ContentChunk::new computes the BMT body (span + data) and lazily the
    // address. Bodies larger than DEFAULT_BODY_SIZE (4096) are rejected here:
    // phase 1 only handles a single content chunk, no file splitting yet.
    let chunk = DefaultContentChunk::new(data).context("failed to build content chunk")?;
    let address = *chunk.address();

    // The full BMT wire body is `Bytes::from(chunk)` (span || data), exactly
    // what UploadChunkRequest.data expects for a content chunk. `bytes` is
    // re-exported from nectar-primitives' public API.
    let body: nectar_primitives::bytes::Bytes = chunk.into();

    // MemoryIssuer assigns the correct (bucket, index) for this address given
    // the batch geometry; BatchStamper signs the resulting digest with EIP-191.
    let issuer = MemoryIssuer::new(batch_id, depth, bucket_depth);
    let mut stamper = BatchStamper::new(issuer, signer);
    let stamp = stamper
        .stamp(&address)
        .context("failed to issue/sign postage stamp")?;

    Ok(PreparedChunk {
        address,
        body: body.to_vec(),
        stamp: stamp.to_bytes().to_vec(),
    })
}

/// Parse a hex chunk address (0x-prefix optional) into a [`SwarmAddress`].
pub(crate) fn parse_address(s: &str) -> Result<SwarmAddress> {
    let raw = s.strip_prefix("0x").unwrap_or(s);
    let bytes = hex::decode(raw).context("chunk address is not valid hex")?;
    if bytes.len() != 32 {
        bail!(
            "chunk address must be 32 bytes (64 hex chars), got {}",
            bytes.len()
        );
    }
    SwarmAddress::from_slice(&bytes).context("invalid chunk address")
}

/// Lowercase hex of an address, no `0x` prefix (the wire format the RPCs use).
pub(crate) fn address_hex(address: &SwarmAddress) -> String {
    hex::encode(address.as_bytes())
}

/// Split a BMT wire body into its payload (everything after the 8-byte span).
///
/// Returns the whole slice if it is somehow shorter than the span header.
pub(crate) fn payload_of(body: &[u8]) -> &[u8] {
    const SPAN_SIZE: usize = 8;
    if body.len() >= SPAN_SIZE {
        &body[SPAN_SIZE..]
    } else {
        body
    }
}
