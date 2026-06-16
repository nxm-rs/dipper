//! Portable postage usage: per-batch issuance counters stored on Swarm as
//! single-owner chunks, so stamping resumes from any device given owner key + batch id.

use alloy_primitives::Address;
use nectar_postage::Batch;
use nectar_postage_usage::{
    ChunkSink, ChunkSource, PublishedSequence, RootInfo, SealedChunk, Snapshot, SwarmAddress,
    seal_plan, usage_chunk_address,
};
use nectar_primitives::bytes::Bytes;
use nectar_primitives::{Chunk, SingleOwnerChunk};
use tonic::Code;
use tonic::transport::Channel;

use crate::proto::chunk::{
    ChunkType, RetrieveChunkRequest, UploadChunkRequest, chunk_client::ChunkClient,
};
use crate::store::BS;

/// Errors surfaced by the usage adapters and ceremony.
#[derive(Debug, thiserror::Error)]
pub(crate) enum UsageAdapterError {
    /// A transport / status error (never chunk absence, which is `Ok(None)`).
    #[error("grpc transport error: {0}")]
    Transport(#[from] tonic::Status),

    /// A fetched single-owner chunk could not be reconstructed.
    #[error("usage chunk decode error: {0}")]
    Decode(#[from] nectar_primitives::PrimitivesError),
}

/// A [`ChunkSource`] over the vertex chunk client.
///
/// Floor safety: only `Code::NotFound` maps to `Ok(None)` (definitive absence);
/// every other status is `Err`, so a flaky read can never downgrade the anti-downgrade floor.
#[derive(Clone)]
pub(crate) struct ChunkClientSource {
    client: ChunkClient<Channel>,
}

impl ChunkClientSource {
    /// Build a source over the chunk client.
    pub(crate) const fn new(client: ChunkClient<Channel>) -> Self {
        Self { client }
    }
}

impl ChunkSource for ChunkClientSource {
    type Error = UsageAdapterError;

    async fn fetch(&self, address: &SwarmAddress) -> Result<Option<Bytes>, Self::Error> {
        let mut client = self.client.clone();
        let address_hex = hex::encode(address.as_slice());

        let resp = match client
            .retrieve_chunk(RetrieveChunkRequest {
                address: address_hex,
            })
            .await
        {
            Ok(resp) => resp.into_inner(),
            // A definitively-absent chunk: the network agrees it does not exist.
            Err(status) if status.code() == Code::NotFound => return Ok(None),
            // Any other status is a read that could not be completed.
            Err(status) => return Err(UsageAdapterError::Transport(status)),
        };

        // An empty body is also treated as absence (a stampless / empty
        // delivery cannot carry a snapshot payload).
        if resp.data.is_empty() {
            return Ok(None);
        }

        // The wire body is the full single-owner encoding (id || signature ||
        // BMT body). Reconstruct it and hand back its inner data payload, which
        // is the snapshot payload the codec parses.
        let wire = Bytes::from(resp.data);
        let chunk = SingleOwnerChunk::<BS>::try_from(wire)?;
        Ok(Some(chunk.data().clone()))
    }
}

/// A [`ChunkSink`] over the vertex chunk client, uploading sealed snapshot chunks unary.
#[derive(Clone)]
pub(crate) struct ChunkClientSink {
    client: ChunkClient<Channel>,
}

impl ChunkClientSink {
    /// Build a sink over the chunk client.
    pub(crate) const fn new(client: ChunkClient<Channel>) -> Self {
        Self { client }
    }
}

impl ChunkSink for ChunkClientSink {
    type Error = UsageAdapterError;

    async fn push(&self, sealed: &SealedChunk) -> Result<(), Self::Error> {
        let mut client = self.client.clone();

        let address = *sealed.chunk.address();
        // The full single-owner wire encoding (id || signature || BMT body).
        let wire = Bytes::from(sealed.chunk.clone());

        client
            .upload_chunk(UploadChunkRequest {
                data: wire.to_vec(),
                stamp: sealed.stamp.to_bytes().to_vec(),
                address: hex::encode(address.as_slice()),
                chunk_type: ChunkType::SingleOwner as i32,
                validate: false,
            })
            .await?;
        Ok(())
    }
}

/// Recover the published usage snapshot for `batch`, or start fresh; a transport failure aborts.
pub(crate) async fn open_snapshot(
    source: &ChunkClientSource,
    batch: &Batch,
) -> Result<Snapshot, anyhow::Error> {
    let batch_id = batch.id();
    let owner = batch.owner();
    let root_addr = usage_chunk_address(&batch_id, &owner, 0);

    match source.fetch(&root_addr).await? {
        Some(root_bytes) => {
            let root = RootInfo::parse(&root_bytes)?;
            let mut leaves: Vec<Bytes> = Vec::with_capacity(root.leaf_count() as usize);
            for leaf in 0..root.leaf_count() {
                let index = leaf + 1;
                let leaf_addr = usage_chunk_address(&batch_id, &owner, index);
                match source.fetch(&leaf_addr).await? {
                    Some(bytes) => leaves.push(bytes),
                    None => {
                        anyhow::bail!(
                            "published usage root commits to leaf {index} but the network \
                             reports it absent (snapshot corruption)"
                        )
                    }
                }
            }
            Ok(root.assemble(&leaves)?)
        }
        // The network confirms no published root: a genuinely fresh batch.
        None => Ok(Snapshot::from_batch(batch)?),
    }
}

/// Persist the snapshot to Swarm: re-read the floor, revalidate, plan, seal, upload.
pub(crate) async fn flush_snapshot(
    snapshot: &mut Snapshot,
    owner: &Address,
    signer: &alloy_signer_local::PrivateKeySigner,
    source: &ChunkClientSource,
    sink: &ChunkClientSink,
) -> Result<(), anyhow::Error> {
    let batch_id = snapshot.table().batch_id();
    let root_addr = usage_chunk_address(&batch_id, owner, 0);

    // Re-read the live root to derive the published floor: a transport failure
    // aborts rather than persisting against a floor it could not read.
    let floor = match source.fetch(&root_addr).await? {
        Some(root_bytes) => PublishedSequence::from(&RootInfo::parse(&root_bytes)?),
        None => PublishedSequence::NONE,
    };

    let plan = snapshot.revalidate(floor)?.plan_persist(owner)?;

    // The seal timestamp must strictly increase across flushes so the reserve
    // overwrites each metadata chunk in place. Take the wall clock, lifted past
    // the previous seal so a coarse clock never trips the in-process guard.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let timestamp = snapshot
        .last_seal_timestamp()
        .map_or(now, |previous| now.max(previous + 1));

    let sealed = seal_plan(snapshot, &plan, timestamp, signer)?;
    for chunk in &sealed {
        sink.push(chunk).await?;
    }

    Ok(())
}
