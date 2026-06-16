//! Streaming file download: level-order BMT tree walk, fetching each level
//! concurrently over the node's `RetrieveChunks` stream.

use std::collections::HashMap;

use anyhow::{Context, Result, anyhow, bail};
use futures::StreamExt;
use nectar_primitives::{ChunkAddress, DEFAULT_BODY_SIZE};
use tonic::transport::Channel;

use crate::proto::chunk::{RetrieveChunkRequest, chunk_client::ChunkClient};

/// Chunk body (payload) size.
const BODY_SIZE: usize = DEFAULT_BODY_SIZE;

/// Span header length prefixing every chunk wire body.
const SPAN_SIZE: usize = 8;

/// Child reference length in an intermediate node's body.
const REF_SIZE: usize = 32;

/// Reconstruct the file rooted at `root` by streaming its chunks from the node.
pub(crate) async fn download_file(channel: Channel, root: ChunkAddress) -> Result<Vec<u8>> {
    let mut client = ChunkClient::new(channel);

    // The current level of addresses, in file order. The corresponding bodies
    // come back from `fetch_level` keyed by address; an intermediate node
    // expands (in order) into the next level, a leaf contributes its data.
    let mut level: Vec<ChunkAddress> = vec![root];
    // Leaf payloads accumulated in file order.
    let mut out: Vec<u8> = Vec::new();
    // Guard against a pathologically deep / cyclic tree.
    let mut depth = 0usize;

    while !level.is_empty() {
        depth += 1;
        if depth > 64 {
            bail!("file tree deeper than 64 levels; refusing to recurse further");
        }

        let bodies = fetch_level(&mut client, &level).await?;

        let mut next: Vec<ChunkAddress> = Vec::new();
        let mut any_intermediate = false;

        for addr in &level {
            let body = bodies.get(addr).ok_or_else(|| {
                anyhow!("node did not return chunk {}", hex::encode(addr.as_bytes()))
            })?;

            if body.len() < SPAN_SIZE {
                bail!(
                    "chunk {} body shorter than span header ({} bytes)",
                    hex::encode(addr.as_bytes()),
                    body.len()
                );
            }
            let span = u64::from_le_bytes(body[..SPAN_SIZE].try_into().expect("8 bytes"));
            let data = &body[SPAN_SIZE..];

            if span as usize <= BODY_SIZE {
                // Leaf: its data is file content for this slice, in order.
                out.extend_from_slice(data);
            } else {
                // Intermediate: data is concatenated 32-byte child addresses.
                any_intermediate = true;
                if data.len() % REF_SIZE != 0 {
                    bail!(
                        "intermediate chunk {} body is not a multiple of {} bytes",
                        hex::encode(addr.as_bytes()),
                        REF_SIZE
                    );
                }
                for child in data.chunks_exact(REF_SIZE) {
                    let bytes: [u8; REF_SIZE] = child.try_into().expect("32 bytes");
                    next.push(ChunkAddress::from(bytes));
                }
            }
        }

        // If a level was all leaves there are no children to expand; the walk
        // ends. Mixed levels cannot occur in a balanced BMT tree, but expanding
        // only the intermediates (and keeping leaf order via the in-order walk
        // above) is correct regardless.
        if !any_intermediate {
            break;
        }
        level = next;
    }

    Ok(out)
}

/// Retry rounds re-streaming a level's straggler addresses before giving up.
const LEVEL_ROUNDS: usize = 15;

/// Fetch every address in `level` over `RetrieveChunks`, retrying stragglers, keyed by address.
async fn fetch_level(
    client: &mut ChunkClient<Channel>,
    level: &[ChunkAddress],
) -> Result<HashMap<ChunkAddress, Vec<u8>>> {
    let mut bodies: HashMap<ChunkAddress, Vec<u8>> = HashMap::with_capacity(level.len());
    let mut wanted: Vec<ChunkAddress> = level.to_vec();
    let mut last_error: Option<anyhow::Error> = None;

    for round in 0..LEVEL_ROUNDS {
        if wanted.is_empty() {
            break;
        }

        let failed = fetch_round(client, &wanted, &mut bodies)
            .await
            .with_context(|| format!("RetrieveChunks stream round {round} failed"))?;

        if failed.is_empty() {
            return Ok(bodies);
        }

        last_error = Some(anyhow!(
            "{} of {} chunks failed in round {round}; retrying",
            failed.len(),
            wanted.len()
        ));
        eprintln!(
            "retrying {} chunk(s) that failed in round {round}",
            failed.len()
        );
        wanted = failed;

        // A whole-round miss under a wide download is the per-peer accounting
        // allowance on the closest peers exhausting, not the chunk being absent
        // (the same address retrieved on its own succeeds). Pause before
        // re-streaming so those token buckets refill; an immediate re-stream just
        // hits the same throttled peers again. The pause grows with the round but
        // is capped so a long tail of rounds stays responsive.
        let backoff = std::cmp::min(
            std::time::Duration::from_millis(500) * (round as u32 + 1),
            std::time::Duration::from_secs(2),
        );
        tokio::time::sleep(backoff).await;
    }

    if bodies.len() == level.len() {
        return Ok(bodies);
    }

    Err(last_error.unwrap_or_else(|| anyhow!("level fetch exhausted its retry rounds")))
}

/// Stream one round of `wanted`, filling `bodies` and returning the stragglers to retry.
async fn fetch_round(
    client: &mut ChunkClient<Channel>,
    wanted: &[ChunkAddress],
    bodies: &mut HashMap<ChunkAddress, Vec<u8>>,
) -> Result<Vec<ChunkAddress>> {
    let requests: Vec<RetrieveChunkRequest> = wanted
        .iter()
        .map(|addr| RetrieveChunkRequest {
            address: hex::encode(addr.as_bytes()),
        })
        .collect();
    let request_stream = futures::stream::iter(requests);

    let mut inbound = client
        .retrieve_chunks(request_stream)
        .await
        .context("RetrieveChunks RPC failed")?
        .into_inner();

    // Responses arrive in request order; pair each with its requested address.
    // A per-chunk `Err` is recorded as a straggler and the stream continues; a
    // `None` before every slot is accounted for means the stream itself ended
    // early, so the remaining addresses are also stragglers.
    let mut failed: Vec<ChunkAddress> = Vec::new();
    let mut idx = 0usize;
    while idx < wanted.len() {
        match inbound.next().await {
            // The node emits an empty-`data` response as the per-address sentinel
            // for "not retrieved" so a single unretrievable chunk does not tear
            // down the whole stream (a terminal gRPC `Status` would). A real chunk
            // wire body is never empty (it carries at least the 8-byte span), so
            // empty `data` unambiguously marks a straggler to retry.
            Some(Ok(resp)) => {
                if resp.data.is_empty() {
                    failed.push(wanted[idx]);
                } else {
                    bodies.insert(wanted[idx], resp.data);
                }
            }
            // A terminal error still ends the stream (e.g. a transport failure):
            // the rest of this round's addresses are stragglers to retry.
            Some(Err(_status)) => {
                failed.extend_from_slice(&wanted[idx..]);
                break;
            }
            None => {
                // Stream ended before all slots were filled: the rest are
                // stragglers to retry.
                failed.extend_from_slice(&wanted[idx..]);
                break;
            }
        }
        idx += 1;
    }

    Ok(failed)
}
