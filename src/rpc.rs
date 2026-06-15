//! gRPC connection helpers.
//!
//! Builds a tonic [`Channel`] from the `--endpoint` URL and constructs the
//! generated service clients over it.

use anyhow::{Context, Result};
use tonic::transport::{Channel, Endpoint};

use crate::proto::chunk::chunk_client::ChunkClient;
use crate::proto::node::node_client::NodeClient;

/// Establish a channel to the vertex node's gRPC endpoint.
///
/// The channel connects lazily on first use, so this does not fail fast if the
/// node is down - the first RPC call surfaces a connection error instead.
pub(crate) async fn connect(endpoint: &str) -> Result<Channel> {
    let channel = Endpoint::from_shared(endpoint.to_string())
        .with_context(|| format!("invalid endpoint URL: {endpoint}"))?
        .connect()
        .await
        .with_context(|| format!("failed to connect to {endpoint}"))?;
    Ok(channel)
}

/// Build a [`NodeClient`] over a fresh channel.
pub(crate) async fn node_client(endpoint: &str) -> Result<NodeClient<Channel>> {
    Ok(NodeClient::new(connect(endpoint).await?))
}

/// Build a [`ChunkClient`] over a fresh channel.
pub(crate) async fn chunk_client(endpoint: &str) -> Result<ChunkClient<Channel>> {
    Ok(ChunkClient::new(connect(endpoint).await?))
}
