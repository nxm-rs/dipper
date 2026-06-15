//! Generated gRPC client modules.
//!
//! `tonic::include_proto!` pulls in the code emitted by `build.rs` for each
//! protobuf package. One module per package; the generated client types are
//! `chunk_client::ChunkClient<Channel>`, `node_client::NodeClient<Channel>`,
//! and `health_client::HealthClient<Channel>`.
//!
//! The generated code marks every item `pub`; since these modules are only used
//! inside this binary, `unreachable_pub` is allowed module-wide rather than
//! patched into machine-generated source.

/// `vertex.swarm.chunk.v1` — chunk retrieval and upload.
pub(crate) mod chunk {
    #![allow(unreachable_pub, clippy::missing_const_for_fn, rustdoc::bare_urls)]
    tonic::include_proto!("vertex.swarm.chunk.v1");
}

/// `vertex.swarm.node.v1` — node status and topology.
pub(crate) mod node {
    #![allow(unreachable_pub, clippy::missing_const_for_fn, rustdoc::bare_urls)]
    tonic::include_proto!("vertex.swarm.node.v1");
}

/// `vertex.health.v1` — gRPC health checking (reserved for later phases).
pub(crate) mod health {
    #![allow(unreachable_pub, clippy::missing_const_for_fn, rustdoc::bare_urls)]
    tonic::include_proto!("vertex.health.v1");
}
