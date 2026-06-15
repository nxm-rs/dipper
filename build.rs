//! Build script: generate tonic gRPC clients from the vendored proto files.
//!
//! Only clients are generated (the node side lives in `vertex`). The three
//! protos map to three Rust modules, one per protobuf package:
//!   - `vertex.swarm.chunk.v1` -> chunk client
//!   - `vertex.swarm.node.v1`  -> node client
//!   - `vertex.health.v1`      -> health client

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(
            &[
                "proto/chunk.proto",
                "proto/node.proto",
                "proto/health.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}
