# dipper

A `cast`-like CLI for Ethereum Swarm. `dipper` talks to a [`vertex`](../vertex)
node over gRPC and uses the [`nectar`](../nectar) primitives to build and stamp
chunks locally.

## Phase 1 commands

```
dipper node status            # Node.GetStatus     — overlay, depth, peer counts
dipper node topology          # Node.GetTopology   — Kademlia bins
dipper chunk download <addr>  # Chunk.RetrieveChunk
dipper chunk upload <file>    # local chunk+stamp -> Chunk.UploadChunk
dipper wallet address         # load a signer, print its address (offline)
```

Global flags: `--endpoint <url>` (default `http://127.0.0.1:1635`) and
`--network <gnosis|sepolia>` (default `gnosis`, reserved for later phases).

### Examples

```bash
# Offline: derive an address from a key or keystore
dipper wallet address --private-key 0x<32-byte-hex>
dipper wallet address --keystore ./key.json          # prompts for password

# Upload a single content chunk (<= 4096 bytes) with a postage stamp
dipper chunk upload ./hello.txt \
    --batch-id 0x<batch> --depth 20 --bucket-depth 16 \
    --private-key 0x<32-byte-hex>

# Download a chunk's payload to a file (use --raw for span + payload)
dipper chunk download <addr> --out ./out.bin
```

## Building

`cargo`/`rustc` and `protoc` come from the swarm nix devshell. Build via:

```bash
nix develop /code/nxm/swarm --command cargo build
nix develop /code/nxm/swarm --command cargo run -- --help
```

## Status

- Phase 1 (this): node status/topology, chunk download/upload, wallet loading.
- Phase 2 (planned): chain operations (batch creation, balances).
- Phase 3 (planned): mantaray manifests and multi-chunk file upload/download.
