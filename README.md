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

Global flags: `--endpoint <url>` (default `http://127.0.0.1:1635`),
`--network <gnosis|sepolia>` (default `gnosis`), and `--rpc-url <url>`
(Ethereum RPC, required by the `batch` subcommands).

## Phase 2 commands: on-chain postage batches

The `batch` subcommands talk directly to the Swarm `PostageStamp` contract over
an Ethereum RPC (`--rpc-url`). The address book and the expected chain id are
selected by `--network` (Gnosis mainnet `100` / Sepolia `11155111`). BZZ is a
16-decimal token; `--amount` is the balance *per chunk* as a decimal BZZ string.

```
dipper batch create --amount <bzz> --depth <d> [--bucket-depth 16] \
        [--immutable] [--owner <addr>] [--nonce 0x<32b>] <signer>
                              # BZZ.approve(total) then PostageStamp.createBatch;
                              # total = amount * 2^depth. Prints the authoritative
                              # batchId read from the BatchCreated receipt.
dipper batch topup  --batch-id 0x<id> --amount <bzz> <signer>
                              # PostageStamp.topUp(batchId, amountPerChunk);
                              # total is computed from the batch's STORED depth.
dipper batch dilute --batch-id 0x<id> --depth <newDepth> <signer>
                              # PostageStamp.increaseDepth (no token transfer)
dipper batch info   --batch-id 0x<id>
                              # read-only: owner / depth / bucketDepth /
                              # immutable / balance
```

`<signer>` is one of `--private-key 0x<hex>` or `--keystore <file>`
(`--password`, `$DIPPER_KEYSTORE_PASSWORD`, or prompt). The owner defaults to
the signer's address; the nonce is random if omitted. `bucket-depth` must be
`16` to match bee.

## Phase 3 commands: mantaray manifests

Multi-chunk file, directory, and archive upload/download via mantaray
manifests, stamped with an existing postage batch.

```
dipper upload <path> --batch-id 0x<id> --depth <d> [--bucket-depth 16] \
        [--index-document index.html] [--error-document <name>] <signer>
                              # <path> is a single file, a directory tree, or a
                              # .tar.gz/.tgz archive. Prints the manifest root.
                              # Directory/archive uploads default the website
                              # index document to index.html.
dipper download <root> [path] [--out <target>]
                              # with [path]: extract one file. Without it: rebuild
                              # the whole tree under --out (a raw-file root is
                              # written directly).
dipper ls <root> [--long]     # list manifest entries (addr / content-type /
                              # path); --long adds a size column (extra RPC/entry)
```

### Examples

```bash
# Create a batch, then upload a website directory under it
dipper batch create --amount 1.0 --depth 20 --rpc-url https://rpc.gnosischain.com \
    --private-key 0x<32-byte-hex>
dipper upload ./site --batch-id 0x<batch> --depth 20 \
    --private-key 0x<32-byte-hex>

# Inspect and pull a manifest back down
dipper ls 0x<root> --long
dipper download 0x<root> index.html --out ./index.html
dipper download 0x<root> --out ./site-copy     # whole tree
```

### Phase 1 examples

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

- Phase 1 (done): node status/topology, chunk download/upload, wallet loading.
- Phase 2 (done): on-chain batch operations (create / topup / dilute / info).
- Phase 3 (done): mantaray manifests and multi-chunk file upload/download.

### Known limitations

- Encrypted manifests are not supported.
- `download` does not yet resolve directory / trailing-slash / index-document
  paths: pass an explicit file path, or omit the path to rebuild the whole tree.
