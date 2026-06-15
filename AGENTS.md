# AGENTS.md

Canonical contract for any agent working in this repository (Claude Code, Codex, Cursor, or a human collaborator). `CLAUDE.md` at the same level is a symlink to this file.

## What dipper is

`dipper` is a `cast`-like CLI for Ethereum Swarm. It talks to a [`vertex`](https://github.com/nxm-rs/vertex) node over gRPC and does the layer-2 work locally: chunking, BMT hashing, mantaray manifests, and postage stamping all run in-process via [`nectar`](https://github.com/nxm-rs/nectar) primitives, and batch / chain operations go on-chain via `alloy`. gRPC is only the transport to a node; it is a fat client, not a thin RPC shell.

Phase 1 (current): node status/topology, chunk download/upload, wallet/key loading.
Phase 2 (planned): chain operations (batch creation, balances).
Phase 3 (planned): mantaray manifests and multi-chunk file upload/download.

## Build, test, lint

- Edition `2024`, MSRV `1.92`. Do not raise MSRV without bumping `Cargo.toml` in the same commit.
- `cargo`/`rustc` and `protoc` come from the dev shell. Enter it with `nix develop` (this repo's `flake.nix`) or, when working inside the umbrella swarm checkout, `nix develop /code/nxm/swarm`.
- `protoc` is required: `build.rs` drives `tonic-build` to generate the gRPC clients from the vendored protos under `proto/`.
- `just ci` runs the full gate: `fmt-check`, `clippy -D warnings`, `test`, `deny`.
- `cargo fmt --all` formats. `cargo clippy --all-targets -- -D warnings` lints. Both are required pre-commit, zero tolerance for warning-bearing pushes.
- Offline smoke test: `cargo run -- wallet address --private-key 0x...` derives an address without touching the network.

## Layout

- `src/main.rs` wires the clap command tree to handlers under `src/commands/`.
- `src/cli.rs` is the clap derive surface; `src/rpc.rs` builds the tonic channel and clients; `src/proto.rs` re-exports the generated gRPC modules.
- `src/wallet.rs` loads a signer from a raw key or an EIP-2335 keystore; `src/chunkops.rs` builds and stamps a content chunk locally via nectar.
- `proto/` holds the protobuf definitions, vendored from `vertex`. Keep them in sync with the node's published API; do not diverge the wire shape.
- `nectar` is consumed via `../nectar` path dependencies. A standalone checkout therefore needs `nectar` checked out as a sibling directory; CI checks out both repos side by side.

## Repo boundary

Primitives (chunks, BMT, addressing, mantaray, postage) live in `nectar`, never here. If you find primitive-shaped code in dipper that another Swarm consumer would want, move it upstream to `nectar` and depend on it. dipper owns only the CLI surface, gRPC transport glue, and command orchestration.

## House rules

- **No em-dashes.** ASCII hyphens or split the sentence. Source, rustdoc, markdown, commits, PR bodies, chat output.
- **No Claude / AI attribution in commit messages or PR bodies.** No "Co-Authored-By: Claude", no robot footer.
- **Conventional Commits**, imperative mood. Scope by area: `feat(chunk): ...`, `fix(wallet): ...`, `chore(deps): ...`.
- PR bodies are markdown: one logical line per paragraph, no hard-wrapping. Let GitHub reflow.
- After every `git push`, run `gh pr checks <N>` and watch CI until green. `MERGEABLE` is not the success signal.
- Destructive git operations (`push --force` to a shared branch, `reset --hard`, deleting branches): confirm with the human owner first.
