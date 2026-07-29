# AGENTS.md

This file is the canonical contract for any agent that works in this repository.
This includes Claude Code, Codex, Cursor, and a human collaborator.
`CLAUDE.md` at the same level is a symlink to this file.

## What dipper is

`dipper` is a `cast`-like CLI for Ethereum Swarm.
It talks to a [`vertex`](https://github.com/nxm-rs/vertex) node over gRPC.
It does the layer-2 work locally.
Chunking, BMT hashing, mantaray manifests, and postage stamping all run in-process through [`nectar`](https://github.com/nxm-rs/nectar) primitives.
Batch and chain operations go on-chain through `alloy`.
gRPC is only the transport to a node.
`dipper` is a fat client, not a thin RPC shell.

Phase 1 (current): node status/topology, chunk download/upload, wallet/key loading.
Phase 2 (planned): chain operations (batch creation, balances).
Phase 3 (planned): mantaray manifests and multi-chunk file upload/download.

## Build, test, lint

- Edition `2024`, MSRV `1.92`.
  Do not raise the MSRV without bumping `Cargo.toml` in the same commit.
- `cargo`, `rustc`, and `protoc` come from the dev shell.
  Enter the dev shell with `nix develop` for this repo's `flake.nix`.
  When you work inside the umbrella swarm checkout, run `nix develop /code/nxm/swarm`.
- `protoc` is required.
  `build.rs` drives `tonic-build` to generate the gRPC clients from the vendored protos under `proto/`.
- `just ci` runs the full gate: `fmt-check`, `clippy -D warnings`, `test` with nextest, and `deny`.
- `cargo fmt --all` formats the code.
  `cargo clippy --all-targets -- -D warnings` lints the code.
  Both are required before a commit.
  Do not push code that has warnings.
- Tests run under `cargo nextest run`, or `just test`.
  dipper is a binary crate, so there are no doctests.
- Claude hooks in `.claude/` run `rustfmt` on every `.rs` edit.
  They also run `cargo nextest run` on touched crates when a turn ends.
  Both hooks do nothing outside the dev shell.
- The offline smoke test is `cargo run -- wallet address --private-key 0x...`.
  It derives an address without any network access.

## Layout

- `src/main.rs` wires the clap command tree to handlers under `src/commands/`.
- `src/cli.rs` is the clap derive surface.
  `src/rpc.rs` builds the tonic channel and clients.
  `src/proto.rs` re-exports the generated gRPC modules.
- `src/wallet.rs` loads a signer from a raw key or an EIP-2335 keystore.
  `src/chunkops.rs` builds and stamps a content chunk locally through nectar.
- `proto/` holds the protobuf definitions, vendored from `vertex`.
  Keep them in sync with the node's published API.
  Do not diverge the wire shape.
- `nectar` comes from crates.io, pinned at `0.2.1` or later.
  `0.2.1` is the first release with the corrected contract addresses.
  dipper builds standalone from a plain clone.
  There is no sibling-checkout requirement.

## Repo boundary

Primitives live in `nectar`, never here.
These primitives are chunks, BMT, addressing, mantaray, and postage.
If you find primitive-shaped code in dipper that another Swarm consumer would want, move it upstream to `nectar` and depend on it.
dipper owns only the CLI surface, the gRPC transport glue, and the command orchestration.

## House rules

- **No em-dashes** in source, rustdoc, or markdown.
  `.claude/hooks/content-lint.sh` blocks any edit that adds one.
  Keep commit messages, PR bodies, and chat free of em-dashes too.
  Use ASCII hyphens or split the sentence.
- **Disclose AI assistance**, which is the nxm-rs org policy.
  Add an honest `AI Assistance: <tool> used for <what>` line to PR bodies and commit messages.
  Never add the `Co-Authored-By: Claude Code` or `Generated with Claude Code` boilerplate footer.
- **Conventional Commits** in the imperative mood.
  Scope by area: `feat(chunk): ...`, `fix(wallet): ...`, and `chore(deps): ...`.
- PR bodies are markdown.
  Keep one logical line per paragraph and do not hard-wrap.
  Let GitHub reflow the text.
- After every `git push`, run `gh pr checks <N>` and watch CI until it is green.
  `MERGEABLE` is not the success signal.
- Confirm destructive git operations with the human owner first.
  These operations are `push --force` to a shared branch, `reset --hard`, and deleting branches.

## Documentation

Write all documentation in ASD-STE100 Simplified Technical English.
Use short sentences, the active voice, and one idea per sentence.
In markdown files, put each sentence on its own line and do not wrap within a sentence; GitHub reflows the file when it displays it.
This keeps a diff to one changed line per changed sentence.
In PR and issue bodies, keep one line per paragraph, because GitHub renders single newlines in a comment as line breaks.
