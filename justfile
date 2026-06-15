default:
    @just --list

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all --check

clippy:
    cargo clippy --all-features -- -D warnings
    cargo clippy --tests --all-features -- -D warnings -A clippy::unwrap_used -A clippy::expect_used

test:
    cargo test --all-features

check:
    cargo check --all-features

build:
    cargo build --all-features

build-release:
    cargo build --release --all-features

doc:
    cargo doc --all-features --no-deps

doc-open:
    cargo doc --all-features --no-deps --open

deny:
    cargo deny check

audit:
    cargo audit

ci: fmt-check clippy test deny

pre-commit: fmt clippy

clean:
    cargo clean

update:
    cargo update

tree:
    cargo tree

run *ARGS:
    cargo run -- {{ARGS}}
