{
  description = "dipper - a cast-like CLI for Ethereum Swarm over a vertex node";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };

        # Toolchain pinned by rust-toolchain.toml (channel 1.92 + rustfmt,
        # clippy). Using fromRustupToolchainFile keeps the flake and the
        # rustup-style file as the single source of truth.
        rustToolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
      in
      {
        devShells.default = pkgs.mkShell {
          name = "dipper-dev";

          buildInputs = with pkgs; [
            rustToolchain
            pkg-config
            openssl
            openssl.dev
            # protoc is required: build.rs drives tonic-build to generate the
            # gRPC clients from the vendored protos.
            protobuf
            just
            # Release / advisory tooling.
            cargo-deny
            cargo-audit
            cargo-release
            git-cliff
            git
          ];

          OPENSSL_DIR = "${pkgs.openssl.dev}";
          OPENSSL_LIB_DIR = "${pkgs.openssl.out}/lib";
          PKG_CONFIG_PATH = "${pkgs.openssl.dev}/lib/pkgconfig";

          shellHook = ''
            echo "dipper dev shell: rust $(rustc --version), protoc $(protoc --version)"
          '';
        };
      }
    );
}
