# SHELLS.NIX
# Development shell definitions for the Cruster project
#
# This module defines two shells:
#   - default: Full shell with Rust + dev tools
#   - rust: Rust-only dev shell (fastest startup)
#
# Usage:
#   nix develop            # default shell
#   nix develop .#rust     # Rust development only

{
  pkgs,
  toolchains,
  arch, # "aarch64" or "x86_64" for RUSTFLAGS hint
}:

let
  inherit (pkgs) lib;
  inherit (pkgs.stdenv) isLinux isDarwin;

  # ===========================================================================
  # SHARED COMPONENTS
  # ===========================================================================

  # Common build inputs for Rust development
  commonBuildInputs = [
    pkgs.zlib
    pkgs.openssl
  ]
  ++ lib.optionals isLinux [ pkgs.elfutils ]
  ++ lib.optionals isDarwin [ pkgs.libiconv pkgs.darwin.apple_sdk.frameworks.Security ];

  # ---------------------------------------------------------------------------
  # Component: Rust (stable from nixpkgs)
  # ---------------------------------------------------------------------------
  rustInputs = [
    pkgs.rustc
    pkgs.cargo
    pkgs.clippy
    pkgs.rustfmt
    pkgs.rust-analyzer
    pkgs.pkg-config
    pkgs.protobuf # Required for prost-build (gRPC)
  ];

  # ---------------------------------------------------------------------------
  # Component: Common dev tools
  # ---------------------------------------------------------------------------
  commonDevInputs = [
    pkgs.git
    pkgs.nodejs_24
  ];

  # ---------------------------------------------------------------------------
  # Shell hook: rulesync setup
  # ---------------------------------------------------------------------------
  rulesyncHook = ''
    # Install Node.js dependencies and run rulesync (suppress output)
    if [ -f package.json ]; then
      corepack pnpm install --frozen-lockfile --silent 2>/dev/null || corepack pnpm install --silent
      corepack pnpm rulesync generate >/dev/null 2>&1 || true
    fi
  '';

in
{
  # ===========================================================================
  # DEFAULT SHELL
  # ===========================================================================
  # Full development environment with all tools.
  default = pkgs.mkShell {
    nativeBuildInputs = rustInputs ++ commonDevInputs;

    buildInputs = commonBuildInputs;

    shellHook = ''
      ${rulesyncHook}

      echo "Cruster Development Environment"
      echo "================================"
      echo "Rust: $(${pkgs.rustc}/bin/rustc --version) (nixpkgs)"
      echo ""
      echo "Commands:"
      echo "  cargo build              - Build all crates"
      echo "  cargo test               - Run tests"
      echo "  cargo test -p cruster    - Run cruster tests only"
      echo ""
    '';
  };

  # ===========================================================================
  # RUST SHELL (Fast)
  # ===========================================================================
  # Optimized for quick shell startup using nixpkgs Rust toolchain.
  rust = pkgs.mkShell {
    nativeBuildInputs = rustInputs ++ commonDevInputs;

    buildInputs = commonBuildInputs;

    shellHook = ''
      ${rulesyncHook}

      echo "Cruster Rust Development Environment"
      echo "====================================="
      echo "Rust: $(rustc --version) (nixpkgs)"
      echo ""
      echo "Commands:"
      echo "  cargo build              - Build all crates"
      echo "  cargo test               - Run tests"
      echo "  cargo clippy             - Run lints"
      echo ""
    '';
  };
}
