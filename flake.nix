# FLAKE.NIX
# Cruster - Distributed Entity Framework - Nix Flake
#
# This flake provides:
#   - devShells: Development environments for Rust
#   - checks: Nix code quality checks (statix, deadnix, nixfmt)
#   - formatter: nixfmt-rfc-style for `nix fmt`
#
# Quick reference:
#   nix develop                 - Enter default dev shell
#   nix develop .#rust          - Enter Rust-only dev shell
#   nix flake check             - Run all Nix lints and format checks

{
  description = "Cruster - Distributed Entity Framework for Rust";

  # ===========================================================================
  # INPUTS
  # ===========================================================================

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.05";

    # Rust toolchain manager - provides stable, nightly, and custom toolchains
    # https://github.com/nix-community/fenix
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  # ===========================================================================
  # OUTPUTS
  # ===========================================================================

  outputs =
    {
      nixpkgs,
      fenix,
      ...
    }:
    let
      # -----------------------------------------------------------------------
      # System Configuration
      # -----------------------------------------------------------------------

      supportedSystems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];

      # Helper to generate attrs for all systems
      forAllSystems = f: nixpkgs.lib.genAttrs supportedSystems f;

      # -----------------------------------------------------------------------
      # Per-System Helpers
      # -----------------------------------------------------------------------

      # Get nixpkgs for a system
      pkgsFor = system: import nixpkgs { inherit system; };

      # Get Rust toolchains for a system (currently unused, kept for future use)
      toolchainsFor = system: { };

      # Get target architecture name for RUSTFLAGS
      archFor = system: if builtins.match "aarch64-.*" system != null then "aarch64" else "x86_64";

    in
    {
      # -----------------------------------------------------------------------
      # Development Shells
      # -----------------------------------------------------------------------
      # Defined in nix/shells.nix

      devShells = forAllSystems (
        system:
        import ./nix/shells.nix {
          pkgs = pkgsFor system;
          toolchains = toolchainsFor system;
          arch = archFor system;
        }
      );

      # -----------------------------------------------------------------------
      # Checks
      # -----------------------------------------------------------------------
      # Nix code quality checks run by `nix flake check`
      # Defined in nix/checks.nix

      checks = forAllSystems (
        system:
        import ./nix/checks.nix {
          pkgs = pkgsFor system;
          src = ./.;
        }
      );

      # -----------------------------------------------------------------------
      # Formatter
      # -----------------------------------------------------------------------
      # Used by `nix fmt` to format Nix files

      formatter = forAllSystems (system: (pkgsFor system).nixfmt-rfc-style);
    };
}
