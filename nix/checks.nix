# CHECKS.NIX
# Nix code quality checks for the Autopilot project
#
# These checks run via `nix flake check`:
#   - statix: Lint for common Nix antipatterns
#   - deadnix: Find unused Nix code
#
# For formatting, use `nix fmt` (nixfmt RFC style)

{ pkgs, src }:

{
  # Lint Nix code for common issues and antipatterns
  statix =
    pkgs.runCommand "statix-check"
      {
        nativeBuildInputs = [ pkgs.statix ];
        inherit src;
      }
      ''
        statix check $src
        touch $out
      '';

  # Find dead/unused Nix code
  deadnix =
    pkgs.runCommand "deadnix-check"
      {
        nativeBuildInputs = [ pkgs.deadnix ];
        inherit src;
      }
      ''
        deadnix --fail $src
        touch $out
      '';
}
