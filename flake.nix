{
  description = "stitch: reliable ssh connections";
  inputs.nelhage.url = "github:nelhage/nix-config";
  inputs.nixpkgs.follows = "nelhage/nixpkgs";

  outputs =
    { nixpkgs, ... }:
    let
      forAllSystems = nixpkgs.lib.genAttrs [
        "aarch64-linux"
        "x86_64-linux"
        "aarch64-darwin"
      ];
    in
    {
      devShells = forAllSystems (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
        in
        rec {
          default = pkgs.mkShell {
            packages = [
              pkgs.cargo
              pkgs.rustc
              pkgs.clippy
              pkgs.rustfmt
              pkgs.rust-analyzer
            ];
            RUST_SRC_PATH = "${pkgs.rustPlatform.rustLibSrc}";
          };
          # The default shell, plus a pinned OpenSSH for the e2e tests
          # (tests/e2e_sshd.rs). Kept separate so that the development shell
          # doesn't shadow the user's own `ssh`.
          ci = pkgs.mkShell {
            inputsFrom = [ default ];
            packages = [ pkgs.openssh ];
            inherit (default) RUST_SRC_PATH;
          };
        }
      );
    };
}
