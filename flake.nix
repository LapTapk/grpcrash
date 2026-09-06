{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-26.05";
    unixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      unixpkgs,
      rust-overlay,
    }:
    let
      forEachSystem = nixpkgs.lib.genAttrs nixpkgs.lib.systems.flakeExposed;
    in
    {
      devShells = forEachSystem (
        system:
        let
          importPkgs =
            inp:
            import inp {
              inherit system;
              overlays = [
                (import rust-overlay)
              ];
            };

          pkgs = importPkgs nixpkgs;
          upkgs = importPkgs unixpkgs;

          rust = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
        in
        {
          default =
            (pkgs.mkShell.override {
              stdenv = pkgs.clangStdenv;
            })
              {
                strictDeps = true;

                nativeBuildInputs = with pkgs; [
                  rust

                  rustPlatform.bindgenHook

                  cmake
                  ninja
                  pkg-config

                  clang-tools
                  gdb
                ];

                RUST_BACKTRACE = "1";
              };
        }
      );
    };
}
