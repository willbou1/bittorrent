{
    inputs = {
        flake-utils.url = "github:numtide/flake-utils";
        nixpkgs.url = "nixpkgs/nixos-unstable";
    };

    outputs = { self, nixpkgs, flake-utils, ... }:
        flake-utils.lib.eachDefaultSystem (system:
            let pkgs = import nixpkgs { inherit system; };
            in with pkgs; {
                devShell = mkShell {
                    packages = with pkgs; [
                        rust-analyzer
                        cargo
                        rustc
                        pkg-config
                    ];
                    buildInputs = [
                        openssl_3
                    ];
                };
    });
}
