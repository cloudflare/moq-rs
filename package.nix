{
  lib,
  rustPlatform,
  pkg-config,
  openssl,
  stdenv,
  darwin,
}:

rustPlatform.buildRustPackage {
  name = "moq-rs";

  src =
    let
      ignoredPaths = [
        "default.nix"
        "flake.nix"
        "flake.lock"
        "package.nix"
      ];
    in
    lib.cleanSourceWith {
      filter = name: type: !(builtins.elem (baseNameOf name) ignoredPaths);
      src = lib.cleanSource ./.;
    };

  cargoLock = {
    lockFile = ./Cargo.lock;
    allowBuiltinFetchGit = true;
  };

  nativeBuildInputs = [
    pkg-config
    rustPlatform.bindgenHook
  ];

  buildInputs =
    [
      openssl
    ]
    ++ lib.optionals stdenv.isDarwin [
      darwin.apple_sdk.frameworks.Security
      darwin.apple_sdk.frameworks.SystemConfiguration
    ];

  meta = {
    description = "Fork of kixelated/moq-rs to continue tracking the IETF MoQ Working Group drafts";
    homepage = "https://github.com/englishm/moq-rs";
    license = with lib.licenses; [
      asl20
      mit
    ];
    maintainers = with lib.maintainers; [ niklaskorz ];
    mainProgram = "moq-pub";
  };
}
