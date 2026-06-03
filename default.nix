{
  pkgs ? import <nixpkgs> { },
}:
rec {
  moq-rs = pkgs.callPackage ./package.nix { };
  default = moq-rs;
  publish = pkgs.dockerTools.buildLayeredImage {
    name = "moq-pub";
    tag = "latest";
    contents = pkgs.buildEnv {
      name = "image-root";
      paths = with pkgs; [
        bashInteractive
        coreutils
        ffmpeg
        wget
        moq-rs
      ];
      pathsToLink = [ "/bin" ];
    };
    config = {
      Entrypoint = [ "bash" ];
      Cmd = [ deploy/publish ];
    };
  };
  relay = pkgs.dockerTools.buildLayeredImage {
    name = "moq-relay";
    tag = "latest";
    contents = pkgs.buildEnv {
      name = "image-root";
      paths = with pkgs; [
        bashInteractive
        coreutils
        curl
        dockerTools.caCertificates
        moq-rs
      ];
      pathsToLink = [
        "/bin"
        "/etc"
      ];
    };
    config = {
      Entrypoint = [ "bash" ];
      Cmd = [ "moq-relay" ];
    };
  };
}
