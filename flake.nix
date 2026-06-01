{
  description = "expeditus-net iperf3 network performance exporter";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { self, nixpkgs }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f system (import nixpkgs { inherit system; }));
    in
    {
      packages = forAllSystems (system: pkgs:
        let
          expeditus-net = pkgs.buildGoModule {
            pname = "expeditus-net";
			version = "1.0.1";
            src = ./.;
            vendorHash = null;

            nativeBuildInputs = [ pkgs.makeWrapper ];

            postInstall = ''
              wrapProgram $out/bin/expeditus-net \
                --prefix PATH : ${pkgs.lib.makeBinPath [ pkgs.iperf3 ]}
            '';
          };
        in
        {
          inherit expeditus-net;
          default = expeditus-net;
        });

      apps = forAllSystems (system: pkgs: {
        default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/expeditus-net";
          meta.description = "iperf3 network performance exporter";
        };
      });

      checks = forAllSystems (system: pkgs: {
        default = self.packages.${system}.default;
      });

      devShells = forAllSystems (system: pkgs: {
        default = pkgs.mkShell {
          packages = [
            pkgs.go
            pkgs.gopls
            pkgs.iperf3
            pkgs.nixpkgs-fmt
          ];
        };
      });

      nixosModules.default = { config, lib, pkgs, ... }:
        let
          cfg = config.services.expeditus-net;
          toInt = value: builtins.fromJSON value;
          portRangeParts = lib.splitString "-" cfg.iperfPortRange;
          iperfPortRange =
            if builtins.length portRangeParts == 1 then
              let port = toInt (builtins.elemAt portRangeParts 0); in { from = port; to = port; }
            else
              {
                from = toInt (builtins.elemAt portRangeParts 0);
                to = toInt (builtins.elemAt portRangeParts 1);
              };
          bindAddress = "${cfg.bindAddress}:${toString cfg.port}";
          args = [
            "--node" cfg.nodeName
            "--bind-address" bindAddress
            "--iperf-bind-address" cfg.bindAddress
            "--iperf-port-range" cfg.iperfPortRange
            "--interval" cfg.interval
            "--duration" cfg.duration
            "--protocol" cfg.protocol
            "--bandwidth" cfg.bandwidth
          ]
          ++ lib.optionals (cfg.advertiseAddress != null) [ "--advertise-address" cfg.advertiseAddress ]
          ++ lib.optionals (cfg.tokenFile != null) [ "--token-file" cfg.tokenFile ]
          ++ lib.concatMap (neighbor: [ "--neighbor" neighbor ]) cfg.neighbors;
        in
        {
          options.services.expeditus-net = {
            enable = lib.mkEnableOption "expeditus-net iperf3 network performance exporter";

            package = lib.mkOption {
              type = lib.types.package;
              default = self.packages.${pkgs.stdenv.hostPlatform.system}.default;
              defaultText = lib.literalExpression "self.packages.\${pkgs.stdenv.hostPlatform.system}.default";
              description = "expeditus-net package to run.";
            };

            nodeName = lib.mkOption {
              type = lib.types.str;
              default = config.networking.hostName;
              defaultText = lib.literalExpression "config.networking.hostName";
              description = "Node label used in exported metrics.";
            };

            bindAddress = lib.mkOption {
              type = lib.types.str;
              default = "0.0.0.0";
              description = "Address for the HTTP control and metrics listener, and the default iperf bind address.";
            };

            port = lib.mkOption {
              type = lib.types.port;
              default = 9119;
              description = "HTTP control and metrics port.";
            };

            advertiseAddress = lib.mkOption {
              type = lib.types.nullOr lib.types.str;
              default = null;
              description = "Address peers should use for negotiated iperf server connections. Defaults to the daemon's bind-derived address.";
            };

            neighbors = lib.mkOption {
              type = lib.types.listOf lib.types.str;
              default = [ ];
              example = [ "10.0.0.2" "10.0.0.3:9119" ];
              description = "Neighbor IPs or control addresses this node should try to reach.";
            };

            iperfPortRange = lib.mkOption {
              type = lib.types.str;
              default = "5201-5210";
              description = "Inclusive server port range used for negotiated iperf3 tests.";
            };

            interval = lib.mkOption {
              type = lib.types.str;
              default = "60s";
              description = "Interval between probe rounds.";
            };

            duration = lib.mkOption {
              type = lib.types.str;
              default = "5s";
              description = "iperf3 test duration.";
            };

            protocol = lib.mkOption {
              type = lib.types.enum [ "both" "udp" "tcp" ];
              default = "both";
              description = "iperf3 protocol mode. The default runs UDP quality and TCP bandwidth probes sequentially.";
            };

            bandwidth = lib.mkOption {
              type = lib.types.str;
              default = "10M";
              description = "Target UDP bandwidth passed to iperf3.";
            };

            tokenFile = lib.mkOption {
              type = lib.types.nullOr lib.types.path;
              default = null;
              description = "Optional file containing the shared bearer token for peer control requests.";
            };

            openFirewall = lib.mkOption {
              type = lib.types.bool;
              default = false;
              description = "Open the HTTP port and negotiated iperf3 server port range in the NixOS firewall.";
            };
          };

          config = lib.mkIf cfg.enable {
            systemd.services.expeditus-net = {
              description = "expeditus-net iperf3 network performance exporter";
              wantedBy = [ "multi-user.target" ];
              wants = [ "network-online.target" ];
              after = [ "network-online.target" ];
              serviceConfig = {
                ExecStart = lib.escapeShellArgs ([ "${cfg.package}/bin/expeditus-net" ] ++ args);
                Restart = "on-failure";
                RestartSec = "5s";
                DynamicUser = true;
                NoNewPrivileges = true;
                PrivateTmp = true;
                ProtectHome = true;
                ProtectSystem = "strict";
              };
            };

            networking.firewall = lib.mkIf cfg.openFirewall {
              allowedTCPPorts = [ cfg.port ];
              allowedTCPPortRanges = [ iperfPortRange ];
              allowedUDPPortRanges = lib.mkIf (cfg.protocol != "tcp") [ iperfPortRange ];
            };
          };
        };
    };
}
