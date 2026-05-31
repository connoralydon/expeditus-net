# expeditus-net

`expeditus-net` is an `iperf3`-based network performance exporter. Each node runs the same daemon, negotiates which side should start an `iperf3` server for each probe, and exports cached Prometheus metrics for Alloy or VictoriaMetrics scraping.

## Metrics

The daemon serves Prometheus text metrics at `/metrics`:

- `expeditus_iperf_probe_success`
- `expeditus_iperf_bandwidth_bits_per_second`
- `expeditus_iperf_jitter_seconds`
- `expeditus_iperf_lost_packets`
- `expeditus_iperf_loss_ratio`
- `expeditus_iperf_probe_duration_seconds`
- `expeditus_iperf_last_run_timestamp_seconds`

Jitter and packet loss are only emitted for UDP probes.

## Ports

- HTTP control and metrics listener: default `:9119`.
- Negotiated `iperf3` server port: default range `5201-5210`.
- No client port is configured or negotiated. Clients use OS-assigned ephemeral source ports.

## Running

```sh
nix run . -- \
  --node node-a \
  --bind-address 10.0.0.1:9119 \
  --advertise-address 10.0.0.1 \
  --neighbor 10.0.0.2 \
  --protocol udp \
  --bandwidth 100M \
  --directions forward,reverse
```

Run the same daemon on the neighbor with the opposite bind and neighbor addresses.

The default role policy alternates per neighbor. On one round the local node asks the neighbor to start an `iperf3` server and runs the client locally. On the next round the local node starts the server and asks the neighbor to run the client.

## Peer API

- `GET /health`
- `GET /metrics`
- `POST /v1/negotiate`
- `POST /v1/iperf/client/run`

Set `--token` or `--token-file` on every node to require a shared bearer token for peer control requests.

## NixOS Module

```nix
{
  inputs.expeditus-net.url = "path:/path/to/expeditus-net";

  outputs = { self, nixpkgs, expeditus-net, ... }: {
    nixosConfigurations.node-a = nixpkgs.lib.nixosSystem {
      system = "x86_64-linux";
      modules = [
        expeditus-net.nixosModules.default
        {
          services.expeditus-net = {
            enable = true;
            bindAddress = "10.0.0.1";
            port = 9119;
            advertiseAddress = "10.0.0.1";
            neighbors = [ "10.0.0.2" ];
            iperfPortRange = "5201-5210";
            protocol = "udp";
            bandwidth = "100M";
            directions = [ "forward" "reverse" ];
            openFirewall = true;
          };
        }
      ];
    };
  };
}
```

`neighbors` can be plain IPs, `host:port`, or full `http://host:port` URLs. Plain IPs use the configured control port.

## Alloy Example

```river
prometheus.scrape "expeditus_net" {
  targets = [
    { __address__ = "10.0.0.1:9119" },
    { __address__ = "10.0.0.2:9119" },
  ]

  forward_to = [prometheus.remote_write.victoriametrics.receiver]
}

prometheus.remote_write "victoriametrics" {
  endpoint {
    url = "http://victoriametrics:8428/api/v1/write"
  }
}
```

## Development

```sh
nix develop
go test ./...
nix flake check
```
