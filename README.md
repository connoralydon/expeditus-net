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
- `expeditus_host_receive_bits_per_second`
- `expeditus_host_transmit_bits_per_second`
- `expeditus_host_traffic_last_run_timestamp_seconds`

By default each peer round acquires a pair lease, then runs a UDP quality probe at `10M` with `iperf3 --bidir` for jitter and loss, followed by two one-way TCP bandwidth probes for maximum bandwidth in each direction. Each active probe emits samples for both `client_node`/`server_node` directions. Jitter and packet loss are only emitted for UDP probes.

The daemon also samples aggregate non-loopback host traffic from `/proc/net/dev` every `30s` and exports receive/transmit rates with only the `local_node` label.

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
  --neighbor 10.0.0.2
```

Run the same daemon on the neighbor with the opposite bind and neighbor addresses.

Use `--protocol udp`, `--protocol tcp`, or `--protocol both` to override the default probe mode. `--bandwidth` only controls the UDP target rate and defaults to `10M`. The default active probe duration is `15s`, with a `3s` `--warmup` omitted from iperf results.

Only one node initiates probes for each peer pair: the node with the lexicographically smaller node name. That owner alternates roles per neighbor. On one round it asks the neighbor to start an `iperf3` server and runs the client locally. On the next round it starts the server and asks the neighbor to run the client.

Each daemon participates in only one active peer lease at a time. A lease covers the full protocol sequence for a peer round, so `--protocol both` runs UDP and TCP without releasing/reacquiring busy state between protocols. If a peer is already busy, the lease request is retried briefly within the same round before per-protocol failures are recorded.

## Peer API

- `GET /health`
- `GET /metrics`
- `GET /v1/info`
- `POST /v1/lease/acquire`
- `POST /v1/lease/release`
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
            protocol = "both";
            bandwidth = "10M";
            duration = "15s";
            warmup = "3s";
            trafficInterval = "30s";
            openFirewall = true;
          };
        }
      ];
    };
  };
}
```

`neighbors` can be plain IPs, `host:port`, or full `http://host:port` URLs. Plain IPs use the configured control port.

## Development

```sh
nix develop
go test ./...
nix flake check
```
