package main

import (
	"math"
	"strings"
	"testing"
)

func TestParsePortRange(t *testing.T) {
	ports, err := parsePortRange("5201-5203")
	if err != nil {
		t.Fatalf("parsePortRange returned error: %v", err)
	}
	want := []int{5201, 5202, 5203}
	if len(ports) != len(want) {
		t.Fatalf("got %v, want %v", ports, want)
	}
	for i := range want {
		if ports[i] != want[i] {
			t.Fatalf("got %v, want %v", ports, want)
		}
	}
}

func TestNormalizeNeighborAddsDefaultPort(t *testing.T) {
	n, err := normalizeNeighbor("10.0.0.2", "9119")
	if err != nil {
		t.Fatalf("normalizeNeighbor returned error: %v", err)
	}
	if n.BaseURL != "http://10.0.0.2:9119" {
		t.Fatalf("got %q", n.BaseURL)
	}
	if n.Node != "10.0.0.2" {
		t.Fatalf("got node %q", n.Node)
	}
}

func TestParseUDPMeasurement(t *testing.T) {
	data := []byte(`{
		"end": {
			"sum": {
				"seconds": 5.0,
				"bits_per_second": 99900000,
				"jitter_ms": 0.42,
				"lost_packets": 2,
				"packets": 1000,
				"lost_percent": 0.2
			}
		}
	}`)
	measurement, err := parseIperfMeasurement("udp", data)
	if err != nil {
		t.Fatalf("parseIperfMeasurement returned error: %v", err)
	}
	if measurement.BandwidthBitsPerSecond != 99900000 {
		t.Fatalf("unexpected bandwidth: %v", measurement.BandwidthBitsPerSecond)
	}
	if math.Abs(measurement.JitterSeconds-0.00042) > 0.000000001 {
		t.Fatalf("unexpected jitter: %v", measurement.JitterSeconds)
	}
	if math.Abs(measurement.LossRatio-0.002) > 0.000000001 {
		t.Fatalf("unexpected loss ratio: %v", measurement.LossRatio)
	}
}

func TestMetricsRender(t *testing.T) {
	store := newMetricStore()
	store.Record(metricSample{
		Key: metricKey{
			LocalNode:  "a",
			PeerNode:   "b",
			ClientNode: "a",
			ServerNode: "b",
			Protocol:   "udp",
		},
		Success:                true,
		BandwidthBitsPerSecond: 100,
		JitterSeconds:          0.001,
		LostPackets:            1,
		LossRatio:              0.01,
	})

	rendered := store.Render()
	if !strings.Contains(rendered, `expeditus_iperf_probe_success{local_node="a",peer_node="b",client_node="a",server_node="b",protocol="udp"} 1`) {
		t.Fatalf("rendered metrics missing success sample:\n%s", rendered)
	}
}
