package main

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
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

func TestParseConfigDefaultsToBothProtocols(t *testing.T) {
	cfg, err := parseConfig(nil)
	if err != nil {
		t.Fatalf("parseConfig returned error: %v", err)
	}
	if cfg.Protocol != "both" {
		t.Fatalf("got protocol %q, want both", cfg.Protocol)
	}
	if cfg.Bandwidth != "10M" {
		t.Fatalf("got bandwidth %q, want 10M", cfg.Bandwidth)
	}
	protocols := cfg.probeProtocols()
	if len(protocols) != 2 || protocols[0] != "udp" || protocols[1] != "tcp" {
		t.Fatalf("got probe protocols %v, want [udp tcp]", protocols)
	}
}

func TestInitiatorOwnsPair(t *testing.T) {
	if !initiatorOwnsPair("node-a", "node-b") {
		t.Fatalf("expected node-a to own node-a/node-b")
	}
	if initiatorOwnsPair("node-b", "node-a") {
		t.Fatalf("expected node-b not to own node-a/node-b")
	}
	if initiatorOwnsPair("node-a", "node-a") {
		t.Fatalf("expected matching node names not to own a pair")
	}
}

func TestHandleInfoPublishesVersion(t *testing.T) {
	a := &app{cfg: &config{NodeName: "node-a"}}
	req := httptest.NewRequest(http.MethodGet, "/v1/info", nil)
	rec := httptest.NewRecorder()

	a.handleInfo(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("got status %d, want %d", rec.Code, http.StatusOK)
	}
	var info infoResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &info); err != nil {
		t.Fatalf("decode info response: %v", err)
	}
	if info.NodeName != "node-a" {
		t.Fatalf("got node name %q, want node-a", info.NodeName)
	}
	if info.Version != appVersion {
		t.Fatalf("got version %q, want %q", info.Version, appVersion)
	}
}

func TestTestSlotAllowsOneActiveTest(t *testing.T) {
	a := &app{}
	release, ok := a.tryAcquireTestSlot()
	if !ok {
		t.Fatalf("first slot acquire failed")
	}
	if _, ok := a.tryAcquireTestSlot(); ok {
		t.Fatalf("second slot acquire succeeded while active")
	}
	release()
	release()
	if release, ok := a.tryAcquireTestSlot(); !ok {
		t.Fatalf("slot acquire failed after release")
	} else {
		release()
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
	store.Record(metricSample{
		Key: metricKey{
			LocalNode:  "a",
			PeerNode:   "b",
			ClientNode: "a",
			ServerNode: "b",
			Protocol:   "tcp",
		},
		Success:                true,
		BandwidthBitsPerSecond: 1000,
	})

	rendered := store.Render()
	if !strings.Contains(rendered, `expeditus_iperf_probe_success{local_node="a",peer_node="b",client_node="a",server_node="b",protocol="udp"} 1`) {
		t.Fatalf("rendered metrics missing success sample:\n%s", rendered)
	}
	if !strings.Contains(rendered, `expeditus_iperf_probe_success{local_node="a",peer_node="b",client_node="a",server_node="b",protocol="tcp"} 1`) {
		t.Fatalf("rendered metrics missing tcp success sample:\n%s", rendered)
	}
	if strings.Contains(rendered, `expeditus_iperf_bandwidth_bits_per_second{local_node="a",peer_node="b",client_node="a",server_node="b",protocol="udp"}`) {
		t.Fatalf("rendered metrics included udp bandwidth sample:\n%s", rendered)
	}
	if !strings.Contains(rendered, `expeditus_iperf_bandwidth_bits_per_second{local_node="a",peer_node="b",client_node="a",server_node="b",protocol="tcp"} 1000`) {
		t.Fatalf("rendered metrics missing tcp bandwidth sample:\n%s", rendered)
	}
	if strings.Contains(rendered, `expeditus_iperf_jitter_seconds{local_node="a",peer_node="b",client_node="a",server_node="b",protocol="tcp"}`) {
		t.Fatalf("rendered metrics included tcp jitter sample:\n%s", rendered)
	}
}
