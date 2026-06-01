package main

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
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

func withBusyRetryTiming(t *testing.T, timeout time.Duration, backoff time.Duration) {
	t.Helper()
	oldTimeout := busyRetryTimeout
	oldBackoff := busyRetryBackoff
	busyRetryTimeout = timeout
	busyRetryBackoff = backoff
	t.Cleanup(func() {
		busyRetryTimeout = oldTimeout
		busyRetryBackoff = oldBackoff
	})
}

func TestIsBusyConflictOnlyMatchesRunningTest409(t *testing.T) {
	busyErr := &httpStatusError{StatusCode: http.StatusConflict, Message: "node is already running a test"}
	if !isBusyConflict(busyErr) {
		t.Fatalf("expected busy 409 to be retryable")
	}

	pairOwnerErr := &httpStatusError{StatusCode: http.StatusConflict, Message: "peer pair is owned by a"}
	if isBusyConflict(pairOwnerErr) {
		t.Fatalf("expected non-busy 409 not to be retryable")
	}

	serverErr := &httpStatusError{StatusCode: http.StatusServiceUnavailable, Message: "node is already running a test"}
	if isBusyConflict(serverErr) {
		t.Fatalf("expected non-409 busy message not to be retryable")
	}
}

func TestAcquirePairLeaseWithBusyRetryWaitsForRelease(t *testing.T) {
	withBusyRetryTiming(t, 100*time.Millisecond, time.Millisecond)
	a := &app{}
	lease, ok := a.tryAcquirePairLease(time.Second)
	if !ok {
		t.Fatalf("initial lease acquire failed")
	}

	released := make(chan struct{})
	go func() {
		time.Sleep(5 * time.Millisecond)
		lease.Release()
		close(released)
	}()

	retryLease, ok := a.acquirePairLeaseWithBusyRetry(context.Background(), time.Second)
	if !ok {
		t.Fatalf("lease acquire did not retry until release")
	}
	retryLease.Release()
	<-released
}

func TestPairLeaseBlocksTestSlotUntilRelease(t *testing.T) {
	a := &app{}
	lease, ok := a.tryAcquirePairLease(time.Second)
	if !ok {
		t.Fatalf("lease acquire failed")
	}
	if _, ok := a.tryAcquireTestSlot(); ok {
		t.Fatalf("test slot acquired while pair lease was active")
	}
	if !a.hasPairLease(lease.ID) {
		t.Fatalf("active pair lease was not recognized")
	}
	lease.Release()
	releaseTest, ok := a.tryAcquireTestSlot()
	if !ok {
		t.Fatalf("test slot not available after pair lease release")
	}
	releaseTest()
}

func TestPostJSONWithBusyRetryRetriesBusyPeer(t *testing.T) {
	withBusyRetryTiming(t, 100*time.Millisecond, time.Millisecond)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/negotiate" {
			t.Fatalf("got path %q, want /v1/negotiate", r.URL.Path)
		}
		if requests.Add(1) == 1 {
			writeJSON(w, http.StatusConflict, negotiateResponse{Accepted: false, Error: "node is already running a test"})
			return
		}
		writeJSON(w, http.StatusOK, negotiateResponse{Accepted: true, ServerNode: "b"})
	}))
	defer server.Close()

	a := &app{cfg: &config{NodeName: "a"}, client: server.Client()}
	var resp negotiateResponse
	err := a.postJSONWithBusyRetry(context.Background(), neighbor{BaseURL: server.URL, Node: "b"}, "/v1/negotiate", negotiateRequest{
		ClientNode:      "a",
		Protocol:        "tcp",
		DurationSeconds: 1,
	}, &resp)
	if err != nil {
		t.Fatalf("postJSONWithBusyRetry returned error: %v", err)
	}
	if requests.Load() != 2 {
		t.Fatalf("got %d requests, want 2", requests.Load())
	}
	if !resp.Accepted {
		t.Fatalf("response was not accepted")
	}
}

func TestRunPeerProbeRecordsFailuresAfterBusyLeaseRetry(t *testing.T) {
	withBusyRetryTiming(t, 5*time.Millisecond, time.Millisecond)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/lease/acquire" {
			t.Fatalf("got path %q, want /v1/lease/acquire", r.URL.Path)
		}
		requests.Add(1)
		writeJSON(w, http.StatusConflict, pairLeaseAcquireResponse{Accepted: false, Error: "node is already running a test"})
	}))
	defer server.Close()

	a := &app{cfg: &config{NodeName: "a", Bandwidth: "10M"}, client: server.Client(), metrics: newMetricStore()}
	a.runPeerProbe(context.Background(), neighbor{BaseURL: server.URL, Node: "b"}, "b", []string{"udp", "tcp"}, true, 1)

	if requests.Load() < 2 {
		t.Fatalf("got %d requests, want at least 2", requests.Load())
	}
	for _, protocol := range []string{"udp", "tcp"} {
		key := metricKey{LocalNode: "a", PeerNode: "b", ClientNode: "a", ServerNode: "b", Protocol: protocol}
		a.metrics.mu.RLock()
		sample, ok := a.metrics.samples[key]
		a.metrics.mu.RUnlock()
		if !ok {
			t.Fatalf("missing failed %s sample", protocol)
		}
		if sample.Success {
			t.Fatalf("got successful %s sample, want failure", protocol)
		}
		if sample.LastRun.IsZero() {
			t.Fatalf("failed %s sample did not update last run timestamp", protocol)
		}
	}
}

func TestIdleActivityWarnsAfterTwentyMinutes(t *testing.T) {
	started := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)
	a := &app{lastActivity: started}

	idleFor, warn, restart := a.checkIdleActivity(started.Add(activityWarningAfter))

	if idleFor != activityWarningAfter {
		t.Fatalf("got idle duration %s, want %s", idleFor, activityWarningAfter)
	}
	if !warn {
		t.Fatalf("expected warning after %s idle", activityWarningAfter)
	}
	if restart {
		t.Fatalf("did not expect restart after %s idle", activityWarningAfter)
	}
}

func TestIdleActivityWarnsOnceUntilActivity(t *testing.T) {
	started := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)
	a := &app{lastActivity: started}

	_, warn, restart := a.checkIdleActivity(started.Add(activityWarningAfter))
	if !warn || restart {
		t.Fatalf("first check got warn=%v restart=%v, want warn=true restart=false", warn, restart)
	}

	_, warn, restart = a.checkIdleActivity(started.Add(activityWarningAfter + time.Minute))
	if warn || restart {
		t.Fatalf("second check got warn=%v restart=%v, want warn=false restart=false", warn, restart)
	}

	activityAt := started.Add(activityWarningAfter + 2*time.Minute)
	a.recordActivity(activityAt)
	_, warn, restart = a.checkIdleActivity(activityAt.Add(activityWarningAfter))
	if !warn || restart {
		t.Fatalf("post-activity check got warn=%v restart=%v, want warn=true restart=false", warn, restart)
	}
}

func TestIdleActivityRestartsAfterOneHour(t *testing.T) {
	started := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)
	a := &app{lastActivity: started}

	idleFor, warn, restart := a.checkIdleActivity(started.Add(activityRestartAfter))

	if idleFor != activityRestartAfter {
		t.Fatalf("got idle duration %s, want %s", idleFor, activityRestartAfter)
	}
	if warn {
		t.Fatalf("did not expect warning when restart is due")
	}
	if !restart {
		t.Fatalf("expected restart after %s idle", activityRestartAfter)
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

func TestParseBidirMeasurements(t *testing.T) {
	data := []byte(`{
		"end": {
			"sum_received": {
				"seconds": 5.0,
				"bits_per_second": 1000,
				"jitter_ms": 0.5,
				"lost_packets": 1,
				"packets": 100,
				"lost_percent": 1
			},
			"sum_received_bidir_reverse": {
				"seconds": 5.0,
				"bits_per_second": 2000,
				"jitter_ms": 1.5,
				"lost_packets": 3,
				"packets": 100,
				"lost_percent": 3
			}
		}
	}`)
	measurements, err := parseIperfBidirMeasurements("udp", data)
	if err != nil {
		t.Fatalf("parseIperfBidirMeasurements returned error: %v", err)
	}
	if measurements.ClientToServer.BandwidthBitsPerSecond != 1000 {
		t.Fatalf("unexpected client-to-server bandwidth: %v", measurements.ClientToServer.BandwidthBitsPerSecond)
	}
	if measurements.ServerToClient.BandwidthBitsPerSecond != 2000 {
		t.Fatalf("unexpected server-to-client bandwidth: %v", measurements.ServerToClient.BandwidthBitsPerSecond)
	}
	if math.Abs(measurements.ServerToClient.JitterSeconds-0.0015) > 0.000000001 {
		t.Fatalf("unexpected server-to-client jitter: %v", measurements.ServerToClient.JitterSeconds)
	}
	if math.Abs(measurements.ServerToClient.LossRatio-0.03) > 0.000000001 {
		t.Fatalf("unexpected server-to-client loss ratio: %v", measurements.ServerToClient.LossRatio)
	}
}

func TestRecordResultSamplesEmitsBothDirections(t *testing.T) {
	a := &app{cfg: &config{NodeName: "a"}, metrics: newMetricStore()}
	a.recordResultSamples("b", "a", "b", "tcp", probeResults{
		ClientToServer: probeResult{Success: true, BandwidthBitsPerSecond: 1000},
		ServerToClient: probeResult{Success: true, BandwidthBitsPerSecond: 2000},
	})

	rendered := a.metrics.Render()
	if !strings.Contains(rendered, `expeditus_iperf_bandwidth_bits_per_second{local_node="a",peer_node="b",client_node="a",server_node="b",protocol="tcp"} 1000`) {
		t.Fatalf("rendered metrics missing client-to-server bandwidth sample:\n%s", rendered)
	}
	if !strings.Contains(rendered, `expeditus_iperf_bandwidth_bits_per_second{local_node="a",peer_node="b",client_node="b",server_node="a",protocol="tcp"} 2000`) {
		t.Fatalf("rendered metrics missing server-to-client bandwidth sample:\n%s", rendered)
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
