package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	appVersion             = "v1.0.3"
	defaultBindAddress     = ":9119"
	defaultIperfPortRange  = "5201-5210"
	defaultProtocol        = "both"
	defaultBandwidth       = "10M"
	defaultInterval        = 60 * time.Second
	defaultDuration        = 15 * time.Second
	defaultWarmup          = 3 * time.Second
	defaultTrafficInterval = 30 * time.Second
	serverReadyDelay       = 150 * time.Millisecond
	activityWarningAfter   = 20 * time.Minute
	activityRestartAfter   = time.Hour
	activityCheckInterval  = time.Minute
)

var (
	busyRetryTimeout = 5 * time.Second
	busyRetryBackoff = 250 * time.Millisecond
)

type stringList []string

func (s *stringList) String() string {
	return strings.Join(*s, ",")
}

func (s *stringList) Set(value string) error {
	*s = append(*s, value)
	return nil
}

type config struct {
	NodeName         string
	BindAddress      string
	AdvertiseAddress string
	IperfBindAddress string
	IperfPorts       []int
	Neighbors        []neighbor
	Interval         time.Duration
	Duration         time.Duration
	Warmup           time.Duration
	TrafficInterval  time.Duration
	TestTimeout      time.Duration
	Protocol         string
	Bandwidth        string
	Token            string
	Once             bool
}

type neighbor struct {
	Raw     string
	BaseURL string
	Node    string
}

type app struct {
	cfg     *config
	client  *http.Client
	metrics *metricStore

	activeMu       sync.Mutex
	activeServers  map[int]struct{}
	testMu         sync.Mutex
	testActive     bool
	testLeaseID    string
	testLeaseTimer *time.Timer

	roleMu     sync.Mutex
	roleCounts map[string]int

	peerMu    sync.Mutex
	peerInfos map[string]infoResponse

	activityMu        sync.Mutex
	lastActivity      time.Time
	idleWarningLogged bool
}

type infoResponse struct {
	NodeName string `json:"node_name"`
	Version  string `json:"version"`
}

type negotiateRequest struct {
	ClientNode      string `json:"client_node"`
	LeaseID         string `json:"lease_id,omitempty"`
	Protocol        string `json:"protocol"`
	DurationSeconds int    `json:"duration_seconds"`
	Bandwidth       string `json:"bandwidth"`
}

type negotiateResponse struct {
	Accepted   bool      `json:"accepted"`
	ServerNode string    `json:"server_node"`
	ClientNode string    `json:"client_node"`
	ServerHost string    `json:"server_host"`
	ServerPort int       `json:"server_port"`
	ExpiresAt  time.Time `json:"expires_at"`
	Error      string    `json:"error,omitempty"`
}

type clientRunRequest struct {
	ServerNode      string `json:"server_node"`
	ServerHost      string `json:"server_host"`
	ServerPort      int    `json:"server_port"`
	LeaseID         string `json:"lease_id,omitempty"`
	Protocol        string `json:"protocol"`
	DurationSeconds int    `json:"duration_seconds"`
	Bandwidth       string `json:"bandwidth"`
	Reverse         bool   `json:"reverse"`
}

type clientRunResponse struct {
	Accepted bool         `json:"accepted"`
	Result   probeResult  `json:"result"`
	Results  probeResults `json:"results"`
	Error    string       `json:"error,omitempty"`
}

type pairLeaseAcquireRequest struct {
	InitiatorNode  string `json:"initiator_node"`
	TimeoutSeconds int    `json:"timeout_seconds"`
}

type pairLeaseAcquireResponse struct {
	Accepted  bool      `json:"accepted"`
	LeaseID   string    `json:"lease_id"`
	ExpiresAt time.Time `json:"expires_at"`
	Error     string    `json:"error,omitempty"`
}

type pairLeaseReleaseRequest struct {
	LeaseID string `json:"lease_id"`
}

type pairLeaseReleaseResponse struct {
	Released bool   `json:"released"`
	Error    string `json:"error,omitempty"`
}

type pairLease struct {
	ID        string
	ExpiresAt time.Time
	Release   func()
}

type probeResult struct {
	Success                bool    `json:"success"`
	BandwidthBitsPerSecond float64 `json:"bandwidth_bits_per_second"`
	JitterSeconds          float64 `json:"jitter_seconds"`
	LostPackets            float64 `json:"lost_packets"`
	LossRatio              float64 `json:"loss_ratio"`
	DurationSeconds        float64 `json:"duration_seconds"`
	Error                  string  `json:"error,omitempty"`
}

type probeResults struct {
	ClientToServer probeResult `json:"client_to_server"`
	ServerToClient probeResult `json:"server_to_client"`
}

func (r probeResult) hasData() bool {
	return r.Success || r.BandwidthBitsPerSecond != 0 || r.JitterSeconds != 0 || r.LostPackets != 0 || r.LossRatio != 0 || r.DurationSeconds != 0 || r.Error != ""
}

func (r probeResults) hasData() bool {
	return r.ClientToServer.hasData() || r.ServerToClient.hasData()
}

func (r probeResults) success() bool {
	return r.ClientToServer.Success && r.ServerToClient.Success
}

func (r clientRunResponse) probeResults() probeResults {
	if r.Results.hasData() {
		return r.Results
	}
	return probeResults{ClientToServer: r.Result, ServerToClient: r.Result}
}

func (r probeResults) errorMessage() string {
	if r.ClientToServer.Error != "" {
		return r.ClientToServer.Error
	}
	return r.ServerToClient.Error
}

type metricKey struct {
	LocalNode  string
	PeerNode   string
	ClientNode string
	ServerNode string
	Protocol   string
}

type metricSample struct {
	Key                    metricKey
	Success                bool
	BandwidthBitsPerSecond float64
	JitterSeconds          float64
	LostPackets            float64
	LossRatio              float64
	ProbeDurationSeconds   float64
	LastRun                time.Time
}

type trafficSample struct {
	LocalNode             string
	ReceiveBitsPerSecond  float64
	TransmitBitsPerSecond float64
	LastRun               time.Time
}

type metricStore struct {
	mu      sync.RWMutex
	samples map[metricKey]metricSample
	traffic *trafficSample
}

type httpStatusError struct {
	StatusCode int
	Message    string
}

type activityIdleError struct {
	IdleFor time.Duration
}

func (e *httpStatusError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("peer returned HTTP %d", e.StatusCode)
	}
	return fmt.Sprintf("peer returned HTTP %d: %s", e.StatusCode, e.Message)
}

func (e *activityIdleError) Error() string {
	return fmt.Sprintf("no probe activity for %s", e.IdleFor.Truncate(time.Second))
}

func main() {
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		log.Fatalf("configuration error: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	a := &app{
		cfg:           cfg,
		client:        &http.Client{},
		metrics:       newMetricStore(),
		activeServers: make(map[int]struct{}),
		roleCounts:    make(map[string]int),
		peerInfos:     make(map[string]infoResponse),
		lastActivity:  time.Now(),
	}

	server := &http.Server{
		Addr:              cfg.BindAddress,
		Handler:           a.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 2)
	go func() {
		log.Printf("listening on %s as node %q", cfg.BindAddress, cfg.NodeName)
		errCh <- server.ListenAndServe()
	}()

	if cfg.Once {
		a.runRound(ctx)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("http shutdown failed: %v", err)
		}
		return
	}

	go a.runScheduler(ctx)
	go a.runTrafficMonitor(ctx)
	go a.runActivityWatchdog(ctx, errCh)

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("http shutdown failed: %v", err)
		}
	case err := <-errCh:
		var idleErr *activityIdleError
		if errors.As(err, &idleErr) {
			log.Printf("error: %v; exiting for restart", idleErr)
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := server.Shutdown(shutdownCtx); err != nil {
				log.Printf("http shutdown failed: %v", err)
			}
			cancel()
			os.Exit(1)
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server failed: %v", err)
		}
	}
}

func parseConfig(args []string) (*config, error) {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "unknown"
	}

	var neighbors stringList
	var portRange string
	var tokenFile string

	cfg := &config{}
	fs := flag.NewFlagSet("expeditus-net", flag.ContinueOnError)
	fs.StringVar(&cfg.NodeName, "node", hostname, "node name used in metric labels")
	fs.StringVar(&cfg.BindAddress, "bind-address", defaultBindAddress, "HTTP bind address for /metrics, /health, and peer control")
	fs.StringVar(&cfg.AdvertiseAddress, "advertise-address", "", "address peers should use for negotiated iperf server connections")
	fs.StringVar(&cfg.IperfBindAddress, "iperf-bind-address", "", "local address for iperf3 servers to bind to")
	fs.Var(&neighbors, "neighbor", "neighbor control address or IP; repeat for multiple neighbors")
	fs.StringVar(&portRange, "iperf-port-range", defaultIperfPortRange, "iperf3 server port or inclusive range, for example 5201-5210")
	fs.DurationVar(&cfg.Interval, "interval", defaultInterval, "interval between probe rounds")
	fs.DurationVar(&cfg.Duration, "duration", defaultDuration, "iperf3 test duration")
	fs.DurationVar(&cfg.Warmup, "warmup", defaultWarmup, "iperf3 warmup duration omitted from results")
	fs.DurationVar(&cfg.TrafficInterval, "traffic-interval", defaultTrafficInterval, "interval between host traffic samples")
	fs.DurationVar(&cfg.TestTimeout, "test-timeout", 0, "overall timeout per iperf3 test; defaults to duration plus warmup plus 15s")
	fs.StringVar(&cfg.Protocol, "protocol", defaultProtocol, "iperf3 protocol: both, udp, or tcp")
	fs.StringVar(&cfg.Bandwidth, "bandwidth", defaultBandwidth, "iperf3 UDP target bandwidth, for example 10M")
	fs.StringVar(&cfg.Token, "token", "", "shared bearer token for peer control requests")
	fs.StringVar(&tokenFile, "token-file", "", "file containing the shared bearer token")
	fs.BoolVar(&cfg.Once, "once", false, "run one probe round and exit")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	cfg.Protocol = strings.ToLower(strings.TrimSpace(cfg.Protocol))
	if cfg.Protocol != "both" && cfg.Protocol != "udp" && cfg.Protocol != "tcp" {
		return nil, fmt.Errorf("protocol must be both, udp, or tcp")
	}
	if cfg.Protocol != "tcp" && strings.TrimSpace(cfg.Bandwidth) == "" {
		return nil, fmt.Errorf("bandwidth is required for udp probes")
	}
	if cfg.Interval <= 0 {
		return nil, fmt.Errorf("interval must be greater than zero")
	}
	if cfg.Duration <= 0 {
		return nil, fmt.Errorf("duration must be greater than zero")
	}
	if cfg.Warmup < 0 {
		return nil, fmt.Errorf("warmup must not be negative")
	}
	if cfg.TrafficInterval <= 0 {
		return nil, fmt.Errorf("traffic-interval must be greater than zero")
	}

	var err error
	cfg.IperfPorts, err = parsePortRange(portRange)
	if err != nil {
		return nil, err
	}

	defaultPort := defaultPortFromBindAddress(cfg.BindAddress)
	for _, raw := range neighbors {
		n, err := normalizeNeighbor(raw, defaultPort)
		if err != nil {
			return nil, err
		}
		cfg.Neighbors = append(cfg.Neighbors, n)
	}

	if tokenFile != "" {
		content, err := os.ReadFile(tokenFile)
		if err != nil {
			return nil, fmt.Errorf("read token file: %w", err)
		}
		cfg.Token = strings.TrimSpace(string(content))
	}

	if cfg.AdvertiseAddress == "" {
		cfg.AdvertiseAddress = defaultAdvertiseAddress(cfg.BindAddress, hostname)
	}
	if cfg.IperfBindAddress == "" {
		cfg.IperfBindAddress = bindHost(cfg.BindAddress)
	}

	return cfg, nil
}

func parsePortRange(value string) ([]int, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, fmt.Errorf("iperf port range is required")
	}

	parsePort := func(raw string) (int, error) {
		port, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil {
			return 0, fmt.Errorf("invalid port %q", raw)
		}
		if port < 1 || port > 65535 {
			return 0, fmt.Errorf("port out of range: %d", port)
		}
		return port, nil
	}

	parts := strings.Split(value, "-")
	if len(parts) == 1 {
		port, err := parsePort(parts[0])
		if err != nil {
			return nil, err
		}
		return []int{port}, nil
	}
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid port range %q", value)
	}

	start, err := parsePort(parts[0])
	if err != nil {
		return nil, err
	}
	end, err := parsePort(parts[1])
	if err != nil {
		return nil, err
	}
	if end < start {
		return nil, fmt.Errorf("invalid port range %q: end is before start", value)
	}
	if end-start > 1000 {
		return nil, fmt.Errorf("port range %q is too large; limit is 1001 ports", value)
	}

	ports := make([]int, 0, end-start+1)
	for port := start; port <= end; port++ {
		ports = append(ports, port)
	}
	return ports, nil
}

func normalizeNeighbor(raw string, defaultPort string) (neighbor, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return neighbor{}, fmt.Errorf("neighbor cannot be empty")
	}
	if defaultPort == "" {
		defaultPort = "9119"
	}

	if !strings.Contains(raw, "://") {
		if ip := net.ParseIP(raw); ip != nil {
			return neighborFromParts(raw, "http", raw, defaultPort)
		}
		if strings.Count(raw, ":") > 1 && !strings.HasPrefix(raw, "[") {
			return neighborFromParts(raw, "http", raw, defaultPort)
		}
		raw = "http://" + raw
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		return neighbor{}, fmt.Errorf("invalid neighbor %q: %w", raw, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return neighbor{}, fmt.Errorf("neighbor %q must use http or https", raw)
	}

	host := parsed.Hostname()
	if host == "" {
		return neighbor{}, fmt.Errorf("neighbor %q is missing a host", raw)
	}
	port := parsed.Port()
	if port == "" {
		port = defaultPort
	}
	return neighborFromParts(raw, parsed.Scheme, host, port)
}

func neighborFromParts(raw string, scheme string, host string, port string) (neighbor, error) {
	if _, err := strconv.Atoi(port); err != nil {
		return neighbor{}, fmt.Errorf("invalid neighbor port %q", port)
	}
	base := url.URL{
		Scheme: scheme,
		Host:   net.JoinHostPort(host, port),
	}
	return neighbor{
		Raw:     raw,
		BaseURL: strings.TrimRight(base.String(), "/"),
		Node:    host,
	}, nil
}

func defaultPortFromBindAddress(bindAddress string) string {
	_, port, err := net.SplitHostPort(bindAddress)
	if err == nil {
		return port
	}
	if strings.HasPrefix(bindAddress, ":") {
		return strings.TrimPrefix(bindAddress, ":")
	}
	return "9119"
}

func bindHost(bindAddress string) string {
	host, _, err := net.SplitHostPort(bindAddress)
	if err == nil {
		return strings.Trim(host, "[]")
	}
	return ""
}

func defaultAdvertiseAddress(bindAddress string, fallback string) string {
	host := bindHost(bindAddress)
	if host != "" && host != "0.0.0.0" && host != "::" {
		return host
	}
	return fallback
}

func secondsCeil(duration time.Duration) int {
	return int(math.Ceil(duration.Seconds()))
}

func (cfg *config) warmupSeconds() int {
	if cfg.Warmup <= 0 {
		return 0
	}
	return secondsCeil(cfg.Warmup)
}

func (cfg *config) probeProtocols() []string {
	if cfg.Protocol == "both" {
		return []string{"udp", "tcp"}
	}
	return []string{cfg.Protocol}
}

func normalizeNodeID(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func pairOwner(a string, b string) string {
	a = normalizeNodeID(a)
	b = normalizeNodeID(b)
	if a <= b {
		return a
	}
	return b
}

func initiatorOwnsPair(initiator string, peer string) bool {
	initiator = normalizeNodeID(initiator)
	peer = normalizeNodeID(peer)
	return initiator != "" && peer != "" && initiator < peer
}

func (a *app) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", a.handleHealth)
	mux.HandleFunc("/metrics", a.handleMetrics)
	mux.HandleFunc("/v1/info", a.handleInfo)
	mux.HandleFunc("/v1/lease/acquire", a.handlePairLeaseAcquire)
	mux.HandleFunc("/v1/lease/release", a.handlePairLeaseRelease)
	mux.HandleFunc("/v1/negotiate", a.handleNegotiate)
	mux.HandleFunc("/v1/iperf/client/run", a.handleClientRun)
	return mux
}

func (a *app) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, "ok\n")
}

func (a *app) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = io.WriteString(w, a.metrics.Render())
}

func (a *app) handleInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	if !a.authorize(w, r) {
		return
	}
	writeJSON(w, http.StatusOK, infoResponse{NodeName: a.cfg.NodeName, Version: appVersion})
}

func (a *app) handlePairLeaseAcquire(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	if !a.authorize(w, r) {
		return
	}

	var req pairLeaseAcquireRequest
	if err := decodeJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, pairLeaseAcquireResponse{Accepted: false, Error: err.Error()})
		return
	}
	if strings.TrimSpace(req.InitiatorNode) == "" {
		writeJSON(w, http.StatusBadRequest, pairLeaseAcquireResponse{Accepted: false, Error: "initiator_node is required"})
		return
	}
	if req.TimeoutSeconds < 1 {
		writeJSON(w, http.StatusBadRequest, pairLeaseAcquireResponse{Accepted: false, Error: "timeout_seconds must be greater than zero"})
		return
	}
	if !initiatorOwnsPair(req.InitiatorNode, a.cfg.NodeName) {
		writeJSON(w, http.StatusConflict, pairLeaseAcquireResponse{Accepted: false, Error: "peer pair is owned by " + pairOwner(req.InitiatorNode, a.cfg.NodeName)})
		return
	}

	lease, ok := a.tryAcquirePairLease(time.Duration(req.TimeoutSeconds) * time.Second)
	if !ok {
		writeJSON(w, http.StatusConflict, pairLeaseAcquireResponse{Accepted: false, Error: "node is already running a test"})
		return
	}

	writeJSON(w, http.StatusOK, pairLeaseAcquireResponse{Accepted: true, LeaseID: lease.ID, ExpiresAt: lease.ExpiresAt.UTC()})
}

func (a *app) handlePairLeaseRelease(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	if !a.authorize(w, r) {
		return
	}

	var req pairLeaseReleaseRequest
	if err := decodeJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, pairLeaseReleaseResponse{Released: false, Error: err.Error()})
		return
	}
	if strings.TrimSpace(req.LeaseID) == "" {
		writeJSON(w, http.StatusBadRequest, pairLeaseReleaseResponse{Released: false, Error: "lease_id is required"})
		return
	}

	writeJSON(w, http.StatusOK, pairLeaseReleaseResponse{Released: a.releasePairLease(req.LeaseID)})
}

func (a *app) handleNegotiate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	if !a.authorize(w, r) {
		return
	}

	var req negotiateRequest
	if err := decodeJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, negotiateResponse{Accepted: false, Error: err.Error()})
		return
	}
	if err := validateProbeRequest(req.Protocol, req.DurationSeconds, req.Bandwidth); err != nil {
		writeJSON(w, http.StatusBadRequest, negotiateResponse{Accepted: false, Error: err.Error()})
		return
	}
	if strings.TrimSpace(req.ClientNode) == "" {
		writeJSON(w, http.StatusBadRequest, negotiateResponse{Accepted: false, Error: "client_node is required"})
		return
	}
	if !initiatorOwnsPair(req.ClientNode, a.cfg.NodeName) {
		writeJSON(w, http.StatusConflict, negotiateResponse{Accepted: false, Error: "peer pair is owned by " + pairOwner(req.ClientNode, a.cfg.NodeName)})
		return
	}
	var releaseTest func()
	if req.LeaseID != "" {
		if !a.hasPairLease(req.LeaseID) {
			writeJSON(w, http.StatusConflict, negotiateResponse{Accepted: false, Error: "pair lease is not active"})
			return
		}
	} else {
		var ok bool
		releaseTest, ok = a.tryAcquireTestSlot()
		if !ok {
			writeJSON(w, http.StatusConflict, negotiateResponse{Accepted: false, Error: "node is already running a test"})
			return
		}
	}

	port, expiresAt, done, _, err := a.startIperfServer(a.timeoutForSeconds(req.DurationSeconds))
	if err != nil {
		if releaseTest != nil {
			releaseTest()
		}
		writeJSON(w, http.StatusServiceUnavailable, negotiateResponse{Accepted: false, Error: err.Error()})
		return
	}
	if releaseTest != nil {
		releaseTestSlotWhenDone(done, releaseTest)
	}

	writeJSON(w, http.StatusOK, negotiateResponse{
		Accepted:   true,
		ServerNode: a.cfg.NodeName,
		ClientNode: req.ClientNode,
		ServerHost: a.cfg.AdvertiseAddress,
		ServerPort: port,
		ExpiresAt:  expiresAt.UTC(),
	})
}

func (a *app) handleClientRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	if !a.authorize(w, r) {
		return
	}

	var req clientRunRequest
	if err := decodeJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, clientRunResponse{Accepted: false, Error: err.Error()})
		return
	}
	if err := validateProbeRequest(req.Protocol, req.DurationSeconds, req.Bandwidth); err != nil {
		writeJSON(w, http.StatusBadRequest, clientRunResponse{Accepted: false, Error: err.Error()})
		return
	}
	if strings.TrimSpace(req.ServerNode) == "" {
		writeJSON(w, http.StatusBadRequest, clientRunResponse{Accepted: false, Error: "server_node is required"})
		return
	}
	if !initiatorOwnsPair(req.ServerNode, a.cfg.NodeName) {
		writeJSON(w, http.StatusConflict, clientRunResponse{Accepted: false, Error: "peer pair is owned by " + pairOwner(req.ServerNode, a.cfg.NodeName)})
		return
	}
	if req.ServerHost == "" || req.ServerPort < 1 || req.ServerPort > 65535 {
		writeJSON(w, http.StatusBadRequest, clientRunResponse{Accepted: false, Error: "server_host and valid server_port are required"})
		return
	}
	var releaseTest func()
	if req.LeaseID != "" {
		if !a.hasPairLease(req.LeaseID) {
			writeJSON(w, http.StatusConflict, clientRunResponse{Accepted: false, Error: "pair lease is not active"})
			return
		}
	} else {
		var ok bool
		releaseTest, ok = a.tryAcquireTestSlot()
		if !ok {
			writeJSON(w, http.StatusConflict, clientRunResponse{Accepted: false, Error: "node is already running a test"})
			return
		}
		defer releaseTest()
	}

	serverNode := req.ServerNode

	if req.Protocol == "tcp" {
		result := a.runIperfClientDirection(r.Context(), req.ServerHost, req.ServerPort, req.Protocol, req.Bandwidth, req.DurationSeconds, req.Reverse)
		results := probeResults{}
		if req.Reverse {
			results.ServerToClient = result
			a.recordSingleResult(serverNode, serverNode, a.cfg.NodeName, req.Protocol, result)
		} else {
			results.ClientToServer = result
			a.recordSingleResult(serverNode, a.cfg.NodeName, serverNode, req.Protocol, result)
		}
		writeJSON(w, http.StatusOK, clientRunResponse{Accepted: true, Result: result, Results: results})
		return
	}

	results := a.runIperfBidirClient(r.Context(), req.ServerHost, req.ServerPort, req.Protocol, req.Bandwidth, req.DurationSeconds)
	a.recordResultSamples(serverNode, a.cfg.NodeName, serverNode, req.Protocol, results)

	writeJSON(w, http.StatusOK, clientRunResponse{Accepted: true, Result: results.ClientToServer, Results: results})
}

func (a *app) authorize(w http.ResponseWriter, r *http.Request) bool {
	if a.cfg.Token == "" {
		return true
	}

	const prefix = "Bearer "
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, prefix) {
		http.Error(w, "missing bearer token", http.StatusUnauthorized)
		return false
	}
	token := strings.TrimSpace(strings.TrimPrefix(header, prefix))
	if subtle.ConstantTimeCompare([]byte(token), []byte(a.cfg.Token)) != 1 {
		http.Error(w, "invalid bearer token", http.StatusForbidden)
		return false
	}
	return true
}

func decodeJSON(r *http.Request, target any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func methodNotAllowed(w http.ResponseWriter) {
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

func validateProbeRequest(protocol string, durationSeconds int, bandwidth string) error {
	if protocol != "udp" && protocol != "tcp" {
		return fmt.Errorf("protocol must be udp or tcp")
	}
	if durationSeconds < 1 {
		return fmt.Errorf("duration_seconds must be greater than zero")
	}
	if protocol == "udp" && strings.TrimSpace(bandwidth) == "" {
		return fmt.Errorf("bandwidth is required for udp probes")
	}
	return nil
}

func (a *app) timeoutForSeconds(durationSeconds int) time.Duration {
	if a.cfg.TestTimeout > 0 {
		return a.cfg.TestTimeout
	}
	return time.Duration(durationSeconds+a.cfg.warmupSeconds())*time.Second + 15*time.Second
}

func (a *app) runScheduler(ctx context.Context) {
	if len(a.cfg.Neighbors) == 0 {
		log.Printf("no neighbors configured; serving /metrics and peer control only")
		return
	}

	a.runRound(ctx)
	ticker := time.NewTicker(a.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.runRound(ctx)
		}
	}
}

type trafficCounters struct {
	ReceiveBytes  uint64
	TransmitBytes uint64
}

func (a *app) runTrafficMonitor(ctx context.Context) {
	previous, err := readHostTrafficCounters("/proc/net/dev")
	if err != nil {
		log.Printf("host traffic monitor disabled: %v", err)
		return
	}
	previousAt := time.Now()
	a.metrics.RecordTraffic(trafficSample{
		LocalNode: a.cfg.NodeName,
		LastRun:   previousAt,
	})

	ticker := time.NewTicker(a.cfg.TrafficInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		current, err := readHostTrafficCounters("/proc/net/dev")
		if err != nil {
			log.Printf("host traffic sample failed: %v", err)
			continue
		}
		now := time.Now()
		elapsed := now.Sub(previousAt).Seconds()
		if elapsed > 0 {
			a.metrics.RecordTraffic(trafficSample{
				LocalNode:             a.cfg.NodeName,
				ReceiveBitsPerSecond:  trafficBitsPerSecond(current.ReceiveBytes, previous.ReceiveBytes, elapsed),
				TransmitBitsPerSecond: trafficBitsPerSecond(current.TransmitBytes, previous.TransmitBytes, elapsed),
				LastRun:               now,
			})
		}
		previous = current
		previousAt = now
	}
}

func readHostTrafficCounters(path string) (trafficCounters, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return trafficCounters{}, err
	}
	return parseProcNetDev(data)
}

func parseProcNetDev(data []byte) (trafficCounters, error) {
	var counters trafficCounters
	for _, line := range strings.Split(string(data), "\n") {
		name, stats, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		iface := strings.TrimSpace(name)
		if iface == "" || iface == "lo" {
			continue
		}
		fields := strings.Fields(stats)
		if len(fields) < 16 {
			return trafficCounters{}, fmt.Errorf("parse /proc/net/dev: interface %q has %d fields", iface, len(fields))
		}
		rxBytes, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			return trafficCounters{}, fmt.Errorf("parse /proc/net/dev rx bytes for %q: %w", iface, err)
		}
		txBytes, err := strconv.ParseUint(fields[8], 10, 64)
		if err != nil {
			return trafficCounters{}, fmt.Errorf("parse /proc/net/dev tx bytes for %q: %w", iface, err)
		}
		counters.ReceiveBytes += rxBytes
		counters.TransmitBytes += txBytes
	}
	return counters, nil
}

func trafficBitsPerSecond(current uint64, previous uint64, elapsedSeconds float64) float64 {
	if elapsedSeconds <= 0 || current < previous {
		return 0
	}
	return float64(current-previous) * 8 / elapsedSeconds
}

func (a *app) runActivityWatchdog(ctx context.Context, errCh chan<- error) {
	ticker := time.NewTicker(activityCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			idleFor, warn, restart := a.checkIdleActivity(now)
			if restart {
				errCh <- &activityIdleError{IdleFor: idleFor}
				return
			}
			if warn {
				log.Printf("warning: no probe activity for %s", idleFor.Truncate(time.Second))
			}
		}
	}
}

func (a *app) checkIdleActivity(now time.Time) (time.Duration, bool, bool) {
	a.activityMu.Lock()
	defer a.activityMu.Unlock()

	if a.lastActivity.IsZero() {
		a.lastActivity = now
	}
	idleFor := now.Sub(a.lastActivity)
	if idleFor < 0 {
		idleFor = 0
	}
	if idleFor >= activityRestartAfter {
		return idleFor, false, true
	}
	if idleFor >= activityWarningAfter && !a.idleWarningLogged {
		a.idleWarningLogged = true
		return idleFor, true, false
	}
	return idleFor, false, false
}

func (a *app) recordActivity(now time.Time) {
	a.activityMu.Lock()
	defer a.activityMu.Unlock()
	a.lastActivity = now
	a.idleWarningLogged = false
}

func (a *app) runRound(ctx context.Context) {
	if len(a.cfg.Neighbors) == 0 {
		return
	}
	durationSeconds := secondsCeil(a.cfg.Duration)
	protocols := a.cfg.probeProtocols()
	for _, n := range a.cfg.Neighbors {
		select {
		case <-ctx.Done():
			return
		default:
		}
		peerName, err := a.peerNodeName(ctx, n)
		if err != nil {
			log.Printf("peer info failed peer=%s: %v", n.Node, err)
			continue
		}
		if normalizeNodeID(peerName) == normalizeNodeID(a.cfg.NodeName) {
			log.Printf("probe skipped peer=%s: peer node name matches local node", n.Node)
			continue
		}
		if !initiatorOwnsPair(a.cfg.NodeName, peerName) {
			continue
		}

		localClient := a.nextLocalClient(peerName)
		a.runPeerProbe(ctx, n, peerName, protocols, localClient, durationSeconds)
	}
}

func (a *app) nextLocalClient(peer string) bool {
	a.roleMu.Lock()
	defer a.roleMu.Unlock()
	count := a.roleCounts[peer]
	a.roleCounts[peer] = count + 1
	return count%2 == 0
}

func (a *app) tryAcquireTestSlot() (func(), bool) {
	a.testMu.Lock()
	defer a.testMu.Unlock()
	if a.testActive {
		return nil, false
	}
	a.testActive = true
	var once sync.Once
	release := func() {
		once.Do(func() {
			a.testMu.Lock()
			defer a.testMu.Unlock()
			a.testActive = false
		})
	}
	return release, true
}

func (a *app) tryAcquirePairLease(timeout time.Duration) (pairLease, bool) {
	if timeout <= 0 {
		timeout = 20 * time.Second
	}

	leaseID := newLeaseID()
	expiresAt := time.Now().Add(timeout)

	a.testMu.Lock()
	if a.testActive {
		a.testMu.Unlock()
		return pairLease{}, false
	}
	a.testActive = true
	a.testLeaseID = leaseID
	a.testLeaseTimer = time.AfterFunc(timeout, func() {
		a.releasePairLease(leaseID)
	})
	a.testMu.Unlock()

	var once sync.Once
	release := func() {
		once.Do(func() {
			a.releasePairLease(leaseID)
		})
	}
	return pairLease{ID: leaseID, ExpiresAt: expiresAt, Release: release}, true
}

func (a *app) releasePairLease(leaseID string) bool {
	a.testMu.Lock()
	defer a.testMu.Unlock()
	if leaseID == "" || a.testLeaseID != leaseID {
		return false
	}
	if a.testLeaseTimer != nil {
		a.testLeaseTimer.Stop()
		a.testLeaseTimer = nil
	}
	a.testLeaseID = ""
	a.testActive = false
	return true
}

func (a *app) hasPairLease(leaseID string) bool {
	a.testMu.Lock()
	defer a.testMu.Unlock()
	return leaseID != "" && a.testActive && a.testLeaseID == leaseID
}

func newLeaseID() string {
	var data [16]byte
	if _, err := rand.Read(data[:]); err == nil {
		return hex.EncodeToString(data[:])
	}
	return strconv.FormatInt(time.Now().UnixNano(), 36)
}

func releaseTestSlotWhenDone(done <-chan error, release func()) {
	go func() {
		<-done
		release()
	}()
}

func isBusyConflict(err error) bool {
	var statusErr *httpStatusError
	return errors.As(err, &statusErr) && statusErr.StatusCode == http.StatusConflict && strings.Contains(strings.ToLower(statusErr.Message), "already running a test")
}

func waitForBusyRetry(ctx context.Context, started time.Time) bool {
	if busyRetryTimeout <= 0 {
		return false
	}
	remaining := busyRetryTimeout - time.Since(started)
	if remaining <= 0 {
		return false
	}
	backoff := busyRetryBackoff
	if backoff <= 0 {
		backoff = time.Millisecond
	}
	if backoff > remaining {
		backoff = remaining
	}

	timer := time.NewTimer(backoff)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (a *app) acquirePairLeaseWithBusyRetry(ctx context.Context, timeout time.Duration) (pairLease, bool) {
	started := time.Now()
	for {
		if lease, ok := a.tryAcquirePairLease(timeout); ok {
			return lease, true
		}
		if !waitForBusyRetry(ctx, started) {
			return pairLease{}, false
		}
	}
}

func (a *app) postJSONWithBusyRetry(ctx context.Context, n neighbor, path string, request any, response any) error {
	started := time.Now()
	for {
		err := a.postJSON(ctx, n, path, request, response)
		if !isBusyConflict(err) {
			return err
		}
		if !waitForBusyRetry(ctx, started) {
			return err
		}
	}
}

func (a *app) runPeerProbe(ctx context.Context, n neighbor, peerName string, protocols []string, localClient bool, durationSeconds int) {
	started := time.Now()
	leaseTimeout := a.pairLeaseTimeout(protocols, durationSeconds)
	localLease, ok := a.acquirePairLeaseWithBusyRetry(ctx, leaseTimeout)
	if !ok {
		log.Printf("probe lease failed peer=%s: node is already running a test", peerName)
		a.recordProbeSequenceFailure(peerName, localClient, protocols, time.Since(started))
		return
	}
	defer localLease.Release()

	peerLeaseID, err := a.acquirePeerPairLease(ctx, n, leaseTimeout)
	if err != nil {
		log.Printf("probe lease failed peer=%s: %v", peerName, err)
		a.recordProbeSequenceFailure(peerName, localClient, protocols, time.Since(started))
		return
	}
	defer a.releasePeerPairLease(n, peerLeaseID)

	for _, protocol := range protocols {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if localClient {
			a.runLocalClientProbe(ctx, n, peerName, peerLeaseID, protocol, durationSeconds)
		} else {
			a.runRemoteClientProbe(ctx, n, peerName, peerLeaseID, protocol, durationSeconds)
		}
	}
}

func (a *app) pairLeaseTimeout(protocols []string, durationSeconds int) time.Duration {
	perProbe := a.timeoutForSeconds(durationSeconds) + 5*time.Second
	steps := 0
	for _, protocol := range protocols {
		if protocol == "tcp" {
			steps += 2
			continue
		}
		steps++
	}
	return time.Duration(steps)*perProbe + 5*time.Second
}

func (a *app) acquirePeerPairLease(ctx context.Context, n neighbor, timeout time.Duration) (string, error) {
	requestCtx, cancel := context.WithTimeout(ctx, busyRetryTimeout+5*time.Second)
	defer cancel()

	var resp pairLeaseAcquireResponse
	err := a.postJSONWithBusyRetry(requestCtx, n, "/v1/lease/acquire", pairLeaseAcquireRequest{
		InitiatorNode:  a.cfg.NodeName,
		TimeoutSeconds: secondsCeil(timeout),
	}, &resp)
	if err != nil {
		return "", err
	}
	if !resp.Accepted {
		return "", errors.New(resp.Error)
	}
	if resp.LeaseID == "" {
		return "", errors.New("peer returned empty lease_id")
	}
	return resp.LeaseID, nil
}

func (a *app) releasePeerPairLease(n neighbor, leaseID string) {
	if leaseID == "" {
		return
	}
	releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var resp pairLeaseReleaseResponse
	if err := a.postJSON(releaseCtx, n, "/v1/lease/release", pairLeaseReleaseRequest{LeaseID: leaseID}, &resp); err != nil {
		log.Printf("peer lease release failed peer=%s: %v", n.Node, err)
	}
}

func (a *app) recordProbeSequenceFailure(peerName string, localClient bool, protocols []string, duration time.Duration) {
	for _, protocol := range protocols {
		if localClient {
			a.recordFailure(peerName, a.cfg.NodeName, peerName, protocol, duration)
		} else {
			a.recordFailure(peerName, peerName, a.cfg.NodeName, protocol, duration)
		}
	}
}

func (a *app) runLocalClientProbe(ctx context.Context, n neighbor, peerName string, peerLeaseID string, protocol string, durationSeconds int) {
	if protocol == "tcp" {
		a.runLocalClientTCPProbe(ctx, n, peerName, peerLeaseID, durationSeconds)
		return
	}

	started := time.Now()

	resp, err := a.negotiatePeerServer(ctx, n, peerLeaseID, protocol, durationSeconds)
	if err != nil {
		if isBusyConflict(err) {
			log.Printf("probe failed peer=%s protocol=%s: %v", peerName, protocol, err)
			a.recordFailure(peerName, a.cfg.NodeName, peerName, protocol, time.Since(started))
			return
		}
		log.Printf("probe negotiation failed peer=%s: %v", peerName, err)
		a.recordFailure(peerName, a.cfg.NodeName, peerName, protocol, time.Since(started))
		return
	}

	serverNode := resp.ServerNode
	if serverNode == "" {
		serverNode = peerName
	}
	serverHost := usableServerHost(resp.ServerHost, n.Node)
	results := a.runIperfBidirClient(ctx, serverHost, resp.ServerPort, protocol, a.cfg.Bandwidth, durationSeconds)
	a.recordResultSamples(peerName, a.cfg.NodeName, serverNode, protocol, results)

	a.logProbeResults(peerName, protocol, "client", results)
}

func (a *app) negotiatePeerServer(ctx context.Context, n neighbor, peerLeaseID string, protocol string, durationSeconds int) (negotiateResponse, error) {
	var resp negotiateResponse
	requestCtx, cancel := context.WithTimeout(ctx, a.timeoutForSeconds(durationSeconds)+5*time.Second)
	defer cancel()
	err := a.postJSON(requestCtx, n, "/v1/negotiate", negotiateRequest{
		ClientNode:      a.cfg.NodeName,
		LeaseID:         peerLeaseID,
		Protocol:        protocol,
		DurationSeconds: durationSeconds,
		Bandwidth:       a.cfg.Bandwidth,
	}, &resp)
	if err != nil {
		return negotiateResponse{}, err
	}
	if !resp.Accepted {
		return negotiateResponse{}, errors.New(resp.Error)
	}
	return resp, nil
}

func (a *app) runLocalClientTCPProbe(ctx context.Context, n neighbor, peerName string, peerLeaseID string, durationSeconds int) {
	results := probeResults{
		ClientToServer: a.runLocalClientTCPDirection(ctx, n, peerName, peerLeaseID, durationSeconds, false),
		ServerToClient: a.runLocalClientTCPDirection(ctx, n, peerName, peerLeaseID, durationSeconds, true),
	}
	a.recordResultSamples(peerName, a.cfg.NodeName, peerName, "tcp", results)
	a.logProbeResults(peerName, "tcp", "client", results)
}

func (a *app) runLocalClientTCPDirection(ctx context.Context, n neighbor, peerName string, peerLeaseID string, durationSeconds int, reverse bool) probeResult {
	started := time.Now()
	resp, err := a.negotiatePeerServer(ctx, n, peerLeaseID, "tcp", durationSeconds)
	if err != nil {
		log.Printf("probe negotiation failed peer=%s protocol=tcp: %v", peerName, err)
		return failedProbeResult(time.Since(started).Seconds(), err.Error())
	}
	serverHost := usableServerHost(resp.ServerHost, n.Node)
	return a.runIperfClientDirection(ctx, serverHost, resp.ServerPort, "tcp", a.cfg.Bandwidth, durationSeconds, reverse)
}

func (a *app) runRemoteClientProbe(ctx context.Context, n neighbor, peerName string, peerLeaseID string, protocol string, durationSeconds int) {
	if protocol == "tcp" {
		a.runRemoteClientTCPProbe(ctx, n, peerName, peerLeaseID, durationSeconds)
		return
	}

	started := time.Now()

	port, _, done, stopServer, err := a.startIperfServer(a.timeoutForSeconds(durationSeconds))
	if err != nil {
		log.Printf("local iperf server failed peer=%s: %v", peerName, err)
		a.recordFailure(peerName, peerName, a.cfg.NodeName, protocol, time.Since(started))
		return
	}

	var resp clientRunResponse
	requestCtx, cancel := context.WithTimeout(ctx, a.timeoutForSeconds(durationSeconds)+5*time.Second)
	defer cancel()
	err = a.postJSON(requestCtx, n, "/v1/iperf/client/run", clientRunRequest{
		ServerNode:      a.cfg.NodeName,
		ServerHost:      a.cfg.AdvertiseAddress,
		ServerPort:      port,
		LeaseID:         peerLeaseID,
		Protocol:        protocol,
		DurationSeconds: durationSeconds,
		Bandwidth:       a.cfg.Bandwidth,
	}, &resp)
	if err != nil || !resp.Accepted {
		stopServer()
		waitForIperfServerDone(done)
		if err == nil {
			err = errors.New(resp.Error)
		}
		if isBusyConflict(err) {
			log.Printf("probe failed peer=%s protocol=%s: %v", peerName, protocol, err)
			a.recordFailure(peerName, peerName, a.cfg.NodeName, protocol, time.Since(started))
			return
		}
		log.Printf("remote client probe failed peer=%s: %v", peerName, err)
		a.recordFailure(peerName, peerName, a.cfg.NodeName, protocol, time.Since(started))
		return
	}
	results := resp.probeResults()
	waitForIperfServerDone(done)
	a.logProbeResults(peerName, protocol, "server", results)
}

func (a *app) runRemoteClientTCPProbe(ctx context.Context, n neighbor, peerName string, peerLeaseID string, durationSeconds int) {
	results := probeResults{
		ClientToServer: a.runRemoteClientTCPDirection(ctx, n, peerName, peerLeaseID, durationSeconds, false),
		ServerToClient: a.runRemoteClientTCPDirection(ctx, n, peerName, peerLeaseID, durationSeconds, true),
	}
	a.logProbeResults(peerName, "tcp", "server", results)
}

func (a *app) runRemoteClientTCPDirection(ctx context.Context, n neighbor, peerName string, peerLeaseID string, durationSeconds int, reverse bool) probeResult {
	started := time.Now()
	port, _, done, stopServer, err := a.startIperfServer(a.timeoutForSeconds(durationSeconds))
	if err != nil {
		log.Printf("local iperf server failed peer=%s: %v", peerName, err)
		result := failedProbeResult(time.Since(started).Seconds(), err.Error())
		a.recordRemoteClientTCPFailure(peerName, reverse, result)
		return result
	}

	var resp clientRunResponse
	requestCtx, cancel := context.WithTimeout(ctx, a.timeoutForSeconds(durationSeconds)+5*time.Second)
	defer cancel()
	err = a.postJSON(requestCtx, n, "/v1/iperf/client/run", clientRunRequest{
		ServerNode:      a.cfg.NodeName,
		ServerHost:      a.cfg.AdvertiseAddress,
		ServerPort:      port,
		LeaseID:         peerLeaseID,
		Protocol:        "tcp",
		DurationSeconds: durationSeconds,
		Bandwidth:       a.cfg.Bandwidth,
		Reverse:         reverse,
	}, &resp)
	if err != nil || !resp.Accepted {
		stopServer()
		waitForIperfServerDone(done)
		if err == nil {
			err = errors.New(resp.Error)
		}
		if isBusyConflict(err) {
			log.Printf("probe failed peer=%s protocol=tcp: %v", peerName, err)
		} else {
			log.Printf("remote client probe failed peer=%s: %v", peerName, err)
		}
		result := failedProbeResult(time.Since(started).Seconds(), err.Error())
		a.recordRemoteClientTCPFailure(peerName, reverse, result)
		return result
	}
	result := resp.directionalResult(reverse)
	if !result.Success {
		stopServer()
	}
	waitForIperfServerDone(done)
	return result
}

func (a *app) recordRemoteClientTCPFailure(peerName string, reverse bool, result probeResult) {
	if reverse {
		a.recordSingleResult(peerName, a.cfg.NodeName, peerName, "tcp", result)
		return
	}
	a.recordSingleResult(peerName, peerName, a.cfg.NodeName, "tcp", result)
}

func (r clientRunResponse) directionalResult(reverse bool) probeResult {
	results := r.probeResults()
	if reverse {
		if results.ServerToClient.hasData() {
			return results.ServerToClient
		}
		return r.Result
	}
	if results.ClientToServer.hasData() {
		return results.ClientToServer
	}
	return r.Result
}

func (a *app) logProbeResults(peerName string, protocol string, role string, results probeResults) {
	if results.success() {
		log.Printf("probe succeeded peer=%s protocol=%s role=%s client_to_server_bandwidth_bps=%.0f server_to_client_bandwidth_bps=%.0f client_to_server_jitter_s=%.6f server_to_client_jitter_s=%.6f", peerName, protocol, role, results.ClientToServer.BandwidthBitsPerSecond, results.ServerToClient.BandwidthBitsPerSecond, results.ClientToServer.JitterSeconds, results.ServerToClient.JitterSeconds)
	} else {
		log.Printf("probe failed peer=%s protocol=%s role=%s: %s", peerName, protocol, role, results.errorMessage())
	}
}

func waitForIperfServerDone(done <-chan error) {
	if done == nil {
		return
	}
	<-done
}

func (a *app) recordFailure(peerNode string, clientNode string, serverNode string, protocol string, duration time.Duration) {
	a.recordResultSamples(peerNode, clientNode, serverNode, protocol, failedProbeResults(duration.Seconds(), ""))
}

func (a *app) recordResultSamples(peerNode string, clientNode string, serverNode string, protocol string, results probeResults) {
	a.recordActivity(time.Now())
	a.metrics.Record(sampleFromResult(a.cfg.NodeName, peerNode, clientNode, serverNode, protocol, results.ClientToServer))
	a.metrics.Record(sampleFromResult(a.cfg.NodeName, peerNode, serverNode, clientNode, protocol, results.ServerToClient))
}

func (a *app) recordSingleResult(peerNode string, clientNode string, serverNode string, protocol string, result probeResult) {
	a.recordActivity(time.Now())
	a.metrics.Record(sampleFromResult(a.cfg.NodeName, peerNode, clientNode, serverNode, protocol, result))
}

func sampleFromResult(localNode string, peerNode string, clientNode string, serverNode string, protocol string, result probeResult) metricSample {
	return metricSample{
		Key: metricKey{
			LocalNode:  localNode,
			PeerNode:   peerNode,
			ClientNode: clientNode,
			ServerNode: serverNode,
			Protocol:   protocol,
		},
		Success:                result.Success,
		BandwidthBitsPerSecond: result.BandwidthBitsPerSecond,
		JitterSeconds:          result.JitterSeconds,
		LostPackets:            result.LostPackets,
		LossRatio:              result.LossRatio,
		ProbeDurationSeconds:   result.DurationSeconds,
		LastRun:                time.Now(),
	}
}

func (a *app) postJSON(ctx context.Context, n neighbor, path string, request any, response any) error {
	body, err := json.Marshal(request)
	if err != nil {
		return err
	}

	requestURL := strings.TrimRight(n.BaseURL, "/") + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if a.cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+a.cfg.Token)
	}

	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if len(responseBody) > 0 && response != nil {
		if err := json.Unmarshal(responseBody, response); err != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return err
		}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message := strings.TrimSpace(string(responseBody))
		var peerError struct {
			Error string `json:"error"`
		}
		if err := json.Unmarshal(responseBody, &peerError); err == nil && peerError.Error != "" {
			message = peerError.Error
		}
		if len(message) > 200 {
			message = message[:200]
		}
		return &httpStatusError{StatusCode: resp.StatusCode, Message: message}
	}
	return nil
}

func (a *app) getJSON(ctx context.Context, n neighbor, path string, response any) error {
	requestURL := strings.TrimRight(n.BaseURL, "/") + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return err
	}
	if a.cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+a.cfg.Token)
	}

	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if len(responseBody) > 0 && response != nil {
		if err := json.Unmarshal(responseBody, response); err != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return err
		}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message := strings.TrimSpace(string(responseBody))
		var peerError struct {
			Error string `json:"error"`
		}
		if err := json.Unmarshal(responseBody, &peerError); err == nil && peerError.Error != "" {
			message = peerError.Error
		}
		if len(message) > 200 {
			message = message[:200]
		}
		return &httpStatusError{StatusCode: resp.StatusCode, Message: message}
	}
	return nil
}

func (a *app) peerNodeName(ctx context.Context, n neighbor) (string, error) {
	a.peerMu.Lock()
	if info := a.peerInfos[n.BaseURL]; info.NodeName != "" {
		a.peerMu.Unlock()
		return info.NodeName, nil
	}
	a.peerMu.Unlock()

	requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var info infoResponse
	if err := a.getJSON(requestCtx, n, "/v1/info", &info); err != nil {
		return "", err
	}
	name := strings.TrimSpace(info.NodeName)
	if name == "" {
		return "", fmt.Errorf("peer returned empty node name")
	}
	info.NodeName = name
	info.Version = strings.TrimSpace(info.Version)
	if info.Version != appVersion {
		peerVersion := info.Version
		if peerVersion == "" {
			peerVersion = "unknown"
		}
		log.Printf("warning: peer version mismatch peer=%s node=%s local_version=%s peer_version=%s", n.Node, name, appVersion, peerVersion)
	}

	a.peerMu.Lock()
	a.peerInfos[n.BaseURL] = info
	a.peerMu.Unlock()
	return name, nil
}

func (a *app) startIperfServer(timeout time.Duration) (int, time.Time, <-chan error, context.CancelFunc, error) {
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	expiresAt := time.Now().Add(timeout)
	var lastErr error

	for _, port := range a.cfg.IperfPorts {
		if !a.reservePort(port) {
			continue
		}

		serverCtx, cancel := context.WithTimeout(context.Background(), timeout)
		args := []string{"-s", "-p", strconv.Itoa(port), "--one-off"}
		if a.cfg.IperfBindAddress != "" {
			args = append(args, "--bind", a.cfg.IperfBindAddress)
		}
		cmd := exec.CommandContext(serverCtx, "iperf3", args...)
		cmd.Stdout = io.Discard
		cmd.Stderr = io.Discard

		if err := cmd.Start(); err != nil {
			cancel()
			a.releasePort(port)
			lastErr = err
			continue
		}

		done := make(chan error, 1)
		go func(port int) {
			err := cmd.Wait()
			cancel()
			a.releasePort(port)
			done <- err
			close(done)
		}(port)

		select {
		case err := <-done:
			if err == nil {
				lastErr = fmt.Errorf("iperf3 server exited before negotiation completed")
			} else {
				lastErr = fmt.Errorf("iperf3 server exited before negotiation completed: %w", err)
			}
			continue
		case <-time.After(serverReadyDelay):
			return port, expiresAt, done, cancel, nil
		}
	}

	if lastErr != nil {
		return 0, time.Time{}, nil, nil, lastErr
	}
	return 0, time.Time{}, nil, nil, fmt.Errorf("no iperf server ports available")
}

func (a *app) reservePort(port int) bool {
	a.activeMu.Lock()
	defer a.activeMu.Unlock()
	if _, ok := a.activeServers[port]; ok {
		return false
	}
	a.activeServers[port] = struct{}{}
	return true
}

func (a *app) releasePort(port int) {
	a.activeMu.Lock()
	defer a.activeMu.Unlock()
	delete(a.activeServers, port)
}

func iperfClientArgs(serverHost string, serverPort int, protocol string, bandwidth string, durationSeconds int, warmupSeconds int, reverse bool, bidirectional bool) []string {
	args := []string{"-c", serverHost, "-p", strconv.Itoa(serverPort), "-t", strconv.Itoa(durationSeconds), "--json"}
	if warmupSeconds > 0 {
		args = append(args, "--omit", strconv.Itoa(warmupSeconds))
	}
	if bidirectional {
		args = append(args, "--bidir")
	}
	if reverse {
		args = append(args, "--reverse")
	}
	if protocol == "udp" {
		args = append(args, "--udp", "--bandwidth", bandwidth)
	}
	return args
}

func (a *app) runIperfBidirClient(ctx context.Context, serverHost string, serverPort int, protocol string, bandwidth string, durationSeconds int) probeResults {
	started := time.Now()
	testCtx, cancel := context.WithTimeout(ctx, a.timeoutForSeconds(durationSeconds))
	defer cancel()

	args := iperfClientArgs(serverHost, serverPort, protocol, bandwidth, durationSeconds, a.cfg.warmupSeconds(), false, true)
	cmd := exec.CommandContext(testCtx, "iperf3", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.Output()
	duration := time.Since(started)
	if testCtx.Err() != nil {
		return failedProbeResults(duration.Seconds(), testCtx.Err().Error())
	}
	if err != nil {
		message := strings.TrimSpace(stderr.String())
		if parsed := parseIperfError(stdout); parsed != "" {
			message = parsed
		}
		if message == "" {
			message = err.Error()
		}
		return failedProbeResults(duration.Seconds(), message)
	}

	measurements, err := parseIperfBidirMeasurements(protocol, stdout)
	if err != nil {
		return failedProbeResults(duration.Seconds(), err.Error())
	}
	return probeResults{
		ClientToServer: probeResult{
			Success:                true,
			BandwidthBitsPerSecond: measurements.ClientToServer.BandwidthBitsPerSecond,
			JitterSeconds:          measurements.ClientToServer.JitterSeconds,
			LostPackets:            measurements.ClientToServer.LostPackets,
			LossRatio:              measurements.ClientToServer.LossRatio,
			DurationSeconds:        duration.Seconds(),
		},
		ServerToClient: probeResult{
			Success:                true,
			BandwidthBitsPerSecond: measurements.ServerToClient.BandwidthBitsPerSecond,
			JitterSeconds:          measurements.ServerToClient.JitterSeconds,
			LostPackets:            measurements.ServerToClient.LostPackets,
			LossRatio:              measurements.ServerToClient.LossRatio,
			DurationSeconds:        duration.Seconds(),
		},
	}
}

func (a *app) runIperfClientDirection(ctx context.Context, serverHost string, serverPort int, protocol string, bandwidth string, durationSeconds int, reverse bool) probeResult {
	started := time.Now()
	testCtx, cancel := context.WithTimeout(ctx, a.timeoutForSeconds(durationSeconds))
	defer cancel()

	args := iperfClientArgs(serverHost, serverPort, protocol, bandwidth, durationSeconds, a.cfg.warmupSeconds(), reverse, false)
	cmd := exec.CommandContext(testCtx, "iperf3", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.Output()
	duration := time.Since(started)
	if testCtx.Err() != nil {
		return failedProbeResult(duration.Seconds(), testCtx.Err().Error())
	}
	if err != nil {
		message := strings.TrimSpace(stderr.String())
		if parsed := parseIperfError(stdout); parsed != "" {
			message = parsed
		}
		if message == "" {
			message = err.Error()
		}
		return failedProbeResult(duration.Seconds(), message)
	}

	measurement, err := parseIperfMeasurement(protocol, stdout)
	if err != nil {
		return failedProbeResult(duration.Seconds(), err.Error())
	}
	return probeResult{
		Success:                true,
		BandwidthBitsPerSecond: measurement.BandwidthBitsPerSecond,
		JitterSeconds:          measurement.JitterSeconds,
		LostPackets:            measurement.LostPackets,
		LossRatio:              measurement.LossRatio,
		DurationSeconds:        duration.Seconds(),
	}
}

func failedProbeResults(durationSeconds float64, message string) probeResults {
	result := failedProbeResult(durationSeconds, message)
	return probeResults{ClientToServer: result, ServerToClient: result}
}

func failedProbeResult(durationSeconds float64, message string) probeResult {
	return probeResult{Success: false, DurationSeconds: durationSeconds, Error: message}
}

func usableServerHost(advertised string, fallback string) string {
	advertised = strings.Trim(advertised, "[]")
	if advertised == "" || advertised == "0.0.0.0" || advertised == "::" {
		return fallback
	}
	return advertised
}

type iperfMeasurement struct {
	BandwidthBitsPerSecond float64
	JitterSeconds          float64
	LostPackets            float64
	LossRatio              float64
}

type iperfMeasurements struct {
	ClientToServer iperfMeasurement
	ServerToClient iperfMeasurement
}

type iperfOutput struct {
	Error string   `json:"error"`
	End   iperfEnd `json:"end"`
}

type iperfEnd struct {
	Sum                     *iperfSum `json:"sum"`
	SumSent                 *iperfSum `json:"sum_sent"`
	SumReceived             *iperfSum `json:"sum_received"`
	SumBidirReverse         *iperfSum `json:"sum_bidir_reverse"`
	SumSentBidirReverse     *iperfSum `json:"sum_sent_bidir_reverse"`
	SumReceivedBidirReverse *iperfSum `json:"sum_received_bidir_reverse"`
}

type iperfSum struct {
	Seconds       float64 `json:"seconds"`
	BitsPerSecond float64 `json:"bits_per_second"`
	JitterMs      float64 `json:"jitter_ms"`
	LostPackets   float64 `json:"lost_packets"`
	Packets       float64 `json:"packets"`
	LostPercent   float64 `json:"lost_percent"`
}

func parseIperfMeasurement(protocol string, data []byte) (iperfMeasurement, error) {
	parsed, err := parseIperfOutput(data)
	if err != nil {
		return iperfMeasurement{}, err
	}
	return measurementFromCandidates(primaryIperfCandidates(protocol, parsed.End))
}

func parseIperfBidirMeasurements(protocol string, data []byte) (iperfMeasurements, error) {
	parsed, err := parseIperfOutput(data)
	if err != nil {
		return iperfMeasurements{}, err
	}

	clientToServer, err := measurementFromCandidates(bidirClientToServerCandidates(protocol, parsed.End))
	if err != nil {
		return iperfMeasurements{}, fmt.Errorf("client-to-server summary: %w", err)
	}
	serverToClient, err := measurementFromCandidates(bidirServerToClientCandidates(parsed.End))
	if err != nil {
		return iperfMeasurements{}, fmt.Errorf("server-to-client summary: %w", err)
	}

	return iperfMeasurements{ClientToServer: clientToServer, ServerToClient: serverToClient}, nil
}

func parseIperfOutput(data []byte) (iperfOutput, error) {
	var parsed iperfOutput
	if err := json.Unmarshal(data, &parsed); err != nil {
		return iperfOutput{}, fmt.Errorf("parse iperf3 json: %w", err)
	}
	if parsed.Error != "" {
		return iperfOutput{}, errors.New(parsed.Error)
	}
	return parsed, nil
}

func primaryIperfCandidates(protocol string, end iperfEnd) []*iperfSum {
	if protocol == "tcp" {
		return []*iperfSum{end.SumReceived, end.Sum, end.SumSent}
	}
	return []*iperfSum{end.Sum, end.SumReceived, end.SumSent}
}

func bidirClientToServerCandidates(protocol string, end iperfEnd) []*iperfSum {
	if protocol == "udp" {
		return []*iperfSum{end.SumReceived, end.Sum, end.SumSent}
	}
	return primaryIperfCandidates(protocol, end)
}

func bidirServerToClientCandidates(end iperfEnd) []*iperfSum {
	return []*iperfSum{end.SumReceivedBidirReverse, end.SumBidirReverse, end.SumSentBidirReverse}
}

func measurementFromCandidates(candidates []*iperfSum) (iperfMeasurement, error) {
	var sum *iperfSum
	for _, candidate := range candidates {
		if candidate == nil {
			continue
		}
		if candidate.BitsPerSecond != 0 || candidate.Seconds != 0 || candidate.Packets != 0 {
			sum = candidate
			break
		}
	}
	if sum == nil {
		return iperfMeasurement{}, fmt.Errorf("iperf3 json did not contain a usable summary")
	}

	lossRatio := 0.0
	if sum.LostPercent != 0 {
		lossRatio = sum.LostPercent / 100
	} else if sum.Packets > 0 {
		lossRatio = sum.LostPackets / sum.Packets
	}

	return iperfMeasurement{
		BandwidthBitsPerSecond: sum.BitsPerSecond,
		JitterSeconds:          sum.JitterMs / 1000,
		LostPackets:            sum.LostPackets,
		LossRatio:              lossRatio,
	}, nil
}

func parseIperfError(data []byte) string {
	var parsed iperfOutput
	if err := json.Unmarshal(data, &parsed); err != nil {
		return ""
	}
	return parsed.Error
}

func newMetricStore() *metricStore {
	return &metricStore{samples: make(map[metricKey]metricSample)}
}

func (m *metricStore) Record(sample metricSample) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.samples[sample.Key] = sample
}

func (m *metricStore) RecordTraffic(sample trafficSample) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.traffic = &sample
}

func (m *metricStore) Render() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var b strings.Builder
	b.WriteString("# HELP expeditus_iperf_probe_success Whether the last iperf probe completed successfully.\n")
	b.WriteString("# TYPE expeditus_iperf_probe_success gauge\n")
	b.WriteString("# HELP expeditus_iperf_bandwidth_bits_per_second Measured uncapped TCP iperf bandwidth in bits per second.\n")
	b.WriteString("# TYPE expeditus_iperf_bandwidth_bits_per_second gauge\n")
	b.WriteString("# HELP expeditus_iperf_jitter_seconds Measured UDP jitter in seconds.\n")
	b.WriteString("# TYPE expeditus_iperf_jitter_seconds gauge\n")
	b.WriteString("# HELP expeditus_iperf_lost_packets Measured UDP lost packets.\n")
	b.WriteString("# TYPE expeditus_iperf_lost_packets gauge\n")
	b.WriteString("# HELP expeditus_iperf_loss_ratio Measured UDP packet loss ratio from 0 to 1.\n")
	b.WriteString("# TYPE expeditus_iperf_loss_ratio gauge\n")
	b.WriteString("# HELP expeditus_iperf_probe_duration_seconds Wall-clock duration of the last iperf probe.\n")
	b.WriteString("# TYPE expeditus_iperf_probe_duration_seconds gauge\n")
	b.WriteString("# HELP expeditus_iperf_last_run_timestamp_seconds Unix timestamp of the last probe run.\n")
	b.WriteString("# TYPE expeditus_iperf_last_run_timestamp_seconds gauge\n")
	b.WriteString("# HELP expeditus_host_receive_bits_per_second Current aggregate non-loopback host receive traffic in bits per second.\n")
	b.WriteString("# TYPE expeditus_host_receive_bits_per_second gauge\n")
	b.WriteString("# HELP expeditus_host_transmit_bits_per_second Current aggregate non-loopback host transmit traffic in bits per second.\n")
	b.WriteString("# TYPE expeditus_host_transmit_bits_per_second gauge\n")
	b.WriteString("# HELP expeditus_host_traffic_last_run_timestamp_seconds Unix timestamp of the last host traffic sample.\n")
	b.WriteString("# TYPE expeditus_host_traffic_last_run_timestamp_seconds gauge\n")

	samples := make([]metricSample, 0, len(m.samples))
	for _, sample := range m.samples {
		samples = append(samples, sample)
	}
	sort.Slice(samples, func(i, j int) bool {
		return samples[i].Key.String() < samples[j].Key.String()
	})

	for _, sample := range samples {
		labels := sample.Key.Labels()
		success := 0
		if sample.Success {
			success = 1
		}
		fmt.Fprintf(&b, "expeditus_iperf_probe_success{%s} %d\n", labels, success)
		if sample.Key.Protocol == "tcp" {
			fmt.Fprintf(&b, "expeditus_iperf_bandwidth_bits_per_second{%s} %g\n", labels, sample.BandwidthBitsPerSecond)
		} else if sample.Key.Protocol == "udp" {
			fmt.Fprintf(&b, "expeditus_iperf_jitter_seconds{%s} %g\n", labels, sample.JitterSeconds)
			fmt.Fprintf(&b, "expeditus_iperf_lost_packets{%s} %g\n", labels, sample.LostPackets)
			fmt.Fprintf(&b, "expeditus_iperf_loss_ratio{%s} %g\n", labels, sample.LossRatio)
		}
		fmt.Fprintf(&b, "expeditus_iperf_probe_duration_seconds{%s} %g\n", labels, sample.ProbeDurationSeconds)
		fmt.Fprintf(&b, "expeditus_iperf_last_run_timestamp_seconds{%s} %d\n", labels, sample.LastRun.Unix())
	}
	if m.traffic != nil {
		labels := `local_node="` + escapeLabel(m.traffic.LocalNode) + `"`
		fmt.Fprintf(&b, "expeditus_host_receive_bits_per_second{%s} %g\n", labels, m.traffic.ReceiveBitsPerSecond)
		fmt.Fprintf(&b, "expeditus_host_transmit_bits_per_second{%s} %g\n", labels, m.traffic.TransmitBitsPerSecond)
		fmt.Fprintf(&b, "expeditus_host_traffic_last_run_timestamp_seconds{%s} %d\n", labels, m.traffic.LastRun.Unix())
	}

	return b.String()
}

func (k metricKey) String() string {
	return strings.Join([]string{k.LocalNode, k.PeerNode, k.ClientNode, k.ServerNode, k.Protocol}, "\xff")
}

func (k metricKey) Labels() string {
	labels := []string{
		`local_node="` + escapeLabel(k.LocalNode) + `"`,
		`peer_node="` + escapeLabel(k.PeerNode) + `"`,
		`client_node="` + escapeLabel(k.ClientNode) + `"`,
		`server_node="` + escapeLabel(k.ServerNode) + `"`,
		`protocol="` + escapeLabel(k.Protocol) + `"`,
	}
	return strings.Join(labels, ",")
}

func escapeLabel(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return value
}
