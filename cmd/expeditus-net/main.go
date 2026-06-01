package main

import (
	"bytes"
	"context"
	"crypto/subtle"
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
	defaultBindAddress    = ":9119"
	defaultIperfPortRange = "5201-5210"
	defaultProtocol       = "both"
	defaultBandwidth      = "10M"
	defaultInterval       = 60 * time.Second
	defaultDuration       = 5 * time.Second
	serverReadyDelay      = 150 * time.Millisecond
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

	activeMu      sync.Mutex
	activeServers map[int]struct{}
	testMu        sync.Mutex
	testActive    bool

	roleMu     sync.Mutex
	roleCounts map[string]int

	peerMu    sync.Mutex
	peerNames map[string]string
}

type infoResponse struct {
	NodeName string `json:"node_name"`
}

type negotiateRequest struct {
	ClientNode      string `json:"client_node"`
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
	Protocol        string `json:"protocol"`
	DurationSeconds int    `json:"duration_seconds"`
	Bandwidth       string `json:"bandwidth"`
}

type clientRunResponse struct {
	Accepted bool        `json:"accepted"`
	Result   probeResult `json:"result"`
	Error    string      `json:"error,omitempty"`
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

type metricStore struct {
	mu      sync.RWMutex
	samples map[metricKey]metricSample
}

type httpStatusError struct {
	StatusCode int
	Message    string
}

func (e *httpStatusError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("peer returned HTTP %d", e.StatusCode)
	}
	return fmt.Sprintf("peer returned HTTP %d: %s", e.StatusCode, e.Message)
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
		peerNames:     make(map[string]string),
	}

	server := &http.Server{
		Addr:              cfg.BindAddress,
		Handler:           a.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
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

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("http shutdown failed: %v", err)
		}
	case err := <-errCh:
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
	fs.DurationVar(&cfg.TestTimeout, "test-timeout", 0, "overall timeout per iperf3 test; defaults to duration plus 15s")
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
	writeJSON(w, http.StatusOK, infoResponse{NodeName: a.cfg.NodeName})
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
	releaseTest, ok := a.tryAcquireTestSlot()
	if !ok {
		writeJSON(w, http.StatusConflict, negotiateResponse{Accepted: false, Error: "node is already running a test"})
		return
	}

	port, expiresAt, done, _, err := a.startIperfServer(a.timeoutForSeconds(req.DurationSeconds))
	if err != nil {
		releaseTest()
		writeJSON(w, http.StatusServiceUnavailable, negotiateResponse{Accepted: false, Error: err.Error()})
		return
	}
	releaseTestSlotWhenDone(done, releaseTest)

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
	releaseTest, ok := a.tryAcquireTestSlot()
	if !ok {
		writeJSON(w, http.StatusConflict, clientRunResponse{Accepted: false, Error: "node is already running a test"})
		return
	}
	defer releaseTest()

	serverNode := req.ServerNode

	result := a.runIperfClient(r.Context(), req.ServerHost, req.ServerPort, req.Protocol, req.Bandwidth, req.DurationSeconds)
	a.metrics.Record(metricSample{
		Key: metricKey{
			LocalNode:  a.cfg.NodeName,
			PeerNode:   serverNode,
			ClientNode: a.cfg.NodeName,
			ServerNode: serverNode,
			Protocol:   req.Protocol,
		},
		Success:                result.Success,
		BandwidthBitsPerSecond: result.BandwidthBitsPerSecond,
		JitterSeconds:          result.JitterSeconds,
		LostPackets:            result.LostPackets,
		LossRatio:              result.LossRatio,
		ProbeDurationSeconds:   result.DurationSeconds,
		LastRun:                time.Now(),
	})

	writeJSON(w, http.StatusOK, clientRunResponse{Accepted: true, Result: result})
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
	return time.Duration(durationSeconds)*time.Second + 15*time.Second
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

func (a *app) runRound(ctx context.Context) {
	if len(a.cfg.Neighbors) == 0 {
		return
	}
	durationSeconds := secondsCeil(a.cfg.Duration)
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
		for _, protocol := range a.cfg.probeProtocols() {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if localClient {
				a.runLocalClientProbe(ctx, n, peerName, protocol, durationSeconds)
			} else {
				a.runRemoteClientProbe(ctx, n, peerName, protocol, durationSeconds)
			}
		}
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

func releaseTestSlotWhenDone(done <-chan error, release func()) {
	go func() {
		<-done
		release()
	}()
}

func isPeerConflict(err error) bool {
	var statusErr *httpStatusError
	return errors.As(err, &statusErr) && statusErr.StatusCode == http.StatusConflict
}

func (a *app) runLocalClientProbe(ctx context.Context, n neighbor, peerName string, protocol string, durationSeconds int) {
	started := time.Now()
	releaseTest, ok := a.tryAcquireTestSlot()
	if !ok {
		log.Printf("probe skipped peer=%s: node is already running a test", peerName)
		return
	}
	defer releaseTest()

	var resp negotiateResponse
	requestCtx, cancel := context.WithTimeout(ctx, a.timeoutForSeconds(durationSeconds)+5*time.Second)
	defer cancel()
	err := a.postJSON(requestCtx, n, "/v1/negotiate", negotiateRequest{
		ClientNode:      a.cfg.NodeName,
		Protocol:        protocol,
		DurationSeconds: durationSeconds,
		Bandwidth:       a.cfg.Bandwidth,
	}, &resp)
	if err != nil || !resp.Accepted {
		if err == nil {
			err = errors.New(resp.Error)
		}
		if isPeerConflict(err) {
			log.Printf("probe skipped peer=%s: %v", peerName, err)
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
	result := a.runIperfClient(ctx, serverHost, resp.ServerPort, protocol, a.cfg.Bandwidth, durationSeconds)
	a.metrics.Record(sampleFromResult(a.cfg.NodeName, serverNode, a.cfg.NodeName, serverNode, protocol, result))

	if result.Success {
		log.Printf("probe succeeded peer=%s protocol=%s role=client bandwidth_bps=%.0f jitter_s=%.6f", peerName, protocol, result.BandwidthBitsPerSecond, result.JitterSeconds)
	} else {
		log.Printf("probe failed peer=%s protocol=%s role=client: %s", peerName, protocol, result.Error)
	}
}

func (a *app) runRemoteClientProbe(ctx context.Context, n neighbor, peerName string, protocol string, durationSeconds int) {
	started := time.Now()
	releaseTest, ok := a.tryAcquireTestSlot()
	if !ok {
		log.Printf("probe skipped peer=%s: node is already running a test", peerName)
		return
	}

	port, _, done, stopServer, err := a.startIperfServer(a.timeoutForSeconds(durationSeconds))
	if err != nil {
		releaseTest()
		log.Printf("local iperf server failed peer=%s: %v", peerName, err)
		a.recordFailure(peerName, peerName, a.cfg.NodeName, protocol, time.Since(started))
		return
	}
	releaseTestSlotWhenDone(done, releaseTest)

	var resp clientRunResponse
	requestCtx, cancel := context.WithTimeout(ctx, a.timeoutForSeconds(durationSeconds)+5*time.Second)
	defer cancel()
	err = a.postJSON(requestCtx, n, "/v1/iperf/client/run", clientRunRequest{
		ServerNode:      a.cfg.NodeName,
		ServerHost:      a.cfg.AdvertiseAddress,
		ServerPort:      port,
		Protocol:        protocol,
		DurationSeconds: durationSeconds,
		Bandwidth:       a.cfg.Bandwidth,
	}, &resp)
	if err != nil || !resp.Accepted {
		stopServer()
		if err == nil {
			err = errors.New(resp.Error)
		}
		if isPeerConflict(err) {
			log.Printf("probe skipped peer=%s: %v", peerName, err)
			return
		}
		log.Printf("remote client probe failed peer=%s: %v", peerName, err)
		a.recordFailure(peerName, peerName, a.cfg.NodeName, protocol, time.Since(started))
		return
	}
	if resp.Result.Success {
		log.Printf("probe succeeded peer=%s protocol=%s role=server bandwidth_bps=%.0f jitter_s=%.6f", peerName, protocol, resp.Result.BandwidthBitsPerSecond, resp.Result.JitterSeconds)
	} else {
		log.Printf("probe failed peer=%s protocol=%s role=server: %s", peerName, protocol, resp.Result.Error)
	}
}

func (a *app) recordFailure(peerNode string, clientNode string, serverNode string, protocol string, duration time.Duration) {
	a.metrics.Record(metricSample{
		Key: metricKey{
			LocalNode:  a.cfg.NodeName,
			PeerNode:   peerNode,
			ClientNode: clientNode,
			ServerNode: serverNode,
			Protocol:   protocol,
		},
		Success:              false,
		ProbeDurationSeconds: duration.Seconds(),
		LastRun:              time.Now(),
	})
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
	if name := a.peerNames[n.BaseURL]; name != "" {
		a.peerMu.Unlock()
		return name, nil
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

	a.peerMu.Lock()
	a.peerNames[n.BaseURL] = name
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

func (a *app) runIperfClient(ctx context.Context, serverHost string, serverPort int, protocol string, bandwidth string, durationSeconds int) probeResult {
	started := time.Now()
	testCtx, cancel := context.WithTimeout(ctx, a.timeoutForSeconds(durationSeconds))
	defer cancel()

	args := []string{"-c", serverHost, "-p", strconv.Itoa(serverPort), "-t", strconv.Itoa(durationSeconds), "--json"}
	if protocol == "udp" {
		args = append(args, "--udp", "--bandwidth", bandwidth)
	}

	cmd := exec.CommandContext(testCtx, "iperf3", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.Output()
	duration := time.Since(started)
	if testCtx.Err() != nil {
		return probeResult{Success: false, DurationSeconds: duration.Seconds(), Error: testCtx.Err().Error()}
	}
	if err != nil {
		message := strings.TrimSpace(stderr.String())
		if parsed := parseIperfError(stdout); parsed != "" {
			message = parsed
		}
		if message == "" {
			message = err.Error()
		}
		return probeResult{Success: false, DurationSeconds: duration.Seconds(), Error: message}
	}

	measurement, err := parseIperfMeasurement(protocol, stdout)
	if err != nil {
		return probeResult{Success: false, DurationSeconds: duration.Seconds(), Error: err.Error()}
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

type iperfOutput struct {
	Error string   `json:"error"`
	End   iperfEnd `json:"end"`
}

type iperfEnd struct {
	Sum         *iperfSum `json:"sum"`
	SumSent     *iperfSum `json:"sum_sent"`
	SumReceived *iperfSum `json:"sum_received"`
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
	var parsed iperfOutput
	if err := json.Unmarshal(data, &parsed); err != nil {
		return iperfMeasurement{}, fmt.Errorf("parse iperf3 json: %w", err)
	}
	if parsed.Error != "" {
		return iperfMeasurement{}, errors.New(parsed.Error)
	}

	var candidates []*iperfSum
	if protocol == "tcp" {
		candidates = []*iperfSum{parsed.End.SumReceived, parsed.End.Sum, parsed.End.SumSent}
	} else {
		candidates = []*iperfSum{parsed.End.Sum, parsed.End.SumReceived, parsed.End.SumSent}
	}

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

func (m *metricStore) Render() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var b strings.Builder
	b.WriteString("# HELP expeditus_iperf_probe_success Whether the last iperf probe completed successfully.\n")
	b.WriteString("# TYPE expeditus_iperf_probe_success gauge\n")
	b.WriteString("# HELP expeditus_iperf_bandwidth_bits_per_second Measured iperf bandwidth in bits per second.\n")
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
		fmt.Fprintf(&b, "expeditus_iperf_bandwidth_bits_per_second{%s} %g\n", labels, sample.BandwidthBitsPerSecond)
		if sample.Key.Protocol == "udp" {
			fmt.Fprintf(&b, "expeditus_iperf_jitter_seconds{%s} %g\n", labels, sample.JitterSeconds)
			fmt.Fprintf(&b, "expeditus_iperf_lost_packets{%s} %g\n", labels, sample.LostPackets)
			fmt.Fprintf(&b, "expeditus_iperf_loss_ratio{%s} %g\n", labels, sample.LossRatio)
		}
		fmt.Fprintf(&b, "expeditus_iperf_probe_duration_seconds{%s} %g\n", labels, sample.ProbeDurationSeconds)
		fmt.Fprintf(&b, "expeditus_iperf_last_run_timestamp_seconds{%s} %d\n", labels, sample.LastRun.Unix())
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
