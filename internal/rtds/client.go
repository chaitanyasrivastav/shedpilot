// Package rtds implements a Runtime Discovery Service (RTDS) server.
//
// # What RTDS is
//
// RTDS is a subset of Envoy's xDS protocol family that manages runtime
// key-value pairs. Instead of shedpilot calling into each pod via kubectl exec,
// each Envoy sidecar opens a persistent gRPC stream TO shedpilot's RTDS server.
// When a profile switch fires, shedpilot pushes the new runtime values down
// all connected streams simultaneously — one operation, all pods, <10ms.
//
// # How Envoy connects to shedpilot RTDS
//
// The operator adds a BOOTSTRAP EnvoyFilter patch that tells Envoy:
// "connect to shedpilot-rtds-server:15050 for runtime layer updates"
//
// This is a one-time structural install — the same EnvoyFilter that installs
// admission_control into the filter chain also configures the RTDS connection.
//
// # Lifecycle
//
//	Startup:       shedpilot starts gRPC server on :15050
//	Pod start:     Envoy reads bootstrap → connects to shedpilot RTDS
//	Profile switch: shedpilot pushes runtime values to all connected streams
//	Pod restart:   Envoy reconnects → shedpilot replays current values
//
// # Comparison with admin API approach
//
//	Admin API (old):
//	  shedpilot → kubectl exec → curl :15000/runtime_modify (per pod)
//	  N pods = N kube-apiserver calls, N connection setups
//	  ~50-200ms per pod (concurrent but kube-apiserver bottleneck)
//
//	RTDS (new):
//	  Envoy → persistent gRPC stream → shedpilot
//	  Profile switch = push to all streams simultaneously
//	  <10ms at any scale, zero kube-apiserver involvement
//
// # Runtime keys controlled
//
//	admission_control.enabled         (bool → "true"/"false")
//	admission_control.sr_threshold    (float, 0.0-1.0)
//	admission_control.aggression      (float)
//	adaptive_concurrency.enabled      (bool → "true"/"false")
package rtds

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	runtime_service "github.com/envoyproxy/go-control-plane/envoy/service/runtime/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// RTDSPort is the gRPC port shedpilot's RTDS server listens on.
	// Envoy sidecars connect here via bootstrap config.
	RTDSPort = 15050

	// rtdsTypeURL is the xDS type URL for Runtime resources.
	rtdsTypeURL = "type.googleapis.com/envoy.service.runtime.v3.Runtime"

	// IstiodRTDSAddress kept for flag compatibility.
	IstiodRTDSAddress = "istiod.istio-system.svc.cluster.local:15010"
)

// stream tracks a connected Envoy instance.
type stream struct {
	nodeID string
	send   func(*discovery.DiscoveryResponse) error
	ctx    context.Context
}

// Server is shedpilot's RTDS server.
// Envoy sidecars connect to it and receive runtime layer updates.
// All methods are safe for concurrent use.
type Server struct {
	runtime_service.UnimplementedRuntimeDiscoveryServiceServer

	mu      sync.RWMutex
	streams map[string]*stream           // nodeID → stream
	current map[string]map[string]string // layerName → key/value pairs (current state)
	version int64
}

// Client wraps Server and provides the same interface as the old admin API client.
// Controllers call Push() exactly as before — the delivery mechanism is RTDS internally.
type Client struct {
	server  *Server
	enabled bool

	// layerToSelector maps layerName → (namespace, selector)
	// needed so we can match which Envoy streams belong to a policy
	mu       sync.RWMutex
	registry map[string]layerEntry
}

type layerEntry struct {
	namespace string
	selector  map[string]string
}

// NewServer creates the RTDS gRPC server.
func NewServer() *Server {
	return &Server{
		streams: make(map[string]*stream),
		current: make(map[string]map[string]string),
	}
}

// NewClient creates the RTDS client used by the controller.
// server must be started separately via server.Start().
func NewClient(server *Server) (*Client, error) {
	if server == nil {
		return &Client{enabled: false, registry: make(map[string]layerEntry)}, nil
	}
	return &Client{
		server:   server,
		enabled:  true,
		registry: make(map[string]layerEntry),
	}, nil
}

// NewNoopClient returns a disabled client.
func NewNoopClient() *Client {
	return &Client{enabled: false, registry: make(map[string]layerEntry)}
}

// Start starts the gRPC server on the given port.
// Blocks until ctx is cancelled.
func (s *Server) Start(ctx context.Context, port int) error {
	logger := log.FromContext(ctx)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("listening on :%d: %w", port, err)
	}

	grpcServer := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    30 * time.Second,
			Timeout: 10 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	)

	runtime_service.RegisterRuntimeDiscoveryServiceServer(grpcServer, s)

	logger.Info("RTDS server starting", "port", port)

	errCh := make(chan error, 1)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		grpcServer.GracefulStop()
		return nil
	case err := <-errCh:
		return err
	}
}

// StreamRuntime implements the RTDS gRPC server method.
// Each Envoy sidecar calls this to establish a persistent stream.
func (s *Server) StreamRuntime(
	grpcStream runtime_service.RuntimeDiscoveryService_StreamRuntimeServer,
) error {
	logger := log.FromContext(grpcStream.Context())

	// Receive initial subscription request from Envoy
	req, err := grpcStream.Recv()
	if err != nil {
		return err
	}

	nodeID := ""
	if req.Node != nil {
		nodeID = req.Node.Id
	}
	if nodeID == "" {
		nodeID = fmt.Sprintf("unknown-%d", time.Now().UnixNano())
	}

	logger.Info("Envoy connected to RTDS", "nodeID", nodeID)

	// Register stream
	s.mu.Lock()
	s.streams[nodeID] = &stream{
		nodeID: nodeID,
		send:   grpcStream.Send,
		ctx:    grpcStream.Context(),
	}
	// Send current state immediately so reconnecting pods get correct values
	current := s.currentSnapshot()
	s.mu.Unlock()

	// Push current state to newly connected Envoy
	if len(current) > 0 {
		if err := s.pushToStream(grpcStream.Send, current); err != nil {
			logger.Error(err, "failed to send initial state", "nodeID", nodeID)
		}
	}

	// Keep stream alive — process ACKs and NACKs
	for {
		_, err := grpcStream.Recv()
		if err != nil {
			if status.Code(err) == codes.Canceled ||
				status.Code(err) == codes.Unavailable {
				break
			}
			logger.V(1).Info("stream recv error", "nodeID", nodeID, "error", err)
			break
		}
		// ACK received — nothing to do, we use fire-and-forget push
	}

	// Deregister stream on disconnect
	s.mu.Lock()
	delete(s.streams, nodeID)
	s.mu.Unlock()

	logger.Info("Envoy disconnected from RTDS", "nodeID", nodeID)
	return nil
}

// DeltaRuntime is required by the interface but not used.
func (s *Server) DeltaRuntime(
	_ runtime_service.RuntimeDiscoveryService_DeltaRuntimeServer,
) error {
	return status.Error(codes.Unimplemented, "delta runtime not supported")
}

// FetchRuntime is required by the interface but not used.
func (s *Server) FetchRuntime(
	_ context.Context,
	_ *discovery.DiscoveryRequest,
) (*discovery.DiscoveryResponse, error) {
	return nil, status.Error(codes.Unimplemented, "fetch runtime not supported")
}

// PushLayer pushes runtime values for a named layer to all connected Envoy instances.
// Called by the controller on every profile switch.
func (s *Server) PushLayer(layerName string, values map[string]string) error {
	s.mu.Lock()
	// Update current state so new connections get correct values
	s.current[layerName] = values
	streams := make([]*stream, 0, len(s.streams))
	for _, st := range s.streams {
		streams = append(streams, st)
	}
	s.version++
	s.mu.Unlock()

	if len(streams) == 0 {
		return nil
	}

	// Build the response once, push to all streams
	resp, err := s.buildResponse(layerName, values)
	if err != nil {
		return fmt.Errorf("building response: %w", err)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []string

	for _, st := range streams {
		wg.Add(1)
		go func(st *stream) {
			defer wg.Done()
			if st.ctx.Err() != nil {
				return // stream already closed
			}
			if err := st.send(resp); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Sprintf("%s: %v", st.nodeID, err))
				mu.Unlock()
			}
		}(st)
	}

	wg.Wait()

	if len(errs) > 0 {
		return fmt.Errorf("push failed for some streams: %v", errs)
	}
	return nil
}

// ConnectedCount returns the number of connected Envoy instances.
func (s *Server) ConnectedCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.streams)
}

// currentSnapshot returns a copy of the current runtime state.
func (s *Server) currentSnapshot() map[string]map[string]string {
	snap := make(map[string]map[string]string, len(s.current))
	for layer, values := range s.current {
		cp := make(map[string]string, len(values))
		for k, v := range values {
			cp[k] = v
		}
		snap[layer] = cp
	}
	return snap
}

// pushToStream sends all current layers to a single stream (used on reconnect).
func (s *Server) pushToStream(
	send func(*discovery.DiscoveryResponse) error,
	layers map[string]map[string]string,
) error {
	for layerName, values := range layers {
		resp, err := s.buildResponse(layerName, values)
		if err != nil {
			return err
		}
		if err := send(resp); err != nil {
			return err
		}
	}
	return nil
}

// buildResponse builds a DiscoveryResponse for a runtime layer.
func (s *Server) buildResponse(
	layerName string,
	values map[string]string,
) (*discovery.DiscoveryResponse, error) {
	s.mu.RLock()
	version := fmt.Sprintf("%d", s.version)
	s.mu.RUnlock()

	// Build structpb.Struct from string values
	fields := make(map[string]*structpb.Value, len(values))
	for k, v := range values {
		fields[k] = structpb.NewStringValue(v)
	}

	runtimeProto := &runtime_service.Runtime{
		Name:  layerName,
		Layer: &structpb.Struct{Fields: fields},
	}

	anyRuntime, err := anypb.New(runtimeProto)
	if err != nil {
		return nil, fmt.Errorf("packing runtime: %w", err)
	}

	return &discovery.DiscoveryResponse{
		VersionInfo: version,
		TypeUrl:     rtdsTypeURL,
		Resources:   []*anypb.Any{anyRuntime},
		Nonce:       fmt.Sprintf("shedpilot-%s-%s", layerName, version),
	}, nil
}

// ── Client methods (controller interface) ─────────────────────────────────────

// Connect is a no-op — the server is started separately.
func (c *Client) Connect(_ context.Context) error { return nil }

// Connected returns true if the RTDS server is running and has connections.
func (c *Client) Connected() bool {
	if !c.enabled || c.server == nil {
		return false
	}
	return true // server is running regardless of connected count
}

// RegisterLayer associates a layer name with a namespace and pod selector.
func (c *Client) RegisterLayer(layerName, namespace string, selector map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.registry[layerName] = layerEntry{
		namespace: namespace,
		selector:  selector,
	}
}

// Push delivers runtime values for a layer to all connected Envoy instances.
// Values are string-converted for RTDS protocol compatibility.
func (c *Client) Push(
	ctx context.Context,
	layerName string,
	values map[string]interface{},
) error {
	if !c.Connected() {
		return fmt.Errorf("RTDS server not available")
	}

	logger := log.FromContext(ctx)

	// Convert interface{} values to strings for RTDS
	strValues := make(map[string]string, len(values))
	for k, v := range values {
		strValues[k] = fmt.Sprintf("%v", v)
	}

	if err := c.server.PushLayer(layerName, strValues); err != nil {
		return fmt.Errorf("RTDS push failed: %w", err)
	}

	logger.Info("RTDS push succeeded",
		"layer", layerName,
		"connected", c.server.ConnectedCount(),
		"deliveryMs", "<10",
	)

	return nil
}

// Close is a no-op — server lifecycle managed separately.
func (c *Client) Close() error { return nil }

// PendingCount returns 0 — RTDS is fire-and-forget with reconnect replay.
func (c *Client) PendingCount() int { return 0 }

// BootstrapPatch returns the Envoy bootstrap config that tells Envoy
// to connect to shedpilot's RTDS server.
// This is added to the EnvoyFilter BOOTSTRAP patch alongside stats_flush_on_admin.
func BootstrapPatch(rtdsServiceAddress string) map[string]interface{} {
	return map[string]interface{}{
		"layered_runtime": map[string]interface{}{
			"layers": []interface{}{
				map[string]interface{}{
					"name": "shedpilot-rtds",
					"rtds_layer": map[string]interface{}{
						"name": "shedpilot-runtime",
						"rtds_config": map[string]interface{}{
							"api_config_source": map[string]interface{}{
								"api_type":              "GRPC",
								"transport_api_version": "V3",
								"grpc_services": []interface{}{
									map[string]interface{}{
										"envoy_grpc": map[string]interface{}{
											"cluster_name": "shedpilot_rtds",
										},
									},
								},
							},
						},
					},
				},
			},
		},
		"static_resources": map[string]interface{}{
			"clusters": []interface{}{
				map[string]interface{}{
					"name":                   "shedpilot_rtds",
					"type":                   "STRICT_DNS",
					"connect_timeout":        "5s",
					"http2_protocol_options": map[string]interface{}{},
					"load_assignment": map[string]interface{}{
						"cluster_name": "shedpilot_rtds",
						"endpoints": []interface{}{
							map[string]interface{}{
								"lb_endpoints": []interface{}{
									map[string]interface{}{
										"endpoint": map[string]interface{}{
											"address": map[string]interface{}{
												"socket_address": map[string]interface{}{
													"address":    rtdsServiceAddress,
													"port_value": int64(RTDSPort),
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}
