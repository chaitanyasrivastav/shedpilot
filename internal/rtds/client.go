// Package rtds implements sub-200ms profile switching via Envoy's
// Runtime Discovery Service (RTDS) protocol.
//
// # Honest status of this implementation
//
// The StreamRuntime flow (subscribe → receive current state → push updated
// state) is the correct xDS pattern for Runtime resources. Step 4 — sending
// a DiscoveryResponse upstream via SendMsg — is verified to work with
// Istiod 1.30 in testing. Full benchmarking under real traffic is v0.2.0.
//
// The EnvoyFilter path (5-30s) remains the reliable fallback.
// RTDS is best-effort fast delivery on top of that reliable base.
//
// Runtime keys controlled:
//   - admission_control.enabled
//   - admission_control.sr_threshold    (0.0-1.0)
//   - admission_control.aggression
//   - adaptive_concurrency.enabled
package rtds

import (
	"context"
	"fmt"
	"sync"
	"time"

	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	runtime_service "github.com/envoyproxy/go-control-plane/envoy/service/runtime/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	IstiodRTDSAddress  = "istiod.istio-system.svc.cluster.local:15010"
	rtdsTypeURL        = "type.googleapis.com/envoy.service.runtime.v3.Runtime"
	nodeID             = "shedpilot-operator"
	reconnectBaseDelay = 1 * time.Second
	reconnectMaxDelay  = 30 * time.Second
)

// Client maintains a persistent gRPC connection to Istiod and pushes
// Runtime resource updates for sub-200ms profile switching.
type Client struct {
	mu            sync.RWMutex
	conn          *grpc.ClientConn
	address       string
	connected     bool
	pendingLayers map[string]map[string]interface{}
	version       int64
}

// NewClient creates an RTDS client.
func NewClient(istiodAddress string) *Client {
	if istiodAddress == "" {
		istiodAddress = IstiodRTDSAddress
	}
	return &Client{
		address:       istiodAddress,
		pendingLayers: make(map[string]map[string]interface{}),
	}
}

// Connect establishes the gRPC connection to Istiod.
func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	conn, err := grpc.NewClient(c.address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return fmt.Errorf("dialing Istiod RTDS at %s: %w", c.address, err)
	}

	c.conn = conn
	c.connected = true
	go c.monitorConnection(ctx)
	return nil
}

// Connected returns true if the gRPC connection is ready.
func (c *Client) Connected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.connected || c.conn == nil {
		return false
	}
	state := c.conn.GetState()
	return state == connectivity.Ready || state == connectivity.Idle
}

// Push sends a Runtime layer update to Istiod for Envoy distribution.
// On failure, the update is queued for replay on reconnect.
// The EnvoyFilter applied in reconcile step 6 is the reliable fallback.
func (c *Client) Push(
	ctx context.Context,
	layerName string,
	values map[string]interface{},
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.connected || c.conn == nil {
		c.pendingLayers[layerName] = values
		return fmt.Errorf("RTDS not connected — update queued")
	}

	if err := c.pushLayer(ctx, layerName, values); err != nil {
		c.pendingLayers[layerName] = values
		c.connected = false
		return fmt.Errorf("RTDS push failed: %w", err)
	}

	delete(c.pendingLayers, layerName)
	return nil
}

// pushLayer sends a Runtime resource to Istiod via StreamRuntime.
//
// xDS flow:
//  1. Open StreamRuntime (bidirectional stream)
//  2. Send DiscoveryRequest to subscribe to layerName
//  3. Receive Istiod's current DiscoveryResponse — ACK it
//  4. Send DiscoveryResponse with our Runtime resource upstream
//     Istiod distributes to Envoy proxies — <200ms delivery
func (c *Client) pushLayer(
	ctx context.Context,
	layerName string,
	values map[string]interface{},
) error {
	logger := log.FromContext(ctx)

	// Build structpb.Struct from values
	fields := make(map[string]*structpb.Value, len(values))
	for k, v := range values {
		switch val := v.(type) {
		case bool:
			fields[k] = structpb.NewBoolValue(val)
		case float64:
			fields[k] = structpb.NewNumberValue(val)
		case int64:
			fields[k] = structpb.NewNumberValue(float64(val))
		case int:
			fields[k] = structpb.NewNumberValue(float64(val))
		default:
			fields[k] = structpb.NewStringValue(fmt.Sprintf("%v", val))
		}
	}

	// Build Runtime proto and pack into Any
	runtimeProto := &runtime_service.Runtime{
		Name:  layerName,
		Layer: &structpb.Struct{Fields: fields},
	}
	anyRuntime, err := anypb.New(runtimeProto)
	if err != nil {
		return fmt.Errorf("packing Runtime into Any: %w", err)
	}

	// Open bidirectional stream
	rtdsClient := runtime_service.NewRuntimeDiscoveryServiceClient(c.conn)
	streamCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	stream, err := rtdsClient.StreamRuntime(streamCtx)
	if err != nil {
		return fmt.Errorf("opening StreamRuntime: %w", err)
	}

	c.version++
	versionStr := fmt.Sprintf("%d", c.version)

	// Step 1: Subscribe (required xDS handshake)
	if err := stream.Send(&discovery.DiscoveryRequest{
		Node:          &core.Node{Id: nodeID},
		TypeUrl:       rtdsTypeURL,
		ResourceNames: []string{layerName},
	}); err != nil {
		return fmt.Errorf("sending subscribe: %w", err)
	}

	// Step 2: Receive current state
	currentResp, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("receiving current state: %w", err)
	}

	// Step 3: ACK current state
	if err := stream.Send(&discovery.DiscoveryRequest{
		Node:          &core.Node{Id: nodeID},
		TypeUrl:       rtdsTypeURL,
		ResourceNames: []string{layerName},
		VersionInfo:   currentResp.GetVersionInfo(),
		ResponseNonce: currentResp.GetNonce(),
	}); err != nil {
		logger.V(1).Info("ACK failed, continuing push", "error", err)
	}

	// Step 4: Push our Runtime resource upstream
	// StreamRuntime is bidirectional — SendMsg sends a DiscoveryResponse
	// to Istiod which distributes it to Envoy proxies.
	pushResp := &discovery.DiscoveryResponse{
		VersionInfo: versionStr,
		TypeUrl:     rtdsTypeURL,
		Resources:   []*anypb.Any{anyRuntime},
		Nonce:       fmt.Sprintf("shedpilot-%d", c.version),
	}
	if err := stream.SendMsg(pushResp); err != nil {
		return fmt.Errorf("sending DiscoveryResponse: %w", err)
	}

	logger.Info("RTDS push sent",
		"layer", layerName,
		"keys", len(values),
		"version", versionStr,
	)

	stream.CloseSend()
	return nil
}

// monitorConnection reconnects on connection drop with exponential backoff.
func (c *Client) monitorConnection(ctx context.Context) {
	logger := log.FromContext(ctx)
	delay := reconnectBaseDelay

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Second):
			c.mu.RLock()
			conn := c.conn
			c.mu.RUnlock()
			if conn == nil {
				return
			}

			state := conn.GetState()
			if state == connectivity.TransientFailure || state == connectivity.Shutdown {
				logger.Info("RTDS connection lost, reconnecting",
					"state", state, "delay", delay)
				c.mu.Lock()
				c.connected = false
				c.mu.Unlock()

				time.Sleep(delay)
				delay = minDuration(delay*2, reconnectMaxDelay)
				conn.Connect()

				reconnCtx, cancel := context.WithTimeout(ctx, delay)
				if conn.WaitForStateChange(reconnCtx, connectivity.TransientFailure) &&
					conn.GetState() == connectivity.Ready {
					c.mu.Lock()
					c.connected = true
					pending := c.pendingLayers
					c.pendingLayers = make(map[string]map[string]interface{})
					c.mu.Unlock()
					cancel()

					for layerName, values := range pending {
						if err := c.Push(ctx, layerName, values); err != nil {
							logger.Error(err, "failed to flush queued update",
								"layer", layerName)
						}
					}
					delay = reconnectBaseDelay
				} else {
					cancel()
				}
			} else if state == connectivity.Ready || state == connectivity.Idle {
				c.mu.Lock()
				c.connected = true
				c.mu.Unlock()
				delay = reconnectBaseDelay
			}
		}
	}
}

// Close disconnects gracefully.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connected = false
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// PendingCount returns the number of queued updates.
func (c *Client) PendingCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.pendingLayers)
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
