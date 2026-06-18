// Package rtds implements sub-200ms profile switching via Envoy's admin API.
//
// # Architecture
//
// Each Envoy sidecar exposes an admin API at localhost:15000. The
// /runtime_modify endpoint accepts POST requests with query parameters
// that immediately change Envoy's runtime flags — no Istiod involvement,
// no xDS propagation delay.
//
// The operator calls this endpoint on every matching pod concurrently
// via the Kubernetes exec API (equivalent to kubectl exec -c istio-proxy).
// For a fleet of 10 pods, all calls complete in parallel — total delivery
// is bounded by the slowest single pod, typically <100ms inside a cluster.
//
// # Why not RTDS (RuntimeDiscoveryService)?
//
// Istiod 1.23+ does not implement RuntimeDiscoveryService as a server.
// Istiod is a consumer of xDS, not a provider of RTDS. The admin API
// approach is the correct fast path for Istio 1.23+.
//
// # Runtime keys controlled
//
//   - admission_control.enabled         (bool)
//   - admission_control.sr_threshold    (float, 0.0-1.0)
//   - admission_control.aggression      (float)
//   - adaptive_concurrency.enabled      (bool)
package rtds

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// IstiodRTDSAddress is kept for backwards compatibility with flag parsing.
	IstiodRTDSAddress = "istiod.istio-system.svc.cluster.local:15010"

	// adminPort is the Envoy admin API port — localhost only, inside each pod.
	adminPort = 15000

	// execTimeout is the maximum time for a single pod exec call.
	execTimeout = 3 * time.Second
)

// layerEntry stores the namespace and selector for a layer name.
type layerEntry struct {
	namespace string
	selector  map[string]string
}

// Client delivers profile switches to Envoy sidecars via the admin API.
// All exported methods are safe for concurrent use.
type Client struct {
	k8sClient  client.Client
	restConfig *rest.Config
	clientset  *kubernetes.Clientset
	mu         sync.RWMutex
	enabled    bool
	registry   map[string]layerEntry // layerName → namespace+selector
}

// NewClient creates a fast-delivery client.
// restConfig is the in-cluster rest config from ctrl.GetConfig().
// If restConfig is nil, returns a disabled client that can be used in tests or offline scenarios.
func NewClient(restConfig *rest.Config, k8sClient client.Client) (*Client, error) {
	if restConfig == nil {
		// Return disabled client for testing or when RTDS is not available
		return &Client{
			k8sClient:  k8sClient,
			restConfig: nil,
			clientset:  nil,
			enabled:    false,
			registry:   make(map[string]layerEntry),
		}, nil
	}
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("creating clientset: %w", err)
	}
	return &Client{
		k8sClient:  k8sClient,
		restConfig: restConfig,
		clientset:  clientset,
		enabled:    true,
		registry:   make(map[string]layerEntry),
	}, nil
}

// NewNoopClient returns a client that always reports not connected.
// Used when --enable-rtds=false.
func NewNoopClient() *Client {
	return &Client{
		enabled:  false,
		registry: make(map[string]layerEntry),
	}
}

// Connect is a no-op — kept for interface compatibility.
// The admin API requires no persistent connection.
func (c *Client) Connect(_ context.Context) error {
	return nil
}

// Connected returns true if fast delivery is available.
func (c *Client) Connected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.enabled && c.clientset != nil
}

// RegisterLayer associates a layer name with a namespace and pod selector.
// Must be called before Push() for each layer.
// Called by the controller when rendering each AdaptivePolicy.
func (c *Client) RegisterLayer(layerName, namespace string, selector map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.registry[layerName] = layerEntry{
		namespace: namespace,
		selector:  selector,
	}
}

// Push delivers runtime values to all pods matching the layer's selector.
// Calls are made concurrently — delivery time is bounded by the slowest pod.
//
// values map keys must match Envoy runtime flag names:
//
//	"admission_control.enabled"      → bool
//	"admission_control.sr_threshold" → float64 (0.0-1.0)
//	"admission_control.aggression"   → float64
//	"adaptive_concurrency.enabled"   → bool
func (c *Client) Push(
	ctx context.Context,
	layerName string,
	values map[string]interface{},
) error {
	if !c.Connected() {
		return fmt.Errorf("fast delivery not available")
	}

	logger := log.FromContext(ctx)

	c.mu.RLock()
	entry, ok := c.registry[layerName]
	c.mu.RUnlock()
	if !ok {
		return fmt.Errorf("layer %q not registered — call RegisterLayer first", layerName)
	}

	queryString := buildQueryString(values)
	if queryString == "" {
		return nil
	}

	// List matching pods
	podList := &corev1.PodList{}
	if err := c.k8sClient.List(ctx, podList,
		client.InNamespace(entry.namespace),
		client.MatchingLabels(entry.selector),
	); err != nil {
		return fmt.Errorf("listing pods: %w", err)
	}

	if len(podList.Items) == 0 {
		logger.V(1).Info("no pods found for fast delivery", "layer", layerName)
		return nil
	}

	// Push to all pods concurrently
	type result struct {
		pod string
		err error
	}
	results := make(chan result, len(podList.Items))
	var wg sync.WaitGroup

	for _, pod := range podList.Items {
		if pod.Status.Phase != corev1.PodRunning || pod.Status.PodIP == "" {
			continue
		}
		wg.Add(1)
		go func(p corev1.Pod) {
			defer wg.Done()
			err := c.pushToPod(ctx, p.Namespace, p.Name, queryString)
			results <- result{pod: p.Name, err: err}
		}(pod)
	}

	wg.Wait()
	close(results)

	var errs []string
	var succeeded int
	for r := range results {
		if r.err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", r.pod, r.err))
			logger.V(1).Info("fast delivery failed for pod",
				"pod", r.pod, "error", r.err)
		} else {
			succeeded++
		}
	}

	logger.Info("fast delivery complete",
		"layer", layerName,
		"succeeded", succeeded,
		"failed", len(errs),
	)

	if len(errs) > 0 && succeeded == 0 {
		return fmt.Errorf("all pods failed: %s", strings.Join(errs, "; "))
	}

	return nil
}

// pushToPod calls the Envoy admin API on a single pod via the k8s exec API.
func (c *Client) pushToPod(ctx context.Context, namespace, podName, queryString string) error {
	execCtx, cancel := context.WithTimeout(ctx, execTimeout)
	defer cancel()

	cmd := []string{
		"curl", "-s", "-X", "POST",
		fmt.Sprintf("http://localhost:%d/runtime_modify?%s", adminPort, queryString),
	}

	req := c.clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: "istio-proxy",
			Command:   cmd,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(c.restConfig, "POST", req.URL())
	if err != nil {
		return fmt.Errorf("creating executor: %w", err)
	}

	var stdout, stderr bytes.Buffer
	err = exec.StreamWithContext(execCtx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if err != nil {
		return fmt.Errorf("exec: %w (stderr: %s)", err, stderr.String())
	}

	output := strings.TrimSpace(stdout.String())
	if output != "OK" {
		return fmt.Errorf("unexpected response: %q", output)
	}

	return nil
}

// buildQueryString converts a values map to a URL-encoded query string.
func buildQueryString(values map[string]interface{}) string {
	params := url.Values{}
	for k, v := range values {
		params.Set(k, fmt.Sprintf("%v", v))
	}
	return params.Encode()
}

// Close is a no-op — no persistent connection to close.
func (c *Client) Close() error { return nil }

// PendingCount always returns 0 — no queuing needed.
func (c *Client) PendingCount() int { return 0 }
