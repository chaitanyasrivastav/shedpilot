// Package signal reads live metrics from Envoy sidecar stats endpoints.
//
// # Why scrape Envoy directly (not cluster Prometheus)
//
// Envoy exposes its internal metrics at http://<pod-ip>:15090/stats/prometheus
// in Istio sidecars. This endpoint is available without any cluster-level
// Prometheus installation — it's just the sidecar itself.
//
// Scraping directly gives us:
//   - Sub-second freshness (no Prometheus scrape interval lag)
//   - Per-pod granularity (not aggregated across replicas)
//   - Admission control filter counters (upstream_rq_total, upstream_rq_2xx)
//   - Adaptive concurrency counters (gradient, limit, min_rtt)
//
// In v2, this scraper is replaced by the OTLP Collector processor which
// provides trace-level signals with causal attribution.
//
// # How pod discovery works
//
// The scraper lists pods matching the AdaptivePolicy selector in the same
// namespace. It scrapes each pod's Envoy stats endpoint and aggregates
// across all replicas to compute fleet-wide success rate and RPS.
package signal

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/chaitanyasrivastav/shedpilot/api/v1alpha1"
	"github.com/chaitanyasrivastav/shedpilot/internal/trigger"
)

const (
	// envoyStatsPort is the Istio sidecar Prometheus stats port.
	// Envoy exposes metrics at http://<pod-ip>:15090/stats/prometheus
	envoyStatsPort = 15090

	// statsPath is the Prometheus-format metrics endpoint on the sidecar.
	statsPath = "/stats/prometheus"

	// scrapeTimeout is the HTTP timeout for a single pod stats scrape.
	// Must be short — we may scrape many pods per reconcile cycle.
	scrapeTimeout = 3 * time.Second

	// Envoy metric names we care about.
	// These are emitted by Envoy's upstream cluster stats.
	metricUpstreamRqTotal = "envoy_cluster_upstream_rq_total"
	metricUpstreamRq2xx   = "envoy_cluster_upstream_rq_2xx"
	metricUpstreamRq3xx   = "envoy_cluster_upstream_rq_3xx"
	metricUpstreamRq4xx   = "envoy_cluster_upstream_rq_4xx"
	metricUpstreamRq5xx   = "envoy_cluster_upstream_rq_5xx"

	// Admission control filter metrics — emitted when the filter is active.
	// These tell us how many requests the filter is actually shedding.
	metricAdmissionRqShed  = "envoy_http_admission_control_requests_ejected"
	metricAdmissionRqTotal = "envoy_http_admission_control_requests_total"
)

// Scraper reads signals from Envoy sidecar stats endpoints.
// It maintains a counter history to compute per-interval rates (not cumulative).
type Scraper struct {
	// k8sClient for listing pods matching the policy selector.
	k8sClient client.Client

	// httpClient for scraping Envoy stats endpoints.
	// Shared and reused — not created per scrape.
	httpClient *http.Client

	// previous holds counter values from the last scrape per pod.
	// Used to compute per-interval rates from cumulative counters.
	// Key: pod name, Value: counter snapshot.
	previous map[string]counterSnapshot
}

// counterSnapshot holds raw Envoy counter values from one scrape.
type counterSnapshot struct {
	rqTotal   float64
	rq2xx     float64
	rq3xx     float64
	rq5xx     float64
	shedTotal float64
	scrapedAt time.Time
}

// NewScraper creates a Scraper. The k8s client is used for pod discovery.
func NewScraper(c client.Client) *Scraper {
	return &Scraper{
		k8sClient: c,
		httpClient: &http.Client{
			Timeout: scrapeTimeout,
			// No redirects — Envoy stats don't redirect
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		previous: make(map[string]counterSnapshot),
	}
}

// ReadSignals discovers pods matching the policy selector, scrapes each pod's
// Envoy stats endpoint, and returns aggregated fleet-wide signals.
//
// Returns safe defaults (99% success rate) if no pods are found or all
// scrapes fail — this prevents false trigger fires during deployment rollouts.
func (s *Scraper) ReadSignals(
	ctx context.Context,
	policy *v1alpha1.AdaptivePolicy,
) (trigger.Signals, error) {

	// Discover pods matching the policy selector
	pods, err := s.discoverPods(ctx, policy)
	if err != nil {
		return safeDefaults(), fmt.Errorf("pod discovery: %w", err)
	}
	if len(pods) == 0 {
		return safeDefaults(), fmt.Errorf("no running pods match selector %v in namespace %s",
			policy.Spec.Selector, policy.Namespace)
	}

	// Scrape all pods and aggregate
	var totalRqDelta float64
	var successDelta float64
	var shedDelta float64
	var scrapedPods int

	now := time.Now()

	for _, pod := range pods {
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}
		if pod.Status.PodIP == "" {
			continue
		}

		current, err := s.scrapePod(ctx, pod.Status.PodIP)
		if err != nil {
			// Non-fatal — skip this pod, use others
			continue
		}

		prev, hasPrev := s.previous[pod.Name]
		s.previous[pod.Name] = current

		if !hasPrev {
			// First scrape for this pod — no delta to compute yet
			continue
		}

		// Compute per-interval deltas from cumulative counters.
		// Envoy counters are monotonically increasing — never reset.
		// Delta = current - previous gives us the per-interval count.
		intervalSeconds := current.scrapedAt.Sub(prev.scrapedAt).Seconds()
		if intervalSeconds < 0.1 {
			// Scrapes too close together — skip to avoid division noise
			continue
		}

		rqDelta := current.rqTotal - prev.rqTotal
		if rqDelta < 0 {
			// Counter reset (pod restart) — skip this interval
			continue
		}

		// Success = 2xx + 3xx (same as our SuccessCodes default 100-399)
		successfulDelta := (current.rq2xx - prev.rq2xx) + (current.rq3xx - prev.rq3xx)
		if successfulDelta < 0 {
			successfulDelta = 0
		}

		shedDeltaPod := current.shedTotal - prev.shedTotal
		if shedDeltaPod < 0 {
			shedDeltaPod = 0
		}

		totalRqDelta += rqDelta
		successDelta += successfulDelta
		shedDelta += shedDeltaPod
		scrapedPods++
	}

	if scrapedPods == 0 || totalRqDelta < 1 {
		// No usable data — return safe defaults to avoid false triggers
		return safeDefaults(), nil
	}

	// Compute interval duration from scrape timestamps
	intervalDuration := now.Sub(s.oldestPreviousTimestamp(pods))
	if intervalDuration < time.Second {
		intervalDuration = 30 * time.Second // default interval
	}

	successRate := successDelta / totalRqDelta
	if successRate > 1.0 {
		successRate = 1.0
	}

	rps := totalRqDelta / intervalDuration.Seconds()

	return trigger.Signals{
		SuccessRate:      successRate,
		ServiceLatencyMs: 0, // v1: not available without traces; v2 OTLP fills this
		TotalLatencyMs:   0, // v1: not available from stats endpoint counters
		RPS:              rps,
		SampleCount:      int64(totalRqDelta),
		CollectedAt:      now,
	}, nil
}

// scrapePod fetches and parses Prometheus-format metrics from one pod's
// Envoy sidecar stats endpoint.
func (s *Scraper) scrapePod(ctx context.Context, podIP string) (counterSnapshot, error) {
	url := fmt.Sprintf("http://%s:%d%s", podIP, envoyStatsPort, statsPath)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return counterSnapshot{}, fmt.Errorf("building request: %w", err)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return counterSnapshot{}, fmt.Errorf("scraping %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return counterSnapshot{}, fmt.Errorf("unexpected status %d from %s", resp.StatusCode, url)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return counterSnapshot{}, fmt.Errorf("reading response: %w", err)
	}

	return parseMetrics(string(body))
}

// parseMetrics parses a Prometheus text format response and extracts the
// specific counter values we need. Only processes lines we care about —
// no full Prometheus parser needed for this small set of metrics.
//
// Example Prometheus line:
//
//	envoy_cluster_upstream_rq_total{envoy_cluster_name="..."} 12345
func parseMetrics(body string) (counterSnapshot, error) {
	snap := counterSnapshot{scrapedAt: time.Now()}

	for _, line := range strings.Split(body, "\n") {
		// Skip comments and empty lines
		if strings.HasPrefix(line, "#") || strings.TrimSpace(line) == "" {
			continue
		}

		// Extract metric name and value
		// Format: metric_name{labels} value [timestamp]
		name, value, ok := parsePromLine(line)
		if !ok {
			continue
		}

		switch {
		case strings.HasPrefix(name, metricUpstreamRqTotal):
			snap.rqTotal += value
		case strings.HasPrefix(name, metricUpstreamRq2xx):
			snap.rq2xx += value
		case strings.HasPrefix(name, metricUpstreamRq3xx):
			snap.rq3xx += value
		case strings.HasPrefix(name, metricUpstreamRq5xx):
			snap.rq5xx += value
		case strings.HasPrefix(name, metricAdmissionRqShed):
			snap.shedTotal += value
		}
	}

	return snap, nil
}

// parsePromLine parses one Prometheus text format line.
// Returns the metric name (without labels), the value, and whether parsing succeeded.
func parsePromLine(line string) (string, float64, bool) {
	// Find where labels end and value begins
	// Line format: name{labels} value  OR  name value
	lastSpace := strings.LastIndex(line, " ")
	if lastSpace < 0 {
		return "", 0, false
	}

	valueStr := strings.TrimSpace(line[lastSpace+1:])
	// Skip timestamp if present (space-separated after value)
	if idx := strings.Index(valueStr, " "); idx >= 0 {
		valueStr = valueStr[:idx]
	}

	value, err := strconv.ParseFloat(valueStr, 64)
	if err != nil {
		return "", 0, false
	}

	// Reject NaN and Inf — only accept finite numbers
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return "", 0, false
	}

	// Extract metric name — everything before { or first space
	nameAndLabels := line[:lastSpace]
	name := nameAndLabels
	if idx := strings.Index(nameAndLabels, "{"); idx >= 0 {
		name = nameAndLabels[:idx]
	}

	return strings.TrimSpace(name), value, true
}

// discoverPods lists running pods matching the policy's workload selector.
func (s *Scraper) discoverPods(
	ctx context.Context,
	policy *v1alpha1.AdaptivePolicy,
) ([]corev1.Pod, error) {
	podList := &corev1.PodList{}
	if err := s.k8sClient.List(ctx, podList,
		client.InNamespace(policy.Namespace),
		client.MatchingLabels(policy.Spec.Selector),
	); err != nil {
		return nil, err
	}
	return podList.Items, nil
}

// oldestPreviousTimestamp returns the oldest timestamp in the previous snapshot
// map for the given pods. Used to compute the actual interval duration.
func (s *Scraper) oldestPreviousTimestamp(pods []corev1.Pod) time.Time {
	oldest := time.Now()
	for _, pod := range pods {
		if snap, ok := s.previous[pod.Name]; ok {
			if snap.scrapedAt.Before(oldest) {
				oldest = snap.scrapedAt
			}
		}
	}
	return oldest
}

// safeDefaults returns signals that will never fire any trigger.
// Used when signals are unavailable to prevent false positives.
func safeDefaults() trigger.Signals {
	return trigger.Signals{
		SuccessRate: 0.99,
		RPS:         0,
		CollectedAt: time.Now(),
	}
}
