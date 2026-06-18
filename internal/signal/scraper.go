// Package signal reads live metrics from Envoy sidecar stats endpoints.
//
// # Why scrape Envoy directly (not cluster Prometheus)
//
// Envoy exposes its internal metrics at http://<pod-ip>:15090/stats/prometheus
// in Istio sidecars. This endpoint is available without any cluster-level
// Prometheus installation — it's just the sidecar itself.
//
// In v2, this scraper is replaced by the OTLP Collector processor which
// provides trace-level signals with causal attribution.
package signal

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/chaitanyasrivastav/shedpilot/api/v1alpha1"
	"github.com/chaitanyasrivastav/shedpilot/internal/trigger"
)

const (
	envoyStatsPort    = 15090
	statsPath         = "/stats/prometheus"
	scrapeTimeout     = 3 * time.Second
	maxStatsBodyBytes = 4 * 1024 * 1024 // 4 MiB — OOM guard

	// metricUpstreamRqTotal    = "envoy_cluster_upstream_rq_total"
	// metricUpstreamRq2xx      = "envoy_cluster_upstream_rq_2xx"
	// metricUpstreamRq3xx      = "envoy_cluster_upstream_rq_3xx"
	metricIstioRequestsTotal = "istio_requests_total"
	// 4xx IS tracked — whether it counts as success depends on policy.successCodes.
	// Default successCodes is 100-399, so 4xx is a failure by default.
	// But validation APIs that return 400 for bad input have a naturally low
	// "success rate" under the hardcoded 2xx+3xx formula even when healthy.
	// Tracking 4xx separately and applying successCodes config fixes this.
	// metricUpstreamRq4xx   = "envoy_cluster_upstream_rq_4xx"
	// metricUpstreamRq5xx   = "envoy_cluster_upstream_rq_5xx"
	metricAdmissionRqShed = "envoy_http_admission_control_requests_ejected"
)

// Scraper reads signals from Envoy sidecar stats endpoints.
// All exported methods are safe for concurrent use.
type Scraper struct {
	k8sClient  client.Client
	httpClient *http.Client
	mu         sync.Mutex
	previous   map[string]counterSnapshot
}

type counterSnapshot struct {
	rqTotal   float64
	rq2xx     float64
	rq3xx     float64
	rq4xx     float64 // tracked for successCodes-aware calculation
	rq5xx     float64
	shedTotal float64
	scrapedAt time.Time
}

func NewScraper(c client.Client) *Scraper {
	return &Scraper{
		k8sClient: c,
		httpClient: &http.Client{
			Timeout:       scrapeTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		previous: make(map[string]counterSnapshot),
	}
}

// ReadSignals discovers pods, scrapes Envoy stats, returns fleet-wide signals.
//
// Success rate respects policy.Spec.AdmissionControl.SuccessCodes — it measures
// the same thing Envoy's admission_control filter measures. Without this, a
// service with naturally high 4xx traffic (validation APIs) reads artificially
// low success rate and triggers false profile switches.
func (s *Scraper) ReadSignals(
	ctx context.Context,
	policy *v1alpha1.AdaptivePolicy,
) (trigger.Signals, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	pods, err := s.discoverPods(ctx, policy)
	if err != nil {
		return safeDefaults(), fmt.Errorf("pod discovery: %w", err)
	}
	if len(pods) == 0 {
		return safeDefaults(), fmt.Errorf("no running pods match selector %v in namespace %s",
			policy.Spec.Selector, policy.Namespace)
	}

	// Use policy successCodes config — match what Envoy's filter counts.
	// Default 100-399 if not configured.
	successRanges := []v1alpha1.HTTPStatusRange{{Start: 100, End: 399}}
	if policy.Spec.AdmissionControl != nil && len(policy.Spec.AdmissionControl.SuccessCodes) > 0 {
		successRanges = policy.Spec.AdmissionControl.SuccessCodes
	}

	var totalRqDelta, successDelta, shedDelta float64
	var scrapedPods int
	now := time.Now()

	for _, pod := range pods {
		if pod.Status.Phase != corev1.PodRunning || pod.Status.PodIP == "" {
			continue
		}

		current, err := s.scrapePod(ctx, pod.Status.PodIP)
		if err != nil {
			continue
		}
		// TEMPORARY DEBUG — remove after fixing
		log.FromContext(ctx).Info("pod scrape",
			"pod", pod.Name,
			"rqTotal", current.rqTotal,
			"rq2xx", current.rq2xx,
			"rq5xx", current.rq5xx,
			"shedTotal", current.shedTotal,
		)

		prev, hasPrev := s.previous[pod.Name]
		s.previous[pod.Name] = current
		// ADD THIS:
		log.FromContext(ctx).Info("prev check",
			"pod", pod.Name,
			"hasPrev", hasPrev,
			"prevTotal", prev.rqTotal,
			"currTotal", current.rqTotal,
		)

		if !hasPrev {
			continue
		}

		intervalSeconds := current.scrapedAt.Sub(prev.scrapedAt).Seconds()
		if intervalSeconds < 0.1 {
			continue
		}

		rqDelta := current.rqTotal - prev.rqTotal
		if rqDelta < 0 {
			continue // counter reset on pod restart
		}
		// ADD THIS:
		log.FromContext(ctx).Info("delta",
			"pod", pod.Name,
			"prevTotal", prev.rqTotal,
			"currTotal", current.rqTotal,
			"delta", rqDelta,
			"intervalSecs", current.scrapedAt.Sub(prev.scrapedAt).Seconds(),
		)

		// Compute success delta using per-class deltas and successCodes config.
		// This matches what admission_control filter measures internally.
		classDelta := map[int]float64{
			2: nonNegDelta(current.rq2xx, prev.rq2xx),
			3: nonNegDelta(current.rq3xx, prev.rq3xx),
			4: nonNegDelta(current.rq4xx, prev.rq4xx),
			5: nonNegDelta(current.rq5xx, prev.rq5xx),
		}
		var podSuccessDelta float64
		for class, delta := range classDelta {
			if statusClassInSuccessRanges(class, successRanges) {
				podSuccessDelta += delta
			}
		}

		totalRqDelta += rqDelta
		successDelta += podSuccessDelta
		shedDelta += nonNegDelta(current.shedTotal, prev.shedTotal)
		scrapedPods++
	}

	// Prune stale pod entries
	activePodNames := make(map[string]struct{}, len(pods))
	for _, pod := range pods {
		activePodNames[pod.Name] = struct{}{}
	}
	for name := range s.previous {
		if _, alive := activePodNames[name]; !alive {
			delete(s.previous, name)
		}
	}

	if scrapedPods == 0 || totalRqDelta < 1 {
		return safeDefaults(), nil
	}

	intervalDuration := now.Sub(s.oldestPreviousTimestamp(pods, now))
	if intervalDuration < time.Second {
		intervalDuration = 30 * time.Second
	}

	successRate := successDelta / totalRqDelta
	if successRate > 1.0 {
		successRate = 1.0
	}

	return trigger.Signals{
		SuccessRate:      successRate,
		ServiceLatencyMs: 0, // v2 OTel fills this
		TotalLatencyMs:   0,
		RPS:              totalRqDelta / intervalDuration.Seconds(),
		SampleCount:      int64(totalRqDelta),
		CollectedAt:      now,
	}, nil
}

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
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxStatsBodyBytes))
	if err != nil {
		return counterSnapshot{}, fmt.Errorf("reading response: %w", err)
	}
	return parseMetrics(string(body))
}

func parseMetrics(body string) (counterSnapshot, error) {
	snap := counterSnapshot{scrapedAt: time.Now()}
	var istioLines int
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "istio_requests_total{") {
			istioLines++
			fmt.Printf("DEBUG istio line: %q\n", line[:min(len(line), 100)])
		}
		if strings.HasPrefix(line, "#") || strings.TrimSpace(line) == "" {
			continue
		}

		if strings.HasPrefix(line, metricAdmissionRqShed) {
			if _, value, ok := parsePromLine(line); ok {
				snap.shedTotal += value
			}
			continue
		}

		// istio_requests_total{...,response_code="200",...} 154
		if !strings.HasPrefix(line, "istio_requests_total{") {
			continue
		}
		_, value, ok := parsePromLine(line)
		if !ok {
			continue
		}
		code := extractLabel(line, "response_code")
		class := httpClass(code)
		switch class {
		case 2:
			snap.rq2xx += value
			snap.rqTotal += value
		case 3:
			snap.rq3xx += value
			snap.rqTotal += value
		case 4:
			snap.rq4xx += value
			snap.rqTotal += value
		case 5:
			snap.rq5xx += value
			snap.rqTotal += value
		}
	}
	fmt.Printf("DEBUG parseMetrics: istioLines=%d rqTotal=%.0f\n", istioLines, snap.rqTotal)
	return snap, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// extractLabel pulls a label value from a Prometheus line.
// e.g. extractLabel(`metric{foo="bar",baz="qux"} 1`, "foo") → "bar"
func extractLabel(line, key string) string {
	search := key + `="`
	idx := strings.Index(line, search)
	if idx < 0 {
		return ""
	}
	start := idx + len(search)
	end := strings.Index(line[start:], `"`)
	if end < 0 {
		return ""
	}
	return line[start : start+end]
}

// httpClass returns the HTTP status class (2, 3, 4, 5) from a status code string.
func httpClass(code string) int {
	if len(code) == 0 {
		return 0
	}
	switch code[0] {
	case '2':
		return 2
	case '3':
		return 3
	case '4':
		return 4
	case '5':
		return 5
	}
	return 0
}

func parsePromLine(line string) (string, float64, bool) {
	var splitIdx int
	if braceIdx := strings.Index(line, "{"); braceIdx >= 0 {
		closeIdx := strings.LastIndex(line, "}")
		if closeIdx < braceIdx {
			return "", 0, false
		}
		splitIdx = closeIdx + 1
	} else {
		splitIdx = strings.Index(line, " ")
		if splitIdx < 0 {
			return "", 0, false
		}
	}
	if splitIdx >= len(line) {
		return "", 0, false
	}
	rest := strings.TrimSpace(line[splitIdx:])
	valueStr := rest
	if spaceIdx := strings.Index(rest, " "); spaceIdx >= 0 {
		valueStr = rest[:spaceIdx]
	}
	value, err := strconv.ParseFloat(valueStr, 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return "", 0, false
	}
	name := line[:splitIdx]
	if idx := strings.Index(name, "{"); idx >= 0 {
		name = name[:idx]
	}
	return strings.TrimSpace(name), value, true
}

func (s *Scraper) discoverPods(ctx context.Context, policy *v1alpha1.AdaptivePolicy) ([]corev1.Pod, error) {
	podList := &corev1.PodList{}
	if err := s.k8sClient.List(ctx, podList,
		client.InNamespace(policy.Namespace),
		client.MatchingLabels(policy.Spec.Selector),
	); err != nil {
		return nil, err
	}
	return podList.Items, nil
}

func (s *Scraper) oldestPreviousTimestamp(pods []corev1.Pod, ceiling time.Time) time.Time {
	oldest := ceiling
	for _, pod := range pods {
		if snap, ok := s.previous[pod.Name]; ok {
			if snap.scrapedAt.Before(oldest) {
				oldest = snap.scrapedAt
			}
		}
	}
	return oldest
}

// statusClassInSuccessRanges returns true if the HTTP status class
// (2=2xx, 3=3xx, 4=4xx, 5=5xx) falls within any configured success range.
// Uses the class representative (200, 300, 400, 500) for range matching.
func statusClassInSuccessRanges(statusClass int, ranges []v1alpha1.HTTPStatusRange) bool {
	representative := int32(statusClass * 100)
	for _, r := range ranges {
		if representative >= r.Start && representative <= r.End {
			return true
		}
	}
	return false
}

func nonNegDelta(current, prev float64) float64 {
	if d := current - prev; d > 0 {
		return d
	}
	return 0
}

func safeDefaults() trigger.Signals {
	return trigger.Signals{SuccessRate: 0.99, RPS: 0, CollectedAt: time.Now()}
}
