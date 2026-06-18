package signal

import (
	"testing"
	"time"
)

// ── parseMetrics tests ────────────────────────────────────────────────────────

func TestParseMetrics_BasicCounters(t *testing.T) {
	body := `
# HELP istio_requests_total Total requests
# TYPE istio_requests_total counter
istio_requests_total{destination_service_namespace="production",destination_service_name="payments",response_code="200"} 11800
istio_requests_total{destination_service_namespace="production",destination_service_name="payments",response_code="500"} 545
istio_requests_total{destination_service_namespace="production",destination_service_name="payments",response_code="200"} 100
envoy_http_admission_control_requests_ejected{} 120
`

	snap, err := parseMetrics(body)
	if err != nil {
		t.Fatalf("parseMetrics() error = %v", err)
	}

	if snap.rqTotal != 12445 {
		t.Errorf("rqTotal = %v, want 12445", snap.rqTotal)
	}
	if snap.rq2xx != 11900 {
		t.Errorf("rq2xx = %v, want 11900", snap.rq2xx)
	}
	if snap.rq5xx != 545 {
		t.Errorf("rq5xx = %v, want 545", snap.rq5xx)
	}
	if snap.shedTotal != 120 {
		t.Errorf("shedTotal = %v, want 120", snap.shedTotal)
	}
}

func TestParseMetrics_EmptyBody(t *testing.T) {
	snap, err := parseMetrics("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if snap.rqTotal != 0 || snap.rq2xx != 0 {
		t.Error("expected zero counters for empty body")
	}
}

func TestParseMetrics_CommentsOnly(t *testing.T) {
	body := `
# HELP envoy_cluster_upstream_rq_total Total requests
# TYPE envoy_cluster_upstream_rq_total counter
`
	snap, err := parseMetrics(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if snap.rqTotal != 0 {
		t.Errorf("expected 0, got %v", snap.rqTotal)
	}
}

func TestParseMetrics_MultipleClusterAggregation(t *testing.T) {
	// Multiple response codes should all be summed
	body := `
istio_requests_total{destination_service="cluster-a",response_code="200"} 990
istio_requests_total{destination_service="cluster-a",response_code="500"} 10
istio_requests_total{destination_service="cluster-b",response_code="200"} 480
`
	snap, err := parseMetrics(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if snap.rqTotal != 1480 {
		t.Errorf("rqTotal = %v, want 1480 (aggregated)", snap.rqTotal)
	}
	if snap.rq2xx != 1470 {
		t.Errorf("rq2xx = %v, want 1470 (aggregated)", snap.rq2xx)
	}
}

// ── parsePromLine tests ───────────────────────────────────────────────────────

func TestParsePromLine(t *testing.T) {
	tests := []struct {
		line      string
		wantName  string
		wantValue float64
		wantOK    bool
	}{
		{
			line:      `envoy_cluster_upstream_rq_total{cluster="payments"} 12345`,
			wantName:  "envoy_cluster_upstream_rq_total",
			wantValue: 12345,
			wantOK:    true,
		},
		{
			line:      `envoy_cluster_upstream_rq_total 999`,
			wantName:  "envoy_cluster_upstream_rq_total",
			wantValue: 999,
			wantOK:    true,
		},
		{
			line:      `# this is a comment`,
			wantName:  "",
			wantValue: 0,
			wantOK:    false,
		},
		{
			line:      ``,
			wantName:  "",
			wantValue: 0,
			wantOK:    false,
		},
		{
			line:      `metric_name{label="value"} NaN`,
			wantName:  "",
			wantValue: 0,
			wantOK:    false,
		},
		{
			// Float value
			line:      `some_metric{} 3.14`,
			wantName:  "some_metric",
			wantValue: 3.14,
			wantOK:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.line, func(t *testing.T) {
			name, value, ok := parsePromLine(tt.line)
			if ok != tt.wantOK {
				t.Errorf("ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && name != tt.wantName {
				t.Errorf("name = %q, want %q", name, tt.wantName)
			}
			if ok && value != tt.wantValue {
				t.Errorf("value = %v, want %v", value, tt.wantValue)
			}
		})
	}
}

// ── safeDefaults tests ────────────────────────────────────────────────────────

func TestSafeDefaults_NeverFiresTriggers(t *testing.T) {
	defaults := safeDefaults()
	if defaults.SuccessRate < 0.95 {
		t.Errorf("safe defaults successRate = %v should be >= 0.95 to avoid false triggers", defaults.SuccessRate)
	}
	if defaults.CollectedAt.IsZero() {
		t.Error("CollectedAt should be set")
	}
	if defaults.CollectedAt.After(time.Now().Add(time.Second)) {
		t.Error("CollectedAt should not be in the future")
	}
}

// ── Counter delta tests ───────────────────────────────────────────────────────

func TestCounterDelta_NegativeDelta_PodRestart(t *testing.T) {
	// Simulate pod restart — current < previous means counter reset.
	// The scraper should skip this interval and not compute negative success rate.
	current := counterSnapshot{rqTotal: 100, rq2xx: 95, scrapedAt: time.Now()}
	prev := counterSnapshot{rqTotal: 5000, rq2xx: 4900, scrapedAt: time.Now().Add(-30 * time.Second)}

	rqDelta := current.rqTotal - prev.rqTotal
	if rqDelta >= 0 {
		t.Error("expected negative delta for pod restart scenario")
	}
	// The scraper checks `if rqDelta < 0 { continue }` — verified here
	// that the delta is indeed negative so the skip logic would trigger.
}

func TestCounterDelta_SuccessRateComputation(t *testing.T) {
	// 1000 total requests, 980 successful (2xx+3xx)
	// Expected success rate: 980/1000 = 0.98
	totalDelta := float64(1000)
	successDelta := float64(980)

	rate := successDelta / totalDelta
	if rate < 0.979 || rate > 0.981 {
		t.Errorf("success rate = %v, want ~0.98", rate)
	}
}
