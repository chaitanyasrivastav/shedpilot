// Package v1alpha1 defines the AdaptivePolicy CRD API.
//
// # What this is
//
// AdaptivePolicy lets platform engineers define named resilience profiles and
// trigger conditions for a service running in an Istio or Cilium service mesh.
// The operator renders these into Envoy filter configuration and executes
// profile switches via RTDS in under 200ms when trigger conditions are met.
//
// # What this is NOT
//
// This is not a replacement for autoscaling. Load shedding buys time during
// the 2-5 minute gap between a traffic spike and new capacity coming online.
// If a service sheds load every day at predictable times, that is a capacity
// problem — use the scalabilityWarning status field to detect this pattern.
//
// # Architecture
//
// Two components:
//
//  1. Muscle (this operator) — watches AdaptivePolicy CRDs, renders Envoy
//     filter config, executes profile switches via RTDS. Deterministic.
//
//  2. Brain (v2, separate process) — reads OTLP traces, evaluates trigger
//     conditions, pushes profile switches autonomously. Built after v1 has
//     production users. Not part of this operator.
//
// # Envoy filters managed
//
//   - envoy.filters.http.admission_control    (outer — success-rate-based shedding)
//   - envoy.filters.http.adaptive_concurrency (inner — gradient-based concurrency)
//   - DestinationRule.connectionPool          (streaming — gRPC/WebSocket)
//
// # Known limitations
//
//   - gRPC streaming and WebSocket: admission_control and adaptive_concurrency
//     are HTTP/1.1-scoped. Use streamingProtection for long-lived connections.
//   - xDS consistency window: 1-3s across the proxy fleet during EnvoyFilter
//     updates. Inherent to xDS v3, cannot be fixed at this layer.
//   - minRTT windows: elevated 503s during baseline measurement. Configure
//     client-side retries.

// +kubebuilder:object:generate=true
// +groupName=resilience.shedpilot.io
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ─── Annotation keys ──────────────────────────────────────────────────────────

const (
	// AnnotationHumanOverride when "true" prevents the v2 brain from patching
	// any spec fields. The controller still reconciles. Envoy still enforces.
	// Set during incidents to freeze thresholds immediately.
	AnnotationHumanOverride = "shedpilot.io/human-override"

	// AnnotationLastReason records why the last spec change was made.
	// Written by the v2 brain. Read during incident review.
	AnnotationLastReason = "shedpilot.io/last-reason"

	// AnnotationLastActionTime is the RFC3339 timestamp of the last brain patch.
	AnnotationLastActionTime = "shedpilot.io/last-action-time"

	// AnnotationPreviousProfile records the profile active before the last switch.
	AnnotationPreviousProfile = "shedpilot.io/previous-profile"

	// AnnotationManagedBy identifies what last modified thresholds.
	// e.g. "adaptive-runtime/v0.3.1" or "human"
	AnnotationManagedBy = "shedpilot.io/managed-by"

	// AnnotationIncidentStart records when degradation was first detected.
	AnnotationIncidentStart = "shedpilot.io/incident-start"

	// AnnotationIncidentDuration records how long the last incident lasted.
	AnnotationIncidentDuration = "shedpilot.io/incident-duration"

	// AnnotationPeakRejectionRate records peak shed rate during last incident.
	AnnotationPeakRejectionRate = "shedpilot.io/peak-rejection-rate"

	// AnnotationBrainMode is the current operating mode of the v2 brain.
	// observe | assisted | autonomous — set by the brain, read by the controller.
	AnnotationBrainMode = "shedpilot.io/brain-mode"
)

// ─── Condition types ─────────────────────────────────────────────────────────

const (
	// ConditionReady indicates all managed resources are reconciled correctly.
	ConditionReady = "Ready"

	// ConditionDegraded indicates reconciliation errors. See message field.
	ConditionDegraded = "Degraded"

	// ConditionMeshDetected indicates whether a supported mesh was found.
	ConditionMeshDetected = "MeshDetected"

	// ConditionProfileActive indicates a non-normal profile is currently active.
	// When this condition is True, the service is under load shedding protection.
	ConditionProfileActive = "ProfileActive"

	// ConditionScalabilityWarning fires when shedding has been active for too
	// long — indicating a capacity problem, not a traffic spike.
	ConditionScalabilityWarning = "ScalabilityWarning"

	// ConditionSignalCollectionAvailable is False when the operator cannot reach
	// pod sidecar stats endpoints (port 15090). When False, triggers will not fire
	// because there are no live success-rate signals. Check NetworkPolicy rules —
	// the operator pod must be able to reach pod IPs on TCP 15090.
	ConditionSignalCollectionAvailable = "SignalCollectionAvailable"

	// ConditionFilterEffective is False when admission_control is installed but
	// has rejected zero requests across multiple intervals while the success rate
	// is below threshold and RPS exceeds minRequestsPerSecond. This indicates the
	// filter is not intercepting traffic — it is installed in the chain but not
	// processing requests. Common causes: the EnvoyFilter has not propagated yet,
	// the filter is matched to a listener context that receives no traffic, or the
	// service port is not proxied by Istio. Check with:
	//   kubectl exec <pod> -c istio-proxy -- curl -s localhost:15000/stats | grep admission_control
	ConditionFilterEffective = "FilterEffective"
)

// ─── Enum types ───────────────────────────────────────────────────────────────

// MeshBackend specifies which service mesh to target.
//
// +kubebuilder:validation:Enum=auto;istio;cilium
type MeshBackend string

const (
	// MeshBackendAuto detects mesh automatically. Detection order:
	// 1. istiod pod in istio-system → Istio
	// 2. cilium-envoy DaemonSet in kube-system → Cilium (v1.1+)
	// 3. Neither found → MeshDetected=False condition
	MeshBackendAuto   MeshBackend = "auto"
	MeshBackendIstio  MeshBackend = "istio"
	MeshBackendCilium MeshBackend = "cilium" // v1.1 — not available in initial release
)

// MeshMode specifies the Istio data plane mode.
// Only relevant when meshBackend is istio or auto with Istio detected.
//
// +kubebuilder:validation:Enum=sidecar;ambient
type MeshMode string

const (
	// MeshModeSidecar targets per-pod Envoy sidecar (classic Istio).
	// EnvoyFilters use SIDECAR_INBOUND context and workloadSelector.
	MeshModeSidecar MeshMode = "sidecar"

	// MeshModeAmbient targets waypoint proxies (Istio Ambient, 1.22+).
	// RTDS semantics on waypoints are still evolving — track upstream.
	MeshModeAmbient MeshMode = "ambient"
)

// Percentile is the latency percentile for the gradient controller baseline.
//
// p50 is strongly recommended for most services. It reflects the median
// experience and responds quickly to load changes without over-reacting
// to tail latency noise.
//
// Use p99 only for latency-critical services where tail latency specifically
// must be controlled. p99 baselines react more slowly.
//
// +kubebuilder:validation:Enum=p50;p75;p90;p99
type Percentile string

const (
	PercentileP50 Percentile = "p50"
	PercentileP75 Percentile = "p75"
	PercentileP90 Percentile = "p90"
	PercentileP99 Percentile = "p99"
)

// EnvoyValue returns the numeric percentile value for Envoy proto config.
func (p Percentile) EnvoyValue() float64 {
	switch p {
	case PercentileP75:
		return 75
	case PercentileP90:
		return 90
	case PercentileP99:
		return 99
	default:
		return 50 // p50 is the default
	}
}

// ─── Filter configuration types ───────────────────────────────────────────────

// AdaptiveConcurrencyConfig configures Envoy's adaptive_concurrency filter.
//
// The filter runs a gradient controller inside Envoy at 100ms granularity:
//
//	gradient = minRTT / sampleRTT
//	newLimit = currentLimit * gradient
//
// Requests arriving when in-flight count exceeds the limit receive 503
// immediately. No queueing occurs.
//
// Protocol support: HTTP/1.1 and unary gRPC only.
// Not supported for gRPC streaming or WebSocket — use streamingProtection.
type AdaptiveConcurrencyConfig struct {
	// Enabled toggles the filter. When false, the EnvoyFilter is deleted.
	// +kubebuilder:default=true
	Enabled bool `json:"enabled"`

	// LatencyPercentile is the latency percentile used as the ideal baseline.
	// +kubebuilder:validation:Enum=p50;p75;p90;p99
	// +kubebuilder:default=p50
	// +optional
	LatencyPercentile Percentile `json:"latencyPercentile,omitempty"`

	// LatencyBaselineInterval is how often Envoy recalculates the minimum RTT baseline.
	// Shorter = adapts faster but more measurement disruption (elevated 503s).
	// +kubebuilder:default="60s"
	// +optional
	LatencyBaselineInterval string `json:"latencyBaselineInterval,omitempty"`

	// LatencyBaselineSampleSize is requests sampled per minRTT measurement window.
	// +kubebuilder:validation:Minimum=10
	// +kubebuilder:validation:Maximum=500
	// +kubebuilder:default=50
	// +optional
	LatencyBaselineSampleSize int32 `json:"latencyBaselineSampleSize,omitempty"`

	// ConcurrencyAdjustInterval is how often the concurrency limit is recomputed.
	// +kubebuilder:default="100ms"
	// +optional
	ConcurrencyAdjustInterval string `json:"concurrencyAdjustInterval,omitempty"`

	// MaxLoadIncrease caps the multiplier between intervals. Prevents runaway scaling.
	// +kubebuilder:default="2.0"
	// +optional
	MaxLoadIncrease string `json:"maxLoadIncrease,omitempty"`

	// ConcurrencyLimit is an optional hard cap on the computed limit.
	// 0 means no hard cap.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=0
	// +optional
	ConcurrencyLimit int32 `json:"concurrencyLimit,omitempty"`

	// MeasurementJitter prevents synchronised measurement across replicas.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=50
	// +kubebuilder:default=10
	// +optional
	MeasurementJitter int32 `json:"measurementJitter,omitempty"`
}

// AdmissionControlConfig configures Envoy's admission_control filter.
//
// The filter tracks a rolling window of request outcomes. When success rate
// drops below successRateThreshold, it probabilistically rejects requests
// using Google's Client-Side Throttling formula:
//
//	P(reject) = max(0, (requests - K×successes) / (requests + 1))
//
// where K = 1/sheddingSpeed.
//
// Protocol support: HTTP/1.1 and unary gRPC only.
// Not supported for gRPC streaming or WebSocket — use streamingProtection.
type AdmissionControlConfig struct {
	// Enabled toggles the filter. When false, the EnvoyFilter is deleted.
	// +kubebuilder:default=true
	Enabled bool `json:"enabled"`

	// SuccessRateWindow is the rolling window for success/failure history.
	// +kubebuilder:default="30s"
	// +optional
	SuccessRateWindow string `json:"successRateWindow,omitempty"`

	// SuccessRateThreshold is the success rate below which shedding begins.
	// "95.0" means: start shedding when fewer than 95% of requests succeed.
	// Profiles override this field when active.
	// +kubebuilder:default="95.0"
	// +optional
	SuccessRateThreshold string `json:"successRateThreshold,omitempty"`

	// SheddingSpeed controls the shedding curve shape.
	// 1.0=linear, 1.5=moderate (recommended), 2.0=aggressive.
	// +kubebuilder:default="1.5"
	// +optional
	SheddingSpeed string `json:"sheddingSpeed,omitempty"`

	// MinRequestsPerSecond is the minimum RPS below which the filter is inactive.
	// Prevents false positives during cold start and very low traffic.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=10000
	// +kubebuilder:default=5
	// +optional
	MinRequestsPerSecond int32 `json:"minRequestsPerSecond,omitempty"`

	// MaxRejectionPercent is the hard cap on rejection rate.
	// Never reject more than this percentage regardless of degradation.
	// Non-negotiable safety valve. Do not set above 95.
	// Must be a decimal string between "0.0" and "95.0".
	// +kubebuilder:validation:Pattern=`^([0-9]|[1-8][0-9]|9[0-5])(\.\d+)?$`
	// +kubebuilder:default="80.0"
	// +optional
	MaxRejectionPercent string `json:"maxRejectionPercent,omitempty"`

	// SuccessCodes defines which HTTP status codes count as success.
	// Defaults to 1xx-3xx (100-399) if not specified.
	// gRPC status 0 (OK) is always counted as success.
	// +optional
	SuccessCodes []HTTPStatusRange `json:"successCodes,omitempty"`

	// SkipPaths is a list of URL path prefixes that bypass admission control.
	// Requests whose path starts with any entry are allowed through immediately,
	// regardless of the current success rate.
	//
	// Primary use: Kubernetes liveness and readiness probe endpoints. Without
	// skip paths, active shedding can reject health probes → kubelet marks the
	// pod unhealthy → pod is killed → cascading failure under the load you were
	// trying to shed.
	//
	// Note: Istio rewrites kubelet probes to port 15021 by default (on since 1.4).
	// SkipPaths is only needed if you disabled Istio probe rewriting, expose health
	// probes on the service port directly, or use Cilium.
	//
	// Paths are matched as prefixes: "/healthz" matches "/healthz" and "/healthz/ready".
	// Matched paths receive an immediate 200 OK from Envoy; they do not reach the service.
	// Do NOT include /metrics here — use a separate port for Prometheus scraping instead.
	//
	// Recommended: ["/healthz", "/readyz", "/livez"]
	// +optional
	SkipPaths []string `json:"skipPaths,omitempty"`
}

// HTTPStatusRange is an inclusive range of HTTP status codes.
type HTTPStatusRange struct {
	// +kubebuilder:validation:Minimum=100
	// +kubebuilder:validation:Maximum=599
	Start int32 `json:"start"`

	// +kubebuilder:validation:Minimum=100
	// +kubebuilder:validation:Maximum=599
	End int32 `json:"end"`
}

// StreamingProtectionConfig provides connection-level limits for gRPC
// streaming and WebSocket connections.
//
// Rendered as Istio DestinationRule.trafficPolicy.connectionPool.
// This is static (configured limits), not adaptive (latency-driven).
// The adaptive_concurrency and admission_control filters cannot intercept
// long-lived streaming connections — this is the correct alternative.
type StreamingProtectionConfig struct {
	// Enabled toggles streaming protection. When false, no DestinationRule
	// is rendered for this policy.
	// +kubebuilder:default=true
	Enabled bool `json:"enabled"`

	// MaxConcurrentStreams is the maximum concurrent active streams.
	// Maps to DestinationRule.connectionPool.http.http2MaxRequests.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100000
	// +kubebuilder:default=200
	// +optional
	MaxConcurrentStreams int32 `json:"maxConcurrentStreams,omitempty"`

	// StreamTimeoutSeconds is the maximum stream duration before force-close.
	// Prevents stalled streams from holding connection slots indefinitely.
	// Maps to DestinationRule.connectionPool.http.idleTimeout.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=86400
	// +kubebuilder:default=300
	// +optional
	StreamTimeoutSeconds int32 `json:"streamTimeoutSeconds,omitempty"`

	// MaxPendingRequests is the queue size before immediate 503.
	// Maps to DestinationRule.connectionPool.http.http1MaxPendingRequests.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100000
	// +kubebuilder:default=1024
	// +optional
	MaxPendingRequests int32 `json:"maxPendingRequests,omitempty"`
}

// ─── Profile system ───────────────────────────────────────────────────────────

// ProfileConfig is a named resilience configuration.
// Profiles are defined by humans in the spec and switched by triggers or
// schedules. The brain (v2) executes these profiles — it does not create them.
//
// When a profile is active, its fields override the corresponding fields in
// the top-level adaptiveConcurrency and admissionControl config.
// Fields not specified in the profile inherit from the top-level config.
type ProfileConfig struct {
	// AdmissionControl overrides for this profile.
	// Only specified fields are overridden — unspecified fields inherit.
	// +optional
	AdmissionControl *AdmissionControlOverride `json:"admissionControl,omitempty"`

	// AdaptiveConcurrency overrides for this profile.
	// Only specified fields are overridden — unspecified fields inherit.
	// +optional
	AdaptiveConcurrency *AdaptiveConcurrencyOverride `json:"adaptiveConcurrency,omitempty"`
}

// AdmissionControlOverride contains per-profile overrides for admission control.
// All fields are optional — only set what you want to change from the baseline.
type AdmissionControlOverride struct {
	// SuccessRateThreshold overrides the shedding trigger threshold.
	// Lower = more aggressive shedding. Typical degraded profile: "85.0".
	// +optional
	SuccessRateThreshold string `json:"successRateThreshold,omitempty"`

	// SheddingSpeed overrides the shedding curve. Higher = sheds faster.
	// Typical degraded profile: "2.0". Critical profile: "3.0".
	// +optional
	SheddingSpeed string `json:"sheddingSpeed,omitempty"`

	// SuccessRateWindow overrides the rolling history window.
	// Shorter = reacts faster. Typical degraded profile: "20s".
	// +optional
	SuccessRateWindow string `json:"successRateWindow,omitempty"`
}

// AdaptiveConcurrencyOverride contains per-profile overrides for concurrency.
type AdaptiveConcurrencyOverride struct {
	// LatencyPercentile overrides the gradient baseline percentile.
	// Typical degraded profile: p75 (more conservative than normal p50).
	// +kubebuilder:validation:Enum=p50;p75;p90;p99
	// +optional
	LatencyPercentile Percentile `json:"latencyPercentile,omitempty"`
}

// ─── Trigger system ───────────────────────────────────────────────────────────

// TriggerConfig defines a condition that switches the active profile.
//
// Triggers are evaluated by the controller (v1: against Prometheus metrics
// via a configurable scrape endpoint) or by the v2 brain (against OTLP traces
// with causal attribution). The evaluation mechanism changes between versions
// but the trigger spec is identical.
//
// Example:
//
//	name: degradation-detected
//	when:
//	  successRate: {below: 0.90, consecutiveSamples: 2}
//	switchTo: degraded
type TriggerConfig struct {
	// Name is a unique identifier for this trigger.
	// Used in status, events, and log output to identify which trigger fired.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9][a-z0-9-]*[a-z0-9]$`
	Name string `json:"name"`

	// When defines the conditions that must be met to fire this trigger.
	When TriggerCondition `json:"when"`

	// SwitchTo is the profile name to activate when conditions are met.
	// Must match a key in spec.profiles.
	// +kubebuilder:validation:MinLength=1
	SwitchTo string `json:"switchTo"`

	// FromProfiles limits this trigger to only fire when currently in one of
	// the listed profiles. Empty means fire from any profile.
	// Useful for recovery triggers: only recover if currently in degraded/critical.
	// +optional
	FromProfiles []string `json:"fromProfiles,omitempty"`

	// CooldownSeconds is the minimum seconds between consecutive firings
	// of this trigger. Prevents rapid oscillation between profiles.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=60
	// +optional
	CooldownSeconds int32 `json:"cooldownSeconds,omitempty"`
}

// TriggerCondition defines the signal conditions for a trigger.
// Multiple conditions are ANDed — all must be true to fire.
type TriggerCondition struct {
	// SuccessRate triggers based on the request success rate (0.0-1.0).
	// +optional
	SuccessRate *RateCondition `json:"successRate,omitempty"`

	// ServiceLatencyMs triggers based on the service's own processing
	// latency — excluding downstream dependency time. Requires v2 brain
	// with OTLP traces for causal attribution. In v1, this falls back to
	// total request latency from Prometheus.
	// +optional
	ServiceLatencyMs *ThresholdCondition `json:"serviceLatencyMs,omitempty"`

	// RPSAbove triggers when requests per second exceeds a threshold.
	// Useful for flash-sale pre-arming via RPS as a leading indicator.
	// +optional
	RPSAbove *int32 `json:"rpsAbove,omitempty"`
}

// RateCondition is a condition on a rate value (0.0-1.0).
// Values are strings to avoid float serialisation precision issues.
// Use decimal notation: "0.90", "0.97", "1.0".
type RateCondition struct {
	// Below triggers when the rate falls below this value.
	// Example: "0.90" fires when less than 90% of requests succeed.
	// +kubebuilder:validation:Pattern=`^(0(\.\d{1,4})?|1(\.0{1,4})?)$`
	// +optional
	Below string `json:"below,omitempty"`

	// Above triggers when the rate rises above this value.
	// Example: "0.97" fires when more than 97% of requests succeed.
	// +kubebuilder:validation:Pattern=`^(0(\.\d{1,4})?|1(\.0{1,4})?)$`
	// +optional
	Above string `json:"above,omitempty"`

	// ConsecutiveSamples is how many consecutive evaluations must meet the
	// condition before the trigger fires. Minimum 1.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=10
	// +kubebuilder:default=2
	// +optional
	ConsecutiveSamples int32 `json:"consecutiveSamples,omitempty"`
}

// ThresholdCondition is a condition on a numeric threshold value.
// Values are strings to avoid float serialisation precision issues.
// Example: "200.0" for 200ms latency threshold.
type ThresholdCondition struct {
	// Above triggers when the value exceeds this threshold.
	// Example: "500.0" triggers when latency exceeds 500ms.
	// +kubebuilder:validation:Pattern=`^\d+(\.\d+)?$`
	// +optional
	Above string `json:"above,omitempty"`

	// Below triggers when the value falls below this threshold.
	// +kubebuilder:validation:Pattern=`^\d+(\.\d+)?$`
	// +optional
	Below string `json:"below,omitempty"`

	// ConsecutiveSamples required before the trigger fires.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=10
	// +kubebuilder:default=2
	// +optional
	ConsecutiveSamples int32 `json:"consecutiveSamples,omitempty"`
}

// ─── Schedule system ─────────────────────────────────────────────────────────

// ScheduleConfig defines a time-based profile switch.
//
// Schedules are proactive — they fire before traffic arrives, not after.
// This is the correct mechanism for known events: flash sales, deployments,
// batch jobs, marketing campaigns.
//
// The schedule runs regardless of current service health. If the service is
// already in a degraded profile, the schedule switch still fires. Design
// schedules with this in mind — use fromProfiles if needed.
type ScheduleConfig struct {
	// Name is a unique identifier for this schedule.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name"`

	// Cron is the schedule expression (standard 5-field cron).
	// Example: "50 13 * * 5" = Friday 1:50 PM (10 min before a 2 PM sale).
	// Times are in UTC.
	// +kubebuilder:validation:MinLength=9
	Cron string `json:"cron"`

	// SwitchTo is the profile to activate at the scheduled time.
	// Must match a key in spec.profiles.
	// +kubebuilder:validation:MinLength=1
	SwitchTo string `json:"switchTo"`

	// FromProfiles limits this schedule to only fire when in one of the
	// listed profiles. Empty means fire from any profile.
	// +optional
	FromProfiles []string `json:"fromProfiles,omitempty"`
}

// ─── Signal configuration ─────────────────────────────────────────────────────

// SignalConfig configures how the controller reads metrics for trigger evaluation.
// In v1, this is a Prometheus scrape endpoint on the Envoy sidecar.
// In v2, this is replaced by the OTLP Collector processor.
type SignalConfig struct {
	// MetricsEndpoint is the Prometheus endpoint to query for trigger evaluation.
	// In Istio, this is typically the Envoy stats endpoint exposed at
	// http://<pod-ip>:15090/stats/prometheus — available without a cluster-level
	// Prometheus installation.
	// Defaults to the Istio sidecar stats endpoint (auto-discovered by pod IP).
	// +optional
	MetricsEndpoint string `json:"metricsEndpoint,omitempty"`

	// EvaluationIntervalSeconds is how often signals are evaluated against triggers.
	// Lower = reacts faster but uses more resources. Minimum 5s.
	// +kubebuilder:validation:Minimum=5
	// +kubebuilder:validation:Maximum=300
	// +kubebuilder:default=30
	// +optional
	EvaluationIntervalSeconds int32 `json:"evaluationIntervalSeconds,omitempty"`

	// CapacityWarningPercent is the percentage of time the service
	// must be in a non-normal profile before a ScalabilityWarning condition
	// fires. This signals a capacity problem, not a traffic spike problem.
	// 0 disables scalability warnings.
	// Example: 10 means "warn if shedding more than 10% of the time over 7 days."
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=10
	// +optional
	CapacityWarningPercent int32 `json:"capacityWarningPercent,omitempty"`

	// CapacityWarningWindowDays is the rolling window for scalability warning.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=30
	// +kubebuilder:default=7
	// +optional
	CapacityWarningWindowDays int32 `json:"capacityWarningWindowDays,omitempty"`
}

// DetectionConfig tunes the fast-poll detection loop introduced in v0.2.
//
// All fields are optional. Zero values use safe defaults (500ms poll,
// 3 consecutive breaches to fire, 4 consecutive clean scrapes to recover).
type DetectionConfig struct {
	// PollIntervalMs is how often each pod's Envoy sidecar stats endpoint
	// (:15090/stats/prometheus) is scraped, in milliseconds.
	//
	// Shorter = faster detection, marginally more in-cluster HTTP traffic.
	// 100 pods at 500ms = 200 GETs/s, each ~10KB — negligible.
	//
	// Range: [100, 10000]. Default: 500.
	// +kubebuilder:validation:Minimum=100
	// +kubebuilder:validation:Maximum=10000
	// +optional
	PollIntervalMs int32 `json:"pollIntervalMs,omitempty"`

	// ConsecutiveBreaches is how many back-to-back scrapes must all confirm
	// a trigger condition before a profile switch fires.
	//
	// Higher = more conservative, slower to react, less flap-prone.
	// Lower  = faster reaction, higher risk of reacting to transient noise.
	//
	// Range: [1, 20]. Default: 3 (1.5s at 500ms poll interval).
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=20
	// +optional
	ConsecutiveBreaches int32 `json:"consecutiveBreaches,omitempty"`

	// ConsecutiveRecoveries is how many back-to-back clean scrapes must pass
	// before switching back to normal profile after a breach.
	//
	// Intentionally higher than ConsecutiveBreaches — recovery should be
	// conservative to avoid oscillation on a borderline-healthy service.
	//
	// Range: [1, 20]. Default: 4 (2s at 500ms poll interval).
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=20
	// +optional
	ConsecutiveRecoveries int32 `json:"consecutiveRecoveries,omitempty"`
}

// ─── Main spec ────────────────────────────────────────────────────────────────

// AdaptivePolicySpec defines the desired state of an AdaptivePolicy.
type AdaptivePolicySpec struct {
	// Selector matches the pods this policy applies to.
	// Rendered as workloadSelector.labels on generated EnvoyFilter resources.
	// Must not be empty — an empty selector matches all pods in the namespace.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinProperties=1
	Selector map[string]string `json:"selector"`

	// MeshBackend selects which mesh to target. "auto" recommended.
	// +kubebuilder:validation:Enum=auto;istio;cilium
	// +kubebuilder:default=auto
	// +optional
	MeshBackend MeshBackend `json:"meshBackend,omitempty"`

	// MeshMode is the Istio data plane mode. Only relevant for Istio.
	// +kubebuilder:validation:Enum=sidecar;ambient
	// +kubebuilder:default=sidecar
	// +optional
	MeshMode MeshMode `json:"meshMode,omitempty"`

	// DryRun installs filters but configures them to never reject any requests.
	// All other behaviour (status updates, metrics, trigger evaluation) works
	// normally — you see what would have happened without any enforcement.
	//
	// This is the recommended first step for every new AdaptivePolicy:
	//  1. Deploy with dryRun: true for 2+ weeks
	//  2. Watch status.lastDecision and status.wouldHaveShedRate
	//  3. Tune profiles and triggers until they look sensible
	//  4. Set dryRun: false to enable enforcement
	//
	// +kubebuilder:default=false
	// +optional
	DryRun bool `json:"dryRun,omitempty"`

	// AdaptiveConcurrency is the baseline config for the gradient controller
	// filter. Profile overrides take precedence when a profile is active.
	// Omit to disable this filter entirely.
	// +optional
	AdaptiveConcurrency *AdaptiveConcurrencyConfig `json:"adaptiveConcurrency,omitempty"`

	// AdmissionControl is the baseline config for the success-rate shedding
	// filter. Profile overrides take precedence when a profile is active.
	// Omit to disable this filter entirely.
	// +optional
	AdmissionControl *AdmissionControlConfig `json:"admissionControl,omitempty"`

	// StreamingProtection provides connection-level limits for gRPC streaming
	// and WebSocket connections. Rendered as DestinationRule.connectionPool.
	// Omit to disable streaming protection.
	// +optional
	StreamingProtection *StreamingProtectionConfig `json:"streamingProtection,omitempty"`

	// Profiles are named resilience configurations defined by platform engineers.
	// Each profile overrides specific fields from the baseline AdmissionControl
	// and AdaptiveConcurrency config when active.
	//
	// Recommended profiles:
	//   normal:     baseline — matches top-level config
	//   degraded:   tighter thresholds for traffic spikes
	//   critical:   most aggressive shedding for severe degradation
	//   flash-sale: pre-armed for known high-traffic events
	//
	// +optional
	Profiles map[string]ProfileConfig `json:"profiles,omitempty"`

	// ActiveProfile is the currently enforced profile name.
	// Must match a key in spec.profiles, or be empty to use baseline config.
	// Changed by: triggers firing, schedules firing, or direct human patch.
	// The controller renders the merged config (baseline + active profile overrides).
	// +optional
	ActiveProfile string `json:"activeProfile,omitempty"`

	// Triggers define conditions that automatically switch the active profile.
	// Evaluated every signalConfig.evaluationIntervalSeconds against live metrics.
	// Triggers fire in order — first matching trigger wins.
	// +optional
	Triggers []TriggerConfig `json:"triggers,omitempty"`

	// Schedules define time-based profile switches for known events.
	// Proactive — fires before traffic arrives, not after.
	// +optional
	Schedules []ScheduleConfig `json:"schedules,omitempty"`

	// SignalConfig controls how the controller reads metrics for trigger evaluation.
	// +optional
	SignalConfig *SignalConfig `json:"signalConfig,omitempty"`

	// Detection tunes the fast-poll signal collection loop.
	// All fields are optional with safe defaults.
	// +optional
	Detection *DetectionConfig `json:"detection,omitempty"`
}

// ─── Status ───────────────────────────────────────────────────────────────────

// DecisionRecord is a single recorded decision — what the controller decided,
// when, why, and what happened afterwards. This is the primary 3am interface:
// one kubectl describe tells the complete story.
type DecisionRecord struct {
	// Timestamp when this decision was made.
	Timestamp metav1.Time `json:"timestamp"`

	// TriggerName is which trigger fired (empty for schedule or manual switch).
	// +optional
	TriggerName string `json:"triggerName,omitempty"`

	// ScheduleName is which schedule fired (empty for trigger or manual switch).
	// +optional
	ScheduleName string `json:"scheduleName,omitempty"`

	// ProfileBefore is the profile that was active before this switch.
	ProfileBefore string `json:"profileBefore"`

	// ProfileAfter is the profile switched to.
	ProfileAfter string `json:"profileAfter"`

	// SignalValues are the metric values that caused this decision.
	// Written in plain English for 3am readability.
	// Example: "successRate=0.882 (below 0.90 for 2 consecutive samples)"
	SignalValues string `json:"signalValues"`

	// DeliveryMethod is how the profile switch was delivered.
	// "rtds" for sub-200ms delivery, "envoyfilter" for standard xDS path.
	DeliveryMethod string `json:"deliveryMethod"`

	// DeliveryLatencyMs is how long the delivery took in milliseconds.
	// +optional
	DeliveryLatencyMs int64 `json:"deliveryLatencyMs,omitempty"`

	// Outcome is what happened after this decision (set 5 minutes later).
	// "service_recovered" | "partially_recovered" | "no_change" |
	// "over_shed" | "worsened" | "pending"
	// +optional
	Outcome string `json:"outcome,omitempty"`

	// OutcomeDetail is a human-readable explanation of the outcome.
	// Example: "success rate returned to 97.2% within 4 minutes"
	// +optional
	OutcomeDetail string `json:"outcomeDetail,omitempty"`
}

// ManagedResource is a Kubernetes resource owned by this policy.
type ManagedResource struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	Namespace  string `json:"namespace"`
}

// AdaptivePolicyStatus reflects the observed state of an AdaptivePolicy.
// Designed for 3am readability — one kubectl describe answers all questions.
type AdaptivePolicyStatus struct {
	// Conditions summarise reconciliation state using standard K8s conventions.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ObservedGeneration is the spec generation last reconciled.
	// If ObservedGeneration < metadata.generation, controller has not
	// processed the latest spec change yet.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// DetectedBackend is the mesh the controller is rendering resources for.
	// "istio" | "cilium" | "none"
	// +optional
	DetectedBackend string `json:"detectedBackend,omitempty"`

	// ActiveProfile is the profile currently being enforced.
	// Empty means baseline config is active (normal operation).
	// +optional
	ActiveProfile string `json:"activeProfile,omitempty"`

	// ProfileActiveSince is when the current profile was switched to.
	// +optional
	ProfileActiveSince *metav1.Time `json:"profileActiveSince,omitempty"`

	// ActiveFilters lists Envoy filter names currently installed.
	// Example: ["admission-control", "adaptive-concurrency", "streaming-protection"]
	// +optional
	ActiveFilters []string `json:"activeFilters,omitempty"`

	// ManagedResources lists all resources this policy has created.
	// All are owned by this AdaptivePolicy and cascade-deleted with it.
	// +optional
	ManagedResources []ManagedResource `json:"managedResources,omitempty"`

	// ShedRateNow is the approximate percentage of inbound requests currently
	// being rejected by the admission control filter.
	// "0%" = no shedding. "unknown" = metrics unavailable.
	// Approximation only — do not alert on this field.
	// +optional
	ShedRateNow string `json:"shedRateNow,omitempty"`

	// WouldHaveShedRate is what the shed rate would be in dryRun=false.
	// Only populated when dryRun=true. Shows what enforcement would look like
	// before you enable it. This is the primary dryRun observation tool.
	// +optional
	WouldHaveShedRate string `json:"wouldHaveShedRate,omitempty"`

	// LastDecision is the most recent profile switch decision.
	// Contains full reasoning, signal values, delivery latency, and outcome.
	// This is the first thing to read during an incident.
	// +optional
	LastDecision *DecisionRecord `json:"lastDecision,omitempty"`

	// DecisionHistory contains the last 10 decisions in reverse chronological order.
	// Provides full incident context without needing to query logs.
	// +optional
	DecisionHistory []DecisionRecord `json:"decisionHistory,omitempty"`

	// NextTriggerEvaluation is when the next trigger evaluation will run.
	// +optional
	NextTriggerEvaluation *metav1.Time `json:"nextTriggerEvaluation,omitempty"`

	// ConsecutiveBadSamples tracks how many consecutive evaluations have met
	// degradation conditions. Counts toward consecutiveSamples in triggers.
	// Useful for understanding how close to a trigger firing the service is.
	// +optional
	ConsecutiveBadSamples int32 `json:"consecutiveBadSamples,omitempty"`

	// ScalabilityWarning is set when the service has been shedding load for
	// too long — indicating a capacity problem rather than a traffic spike.
	// When true: consider permanently scaling up the service rather than
	// relying on load shedding as an ongoing strategy.
	// +optional
	ScalabilityWarning bool `json:"scalabilityWarning,omitempty"`

	// ScalabilityWarningDetail explains why the scalability warning fired.
	// Example: "service was in non-normal profile for 18% of the last 7 days
	// (threshold: 10%). This suggests sustained capacity insufficiency."
	// +optional
	ScalabilityWarningDetail string `json:"scalabilityWarningDetail,omitempty"`

	// LastReconcileTime is when the controller last successfully reconciled.
	// Used to detect stale/stuck reconciliation.
	// +optional
	LastReconcileTime *metav1.Time `json:"lastReconcileTime,omitempty"`

	// RTDSConnected indicates whether the RTDS gRPC stream to Istiod is active.
	// When false, profile switches fall back to the EnvoyFilter path (slower).
	// +optional
	RTDSConnected bool `json:"rtdsConnected,omitempty"`
}

// ─── Root type ────────────────────────────────────────────────────────────────

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=adp,scope=Namespaced,categories=shedpilot
// +kubebuilder:printcolumn:name="Backend",type=string,JSONPath=`.status.detectedBackend`
// +kubebuilder:printcolumn:name="Profile",type=string,JSONPath=`.status.activeProfile`
// +kubebuilder:printcolumn:name="DryRun",type=boolean,JSONPath=`.spec.dryRun`
// +kubebuilder:printcolumn:name="Shed",type=string,JSONPath=`.status.shedRateNow`
// +kubebuilder:printcolumn:name="RTDS",type=boolean,JSONPath=`.status.rtdsConnected`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// AdaptivePolicy configures adaptive load management for a workload in an
// Istio or Cilium service mesh.
//
// Quick start — the simplest useful policy:
//
//	spec:
//	  selector: {app: payments}
//	  dryRun: true                    # observe for 2 weeks first
//	  adaptiveConcurrency:
//	    enabled: true
//	  admissionControl:
//	    enabled: true
//
// With profiles and triggers:
//
//	spec:
//	  selector: {app: payments}
//	  adaptiveConcurrency: {enabled: true}
//	  admissionControl: {enabled: true, successRateThreshold: "95.0"}
//	  profiles:
//	    degraded:
//	      admissionControl: {successRateThreshold: "85.0", sheddingSpeed: "2.0"}
//	  triggers:
//	  - name: degradation-detected
//	    when: {successRate: {below: 0.90, consecutiveSamples: 2}}
//	    switchTo: degraded
//
// Escape hatches:
//
//	# Freeze brain patches (keep enforcement running):
//	kubectl annotate adaptivepolicy payments shedpilot.io/human-override=true
//
//	# Observe only (keep filters installed, disable rejection):
//	kubectl patch adaptivepolicy payments --type merge -p '{"spec":{"dryRun":true}}'
//
//	# Remove everything (cascade deletes all EnvoyFilters):
//	kubectl delete adaptivepolicy payments
type AdaptivePolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AdaptivePolicySpec   `json:"spec,omitempty"`
	Status AdaptivePolicyStatus `json:"status,omitempty"`
}

// ─── Predicate methods ────────────────────────────────────────────────────────

// IsHumanOverrideEnabled returns true if the v2 brain is frozen.
func (ap *AdaptivePolicy) IsHumanOverrideEnabled() bool {
	return ap.Annotations[AnnotationHumanOverride] == "true"
}

// IsDryRun returns true if the policy is in observe-only mode.
func (ap *AdaptivePolicy) IsDryRun() bool { return ap.Spec.DryRun }

// HasAdaptiveConcurrency returns true if the filter is configured and enabled.
func (ap *AdaptivePolicy) HasAdaptiveConcurrency() bool {
	return ap.Spec.AdaptiveConcurrency != nil && ap.Spec.AdaptiveConcurrency.Enabled
}

// HasAdmissionControl returns true if the filter is configured and enabled.
func (ap *AdaptivePolicy) HasAdmissionControl() bool {
	return ap.Spec.AdmissionControl != nil && ap.Spec.AdmissionControl.Enabled
}

// HasStreamingProtection returns true if streaming protection is configured.
func (ap *AdaptivePolicy) HasStreamingProtection() bool {
	return ap.Spec.StreamingProtection != nil && ap.Spec.StreamingProtection.Enabled
}

// HasProfiles returns true if any profiles are defined.
func (ap *AdaptivePolicy) HasProfiles() bool { return len(ap.Spec.Profiles) > 0 }

// HasTriggers returns true if any triggers are defined.
func (ap *AdaptivePolicy) HasTriggers() bool { return len(ap.Spec.Triggers) > 0 }

// HasSchedules returns true if any schedules are defined.
func (ap *AdaptivePolicy) HasSchedules() bool { return len(ap.Spec.Schedules) > 0 }

// EffectiveMeshMode returns the mesh mode, defaulting to sidecar.
func (ap *AdaptivePolicy) EffectiveMeshMode() MeshMode {
	if ap.Spec.MeshMode == "" {
		return MeshModeSidecar
	}
	return ap.Spec.MeshMode
}

// EffectiveMeshBackend returns the mesh backend, defaulting to auto.
func (ap *AdaptivePolicy) EffectiveMeshBackend() MeshBackend {
	if ap.Spec.MeshBackend == "" {
		return MeshBackendAuto
	}
	return ap.Spec.MeshBackend
}

// ActiveProfileConfig returns the merged config for the currently active profile.
// Returns the baseline config if no profile is active or the active profile
// doesn't exist in the profiles map.
func (ap *AdaptivePolicy) ActiveProfileConfig() (ProfileConfig, bool) {
	if ap.Spec.ActiveProfile == "" {
		return ProfileConfig{}, false
	}
	profile, ok := ap.Spec.Profiles[ap.Spec.ActiveProfile]
	return profile, ok
}

// EffectiveSuccessRateThreshold returns the success rate threshold currently
// in effect — considering active profile overrides. Falls back to baseline,
// then to "95.0" if neither is configured.
func (ap *AdaptivePolicy) EffectiveSuccessRateThreshold() string {
	if profile, ok := ap.ActiveProfileConfig(); ok {
		if profile.AdmissionControl != nil && profile.AdmissionControl.SuccessRateThreshold != "" {
			return profile.AdmissionControl.SuccessRateThreshold
		}
	}
	if ap.Spec.AdmissionControl != nil && ap.Spec.AdmissionControl.SuccessRateThreshold != "" {
		return ap.Spec.AdmissionControl.SuccessRateThreshold
	}
	return "95.0"
}

// EffectiveSheddingSpeed returns the sheddingSpeed value currently in effect.
func (ap *AdaptivePolicy) EffectiveSheddingSpeed() string {
	if profile, ok := ap.ActiveProfileConfig(); ok {
		if profile.AdmissionControl != nil && profile.AdmissionControl.SheddingSpeed != "" {
			return profile.AdmissionControl.SheddingSpeed
		}
	}
	if ap.Spec.AdmissionControl != nil && ap.Spec.AdmissionControl.SheddingSpeed != "" {
		return ap.Spec.AdmissionControl.SheddingSpeed
	}
	return "1.5"
}

// EffectiveSuccessRateWindow returns the sampling window currently in effect.
func (ap *AdaptivePolicy) EffectiveSuccessRateWindow() string {
	if profile, ok := ap.ActiveProfileConfig(); ok {
		if profile.AdmissionControl != nil && profile.AdmissionControl.SuccessRateWindow != "" {
			return profile.AdmissionControl.SuccessRateWindow
		}
	}
	if ap.Spec.AdmissionControl != nil && ap.Spec.AdmissionControl.SuccessRateWindow != "" {
		return ap.Spec.AdmissionControl.SuccessRateWindow
	}
	return "30s"
}

// EffectiveLatencyPercentile returns the target percentile currently in effect.
func (ap *AdaptivePolicy) EffectiveLatencyPercentile() Percentile {
	if profile, ok := ap.ActiveProfileConfig(); ok {
		if profile.AdaptiveConcurrency != nil && profile.AdaptiveConcurrency.LatencyPercentile != "" {
			return profile.AdaptiveConcurrency.LatencyPercentile
		}
	}
	if ap.Spec.AdaptiveConcurrency != nil && ap.Spec.AdaptiveConcurrency.LatencyPercentile != "" {
		return ap.Spec.AdaptiveConcurrency.LatencyPercentile
	}
	return PercentileP50
}

// +kubebuilder:object:root=true

// AdaptivePolicyList contains a list of AdaptivePolicy resources.
type AdaptivePolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AdaptivePolicy `json:"items"`
}
