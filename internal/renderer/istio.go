package renderer

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/chaitanyasrivastav/shedpilot/api/v1alpha1"
)

const (
	// Istio API versions
	istioNetworkingAPIVersion = "networking.istio.io/v1alpha3"

	// Envoy filter names — these are the exact strings Envoy expects
	filterNameAdmissionControl    = "envoy.filters.http.admission_control"
	filterNameAdaptiveConcurrency = "envoy.filters.http.adaptive_concurrency"
	filterNameHTTPConnManager     = "envoy.filters.network.http_connection_manager"

	// Envoy proto type URLs for typed_config
	typeURLAdmissionControl    = "type.googleapis.com/envoy.extensions.filters.http.admission_control.v3.AdmissionControl"
	typeURLAdaptiveConcurrency = "type.googleapis.com/envoy.extensions.filters.http.adaptive_concurrency.v3.AdaptiveConcurrency"

	// RTDS runtime keys — these match the runtime_key fields in the filter config.
	// Changing these values via RTDS updates filter behaviour without re-rendering.
	rtdsKeyAdmissionEnabled    = "admission_control.enabled"
	rtdsKeyAdmissionThreshold  = "admission_control.sr_threshold"
	rtdsKeyAdmissionAggression = "admission_control.aggression"
	rtdsKeyConcurrencyEnabled  = "adaptive_concurrency.enabled"

	// Label keys on generated resources
	labelManagedBy    = "app.kubernetes.io/managed-by"
	labelManagedByVal = "shedpilot"
	labelPolicy       = "resilience.shedpilot.io/policy"
	labelPolicyNS     = "resilience.shedpilot.io/policy-namespace"
)

// IstioRenderer generates EnvoyFilter and DestinationRule resources for Istio.
// It supports both sidecar and ambient (waypoint) mesh modes.
//
// Resource ordering matters for Envoy filter chain:
//  1. admission_control — outer layer, success-rate-based shedding
//  2. adaptive_concurrency — inner layer, gradient-based limiting
//  3. DestinationRule — streaming protection via connectionPool
type IstioRenderer struct{}

// NewIstioRenderer creates a new IstioRenderer.
func NewIstioRenderer() *IstioRenderer {
	return &IstioRenderer{}
}

func (r *IstioRenderer) Name() string { return "istio" }

// Detect returns true if Istiod is running in the cluster.
// Checks for the istiod pod in the istio-system namespace.
func (r *IstioRenderer) Detect(ctx context.Context, c client.Client) (bool, error) {
	podList := &corev1.PodList{}
	if err := c.List(ctx, podList,
		client.InNamespace("istio-system"),
		client.MatchingLabels{"app": "istiod"},
	); err != nil {
		// If we can't list pods (e.g. RBAC), assume not present
		return false, nil
	}
	for _, pod := range podList.Items {
		if pod.Status.Phase == corev1.PodRunning {
			return true, nil
		}
	}
	return false, nil
}

// RTDSSupported returns true — Istio supports RTDS for runtime key updates.
func (r *IstioRenderer) RTDSSupported() bool { return true }

// Render generates EnvoyFilter and DestinationRule resources for the policy.
func (r *IstioRenderer) Render(policy *v1alpha1.AdaptivePolicy) (*RenderResult, error) {
	result := &RenderResult{
		RTDSLayers: make(map[string]map[string]interface{}),
	}

	context := r.listenerContext(policy.EffectiveMeshMode())

	// Layer 1 — admission control (outer, applied first)
	if policy.HasAdmissionControl() {
		ef, rtds, err := r.renderAdmissionControl(policy, context)
		if err != nil {
			return nil, fmt.Errorf("rendering admission control: %w", err)
		}
		result.Resources = append(result.Resources, ef)
		result.ActiveFilters = append(result.ActiveFilters, "admission-control")
		result.ManagedResourceRefs = append(result.ManagedResourceRefs, v1alpha1.ManagedResource{
			APIVersion: istioNetworkingAPIVersion,
			Kind:       "EnvoyFilter",
			Name:       admissionControlName(policy),
			Namespace:  policy.Namespace,
		})
		layerName := fmt.Sprintf("shedpilot-%s-admission-control", policy.Name)
		result.RTDSLayers[layerName] = rtds
	}

	// Layer 2 — adaptive concurrency (inner, applied second)
	if policy.HasAdaptiveConcurrency() {
		ef, rtds, err := r.renderAdaptiveConcurrency(policy, context)
		if err != nil {
			return nil, fmt.Errorf("rendering adaptive concurrency: %w", err)
		}
		result.Resources = append(result.Resources, ef)
		result.ActiveFilters = append(result.ActiveFilters, "adaptive-concurrency")
		result.ManagedResourceRefs = append(result.ManagedResourceRefs, v1alpha1.ManagedResource{
			APIVersion: istioNetworkingAPIVersion,
			Kind:       "EnvoyFilter",
			Name:       adaptiveConcurrencyName(policy),
			Namespace:  policy.Namespace,
		})
		layerName := fmt.Sprintf("shedpilot-%s-adaptive-concurrency", policy.Name)
		result.RTDSLayers[layerName] = rtds
	}

	// Layer 0 — bootstrap stats-flush patch (rendered whenever any filter is active).
	// Without this Envoy buffers stats for up to 5s; with it every GET to
	// :15090/stats/prometheus bypasses the internal flush timer and returns fresh data.
	if policy.HasAdmissionControl() || policy.HasAdaptiveConcurrency() {
		ef := r.renderStatsFlush(policy)
		result.Resources = append(result.Resources, ef)
		result.ManagedResourceRefs = append(result.ManagedResourceRefs, v1alpha1.ManagedResource{
			APIVersion: istioNetworkingAPIVersion,
			Kind:       "EnvoyFilter",
			Name:       statsFlushName(policy),
			Namespace:  policy.Namespace,
		})
	}

	// Layer 3 — streaming protection (DestinationRule.connectionPool)
	if policy.HasStreamingProtection() {
		dr, err := r.renderStreamingProtection(policy)
		if err != nil {
			return nil, fmt.Errorf("rendering streaming protection: %w", err)
		}
		result.Resources = append(result.Resources, dr)
		result.ActiveFilters = append(result.ActiveFilters, "streaming-protection")
		result.ManagedResourceRefs = append(result.ManagedResourceRefs, v1alpha1.ManagedResource{
			APIVersion: istioNetworkingAPIVersion,
			Kind:       "DestinationRule",
			Name:       streamingProtectionName(policy),
			Namespace:  policy.Namespace,
		})
	}

	return result, nil
}

// renderAdmissionControl generates the EnvoyFilter for admission_control.
// Returns the EnvoyFilter resource and the RTDS runtime layer for profile switching.
func (r *IstioRenderer) renderAdmissionControl(
	policy *v1alpha1.AdaptivePolicy,
	listenerContext string,
) (*unstructured.Unstructured, map[string]interface{}, error) {

	// Use effective values — merges active profile overrides with baseline
	threshold := policy.EffectiveSuccessRateThreshold()
	sheddingSpeed := policy.EffectiveSheddingSpeed()
	successRateWindow := policy.EffectiveSuccessRateWindow()
	cfg := policy.Spec.AdmissionControl

	// Build success criteria — which HTTP status codes count as success
	successRanges := cfg.SuccessCodes
	if len(successRanges) == 0 {
		successRanges = []v1alpha1.HTTPStatusRange{{Start: 100, End: 399}}
	}
	httpRanges := make([]interface{}, len(successRanges))
	for i, sr := range successRanges {
		httpRanges[i] = map[string]interface{}{
			"start": int64(sr.Start),
			"end":   int64(sr.End),
		}
	}

	// enabled flag — false in dryRun mode (filter installed but enforces nothing)
	enabled := !policy.IsDryRun()

	// Envoy admission_control proto JSON format — verified working with Istio 1.30:
	//
	// enabled:          RuntimeFeatureFlag — default_value is plain bool
	// sampling_window:  Duration string e.g. "30s"
	// sr_threshold:     RuntimePercent — default_value is nested {value: float64} in 0.0-1.0
	// sheddingSpeed:       RuntimeDouble — default_value is plain float64
	// rps_threshold:    RuntimeUInt32 — default_value is plain int, with runtime_key
	// max_rejection_probability: RuntimePercent — default_value is nested {value: float64} 0-100
	//
	// Verified by applying raw EnvoyFilter YAML to Istio 1.30 before coding this.
	typedConfig := map[string]interface{}{
		"@type": typeURLAdmissionControl,
		"enabled": map[string]interface{}{
			"default_value": enabled,
			"runtime_key":   rtdsKeyAdmissionEnabled,
		},
		"sampling_window": successRateWindow,
		"sr_threshold": map[string]interface{}{
			"default_value": map[string]interface{}{
				"value": mustParseFloat(threshold) / 100.0, // 0.0-1.0 range
			},
			"runtime_key": rtdsKeyAdmissionThreshold,
		},
		"aggression": map[string]interface{}{
			"default_value": mustParseFloat(sheddingSpeed),
			"runtime_key":   rtdsKeyAdmissionAggression,
		},
		"rps_threshold": map[string]interface{}{
			"default_value": int64(cfg.MinRequestsPerSecond),
			"runtime_key":   "admission_control.rps_threshold",
		},
		"max_rejection_probability": map[string]interface{}{
			"default_value": map[string]interface{}{
				"value": mustParseFloat(cfg.MaxRejectionPercent), // 0-100 range
			},
		},
		"success_criteria": map[string]interface{}{
			"http_criteria": map[string]interface{}{
				"http_success_status": httpRanges,
			},
			"grpc_criteria": map[string]interface{}{
				"grpc_success_status": []interface{}{int64(0)},
			},
		},
	}

	ef := r.buildEnvoyFilter(
		policy,
		admissionControlName(policy),
		listenerContext,
		filterNameAdmissionControl,
		typedConfig,
	)

	// RTDS layer — these keys can be updated at runtime without re-rendering
	// the EnvoyFilter. Used for sub-200ms profile switching.
	rtdsLayer := map[string]interface{}{
		rtdsKeyAdmissionEnabled:    enabled,
		rtdsKeyAdmissionThreshold:  mustParseFloat(threshold) / 100.0, // 0.0-1.0, matching typedConfig
		rtdsKeyAdmissionAggression: mustParseFloat(sheddingSpeed),
	}

	return ef, rtdsLayer, nil
}

// renderAdaptiveConcurrency generates the EnvoyFilter for adaptive_concurrency.
func (r *IstioRenderer) renderAdaptiveConcurrency(
	policy *v1alpha1.AdaptivePolicy,
	listenerContext string,
) (*unstructured.Unstructured, map[string]interface{}, error) {

	cfg := policy.Spec.AdaptiveConcurrency
	percentile := policy.EffectiveLatencyPercentile()
	enabled := !policy.IsDryRun()

	concurrencyLimitParams := map[string]interface{}{
		"concurrency_update_interval": cfg.ConcurrencyAdjustInterval,
	}
	if cfg.ConcurrencyLimit > 0 {
		concurrencyLimitParams["max_concurrency_limit"] = int64(cfg.ConcurrencyLimit)
	}

	// Envoy adaptive_concurrency proto JSON format:
	// - sample_aggregate_percentile uses Percent proto: {"value": 50.0}  ← this one IS nested
	// - jitter uses Percent proto: {"value": 10.0}                       ← this one IS nested
	// - interval is a Duration string e.g. "60s"
	// - request_count is uint32
	typedConfig := map[string]interface{}{
		"@type": typeURLAdaptiveConcurrency,
		"gradient_controller_config": map[string]interface{}{
			"sample_aggregate_percentile": map[string]interface{}{
				"value": percentile.EnvoyValue(), // Percent proto — nested is correct here
			},
			"concurrency_limit_params": concurrencyLimitParams,
			"min_rtt_calc_params": map[string]interface{}{
				"interval":      cfg.LatencyBaselineInterval,
				"request_count": int64(cfg.LatencyBaselineSampleSize),
				"jitter": map[string]interface{}{
					"value": float64(cfg.MeasurementJitter), // Percent proto — nested is correct here
				},
			},
		},
		"enabled": map[string]interface{}{
			"default_value": enabled,
			"runtime_key":   rtdsKeyConcurrencyEnabled,
		},
	}

	ef := r.buildEnvoyFilter(
		policy,
		adaptiveConcurrencyName(policy),
		listenerContext,
		filterNameAdaptiveConcurrency,
		typedConfig,
	)

	rtdsLayer := map[string]interface{}{
		rtdsKeyConcurrencyEnabled: enabled,
	}

	return ef, rtdsLayer, nil
}

// renderStatsFlush generates an EnvoyFilter BOOTSTRAP patch that sets
// stats_flush_on_admin: true. This makes every GET to :15090/stats/prometheus
// bypass Envoy's internal 5s stats flush timer and return genuinely fresh data.
// New pods pick up the bootstrap config at startup; existing pods need one rolling
// restart (which the operator can trigger via an annotation bump).
func (r *IstioRenderer) renderStatsFlush(policy *v1alpha1.AdaptivePolicy) *unstructured.Unstructured {
	ef := &unstructured.Unstructured{}
	ef.SetAPIVersion(istioNetworkingAPIVersion)
	ef.SetKind("EnvoyFilter")
	ef.SetName(statsFlushName(policy))
	ef.SetNamespace(policy.Namespace)
	ef.SetLabels(r.resourceLabels(policy))
	ef.SetOwnerReferences(ownerReferences(policy))

	_ = unstructured.SetNestedStringMap(ef.Object,
		policy.Spec.Selector,
		"spec", "workloadSelector", "labels",
	)

	_ = unstructured.SetNestedSlice(ef.Object,
		[]interface{}{
			map[string]interface{}{
				"applyTo": "BOOTSTRAP",
				"patch": map[string]interface{}{
					"operation": "MERGE",
					"value": map[string]interface{}{
						"stats_flush_on_admin": true,
					},
				},
			},
		},
		"spec", "configPatches",
	)

	return ef
}

// renderStreamingProtection generates a DestinationRule for gRPC/WebSocket limits.
func (r *IstioRenderer) renderStreamingProtection(
	policy *v1alpha1.AdaptivePolicy,
) (*unstructured.Unstructured, error) {

	cfg := policy.Spec.StreamingProtection

	dr := &unstructured.Unstructured{}
	dr.SetAPIVersion(istioNetworkingAPIVersion)
	dr.SetKind("DestinationRule")
	dr.SetName(streamingProtectionName(policy))
	dr.SetNamespace(policy.Namespace)
	dr.SetLabels(r.resourceLabels(policy))
	dr.SetOwnerReferences(ownerReferences(policy))

	// Build the service hostname for this policy's selector
	// In Istio, DestinationRule host is the k8s service name
	host := selectorToHost(policy.Spec.Selector)

	_ = unstructured.SetNestedField(dr.Object, host, "spec", "host")
	_ = unstructured.SetNestedField(dr.Object,
		int64(cfg.MaxConcurrentStreams),
		"spec", "trafficPolicy", "connectionPool", "http", "http2MaxRequests",
	)
	_ = unstructured.SetNestedField(dr.Object,
		int64(cfg.MaxPendingRequests),
		"spec", "trafficPolicy", "connectionPool", "http", "http1MaxPendingRequests",
	)
	_ = unstructured.SetNestedField(dr.Object,
		fmt.Sprintf("%ds", cfg.StreamTimeoutSeconds),
		"spec", "trafficPolicy", "connectionPool", "http", "idleTimeout",
	)

	return dr, nil
}

// buildEnvoyFilter constructs an EnvoyFilter unstructured object.
func (r *IstioRenderer) buildEnvoyFilter(
	policy *v1alpha1.AdaptivePolicy,
	name string,
	listenerContext string,
	filterName string,
	typedConfig map[string]interface{},
) *unstructured.Unstructured {

	ef := &unstructured.Unstructured{}
	ef.SetAPIVersion(istioNetworkingAPIVersion)
	ef.SetKind("EnvoyFilter")
	ef.SetName(name)
	ef.SetNamespace(policy.Namespace)
	ef.SetLabels(r.resourceLabels(policy))
	ef.SetOwnerReferences(ownerReferences(policy))

	// workloadSelector — matches the pods this filter applies to
	_ = unstructured.SetNestedStringMap(ef.Object,
		policy.Spec.Selector,
		"spec", "workloadSelector", "labels",
	)

	// configPatches — INSERT_BEFORE the router filter in the HTTP filter chain
	configPatch := map[string]interface{}{
		"applyTo": "HTTP_FILTER",
		"match": map[string]interface{}{
			"context": listenerContext,
			"listener": map[string]interface{}{
				"filterChain": map[string]interface{}{
					"filter": map[string]interface{}{
						"name": filterNameHTTPConnManager,
					},
				},
			},
		},
		"patch": map[string]interface{}{
			"operation": "INSERT_BEFORE",
			"value": map[string]interface{}{
				"name":         filterName,
				"typed_config": typedConfig,
			},
		},
	}

	_ = unstructured.SetNestedSlice(ef.Object,
		[]interface{}{configPatch},
		"spec", "configPatches",
	)

	return ef
}

// listenerContext returns the Envoy listener context for the given mesh mode.
func (r *IstioRenderer) listenerContext(mode v1alpha1.MeshMode) string {
	// Ambient waypoints use the same SIDECAR_INBOUND context for now.
	// This may change as Istio ambient matures — tracked upstream.
	return "SIDECAR_INBOUND"
}

// resourceLabels returns the standard labels for all generated resources.
func (r *IstioRenderer) resourceLabels(policy *v1alpha1.AdaptivePolicy) map[string]string {
	return map[string]string{
		labelManagedBy: labelManagedByVal,
		labelPolicy:    policy.Name,
		labelPolicyNS:  policy.Namespace,
	}
}

// ── Resource naming ────────────────────────────────────────────────────────────

func admissionControlName(policy *v1alpha1.AdaptivePolicy) string {
	return fmt.Sprintf("%s-admission-control", policy.Name)
}

func adaptiveConcurrencyName(policy *v1alpha1.AdaptivePolicy) string {
	return fmt.Sprintf("%s-adaptive-concurrency", policy.Name)
}

func streamingProtectionName(policy *v1alpha1.AdaptivePolicy) string {
	return fmt.Sprintf("%s-streaming", policy.Name)
}

func statsFlushName(policy *v1alpha1.AdaptivePolicy) string {
	return fmt.Sprintf("%s-stats-flush", policy.Name)
}

// ── Helpers ────────────────────────────────────────────────────────────────────

// ownerReferences returns typed OwnerReferences pointing to the AdaptivePolicy.
// When the AdaptivePolicy is deleted, Kubernetes GC cascades to all owned
// resources automatically — no explicit cleanup needed in the controller.
//
// Both pointer fields (Controller, BlockOwnerDeletion) must be non-nil pointers
// to bool — the Kubernetes API rejects nil here.
func ownerReferences(policy *v1alpha1.AdaptivePolicy) []metav1.OwnerReference {
	t := true
	return []metav1.OwnerReference{
		{
			APIVersion:         "resilience.shedpilot.io/v1alpha1",
			Kind:               "AdaptivePolicy",
			Name:               policy.Name,
			UID:                policy.UID,
			Controller:         &t,
			BlockOwnerDeletion: &t,
		},
	}
}

// selectorToHost derives a Kubernetes service hostname from a label selector.
// In Istio DestinationRules, the host is the service name, not the pod label.
// We use the "app" label as the service name by convention.
// This is a best-effort derivation — for precise control, a future version
// should allow the user to specify the host explicitly in the CRD.
func selectorToHost(selector map[string]string) string {
	if app, ok := selector["app"]; ok {
		return app
	}
	// Fallback: use the first selector value
	for _, v := range selector {
		return v
	}
	return "*"
}

// mustParseFloat parses a string float, returning 0 on error.
// Used for converting CRD string fields to Envoy proto numeric values.
// Validation markers in the CRD ensure these are always valid floats,
// so errors here indicate a programming bug, not user error.
func mustParseFloat(s string) float64 {
	var f float64
	fmt.Sscanf(s, "%f", &f)
	return f
}
