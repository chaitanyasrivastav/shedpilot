package renderer

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/chaitanyasrivastav/shedpilot/api/v1alpha1"
)

const (
	ciliumAPIVersion      = "cilium.io/v2alpha1"
	ciliumEnvoyConfigKind = "CiliumEnvoyConfig"
)

// CiliumRenderer generates CiliumEnvoyConfig resources for Cilium service mesh.
//
// Cilium uses CiliumEnvoyConfig (CEC) instead of Istio's EnvoyFilter.
// The same filter protos (admission_control, adaptive_concurrency) work
// unchanged — both Istio and Cilium run Envoy underneath.
//
// Key differences from Istio:
//   - CEC embeds the full filter chain (not patches to an existing chain)
//   - CEC targets services via spec.services (not workloadSelector)
//   - RTDSSupported() returns false — Cilium RTDS is v1.1
//
// Cilium 1.14+ required for CiliumEnvoyConfig support.
type CiliumRenderer struct{}

func NewCiliumRenderer() *CiliumRenderer { return &CiliumRenderer{} }
func (r *CiliumRenderer) Name() string   { return "cilium" }

// Detect returns true if the cilium-envoy DaemonSet is running.
func (r *CiliumRenderer) Detect(ctx context.Context, c client.Client) (bool, error) {
	dsList := &appsv1.DaemonSetList{}
	if err := c.List(ctx, dsList,
		client.InNamespace("kube-system"),
		client.MatchingLabels{"k8s-app": "cilium-envoy"},
	); err != nil {
		return false, nil
	}
	for _, ds := range dsList.Items {
		if ds.Status.NumberReady > 0 {
			return true, nil
		}
	}
	return false, nil
}

// RTDSSupported returns false — Cilium RTDS support is v1.1.
// Profile switches use CiliumEnvoyConfig re-render (5-30s).
func (r *CiliumRenderer) RTDSSupported() bool { return false }

// Render generates CiliumEnvoyConfig resources for the policy.
func (r *CiliumRenderer) Render(policy *v1alpha1.AdaptivePolicy) (*RenderResult, error) {
	result := &RenderResult{
		RTDSLayers: make(map[string]map[string]interface{}),
	}

	var resources []interface{}
	var activeFilters []string

	if policy.HasAdmissionControl() {
		acResource, rtds, err := r.buildAdmissionControlResource(policy)
		if err != nil {
			return nil, fmt.Errorf("building admission control resource: %w", err)
		}
		resources = append(resources, acResource)
		activeFilters = append(activeFilters, "admission-control")
		result.RTDSLayers[fmt.Sprintf("shedpilot-%s-admission-control", policy.Name)] = rtds
	}

	if policy.HasAdaptiveConcurrency() {
		concResource, rtds, err := r.buildAdaptiveConcurrencyResource(policy)
		if err != nil {
			return nil, fmt.Errorf("building adaptive concurrency resource: %w", err)
		}
		resources = append(resources, concResource)
		activeFilters = append(activeFilters, "adaptive-concurrency")
		result.RTDSLayers[fmt.Sprintf("shedpilot-%s-adaptive-concurrency", policy.Name)] = rtds
	}

	if len(resources) == 0 {
		return result, nil
	}

	cec, err := r.buildCiliumEnvoyConfig(policy, resources)
	if err != nil {
		return nil, fmt.Errorf("building CiliumEnvoyConfig: %w", err)
	}

	result.Resources = append(result.Resources, cec)
	result.ActiveFilters = activeFilters
	result.ManagedResourceRefs = append(result.ManagedResourceRefs, v1alpha1.ManagedResource{
		APIVersion: ciliumAPIVersion,
		Kind:       ciliumEnvoyConfigKind,
		Name:       ciliumEnvoyConfigName(policy),
		Namespace:  policy.Namespace,
	})

	return result, nil
}

// buildAdmissionControlResource builds the admission_control filter proto.
// Identical format to Istio — same type URLs, same field structure.
// NOTE: uses rtdsKeyAdmissionAggression (not rtdsKeyAdmissionSheddingSpeed)
// because "aggression" is the actual Envoy runtime key name, not our CRD name.
func (r *CiliumRenderer) buildAdmissionControlResource(
	policy *v1alpha1.AdaptivePolicy,
) (map[string]interface{}, map[string]interface{}, error) {

	threshold := policy.EffectiveSuccessRateThreshold()
	sheddingSpeed := policy.EffectiveSheddingSpeed()
	successRateWindow := policy.EffectiveSuccessRateWindow()
	cfg := policy.Spec.AdmissionControl

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

	enabled := !policy.IsDryRun()

	filterResource := map[string]interface{}{
		"@type": typeURLAdmissionControl,
		"enabled": map[string]interface{}{
			"default_value": enabled,
			"runtime_key":   rtdsKeyAdmissionEnabled,
		},
		"sampling_window": successRateWindow,
		"sr_threshold": map[string]interface{}{
			"default_value": map[string]interface{}{
				"value": mustParseFloat(threshold) / 100.0,
			},
			"runtime_key": rtdsKeyAdmissionThreshold,
		},
		// Note: Envoy's runtime key is "aggression" regardless of our CRD field name.
		// Our CRD calls it "sheddingSpeed" for clarity but Envoy's proto says "aggression".
		"aggression": map[string]interface{}{
			"default_value": mustParseFloat(sheddingSpeed),
			"runtime_key":   rtdsKeyAdmissionAggression, // ← correct constant from istio.go
		},
		"rps_threshold": map[string]interface{}{
			"default_value": int64(cfg.MinRequestsPerSecond),
			"runtime_key":   "admission_control.rps_threshold",
		},
		"max_rejection_probability": map[string]interface{}{
			"default_value": map[string]interface{}{
				"value": mustParseFloat(cfg.MaxRejectionPercent),
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

	rtdsLayer := map[string]interface{}{
		rtdsKeyAdmissionEnabled:    enabled,
		rtdsKeyAdmissionThreshold:  mustParseFloat(threshold) / 100.0,
		rtdsKeyAdmissionAggression: mustParseFloat(sheddingSpeed), // Envoy key = aggression
	}

	return filterResource, rtdsLayer, nil
}

// buildAdaptiveConcurrencyResource builds the adaptive_concurrency filter proto.
func (r *CiliumRenderer) buildAdaptiveConcurrencyResource(
	policy *v1alpha1.AdaptivePolicy,
) (map[string]interface{}, map[string]interface{}, error) {

	cfg := policy.Spec.AdaptiveConcurrency
	percentile := policy.EffectiveLatencyPercentile()
	enabled := !policy.IsDryRun()

	concurrencyLimitParams := map[string]interface{}{
		"concurrency_update_interval": cfg.ConcurrencyAdjustInterval,
	}
	if cfg.ConcurrencyLimit > 0 {
		concurrencyLimitParams["max_concurrency_limit"] = int64(cfg.ConcurrencyLimit)
	}

	filterResource := map[string]interface{}{
		"@type": typeURLAdaptiveConcurrency,
		"gradient_controller_config": map[string]interface{}{
			"sample_aggregate_percentile": map[string]interface{}{
				"value": percentile.EnvoyValue(),
			},
			"concurrency_limit_params": concurrencyLimitParams,
			"min_rtt_calc_params": map[string]interface{}{
				"interval":      cfg.LatencyBaselineInterval,
				"request_count": int64(cfg.LatencyBaselineSampleSize),
				"jitter": map[string]interface{}{
					"value": float64(cfg.MeasurementJitter),
				},
			},
		},
		"enabled": map[string]interface{}{
			"default_value": enabled,
			"runtime_key":   rtdsKeyConcurrencyEnabled,
		},
	}

	rtdsLayer := map[string]interface{}{
		rtdsKeyConcurrencyEnabled: enabled,
	}

	return filterResource, rtdsLayer, nil
}

// buildCiliumEnvoyConfig constructs the CiliumEnvoyConfig resource.
// CEC embeds the full HTTP filter chain directly (not patches like EnvoyFilter).
func (r *CiliumRenderer) buildCiliumEnvoyConfig(
	policy *v1alpha1.AdaptivePolicy,
	filterResources []interface{},
) (*unstructured.Unstructured, error) {

	cec := &unstructured.Unstructured{}
	cec.SetAPIVersion(ciliumAPIVersion)
	cec.SetKind(ciliumEnvoyConfigKind)
	cec.SetName(ciliumEnvoyConfigName(policy))
	cec.SetNamespace(policy.Namespace)
	cec.SetLabels(r.resourceLabels(policy))
	cec.SetOwnerReferences(ownerReferences(policy))

	serviceName := selectorToHost(policy.Spec.Selector)
	services := []interface{}{
		map[string]interface{}{
			"name":      serviceName,
			"namespace": policy.Namespace,
		},
	}

	// Build HTTP filter chain: our filters + required router (must be last)
	httpFilters := make([]interface{}, 0, len(filterResources)+1)
	for _, fr := range filterResources {
		httpFilters = append(httpFilters, fr)
	}
	httpFilters = append(httpFilters, map[string]interface{}{
		"name": "envoy.filters.http.router",
		"typed_config": map[string]interface{}{
			"@type": "type.googleapis.com/envoy.extensions.filters.http.router.v3.Router",
		},
	})

	listenerResource := map[string]interface{}{
		"@type": "type.googleapis.com/envoy.config.listener.v3.Listener",
		"name":  fmt.Sprintf("shedpilot-%s", policy.Name),
		"filter_chains": []interface{}{
			map[string]interface{}{
				"filters": []interface{}{
					map[string]interface{}{
						"name": filterNameHTTPConnManager,
						"typed_config": map[string]interface{}{
							"@type":        "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager",
							"stat_prefix":  fmt.Sprintf("shedpilot_%s", policy.Name),
							"http_filters": httpFilters,
						},
					},
				},
			},
		},
	}

	_ = unstructured.SetNestedSlice(cec.Object, services, "spec", "services")
	_ = unstructured.SetNestedSlice(cec.Object,
		[]interface{}{listenerResource},
		"spec", "resources",
	)

	return cec, nil
}

func (r *CiliumRenderer) resourceLabels(policy *v1alpha1.AdaptivePolicy) map[string]string {
	return map[string]string{
		labelManagedBy: labelManagedByVal,
		labelPolicy:    policy.Name,
		labelPolicyNS:  policy.Namespace,
	}
}

func ciliumEnvoyConfigName(policy *v1alpha1.AdaptivePolicy) string {
	return fmt.Sprintf("shedpilot-%s", policy.Name)
}

// Unused but kept for symmetry — owner references are set directly above.
func ownerReferencesCilium(policy *v1alpha1.AdaptivePolicy) []metav1.OwnerReference {
	return ownerReferences(policy)
}
