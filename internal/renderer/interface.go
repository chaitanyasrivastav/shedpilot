// Package renderer translates AdaptivePolicy specs into mesh-native resources.
//
// Architecture:
//
//	AdaptivePolicy CRD
//	      ↓
//	Renderer interface   ← this package defines the contract
//	      ↓
//	IstioRenderer        ← EnvoyFilter + DestinationRule (v1)
//	CiliumRenderer       ← CiliumEnvoyConfig (v1.1, not yet implemented)
//
// The controller calls Detect() on each registered renderer in priority order,
// uses the first one that returns true, and calls Render() to get resources.
//
// Profile-aware rendering:
// When a profile is active, the renderer merges the profile overrides into the
// baseline config before generating filter config. The merged result is what
// Envoy actually enforces. RTDS profile switching updates runtime keys only —
// the base EnvoyFilter stays in place.
package renderer

import (
	"context"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/chaitanyasrivastav/shedpilot/api/v1alpha1"
)

// Renderer is the contract every mesh backend must implement.
// Each backend translates an AdaptivePolicy into mesh-native resources
// and reports whether it is present in the current cluster.
type Renderer interface {
	// Name returns the backend identifier used in logs and status.
	// e.g. "istio", "cilium"
	Name() string

	// Detect returns true if this mesh backend is present in the cluster.
	// Called once at controller startup and cached. Each backend checks for
	// its own control plane pods/daemonsets.
	Detect(ctx context.Context, c client.Client) (bool, error)

	// Render generates all mesh-native resources for the given policy.
	// The returned resources should be applied via server-side apply.
	// Resources carry owner references pointing to the AdaptivePolicy
	// so they are cascade-deleted when the policy is deleted.
	//
	// Render must be idempotent — calling it twice with the same policy
	// must produce identical output.
	//
	// When policy.IsDryRun() is true, the renderer still generates resources
	// but sets filter runtime flags to disable enforcement. This lets engineers
	// observe what would happen without affecting traffic.
	Render(policy *v1alpha1.AdaptivePolicy) (*RenderResult, error)

	// RTDSSupported returns true if this backend supports RTDS profile
	// switching (sub-200ms delivery). When false, profile switches
	// go through the full EnvoyFilter re-render path (5-30s delivery).
	RTDSSupported() bool
}

// RenderResult holds all resources generated for a single AdaptivePolicy.
// Resources are applied in order — ordering matters for filter chain correctness.
type RenderResult struct {
	// Resources is the ordered list of mesh-native resources to apply.
	// For Istio: [EnvoyFilter(admission-control), EnvoyFilter(adaptive-concurrency),
	//             DestinationRule(streaming)]
	// For Cilium: [CiliumEnvoyConfig]
	Resources []*unstructured.Unstructured

	// ManagedResourceRefs describes what was generated, for the status block.
	// Written to AdaptivePolicy.Status.ManagedResources after apply.
	ManagedResourceRefs []v1alpha1.ManagedResource

	// ActiveFilters lists the Envoy filter names that are active.
	// Written to AdaptivePolicy.Status.ActiveFilters after apply.
	// e.g. ["admission-control", "adaptive-concurrency", "streaming-protection"]
	ActiveFilters []string

	// RTDSLayers contains the RTDS runtime layer config for each active filter.
	// Used for sub-200ms profile switching without re-rendering EnvoyFilters.
	// Key: layer name (e.g. "shedpilot-payments-admission-control")
	// Value: map of runtime key → value pairs to set
	RTDSLayers map[string]map[string]interface{}
}
