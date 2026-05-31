package renderer

import (
	"context"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/chaitanyasrivastav/shedpilot/api/v1alpha1"
)

// DetectAndBuild auto-detects the mesh backend and returns the correct
// Renderer implementation. Detection runs in priority order:
//
//  1. Istio — checks for istiod pod in istio-system
//  2. Cilium — checks for cilium-envoy DaemonSet in kube-system (v1.1)
//  3. Error — surfaces as MeshDetected=False condition on the CRD
//
// If the policy specifies an explicit meshBackend, detection is skipped
// and that backend is used directly.
func DetectAndBuild(
	ctx context.Context,
	c client.Client,
	policy *v1alpha1.AdaptivePolicy,
) (Renderer, error) {

	backend := policy.EffectiveMeshBackend()

	switch backend {
	case v1alpha1.MeshBackendIstio:
		return NewIstioRenderer(), nil

	case v1alpha1.MeshBackendCilium:
		return nil, fmt.Errorf(
			"cilium backend is not yet implemented (v1.1 roadmap). " +
				"Set meshBackend: istio or meshBackend: auto to use Istio",
		)

	default: // MeshBackendAuto
		return detect(ctx, c)
	}
}

// detect runs auto-detection in priority order.
func detect(ctx context.Context, c client.Client) (Renderer, error) {
	renderers := []Renderer{
		NewIstioRenderer(),
		// NewCiliumRenderer() — add in v1.1
	}

	for _, r := range renderers {
		found, err := r.Detect(ctx, c)
		if err != nil {
			// Non-fatal — log and try next
			continue
		}
		if found {
			return r, nil
		}
	}

	return nil, fmt.Errorf(
		"no supported service mesh detected in the cluster. " +
			"Supported meshes: Istio (istiod pod in istio-system). " +
			"Set meshBackend explicitly if detection is incorrect",
	)
}
