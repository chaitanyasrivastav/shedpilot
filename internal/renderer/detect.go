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
//  1. Istio  — checks for istiod pod in istio-system
//  2. Cilium — checks for cilium-envoy DaemonSet in kube-system
//  3. Error  — surfaces as MeshDetected=False condition on the CRD
//
// If the policy specifies an explicit meshBackend, detection is skipped.
func DetectAndBuild(
	ctx context.Context,
	c client.Client,
	policy *v1alpha1.AdaptivePolicy,
) (Renderer, error) {

	switch policy.EffectiveMeshBackend() {
	case v1alpha1.MeshBackendIstio:
		return NewIstioRenderer(), nil

	case v1alpha1.MeshBackendCilium:
		return NewCiliumRenderer(), nil

	default: // MeshBackendAuto
		return detect(ctx, c)
	}
}

// detect runs auto-detection in priority order.
// Istio is checked first — if both are present, Istio wins.
func detect(ctx context.Context, c client.Client) (Renderer, error) {
	renderers := []Renderer{
		NewIstioRenderer(),
		NewCiliumRenderer(),
	}

	for _, r := range renderers {
		found, err := r.Detect(ctx, c)
		if err != nil {
			continue
		}
		if found {
			return r, nil
		}
	}

	return nil, fmt.Errorf(
		"no supported service mesh detected. " +
			"Supported: Istio (istiod pod in istio-system), " +
			"Cilium (cilium-envoy DaemonSet in kube-system). " +
			"Set meshBackend explicitly if detection is incorrect",
	)
}
