// Package v1alpha1 contains the v1alpha1 API types for shedpilot.
// The API group is resilience.shedpilot.io.
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is the group and version for the shedpilot API.
	GroupVersion = schema.GroupVersion{
		Group:   "resilience.shedpilot.io",
		Version: "v1alpha1",
	}

	// SchemeBuilder registers the types in this package with a scheme.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

	// AddToScheme adds the types in this group-version to a given scheme.
	// Called from main.go during operator startup.
	AddToScheme = SchemeBuilder.AddToScheme
)

func init() {
	SchemeBuilder.Register(&AdaptivePolicy{}, &AdaptivePolicyList{})
}
