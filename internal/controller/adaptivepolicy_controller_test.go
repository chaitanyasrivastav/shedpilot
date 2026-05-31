/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	resiliencev1alpha1 "github.com/chaitanyasrivastav/shedpilot/api/v1alpha1"
	"github.com/chaitanyasrivastav/shedpilot/internal/signal"
	"github.com/chaitanyasrivastav/shedpilot/internal/trigger"
)

var _ = Describe("AdaptivePolicy Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default", // TODO(user):Modify as needed
		}
		adaptivepolicy := &resiliencev1alpha1.AdaptivePolicy{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind AdaptivePolicy")
			err := k8sClient.Get(ctx, typeNamespacedName, adaptivepolicy)
			if err != nil && errors.IsNotFound(err) {
				resource := &resiliencev1alpha1.AdaptivePolicy{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					Spec: resiliencev1alpha1.AdaptivePolicySpec{
						// Selector is required — must not be empty
						Selector: map[string]string{
							"app": "payments",
						},

						// Use dryRun: true in tests — filters install but never reject
						DryRun:      true,
						MeshBackend: resiliencev1alpha1.MeshBackendIstio,
						MeshMode:    resiliencev1alpha1.MeshModeSidecar,

						AdmissionControl: &resiliencev1alpha1.AdmissionControlConfig{
							Enabled:              true,
							SuccessRateWindow:    "30s",
							SuccessRateThreshold: "95.0",
							SheddingSpeed:        "1.5",
							MinRequestsPerSecond: 5,
							MaxRejectionPercent:  "80.0",
						},

						AdaptiveConcurrency: &resiliencev1alpha1.AdaptiveConcurrencyConfig{
							Enabled:                   true,
							LatencyPercentile:         resiliencev1alpha1.PercentileP50,
							LatencyBaselineInterval:   "60s",
							ConcurrencyAdjustInterval: "100ms",
							MaxLoadIncrease:           "2.0",
							MeasurementJitter:         10,
						},

						StreamingProtection: &resiliencev1alpha1.StreamingProtectionConfig{
							Enabled:              true,
							MaxConcurrentStreams: 200,
							StreamTimeoutSeconds: 300,
							MaxPendingRequests:   1024,
						},

						Profiles: map[string]resiliencev1alpha1.ProfileConfig{
							"normal": {
								AdmissionControl: &resiliencev1alpha1.AdmissionControlOverride{
									SuccessRateThreshold: "95.0",
									SheddingSpeed:        "1.5",
								},
							},
							"degraded": {
								AdmissionControl: &resiliencev1alpha1.AdmissionControlOverride{
									SuccessRateThreshold: "85.0",
									SheddingSpeed:        "2.0",
									SuccessRateWindow:    "20s",
								},
								AdaptiveConcurrency: &resiliencev1alpha1.AdaptiveConcurrencyOverride{
									LatencyPercentile: resiliencev1alpha1.PercentileP75,
								},
							},
						},

						ActiveProfile: "normal",

						Triggers: []resiliencev1alpha1.TriggerConfig{
							{
								Name: "degradation-detected",
								When: resiliencev1alpha1.TriggerCondition{
									SuccessRate: &resiliencev1alpha1.RateCondition{
										Below:              "0.90",
										ConsecutiveSamples: 2,
									},
								},
								SwitchTo:        "degraded",
								CooldownSeconds: 60,
							},
							{
								Name: "recovery",
								When: resiliencev1alpha1.TriggerCondition{
									SuccessRate: &resiliencev1alpha1.RateCondition{
										Above:              "0.97",
										ConsecutiveSamples: 3,
									},
								},
								SwitchTo:        "normal",
								FromProfiles:    []string{"degraded"},
								CooldownSeconds: 60,
							},
						},

						SignalConfig: &resiliencev1alpha1.SignalConfig{
							EvaluationIntervalSeconds: 30,
							CapacityWarningPercent:    10,
							CapacityWarningWindowDays: 7,
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &resiliencev1alpha1.AdaptivePolicy{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance AdaptivePolicy")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &AdaptivePolicyReconciler{
				Client:       k8sClient,
				Scheme:       k8sClient.Scheme(),
				scraper:      signal.NewScraper(k8sClient),
				triggerState: make(map[string]*trigger.State),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			// TODO(user): Add more specific assertions depending on your controller's reconciliation logic.
			// Example: If you expect a certain status condition after reconciliation, verify it here.
		})
	})
})
