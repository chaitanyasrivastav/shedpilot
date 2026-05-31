// Package controller implements the AdaptivePolicy reconcile loop.
//
// Reconcile steps:
//  1. Fetch AdaptivePolicy — if not found, deleted (owner refs handle cleanup)
//  2. Detect mesh backend (Istio / Cilium)
//  3. Read live signals from Envoy stats endpoint
//  4. Evaluate trigger conditions — switch profile if needed
//  5. Render mesh-native resources (EnvoyFilter, DestinationRule)
//  6. Apply rendered resources via server-side apply
//  7. If profile switched and RTDS connected — push sub-200ms runtime update
//  8. Update status block (3am-legible, one kubectl describe tells all)
package controller

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/shedpilot-io/operator/api/v1alpha1"
	"github.com/shedpilot-io/operator/internal/renderer"
	"github.com/shedpilot-io/operator/internal/rtds"
	"github.com/shedpilot-io/operator/internal/signal"
	"github.com/shedpilot-io/operator/internal/trigger"
)

// +kubebuilder:rbac:groups=resilience.shedpilot.io,resources=adaptivepolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=resilience.shedpilot.io,resources=adaptivepolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=resilience.shedpilot.io,resources=adaptivepolicies/finalizers,verbs=update
// +kubebuilder:rbac:groups=networking.istio.io,resources=envoyfilters;destinationrules,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch

// AdaptivePolicyReconciler reconciles AdaptivePolicy objects.
type AdaptivePolicyReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// scraper reads live signals from Envoy sidecar stats endpoints.
	// Shared across all reconcile calls — maintains counter history.
	scraper *signal.Scraper

	// rtdsClient pushes profile switches sub-200ms via RTDS gRPC stream.
	// Nil when RTDS is not connected (falls back to EnvoyFilter re-render).
	rtdsClient *rtds.Client

	// triggerState persists consecutive sample counts across reconcile loops.
	// Keyed by namespace/name.
	triggerState map[string]*trigger.State
}

// NewAdaptivePolicyReconciler creates a reconciler with all dependencies wired.
func NewAdaptivePolicyReconciler(
	c client.Client,
	scheme *runtime.Scheme,
	rtdsClient *rtds.Client,
) *AdaptivePolicyReconciler {
	return &AdaptivePolicyReconciler{
		Client:       c,
		Scheme:       scheme,
		scraper:      signal.NewScraper(c),
		rtdsClient:   rtdsClient,
		triggerState: make(map[string]*trigger.State),
	}
}

// Reconcile is the main reconcile loop.
func (r *AdaptivePolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("policy", req.NamespacedName)

	// ── 1. Fetch ──────────────────────────────────────────────────────────────

	policy := &v1alpha1.AdaptivePolicy{}
	if err := r.Get(ctx, req.NamespacedName, policy); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	logger.Info("reconciling",
		"activeProfile", policy.Spec.ActiveProfile,
		"dryRun", policy.Spec.DryRun,
		"meshBackend", policy.Spec.MeshBackend,
	)

	// ── 2. Detect mesh ────────────────────────────────────────────────────────

	meshRenderer, err := renderer.DetectAndBuild(ctx, r.Client, policy)
	if err != nil {
		logger.Error(err, "mesh detection failed")
		_ = r.markDegraded(ctx, policy, "MeshDetectionFailed", err.Error())
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// ── 3. Read signals ───────────────────────────────────────────────────────

	// Lazily initialize triggerState if nil (handles manual instantiation in tests)
	if r.triggerState == nil {
		r.triggerState = make(map[string]*trigger.State)
	}

	key := req.NamespacedName.String()
	if _, ok := r.triggerState[key]; !ok {
		r.triggerState[key] = trigger.NewState()
	}

	signals, signalErr := r.scraper.ReadSignals(ctx, policy)
	if signalErr != nil {
		// Non-fatal — log and use safe defaults.
		// This is expected during pod startup before Envoy is ready.
		logger.V(1).Info("signal read failed, using defaults", "error", signalErr)
		signals = trigger.Signals{
			SuccessRate: 0.99,
			RPS:         0,
			CollectedAt: time.Now(),
		}
	}

	// ── 4. Evaluate triggers ──────────────────────────────────────────────────

	decision, profileSwitched, switchErr := r.evaluateAndSwitch(ctx, policy, signals, key)
	if switchErr != nil {
		logger.Error(switchErr, "trigger evaluation failed")
		// Non-fatal — continue with current profile
	}

	if profileSwitched {
		logger.Info("profile switched",
			"trigger", decision.TriggerName,
			"to", decision.TargetProfile,
			"reason", decision.Reason,
		)
		// Re-fetch after spec patch to get updated activeProfile
		if err := r.Get(ctx, req.NamespacedName, policy); err != nil {
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
	}

	// ── 5. Render ─────────────────────────────────────────────────────────────

	renderResult, err := meshRenderer.Render(policy)
	if err != nil {
		logger.Error(err, "render failed")
		_ = r.markDegraded(ctx, policy, "RenderFailed", err.Error())
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	// ── 6. Apply ──────────────────────────────────────────────────────────────

	for _, resource := range renderResult.Resources {
		if err := r.Patch(ctx, resource, client.Apply,
			client.FieldOwner("shedpilot"),
			client.ForceOwnership,
		); err != nil {
			logger.Error(err, "apply failed",
				"kind", resource.GetKind(),
				"name", resource.GetName(),
			)
			_ = r.markDegraded(ctx, policy, "ApplyFailed", err.Error())
			return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
		}
	}

	// ── 7. RTDS push (sub-200ms delivery) ─────────────────────────────────────

	rtdsConnected := false
	if profileSwitched && r.rtdsClient != nil && r.rtdsClient.Connected() && meshRenderer.RTDSSupported() {
		rtdsConnected = true
		for layerName, values := range renderResult.RTDSLayers {
			if err := r.rtdsClient.Push(ctx, layerName, values); err != nil {
				// Non-fatal — EnvoyFilter re-render (step 6) already applied the change.
				// RTDS is the fast path; EnvoyFilter is the reliable fallback.
				logger.Error(err, "RTDS push failed, relying on EnvoyFilter path", "layer", layerName)
				rtdsConnected = false
			} else {
				logger.Info("RTDS push succeeded", "layer", layerName, "deliveryMs", "<200")
			}
		}
	}

	// ── 8. Update status ──────────────────────────────────────────────────────

	now := metav1.Now()
	next := metav1.NewTime(now.Add(r.scrapeInterval(policy)))
	patch := client.MergeFrom(policy.DeepCopy())

	policy.Status.ObservedGeneration = policy.Generation
	policy.Status.DetectedBackend = meshRenderer.Name()
	policy.Status.ActiveProfile = policy.Spec.ActiveProfile
	policy.Status.ActiveFilters = renderResult.ActiveFilters
	policy.Status.ManagedResources = renderResult.ManagedResourceRefs
	policy.Status.LastReconcileTime = &now
	policy.Status.NextTriggerEvaluation = &next
	policy.Status.ConsecutiveBadSamples = decision.ConsecutiveBadSamples
	policy.Status.RTDSConnected = rtdsConnected

	// Shed rate — approximate from success rate signal
	// 0% when healthy, non-zero when admission control is active
	policy.Status.ShedRateNow = estimateShedRate(signals, policy)

	if decision.Reason != "" && decision.ShouldSwitch {
		deliveryMethod := "envoyfilter"
		if rtdsConnected && profileSwitched {
			deliveryMethod = "rtds"
		}
		record := &v1alpha1.DecisionRecord{
			Timestamp:      now,
			TriggerName:    decision.TriggerName,
			ProfileBefore:  policy.Spec.ActiveProfile,
			ProfileAfter:   decision.TargetProfile,
			SignalValues:   decision.Reason,
			DeliveryMethod: deliveryMethod,
			Outcome:        "pending",
		}
		policy.Status.LastDecision = record
		// Prepend to history, keep last 10
		history := append([]v1alpha1.DecisionRecord{*record}, policy.Status.DecisionHistory...)
		if len(history) > 10 {
			history = history[:10]
		}
		policy.Status.DecisionHistory = history
	}

	setCondition(&policy.Status.Conditions, metav1.Condition{
		Type:               v1alpha1.ConditionReady,
		Status:             metav1.ConditionTrue,
		Reason:             "Reconciled",
		Message:            fmt.Sprintf("%d resources applied via %s, backend: %s", len(renderResult.Resources), deliveryMethodStr(rtdsConnected, profileSwitched), meshRenderer.Name()),
		ObservedGeneration: policy.Generation,
	})

	if err := r.Status().Patch(ctx, policy, patch); err != nil {
		logger.Error(err, "status patch failed")
	}

	logger.Info("reconcile complete",
		"resources", len(renderResult.Resources),
		"nextEvalIn", r.scrapeInterval(policy),
		"shedRate", policy.Status.ShedRateNow,
	)

	return ctrl.Result{RequeueAfter: r.scrapeInterval(policy)}, nil
}

// evaluateAndSwitch runs trigger evaluation and patches spec.activeProfile
// if a trigger fires. Returns the decision and whether a switch occurred.
func (r *AdaptivePolicyReconciler) evaluateAndSwitch(
	ctx context.Context,
	policy *v1alpha1.AdaptivePolicy,
	signals trigger.Signals,
	key string,
) (trigger.Decision, bool, error) {

	if !policy.HasTriggers() {
		return trigger.Decision{Reason: "no triggers configured"}, false, nil
	}

	decision := trigger.Evaluate(policy, signals, r.triggerState[key])

	if !decision.ShouldSwitch {
		return decision, false, nil
	}

	// Patch spec.activeProfile — the ONLY spec field the controller ever writes.
	// Everything else in spec is human-authored or brain-authored.
	patch := []byte(fmt.Sprintf(
		`{"spec":{"activeProfile":%q},"metadata":{"annotations":{%q:%q}}}`,
		decision.TargetProfile,
		v1alpha1.AnnotationPreviousProfile,
		policy.Spec.ActiveProfile,
	))

	if err := r.Patch(ctx, policy, client.RawPatch(types.MergePatchType, patch)); err != nil {
		return decision, false, fmt.Errorf("patching activeProfile: %w", err)
	}

	return decision, true, nil
}

// markDegraded updates the CRD status with a degraded condition.
func (r *AdaptivePolicyReconciler) markDegraded(
	ctx context.Context,
	policy *v1alpha1.AdaptivePolicy,
	reason, message string,
) error {
	patch := client.MergeFrom(policy.DeepCopy())
	setCondition(&policy.Status.Conditions, metav1.Condition{
		Type:               v1alpha1.ConditionDegraded,
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: policy.Generation,
	})
	setCondition(&policy.Status.Conditions, metav1.Condition{
		Type:               v1alpha1.ConditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: policy.Generation,
	})
	return r.Status().Patch(ctx, policy, patch)
}

// scrapeInterval returns the trigger evaluation frequency.
func (r *AdaptivePolicyReconciler) scrapeInterval(policy *v1alpha1.AdaptivePolicy) time.Duration {
	if policy.Spec.SignalConfig != nil && policy.Spec.SignalConfig.EvaluationIntervalSeconds > 0 {
		return time.Duration(policy.Spec.SignalConfig.EvaluationIntervalSeconds) * time.Second
	}
	return 30 * time.Second
}

// SetupWithManager registers the controller with the controller-manager.
func (r *AdaptivePolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.AdaptivePolicy{}).
		Complete(r)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// estimateShedRate approximates the current shed percentage from signals.
// When success rate is below the effective threshold, admission control is
// actively shedding. This is an approximation — not suitable for alerting.
func estimateShedRate(signals trigger.Signals, policy *v1alpha1.AdaptivePolicy) string {
	if !policy.HasAdmissionControl() || policy.IsDryRun() {
		return "0%"
	}
	if signals.RPS < 1 {
		return "0%"
	}
	// If success rate is below threshold, shedding is active.
	// The actual shed rate depends on sheddingSpeed — we show an approximation.
	threshold := 0.95
	fmt.Sscanf(policy.EffectiveSuccessRateThreshold(), "%f", &threshold)
	threshold /= 100

	if signals.SuccessRate >= threshold {
		return "0%"
	}

	// Approximate shed rate using the Client-Side Throttling formula inverse:
	// shedRate ≈ 1 - successRate/threshold
	shed := (1 - signals.SuccessRate/threshold) * 100
	if shed < 0 {
		shed = 0
	}
	if shed > 80 {
		shed = 80 // maxRejectionPercent default cap
	}
	return fmt.Sprintf("~%.0f%%", shed)
}

// deliveryMethodStr returns a human-readable delivery method string.
func deliveryMethodStr(rtdsConnected, profileSwitched bool) string {
	if profileSwitched && rtdsConnected {
		return "rtds (<200ms)"
	}
	return "envoyfilter"
}

// setCondition upserts a condition — updates LastTransitionTime only on status change.
func setCondition(conditions *[]metav1.Condition, condition metav1.Condition) {
	condition.LastTransitionTime = metav1.Now()
	for i, c := range *conditions {
		if c.Type == condition.Type {
			if c.Status != condition.Status {
				(*conditions)[i] = condition
			} else {
				(*conditions)[i].Message = condition.Message
				(*conditions)[i].Reason = condition.Reason
				(*conditions)[i].ObservedGeneration = condition.ObservedGeneration
			}
			return
		}
	}
	*conditions = append(*conditions, condition)
}
