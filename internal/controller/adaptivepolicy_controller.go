// Package controller implements the AdaptivePolicy reconcile loop.
//
// Reconcile steps:
//  1. Fetch AdaptivePolicy — if not found, clean up state and return
//  2. Detect mesh backend (Istio / Cilium)
//  3. Read live signals from Envoy stats endpoint
//  4. Evaluate trigger conditions — switch profile if needed
//  5. Render mesh-native resources (EnvoyFilter, DestinationRule)
//  6. Apply rendered resources via server-side apply
//     6b. Prune resources that were rendered before but are no longer needed
//  7. If profile switched and RTDS connected — push sub-200ms runtime update
//  8. Update status block (3am-legible, one kubectl describe tells all)
package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/chaitanyasrivastav/shedpilot/api/v1alpha1"
	"github.com/chaitanyasrivastav/shedpilot/internal/renderer"
	"github.com/chaitanyasrivastav/shedpilot/internal/rtds"
	"github.com/chaitanyasrivastav/shedpilot/internal/signal"
	"github.com/chaitanyasrivastav/shedpilot/internal/trigger"
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
	// Keyed by namespace/name. Protected by tsMu — concurrent reconciles for
	// different policies share this map and must not race on it.
	tsMu         sync.Mutex
	triggerState map[string]*trigger.State

	// pollers holds one running Poller goroutine per AdaptivePolicy.
	// Each poller scrapes at its configured PollInterval and emits debounced
	// signals on confirmed breach/recovery. Protected by pollerMu.
	pollerMu          sync.Mutex
	pollers           map[string]*signal.Poller
	pollerCancels     map[string]context.CancelFunc
	pollerGenerations map[string]int64

	pollerScraper *signal.Scraper // poller scraper — separate from reconcile loop scraper to avoid mutex contention on previous signal values
}

// NewAdaptivePolicyReconciler creates a reconciler with all dependencies wired.
func NewAdaptivePolicyReconciler(
	c client.Client,
	scheme *runtime.Scheme,
	rtdsClient *rtds.Client,
) *AdaptivePolicyReconciler {
	return &AdaptivePolicyReconciler{
		Client:            c,
		Scheme:            scheme,
		scraper:           signal.NewScraper(c),
		rtdsClient:        rtdsClient,
		triggerState:      make(map[string]*trigger.State),
		pollers:           make(map[string]*signal.Poller),
		pollerCancels:     make(map[string]context.CancelFunc),
		pollerGenerations: make(map[string]int64),
		pollerScraper:     signal.NewScraper(c), // poller scraper — separate previous map
	}
}

// Reconcile is the main reconcile loop.
func (r *AdaptivePolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("policy", req.NamespacedName)

	// ── 1. Fetch ──────────────────────────────────────────────────────────────

	policy := &v1alpha1.AdaptivePolicy{}
	if err := r.Get(ctx, req.NamespacedName, policy); err != nil {
		if apierrors.IsNotFound(err) {
			key := req.NamespacedName.String()
			// Policy deleted — remove trigger state to prevent unbounded map growth.
			r.tsMu.Lock()
			delete(r.triggerState, key)
			r.tsMu.Unlock()
			// Stop the background poller for this policy.
			r.pollerMu.Lock()
			if cancel, ok := r.pollerCancels[key]; ok {
				cancel()
				delete(r.pollerCancels, key)
				delete(r.pollers, key)
			}
			r.pollerMu.Unlock()
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	logger.Info("reconciling",
		"activeProfile", policy.Spec.ActiveProfile,
		"dryRun", policy.Spec.DryRun,
		"meshBackend", policy.Spec.MeshBackend,
	)

	// ── 1.5. Validate policy ──────────────────────────────────────────────────

	if !r.validatePolicy(ctx, policy) {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// ── 2. Detect mesh ────────────────────────────────────────────────────────

	meshRenderer, err := renderer.DetectAndBuild(ctx, r.Client, policy)
	if err != nil {
		logger.Error(err, "mesh detection failed")
		_ = r.markDegraded(ctx, policy, "MeshDetectionFailed", err.Error())
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// ── 3. Read signals ───────────────────────────────────────────────────────

	// Retrieve (or lazily create) the per-policy trigger state under the mutex.
	// The pointer itself is stable after this block — the State struct is only
	// accessed from this policy's reconcile goroutine, so no further locking
	// is needed when reading/writing State fields via the extracted pointer.
	r.tsMu.Lock()
	if r.triggerState == nil {
		r.triggerState = make(map[string]*trigger.State)
	}
	key := req.NamespacedName.String()
	if _, ok := r.triggerState[key]; !ok {
		r.triggerState[key] = trigger.NewState()
	}
	policyState := r.triggerState[key]
	r.tsMu.Unlock()

	// Start the per-policy poller (idempotent — no-op if already running).
	poller := r.ensurePoller(key, policy)

	// Non-blocking: consume a debounced poller signal if one is ready.
	// Otherwise fall back to a direct scrape so status fields stay fresh.
	var signals trigger.Signals
	scrapeAvailable := true
	select {
	case signals = <-poller.Signals():
		logger.V(1).Info("poller signal ready",
			"successRate", signals.SuccessRate,
			"rps", signals.RPS,
		)
	default:
		var signalErr error
		signals, signalErr = r.scraper.ReadSignals(ctx, policy)
		if signalErr != nil {
			scrapeAvailable = false
			logger.Error(signalErr, "signal collection unavailable — triggers will not fire",
				"hint", "ensure the operator pod can reach pod IPs on TCP 15090 (NetworkPolicy may be blocking)")
			signals = trigger.Signals{
				SuccessRate: 0.99,
				RPS:         0,
				CollectedAt: time.Now(),
			}
		} else {
			logger.V(1).Info("signal read",
				"successRate", fmt.Sprintf("%.4f", signals.SuccessRate),
				"rps", fmt.Sprintf("%.1f", signals.RPS),
				"samples", signals.SampleCount,
			)
		}
	}

	// ── 4. Evaluate triggers ──────────────────────────────────────────────────

	// Capture before evaluateAndSwitch patches spec.activeProfile.
	originalProfile := policy.Spec.ActiveProfile

	decision, profileSwitched, switchErr := r.evaluateAndSwitch(ctx, policy, signals, policyState)
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

	if !profileSwitched {
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
	}

	// ── 6b. Prune orphaned resources ─────────────────────────────────────────────
	// Owner references cascade-delete resources only when the AdaptivePolicy
	// itself is deleted. When a filter is disabled mid-lifecycle (e.g. setting
	// admissionControl.enabled=false), its EnvoyFilter is no longer rendered
	// and must be explicitly deleted here. We compare the previous
	// status.managedResources (what existed before this reconcile) with the
	// current renderResult (what should exist now) and delete the difference.
	r.pruneOrphanedResources(ctx, policy, renderResult.ManagedResourceRefs)

	// ── 7. RTDS push (sub-200ms delivery) ─────────────────────────────────────

	if r.rtdsClient != nil {
		for layerName := range renderResult.RTDSLayers {
			r.rtdsClient.RegisterLayer(layerName, policy.Namespace, policy.Spec.Selector)
		}
	}

	rtdsConnected := false
	if profileSwitched && r.rtdsClient != nil && r.rtdsClient.Connected() && meshRenderer.RTDSSupported() {
		rtdsConnected = true
		for layerName, values := range renderResult.RTDSLayers {
			start := time.Now()
			if err := r.rtdsClient.Push(ctx, layerName, values); err != nil {
				// Non-fatal — EnvoyFilter re-render (step 6) already applied the change.
				// RTDS is the fast path; EnvoyFilter is the reliable fallback.
				logger.Error(err, "RTDS push failed, relying on EnvoyFilter path", "layer", layerName)
				rtdsConnected = false
			} else {
				deliveryMs := time.Since(start).Milliseconds()
				logger.Info("RTDS push succeeded", "layer", layerName, "deliveryMs", deliveryMs)
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

	// Shed rate — from actual envoy_http_admission_control_requests_ejected counter.
	// Distinct from the old success-rate estimate: this is zero when the filter
	// is not rejecting (whether healthy or misconfigured), non-zero only when
	// Envoy's admission_control is actively ejecting requests.
	policy.Status.ShedRateNow = actualShedRate(signals, policy)

	if decision.Reason != "" && decision.ShouldSwitch {
		deliveryMethod := "envoyfilter"
		if rtdsConnected && profileSwitched {
			deliveryMethod = "rtds"
		}
		record := &v1alpha1.DecisionRecord{
			Timestamp:      now,
			TriggerName:    decision.TriggerName,
			ProfileBefore:  originalProfile,
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

	if scrapeAvailable {
		setCondition(&policy.Status.Conditions, metav1.Condition{
			Type:               v1alpha1.ConditionSignalCollectionAvailable,
			Status:             metav1.ConditionTrue,
			Reason:             "ScrapingOK",
			Message:            "sidecar stats endpoints reachable on TCP 15090",
			ObservedGeneration: policy.Generation,
		})
	} else {
		setCondition(&policy.Status.Conditions, metav1.Condition{
			Type:               v1alpha1.ConditionSignalCollectionAvailable,
			Status:             metav1.ConditionFalse,
			Reason:             "ScrapingFailed",
			Message:            "cannot reach pod sidecar stats endpoints on TCP 15090 — triggers will not fire. Ensure NetworkPolicy allows operator→pod:15090. Check logs for the specific error.",
			ObservedGeneration: policy.Generation,
		})
	}

	// FilterEffective — detect filter installed but not processing traffic.
	// Only meaningful when admission_control is enabled, not dryRun, and we have
	// live signal data. Fires after 3 consecutive intervals of evidence.
	if policy.HasAdmissionControl() && !policy.IsDryRun() && scrapeAvailable {
		minRPS := float64(5)
		if policy.Spec.AdmissionControl.MinRequestsPerSecond > 0 {
			minRPS = float64(policy.Spec.AdmissionControl.MinRequestsPerSecond)
		}
		threshold := mustParsePercent(policy.EffectiveSuccessRateThreshold()) / 100.0

		shouldBeShedding := signals.RPS > minRPS && signals.SuccessRate < threshold
		filterIsEjecting := signals.ShedRejectedRPS > 0

		if shouldBeShedding && !filterIsEjecting {
			policyState.ConsecutiveFilterInactive++
		} else {
			policyState.ConsecutiveFilterInactive = 0
		}

		const filterInactiveThreshold = 3
		if policyState.ConsecutiveFilterInactive >= filterInactiveThreshold {
			setCondition(&policy.Status.Conditions, metav1.Condition{
				Type:   v1alpha1.ConditionFilterEffective,
				Status: metav1.ConditionFalse,
				Reason: "ZeroEjections",
				Message: fmt.Sprintf(
					"admission_control has rejected 0 requests over %d consecutive intervals "+
						"while successRate=%.3f is below threshold=%.3f and RPS=%.0f exceeds minimum=%.0f. "+
						"The filter may not be intercepting traffic. Verify with: "+
						"kubectl exec -n %s <pod> -c istio-proxy -- curl -s localhost:15000/stats | grep admission_control",
					policyState.ConsecutiveFilterInactive,
					signals.SuccessRate, threshold, signals.RPS, minRPS,
					policy.Namespace,
				),
				ObservedGeneration: policy.Generation,
			})
		} else if filterIsEjecting {
			setCondition(&policy.Status.Conditions, metav1.Condition{
				Type:               v1alpha1.ConditionFilterEffective,
				Status:             metav1.ConditionTrue,
				Reason:             "EjectionsObserved",
				Message:            fmt.Sprintf("admission_control is actively rejecting requests (%.1f/s)", signals.ShedRejectedRPS),
				ObservedGeneration: policy.Generation,
			})
		}
	}

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
	state *trigger.State,
) (trigger.Decision, bool, error) {

	// Human override — freeze all automatic switching
	if policy.IsHumanOverrideEnabled() {
		return trigger.Decision{Reason: "human-override active — automatic switching frozen"}, false, nil
	}

	if !policy.HasTriggers() {
		return trigger.Decision{Reason: "no triggers configured"}, false, nil
	}

	decision := trigger.Evaluate(policy, signals, state)

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

// actualShedRate returns the real rejection rate from the admission_control
// Envoy filter, derived from the envoy_http_admission_control_requests_ejected
// counter delta. Returns "0%" when no requests are being rejected, whether
// because the service is healthy or because the filter is not processing traffic.
// This replaces the old success-rate estimate which could mislead operators into
// thinking the filter was active when it was actually counting zero requests.
func actualShedRate(signals trigger.Signals, policy *v1alpha1.AdaptivePolicy) string {
	if !policy.HasAdmissionControl() || policy.IsDryRun() {
		return "0%"
	}
	if signals.ShedRejectedRPS <= 0 || signals.RPS <= 0 {
		return "0%"
	}
	// Rejected requests are counted in signals.RPS (they appear as 503s in
	// istio_requests_total). So total = RPS, and shed fraction = rejected/total.
	rate := signals.ShedRejectedRPS / signals.RPS * 100
	if rate > 100 {
		rate = 100
	}
	return fmt.Sprintf("~%.0f%%", rate)
}

// mustParsePercent parses a percent string like "95.0" to float64.
func mustParsePercent(s string) float64 {
	var f float64
	fmt.Sscanf(s, "%f", &f)
	return f
}

// deliveryMethodStr returns a human-readable delivery method string.
func deliveryMethodStr(rtdsConnected, profileSwitched bool) string {
	if profileSwitched && rtdsConnected {
		return "rtds (<200ms)"
	}
	return "envoyfilter"
}

// pruneOrphanedResources deletes mesh resources that existed in the previous
// reconcile but are no longer in the current render result.
//
// Owner references cascade-delete when the AdaptivePolicy is deleted, but they
// do NOT help when a filter is merely disabled (e.g. admissionControl.enabled=false).
// In that case the EnvoyFilter stays in the cluster enforcing the old config until
// explicitly deleted. This function closes that gap by comparing
// policy.Status.ManagedResources (old) with current (new) and deleting the diff.
func (r *AdaptivePolicyReconciler) pruneOrphanedResources(
	ctx context.Context,
	policy *v1alpha1.AdaptivePolicy,
	current []v1alpha1.ManagedResource,
) {
	logger := log.FromContext(ctx)

	// Build a set of currently-rendered resources keyed by Kind/Name.
	active := make(map[string]struct{}, len(current))
	for _, mr := range current {
		active[mr.Kind+"/"+mr.Name] = struct{}{}
	}

	for _, prev := range policy.Status.ManagedResources {
		if _, ok := active[prev.Kind+"/"+prev.Name]; ok {
			continue
		}
		obj := &unstructured.Unstructured{}
		obj.SetAPIVersion(prev.APIVersion)
		obj.SetKind(prev.Kind)
		obj.SetName(prev.Name)
		obj.SetNamespace(prev.Namespace)
		if err := r.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
			logger.Error(err, "failed to prune orphaned resource",
				"kind", prev.Kind, "name", prev.Name)
		} else if err == nil {
			logger.Info("pruned orphaned resource",
				"kind", prev.Kind, "name", prev.Name)
		}
	}
}

// pollerConfig converts the CRD's DetectionConfig into a signal.PollerConfig.
// Zero / nil values fall back to signal package defaults (applied in NewPoller).
func pollerConfig(d *v1alpha1.DetectionConfig) signal.PollerConfig {
	if d == nil {
		return signal.PollerConfig{} // all defaults
	}
	return signal.PollerConfig{
		PollInterval: func() time.Duration {
			if d.PollIntervalMs > 0 {
				return time.Duration(d.PollIntervalMs) * time.Millisecond
			}
			return signal.DefaultPollInterval
		}(),
		ConsecutiveBreaches: func() int {
			if d.ConsecutiveBreaches > 0 {
				return int(d.ConsecutiveBreaches)
			}
			return signal.DefaultConsecutiveBreaches
		}(),
		ConsecutiveRecoveries: func() int {
			if d.ConsecutiveRecoveries > 0 {
				return int(d.ConsecutiveRecoveries)
			}
			return signal.DefaultConsecutiveRecoveries
		}(),
	}
}

// ensurePoller returns the running Poller for this policy, starting one if needed.
// Uses a background context — the poller outlives individual reconcile calls and
// is cancelled explicitly when the policy is deleted.
func (r *AdaptivePolicyReconciler) ensurePoller(key string, policy *v1alpha1.AdaptivePolicy) *signal.Poller {
	r.pollerMu.Lock()
	defer r.pollerMu.Unlock()
	if r.pollers == nil {
		r.pollers = make(map[string]*signal.Poller)
		r.pollerCancels = make(map[string]context.CancelFunc)
		r.pollerGenerations = make(map[string]int64)
	}
	if p, ok := r.pollers[key]; ok {
		// Check generation — restart Poller if spec.detection changed
		if gen, ok := r.pollerGenerations[key]; ok && gen == policy.Generation {
			return p
		}
		// Generation changed — cancel old Poller
		if cancel, ok := r.pollerCancels[key]; ok {
			cancel()
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := signal.NewPoller(r.pollerScraper, policy, pollerConfig(policy.Spec.Detection))
	r.pollers[key] = p
	r.pollerCancels[key] = cancel
	r.pollerGenerations[key] = policy.Generation
	go p.Run(ctx)
	return p
}

// validatePolicy checks semantic invariants and surfaces violations as Degraded
// conditions. Returns false when reconciliation should halt (misconfiguration that
// would result in a no-op or a guaranteed mis-delivery).
func (r *AdaptivePolicyReconciler) validatePolicy(ctx context.Context, policy *v1alpha1.AdaptivePolicy) bool {
	// A policy with no active filters does nothing and is almost certainly a mistake.
	if !policy.HasAdmissionControl() && !policy.HasAdaptiveConcurrency() && !policy.HasStreamingProtection() {
		_ = r.markDegraded(ctx, policy, "NoActiveFilters",
			"no filters are configured — this policy has no effect. "+
				"Set admissionControl.enabled, adaptiveConcurrency.enabled, or "+
				"streamingProtection.enabled to true.")
		return false
	}

	// activeProfile must exist in spec.profiles when set.
	if policy.Spec.ActiveProfile != "" {
		if _, ok := policy.Spec.Profiles[policy.Spec.ActiveProfile]; !ok {
			names := profileNames(policy.Spec.Profiles)
			msg := fmt.Sprintf("activeProfile %q not found in spec.profiles", policy.Spec.ActiveProfile)
			if names != "" {
				msg += fmt.Sprintf(" — defined profiles: %s", names)
			} else {
				msg += " — spec.profiles is empty"
			}
			_ = r.markDegraded(ctx, policy, "InvalidActiveProfile", msg)
			return false
		}
	}

	// trigger.switchTo and schedule.switchTo must reference profiles that exist.
	// A trigger that fires and switches to a non-existent profile silently falls
	// back to baseline, masking the misconfiguration.
	for _, t := range policy.Spec.Triggers {
		if _, ok := policy.Spec.Profiles[t.SwitchTo]; !ok {
			_ = r.markDegraded(ctx, policy, "InvalidTrigger",
				fmt.Sprintf("trigger %q switchTo %q not found in spec.profiles — "+
					"define the profile or correct the name. Defined profiles: %s",
					t.Name, t.SwitchTo, profileNames(policy.Spec.Profiles)))
			return false
		}
	}
	for _, s := range policy.Spec.Schedules {
		if _, ok := policy.Spec.Profiles[s.SwitchTo]; !ok {
			_ = r.markDegraded(ctx, policy, "InvalidSchedule",
				fmt.Sprintf("schedule %q switchTo %q not found in spec.profiles — "+
					"define the profile or correct the name. Defined profiles: %s",
					s.Name, s.SwitchTo, profileNames(policy.Spec.Profiles)))
			return false
		}
	}

	return true
}

// profileNames returns a sorted, comma-joined list of profile names for error messages.
func profileNames(profiles map[string]v1alpha1.ProfileConfig) string {
	if len(profiles) == 0 {
		return ""
	}
	names := make([]string, 0, len(profiles))
	for name := range profiles {
		names = append(names, name)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
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
