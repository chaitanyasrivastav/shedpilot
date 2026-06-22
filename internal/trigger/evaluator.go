// Package trigger evaluates AdaptivePolicy trigger conditions against live
// signal values and determines whether a profile switch should occur.
package trigger

import (
	"fmt"
	"strconv"
	"time"

	"github.com/chaitanyasrivastav/shedpilot/api/v1alpha1"
)

// Signals holds the live metric values read from the service.
type Signals struct {
	SuccessRate      float64
	ServiceLatencyMs float64
	TotalLatencyMs   float64
	RPS              float64
	SampleCount      int64
	CollectedAt      time.Time

	// ShedRejectedRPS is the rate of requests actually rejected by the
	// admission_control Envoy filter (from envoy_http_admission_control_requests_ejected).
	// Zero means the filter is either not shedding or not processing any traffic.
	// Distinct from the SuccessRate-based estimate — this is the ground truth.
	ShedRejectedRPS float64
}

// Decision is what the evaluator decided should happen.
type Decision struct {
	ShouldSwitch          bool
	TargetProfile         string
	TriggerName           string
	Reason                string
	ConsecutiveBadSamples int32
}

// State tracks per-trigger evaluation state across reconcile loops.
type State struct {
	ConsecutiveBadSamples map[string]int32
	LastTriggerFireTime   map[string]time.Time

	// ConsecutiveFilterInactive counts intervals where admission_control should
	// be rejecting requests (success rate below threshold, RPS above minimum)
	// but ShedRejectedRPS is zero. Used to set the FilterEffective condition.
	ConsecutiveFilterInactive int32
}

// NewState creates an empty evaluation state.
func NewState() *State {
	return &State{
		ConsecutiveBadSamples: make(map[string]int32),
		LastTriggerFireTime:   make(map[string]time.Time),
	}
}

// Evaluate checks all triggers in order, returns the first actionable decision.
// Triggers are evaluated in spec order — first matching trigger wins.
// All trigger counters are updated on every call regardless of which trigger fires,
// so that a low-priority trigger accumulating samples does not block a higher-priority
// trigger from firing in the same reconcile loop.
func Evaluate(policy *v1alpha1.AdaptivePolicy, signals Signals, state *State) Decision {
	if signals.SampleCount == 0 {
		return Decision{
			Reason: "skipping evaluation — no samples collected this interval",
		}
	}

	if !policy.HasTriggers() {
		return Decision{Reason: "no triggers configured"}
	}

	currentProfile := policy.Spec.ActiveProfile

	// closestToFiring holds the first trigger that has partial consecutive samples
	// but hasn't fired yet — used for status visibility when no trigger fires.
	var closestToFiring *Decision

	for _, trigger := range policy.Spec.Triggers {
		// fromProfiles constraint — only fire from listed profiles
		if len(trigger.FromProfiles) > 0 && !contains(trigger.FromProfiles, currentProfile) {
			continue
		}
		// no-op if already in target
		if currentProfile == trigger.SwitchTo {
			continue
		}
		// cooldown — don't re-fire within window
		if lastFired, ok := state.LastTriggerFireTime[trigger.Name]; ok {
			if time.Since(lastFired) < time.Duration(trigger.CooldownSeconds)*time.Second {
				continue
			}
		}

		met, reason := conditionMet(trigger.When, signals)
		if met {
			state.ConsecutiveBadSamples[trigger.Name]++
		} else {
			state.ConsecutiveBadSamples[trigger.Name] = 0
		}

		consecutive := state.ConsecutiveBadSamples[trigger.Name]
		required := requiredSamples(trigger.When)

		if met && consecutive >= required {
			// Trigger fires — first match wins
			state.LastTriggerFireTime[trigger.Name] = time.Now()
			state.ConsecutiveBadSamples[trigger.Name] = 0
			return Decision{
				ShouldSwitch:          true,
				TargetProfile:         trigger.SwitchTo,
				TriggerName:           trigger.Name,
				Reason:                fmt.Sprintf("%s (%d consecutive samples)", reason, consecutive),
				ConsecutiveBadSamples: consecutive,
			}
		}

		// Track first trigger with partial progress for status visibility.
		// Do NOT return early — all triggers must be evaluated so their counters
		// are updated and any later trigger that has reached its threshold can fire.
		if consecutive > 0 && closestToFiring == nil {
			d := Decision{
				ShouldSwitch:          false,
				TriggerName:           trigger.Name,
				Reason:                fmt.Sprintf("%s — %d/%d consecutive samples", reason, consecutive, required),
				ConsecutiveBadSamples: consecutive,
			}
			closestToFiring = &d
		}
	}

	if closestToFiring != nil {
		return *closestToFiring
	}

	return Decision{
		Reason: fmt.Sprintf("no conditions met — successRate=%.3f rps=%.0f", signals.SuccessRate, signals.RPS),
	}
}

// conditionMet evaluates a TriggerCondition against live signals.
// All sub-conditions are ANDed — all must be true to fire.
// Returns (conditionMet bool, humanReadableReason string).
func conditionMet(cond v1alpha1.TriggerCondition, signals Signals) (bool, string) {
	var results []bool
	var reasons []string

	if sr := cond.SuccessRate; sr != nil {
		if sr.Below != "" {
			threshold := mustParseRate(sr.Below)
			met := signals.SuccessRate < threshold
			results = append(results, met)
			reasons = append(reasons, fmt.Sprintf(
				"successRate=%.3f %s %s", signals.SuccessRate, ltgt(met, true), sr.Below,
			))
		}
		if sr.Above != "" {
			threshold := mustParseRate(sr.Above)
			met := signals.SuccessRate > threshold
			results = append(results, met)
			reasons = append(reasons, fmt.Sprintf(
				"successRate=%.3f %s %s", signals.SuccessRate, ltgt(met, false), sr.Above,
			))
		}
	}

	if lat := cond.ServiceLatencyMs; lat != nil {
		if lat.Above != "" {
			threshold := mustParseThreshold(lat.Above)
			met := signals.ServiceLatencyMs > threshold
			results = append(results, met)
			reasons = append(reasons, fmt.Sprintf(
				"ownLatency=%.0fms %s %sms", signals.ServiceLatencyMs, ltgt(met, false), lat.Above,
			))
		}
		if lat.Below != "" {
			threshold := mustParseThreshold(lat.Below)
			met := signals.ServiceLatencyMs < threshold
			results = append(results, met)
			reasons = append(reasons, fmt.Sprintf(
				"ownLatency=%.0fms %s %sms", signals.ServiceLatencyMs, ltgt(met, true), lat.Below,
			))
		}
	}

	if cond.RPSAbove != nil {
		met := signals.RPS > float64(*cond.RPSAbove)
		results = append(results, met)
		reasons = append(reasons, fmt.Sprintf("rps=%.0f above %d", signals.RPS, *cond.RPSAbove))
	}

	if len(results) == 0 {
		return false, "no conditions defined"
	}

	// AND semantics — all conditions must be met
	allMet := true
	for _, r := range results {
		if !r {
			allMet = false
			break
		}
	}

	reason := ""
	for i, r := range reasons {
		if i > 0 {
			reason += ", "
		}
		reason += r
	}
	return allMet, reason
}

// requiredSamples returns the consecutiveSamples required by the condition.
func requiredSamples(cond v1alpha1.TriggerCondition) int32 {
	if sr := cond.SuccessRate; sr != nil && sr.ConsecutiveSamples > 0 {
		return sr.ConsecutiveSamples
	}
	if lat := cond.ServiceLatencyMs; lat != nil && lat.ConsecutiveSamples > 0 {
		return lat.ConsecutiveSamples
	}
	return 1
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// mustParseRate parses a rate string like "0.90" to float64.
// Returns 0 on error — validation markers ensure these are always valid.
func mustParseRate(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
}

// mustParseThreshold parses a threshold string like "200.0" to float64.
func mustParseThreshold(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
}

// ltgt returns the comparison operator string for human-readable reasons.
// forBelow=true → "below threshold" direction (<), false → "above" direction (>)
func ltgt(conditionMet bool, forBelow bool) string {
	if forBelow {
		if conditionMet {
			return "<"
		}
		return "≥"
	}
	if conditionMet {
		return ">"
	}
	return "≤"
}

func contains(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}
