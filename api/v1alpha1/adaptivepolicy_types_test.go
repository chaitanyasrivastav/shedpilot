package v1alpha1

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ─── Helpers ──────────────────────────────────────────────────────────────────

func strPtr(s string) *string { return &s }
func i32Ptr(i int32) *int32   { return &i }

func basePolicy(mutate func(*AdaptivePolicy)) *AdaptivePolicy {
	ap := &AdaptivePolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "payments",
			Namespace: "production",
		},
		Spec: AdaptivePolicySpec{
			Selector: map[string]string{"app": "payments"},
		},
	}
	if mutate != nil {
		mutate(ap)
	}
	return ap
}

func fullPolicy() *AdaptivePolicy {
	return &AdaptivePolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "payments",
			Namespace: "production",
			Annotations: map[string]string{
				AnnotationHumanOverride: "false",
			},
		},
		Spec: AdaptivePolicySpec{
			Selector:    map[string]string{"app": "payments"},
			MeshBackend: MeshBackendIstio,
			MeshMode:    MeshModeSidecar,
			DryRun:      false,
			AdaptiveConcurrency: &AdaptiveConcurrencyConfig{
				Enabled:                   true,
				LatencyPercentile:         PercentileP50,
				LatencyBaselineInterval:   "60s",
				LatencyBaselineSampleSize: 50,
				ConcurrencyAdjustInterval: "100ms",
				MaxLoadIncrease:           "2.0",
				ConcurrencyLimit:          0,
				MeasurementJitter:         10,
			},
			AdmissionControl: &AdmissionControlConfig{
				Enabled:              true,
				SuccessRateWindow:    "30s",
				SuccessRateThreshold: "95.0",
				SheddingSpeed:        "1.5",
				MinRequestsPerSecond: 5,
				MaxRejectionPercent:  "80.0",
				SuccessCodes: []HTTPStatusRange{
					{Start: 100, End: 399},
				},
			},
			StreamingProtection: &StreamingProtectionConfig{
				Enabled:              true,
				MaxConcurrentStreams: 200,
				StreamTimeoutSeconds: 300,
				MaxPendingRequests:   1024,
			},
			Profiles: map[string]ProfileConfig{
				"normal": {
					AdmissionControl: &AdmissionControlOverride{
						SuccessRateThreshold: "95.0",
						SheddingSpeed:        "1.5",
					},
				},
				"degraded": {
					AdmissionControl: &AdmissionControlOverride{
						SuccessRateThreshold: "85.0",
						SheddingSpeed:        "2.0",
						SuccessRateWindow:    "20s",
					},
					AdaptiveConcurrency: &AdaptiveConcurrencyOverride{
						LatencyPercentile: PercentileP75,
					},
				},
				"critical": {
					AdmissionControl: &AdmissionControlOverride{
						SuccessRateThreshold: "75.0",
						SheddingSpeed:        "3.0",
					},
				},
				"flash-sale": {
					AdmissionControl: &AdmissionControlOverride{
						SuccessRateThreshold: "90.0",
						SheddingSpeed:        "1.8",
					},
				},
			},
			ActiveProfile: "normal",
			Triggers: []TriggerConfig{
				{
					Name: "degradation-detected",
					When: TriggerCondition{
						SuccessRate: &RateCondition{
							Below:              "0.90",
							ConsecutiveSamples: 2,
						},
					},
					SwitchTo:        "degraded",
					CooldownSeconds: 60,
				},
				{
					Name: "critical-degradation",
					When: TriggerCondition{
						SuccessRate: &RateCondition{
							Below:              "0.80",
							ConsecutiveSamples: 2,
						},
					},
					SwitchTo:        "critical",
					CooldownSeconds: 60,
				},
				{
					Name: "recovery",
					When: TriggerCondition{
						SuccessRate: &RateCondition{
							Above:              "0.97",
							ConsecutiveSamples: 3,
						},
					},
					SwitchTo:     "normal",
					FromProfiles: []string{"degraded", "critical"},
				},
			},
			Schedules: []ScheduleConfig{
				{
					Name:     "friday-flash-sale",
					Cron:     "50 13 * * 5",
					SwitchTo: "flash-sale",
				},
				{
					Name:     "friday-sale-end",
					Cron:     "0 16 * * 5",
					SwitchTo: "normal",
				},
			},
			SignalConfig: &SignalConfig{
				EvaluationIntervalSeconds: 30,
				CapacityWarningPercent:    10,
				CapacityWarningWindowDays: 7,
			},
		},
	}
}

// ─── Percentile ───────────────────────────────────────────────────────────────

func TestPercentile_EnvoyValue(t *testing.T) {
	tests := []struct {
		percentile Percentile
		want       float64
	}{
		{PercentileP50, 50},
		{PercentileP75, 75},
		{PercentileP90, 90},
		{PercentileP99, 99},
		{Percentile("unknown"), 50}, // unknown → p50 default
		{Percentile(""), 50},        // empty → p50 default
	}
	for _, tt := range tests {
		t.Run(string(tt.percentile), func(t *testing.T) {
			if got := tt.percentile.EnvoyValue(); got != tt.want {
				t.Errorf("EnvoyValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ─── IsHumanOverrideEnabled ───────────────────────────────────────────────────

func TestIsHumanOverrideEnabled(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		want        bool
	}{
		{"nil annotations", nil, false},
		{"empty annotations", map[string]string{}, false},
		{"override false", map[string]string{AnnotationHumanOverride: "false"}, false},
		{"override true", map[string]string{AnnotationHumanOverride: "true"}, true},
		{"override TRUE (case sensitive)", map[string]string{AnnotationHumanOverride: "TRUE"}, false},
		{"unrelated annotation", map[string]string{"other.io/key": "true"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ap := basePolicy(func(ap *AdaptivePolicy) { ap.Annotations = tt.annotations })
			if got := ap.IsHumanOverrideEnabled(); got != tt.want {
				t.Errorf("IsHumanOverrideEnabled() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ─── Boolean predicates ───────────────────────────────────────────────────────

func TestIsDryRun(t *testing.T) {
	tests := []struct{ dryRun, want bool }{
		{false, false},
		{true, true},
	}
	for _, tt := range tests {
		ap := basePolicy(func(ap *AdaptivePolicy) { ap.Spec.DryRun = tt.dryRun })
		if got := ap.IsDryRun(); got != tt.want {
			t.Errorf("IsDryRun(%v) = %v, want %v", tt.dryRun, got, tt.want)
		}
	}
}

func TestHasAdaptiveConcurrency(t *testing.T) {
	tests := []struct {
		name string
		cfg  *AdaptiveConcurrencyConfig
		want bool
	}{
		{"nil", nil, false},
		{"disabled", &AdaptiveConcurrencyConfig{Enabled: false}, false},
		{"enabled", &AdaptiveConcurrencyConfig{Enabled: true}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ap := basePolicy(func(ap *AdaptivePolicy) { ap.Spec.AdaptiveConcurrency = tt.cfg })
			if got := ap.HasAdaptiveConcurrency(); got != tt.want {
				t.Errorf("HasAdaptiveConcurrency() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHasAdmissionControl(t *testing.T) {
	tests := []struct {
		name string
		cfg  *AdmissionControlConfig
		want bool
	}{
		{"nil", nil, false},
		{"disabled", &AdmissionControlConfig{Enabled: false}, false},
		{"enabled", &AdmissionControlConfig{Enabled: true}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ap := basePolicy(func(ap *AdaptivePolicy) { ap.Spec.AdmissionControl = tt.cfg })
			if got := ap.HasAdmissionControl(); got != tt.want {
				t.Errorf("HasAdmissionControl() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHasStreamingProtection(t *testing.T) {
	tests := []struct {
		name string
		cfg  *StreamingProtectionConfig
		want bool
	}{
		{"nil", nil, false},
		{"disabled", &StreamingProtectionConfig{Enabled: false}, false},
		{"enabled", &StreamingProtectionConfig{Enabled: true}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ap := basePolicy(func(ap *AdaptivePolicy) { ap.Spec.StreamingProtection = tt.cfg })
			if got := ap.HasStreamingProtection(); got != tt.want {
				t.Errorf("HasStreamingProtection() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHasProfiles(t *testing.T) {
	ap := basePolicy(nil)
	if ap.HasProfiles() {
		t.Error("expected HasProfiles()=false for empty profiles")
	}
	ap.Spec.Profiles = map[string]ProfileConfig{"degraded": {}}
	if !ap.HasProfiles() {
		t.Error("expected HasProfiles()=true for non-empty profiles")
	}
}

func TestHasTriggers(t *testing.T) {
	ap := basePolicy(nil)
	if ap.HasTriggers() {
		t.Error("expected HasTriggers()=false for empty triggers")
	}
	ap.Spec.Triggers = []TriggerConfig{{Name: "t1", SwitchTo: "degraded"}}
	if !ap.HasTriggers() {
		t.Error("expected HasTriggers()=true for non-empty triggers")
	}
}

func TestHasSchedules(t *testing.T) {
	ap := basePolicy(nil)
	if ap.HasSchedules() {
		t.Error("expected HasSchedules()=false for empty schedules")
	}
	ap.Spec.Schedules = []ScheduleConfig{{Name: "s1", Cron: "0 * * * *", SwitchTo: "normal"}}
	if !ap.HasSchedules() {
		t.Error("expected HasSchedules()=true for non-empty schedules")
	}
}

// ─── EffectiveMeshMode / MeshBackend ─────────────────────────────────────────

func TestEffectiveMeshMode(t *testing.T) {
	tests := []struct{ mode, want MeshMode }{
		{"", MeshModeSidecar},
		{MeshModeSidecar, MeshModeSidecar},
		{MeshModeAmbient, MeshModeAmbient},
	}
	for _, tt := range tests {
		ap := basePolicy(func(ap *AdaptivePolicy) { ap.Spec.MeshMode = tt.mode })
		if got := ap.EffectiveMeshMode(); got != tt.want {
			t.Errorf("EffectiveMeshMode(%q) = %q, want %q", tt.mode, got, tt.want)
		}
	}
}

func TestEffectiveMeshBackend(t *testing.T) {
	tests := []struct{ backend, want MeshBackend }{
		{"", MeshBackendAuto},
		{MeshBackendAuto, MeshBackendAuto},
		{MeshBackendIstio, MeshBackendIstio},
		{MeshBackendCilium, MeshBackendCilium},
	}
	for _, tt := range tests {
		ap := basePolicy(func(ap *AdaptivePolicy) { ap.Spec.MeshBackend = tt.backend })
		if got := ap.EffectiveMeshBackend(); got != tt.want {
			t.Errorf("EffectiveMeshBackend(%q) = %q, want %q", tt.backend, got, tt.want)
		}
	}
}

// ─── ActiveProfileConfig ─────────────────────────────────────────────────────

func TestActiveProfileConfig(t *testing.T) {
	ap := fullPolicy()

	// active profile = "normal" — should find it
	ap.Spec.ActiveProfile = "normal"
	profile, ok := ap.ActiveProfileConfig()
	if !ok {
		t.Fatal("expected to find normal profile")
	}
	if profile.AdmissionControl.SuccessRateThreshold != "95.0" {
		t.Errorf("expected 95.0, got %q", profile.AdmissionControl.SuccessRateThreshold)
	}

	// active profile = "degraded" — should find overrides
	ap.Spec.ActiveProfile = "degraded"
	profile, ok = ap.ActiveProfileConfig()
	if !ok {
		t.Fatal("expected to find degraded profile")
	}
	if profile.AdmissionControl.SuccessRateThreshold != "85.0" {
		t.Errorf("expected 85.0, got %q", profile.AdmissionControl.SuccessRateThreshold)
	}
	if profile.AdaptiveConcurrency.LatencyPercentile != PercentileP75 {
		t.Errorf("expected p75, got %q", profile.AdaptiveConcurrency.LatencyPercentile)
	}

	// no active profile
	ap.Spec.ActiveProfile = ""
	_, ok = ap.ActiveProfileConfig()
	if ok {
		t.Error("expected ok=false when no active profile")
	}

	// non-existent profile
	ap.Spec.ActiveProfile = "nonexistent"
	_, ok = ap.ActiveProfileConfig()
	if ok {
		t.Error("expected ok=false for non-existent profile name")
	}
}

// ─── Effective* methods — profile override priority ───────────────────────────

func TestEffectiveSuccessRateThreshold(t *testing.T) {
	tests := []struct {
		name          string
		activeProfile string
		want          string
	}{
		{"no profile — baseline", "", "95.0"},
		{"normal profile", "normal", "95.0"},
		{"degraded profile overrides", "degraded", "85.0"},
		{"critical profile overrides", "critical", "75.0"},
		{"flash-sale profile overrides", "flash-sale", "90.0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ap := fullPolicy()
			ap.Spec.ActiveProfile = tt.activeProfile
			if got := ap.EffectiveSuccessRateThreshold(); got != tt.want {
				t.Errorf("EffectiveSuccessRateThreshold() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestEffectiveSuccessRateThreshold_FallbackDefaults(t *testing.T) {
	// No baseline config, no profile — should return "95.0" hardcoded default
	ap := basePolicy(nil)
	if got := ap.EffectiveSuccessRateThreshold(); got != "95.0" {
		t.Errorf("expected hardcoded default 95.0, got %q", got)
	}
}

func TestEffectiveSheddingSpeed(t *testing.T) {
	ap := fullPolicy()

	ap.Spec.ActiveProfile = ""
	if got := ap.EffectiveSheddingSpeed(); got != "1.5" {
		t.Errorf("baseline sheddingSpeed = %q, want 1.5", got)
	}

	ap.Spec.ActiveProfile = "degraded"
	if got := ap.EffectiveSheddingSpeed(); got != "2.0" {
		t.Errorf("degraded sheddingSpeed = %q, want 2.0", got)
	}

	ap.Spec.ActiveProfile = "critical"
	if got := ap.EffectiveSheddingSpeed(); got != "3.0" {
		t.Errorf("critical sheddingSpeed = %q, want 3.0", got)
	}
}

func TestEffectiveSuccessRateWindow(t *testing.T) {
	ap := fullPolicy()

	ap.Spec.ActiveProfile = ""
	if got := ap.EffectiveSuccessRateWindow(); got != "30s" {
		t.Errorf("baseline success rate calculation window = %q, want 30s", got)
	}

	ap.Spec.ActiveProfile = "degraded"
	if got := ap.EffectiveSuccessRateWindow(); got != "20s" {
		t.Errorf("degraded success rate calculation window = %q, want 20s", got)
	}

	// normal profile has no success rate calculation window override — should inherit baseline
	ap.Spec.ActiveProfile = "normal"
	if got := ap.EffectiveSuccessRateWindow(); got != "30s" {
		t.Errorf("normal (no override) success rate calculation window = %q, want 30s", got)
	}
}

func TestEffectiveLatencyPercentile(t *testing.T) {
	ap := fullPolicy()

	ap.Spec.ActiveProfile = ""
	if got := ap.EffectiveLatencyPercentile(); got != PercentileP50 {
		t.Errorf("baseline percentile = %q, want p50", got)
	}

	ap.Spec.ActiveProfile = "degraded"
	if got := ap.EffectiveLatencyPercentile(); got != PercentileP75 {
		t.Errorf("degraded percentile = %q, want p75", got)
	}

	// critical has no percentile override — inherits baseline p50
	ap.Spec.ActiveProfile = "critical"
	if got := ap.EffectiveLatencyPercentile(); got != PercentileP50 {
		t.Errorf("critical (no override) percentile = %q, want p50", got)
	}
}

// ─── Trigger structure validation ────────────────────────────────────────────

func TestTriggerConfig_Structure(t *testing.T) {
	ap := fullPolicy()

	if len(ap.Spec.Triggers) != 3 {
		t.Fatalf("expected 3 triggers, got %d", len(ap.Spec.Triggers))
	}

	deg := ap.Spec.Triggers[0]
	if deg.Name != "degradation-detected" {
		t.Errorf("trigger[0].Name = %q, want degradation-detected", deg.Name)
	}
	if deg.SwitchTo != "degraded" {
		t.Errorf("trigger[0].SwitchTo = %q, want degraded", deg.SwitchTo)
	}
	if deg.When.SuccessRate == nil {
		t.Fatal("trigger[0].When.SuccessRate is nil")
	}
	if deg.When.SuccessRate.Below != "0.90" {
		t.Errorf("trigger[0] below = %v, want 0.90", deg.When.SuccessRate.Below)
	}
	if deg.When.SuccessRate.ConsecutiveSamples != 2 {
		t.Errorf("trigger[0] consecutiveSamples = %v, want 2", deg.When.SuccessRate.ConsecutiveSamples)
	}

	rec := ap.Spec.Triggers[2]
	if rec.Name != "recovery" {
		t.Errorf("trigger[2].Name = %q, want recovery", rec.Name)
	}
	if len(rec.FromProfiles) != 2 {
		t.Errorf("recovery trigger fromProfiles len = %d, want 2", len(rec.FromProfiles))
	}
	if rec.When.SuccessRate.Above == "" {
		t.Fatal("recovery trigger.Above is empty")
	}
	if rec.When.SuccessRate.Above != "0.97" {
		t.Errorf("recovery trigger above = %v, want 0.97", rec.When.SuccessRate.Above)
	}
}

// ─── Schedule structure validation ───────────────────────────────────────────

func TestScheduleConfig_Structure(t *testing.T) {
	ap := fullPolicy()

	if len(ap.Spec.Schedules) != 2 {
		t.Fatalf("expected 2 schedules, got %d", len(ap.Spec.Schedules))
	}

	sale := ap.Spec.Schedules[0]
	if sale.Name != "friday-flash-sale" {
		t.Errorf("schedule[0].Name = %q", sale.Name)
	}
	if sale.Cron != "50 13 * * 5" {
		t.Errorf("schedule[0].Cron = %q", sale.Cron)
	}
	if sale.SwitchTo != "flash-sale" {
		t.Errorf("schedule[0].SwitchTo = %q", sale.SwitchTo)
	}
}

// ─── Constants completeness ───────────────────────────────────────────────────

func TestAnnotationConstants_NonEmpty(t *testing.T) {
	constants := map[string]string{
		"AnnotationHumanOverride":     AnnotationHumanOverride,
		"AnnotationLastReason":        AnnotationLastReason,
		"AnnotationLastActionTime":    AnnotationLastActionTime,
		"AnnotationPreviousProfile":   AnnotationPreviousProfile,
		"AnnotationManagedBy":         AnnotationManagedBy,
		"AnnotationIncidentStart":     AnnotationIncidentStart,
		"AnnotationIncidentDuration":  AnnotationIncidentDuration,
		"AnnotationPeakRejectionRate": AnnotationPeakRejectionRate,
		"AnnotationBrainMode":         AnnotationBrainMode,
	}
	for name, val := range constants {
		if val == "" {
			t.Errorf("annotation constant %s is empty string", name)
		}
	}
}

func TestConditionConstants_NonEmpty(t *testing.T) {
	constants := map[string]string{
		"ConditionReady":              ConditionReady,
		"ConditionDegraded":           ConditionDegraded,
		"ConditionMeshDetected":       ConditionMeshDetected,
		"ConditionProfileActive":      ConditionProfileActive,
		"ConditionScalabilityWarning": ConditionScalabilityWarning,
	}
	for name, val := range constants {
		if val == "" {
			t.Errorf("condition constant %s is empty string", name)
		}
	}
}

// ─── Full policy round-trip ───────────────────────────────────────────────────

func TestFullPolicy_AllPredicates(t *testing.T) {
	ap := fullPolicy()

	if !ap.HasAdaptiveConcurrency() {
		t.Error("HasAdaptiveConcurrency() = false")
	}
	if !ap.HasAdmissionControl() {
		t.Error("HasAdmissionControl() = false")
	}
	if !ap.HasStreamingProtection() {
		t.Error("HasStreamingProtection() = false")
	}
	if !ap.HasProfiles() {
		t.Error("HasProfiles() = false")
	}
	if !ap.HasTriggers() {
		t.Error("HasTriggers() = false")
	}
	if !ap.HasSchedules() {
		t.Error("HasSchedules() = false")
	}
	if ap.IsDryRun() {
		t.Error("IsDryRun() = true, want false")
	}
	if ap.IsHumanOverrideEnabled() {
		t.Error("IsHumanOverrideEnabled() = true, want false")
	}
	if ap.EffectiveMeshMode() != MeshModeSidecar {
		t.Errorf("EffectiveMeshMode() = %q", ap.EffectiveMeshMode())
	}
	if ap.EffectiveMeshBackend() != MeshBackendIstio {
		t.Errorf("EffectiveMeshBackend() = %q", ap.EffectiveMeshBackend())
	}
}

// ─── DecisionRecord ───────────────────────────────────────────────────────────

func TestDecisionRecord_Structure(t *testing.T) {
	ap := fullPolicy()
	now := metav1.Now()
	ap.Status.LastDecision = &DecisionRecord{
		Timestamp:         now,
		TriggerName:       "degradation-detected",
		ProfileBefore:     "normal",
		ProfileAfter:      "degraded",
		SignalValues:      "successRate=0.882 (below 0.90 for 2 consecutive samples)",
		DeliveryMethod:    "rtds",
		DeliveryLatencyMs: 143,
		Outcome:           "service_recovered",
		OutcomeDetail:     "success rate returned to 97.2% within 4 minutes",
	}

	d := ap.Status.LastDecision
	if d.TriggerName != "degradation-detected" {
		t.Errorf("TriggerName = %q", d.TriggerName)
	}
	if d.DeliveryMethod != "rtds" {
		t.Errorf("DeliveryMethod = %q", d.DeliveryMethod)
	}
	if d.DeliveryLatencyMs != 143 {
		t.Errorf("DeliveryLatencyMs = %d", d.DeliveryLatencyMs)
	}
	if d.Outcome != "service_recovered" {
		t.Errorf("Outcome = %q", d.Outcome)
	}
}
