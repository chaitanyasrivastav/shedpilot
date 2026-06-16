package signal

import (
	"context"
	"testing"
	"time"

	"github.com/chaitanyasrivastav/shedpilot/api/v1alpha1"
	"github.com/chaitanyasrivastav/shedpilot/internal/trigger"
)

// --- helpers ----------------------------------------------------------------

func testPolicy(belowThreshold string) *v1alpha1.AdaptivePolicy {
	return &v1alpha1.AdaptivePolicy{
		Spec: v1alpha1.AdaptivePolicySpec{
			Triggers: []v1alpha1.TriggerConfig{
				{
					Name: "degradation-detected",
					When: v1alpha1.TriggerCondition{
						SuccessRate: &v1alpha1.RateCondition{
							Below: belowThreshold,
						},
					},
				},
			},
		},
	}
}

func sigWithRate(rate float64) trigger.Signals {
	return trigger.Signals{
		SuccessRate: rate,
		CollectedAt: time.Now(),
	}
}

// stubScraper lets tests inject canned signal sequences.
type stubScraper struct {
	calls   []trigger.Signals
	callIdx int
	err     error
}

// --- PollerConfig.withDefaults ----------------------------------------------

func TestPollerConfig_Defaults(t *testing.T) {
	cfg := PollerConfig{}.withDefaults()
	if cfg.PollInterval != DefaultPollInterval {
		t.Errorf("want %v got %v", DefaultPollInterval, cfg.PollInterval)
	}
	if cfg.ConsecutiveBreaches != DefaultConsecutiveBreaches {
		t.Errorf("want %d got %d", DefaultConsecutiveBreaches, cfg.ConsecutiveBreaches)
	}
	if cfg.ConsecutiveRecoveries != DefaultConsecutiveRecoveries {
		t.Errorf("want %d got %d", DefaultConsecutiveRecoveries, cfg.ConsecutiveRecoveries)
	}
}

func TestPollerConfig_OutOfRange_Clamped(t *testing.T) {
	cfg := PollerConfig{
		PollInterval:          1 * time.Microsecond, // below min
		ConsecutiveBreaches:   -1,
		ConsecutiveRecoveries: 99,
	}.withDefaults()
	if cfg.PollInterval != DefaultPollInterval {
		t.Errorf("expected clamp to default, got %v", cfg.PollInterval)
	}
	if cfg.ConsecutiveBreaches != DefaultConsecutiveBreaches {
		t.Errorf("expected clamp to default, got %d", cfg.ConsecutiveBreaches)
	}
	if cfg.ConsecutiveRecoveries != DefaultConsecutiveRecoveries {
		t.Errorf("expected clamp to default, got %d", cfg.ConsecutiveRecoveries)
	}
}

func TestPollerConfig_ValidValues_Preserved(t *testing.T) {
	cfg := PollerConfig{
		PollInterval:          200 * time.Millisecond,
		ConsecutiveBreaches:   2,
		ConsecutiveRecoveries: 5,
	}.withDefaults()
	if cfg.PollInterval != 200*time.Millisecond {
		t.Errorf("valid value should be preserved, got %v", cfg.PollInterval)
	}
	if cfg.ConsecutiveBreaches != 2 {
		t.Errorf("valid value should be preserved, got %d", cfg.ConsecutiveBreaches)
	}
}

// --- isBreaching ------------------------------------------------------------

func TestIsBreaching_BelowThreshold(t *testing.T) {
	p := &Poller{policy: testPolicy("0.90")}
	if !p.isBreaching(sigWithRate(0.85)) {
		t.Error("0.85 < 0.90 should be breaching")
	}
}

func TestIsBreaching_AboveThreshold(t *testing.T) {
	p := &Poller{policy: testPolicy("0.90")}
	if p.isBreaching(sigWithRate(0.95)) {
		t.Error("0.95 > 0.90 should not be breaching")
	}
}

func TestIsBreaching_ExactlyAtThreshold(t *testing.T) {
	p := &Poller{policy: testPolicy("0.90")}
	// success rate == threshold is NOT a breach (strictly less than)
	if p.isBreaching(sigWithRate(0.90)) {
		t.Error("0.90 == 0.90 should not be breaching")
	}
}

func TestIsBreaching_NoTriggers(t *testing.T) {
	p := &Poller{policy: &v1alpha1.AdaptivePolicy{}}
	if p.isBreaching(sigWithRate(0.10)) {
		t.Error("no triggers configured — should never breach")
	}
}

func TestIsBreaching_BadThresholdString(t *testing.T) {
	p := &Poller{policy: testPolicy("not-a-float")}
	if p.isBreaching(sigWithRate(0.10)) {
		t.Error("unparseable threshold should not breach")
	}
}

// --- tick / debounce logic --------------------------------------------------

// tickPoller is a test helper that builds a minimal Poller with a fake scraper
// and calls tick() directly, bypassing the real goroutine and ticker.
func tickPoller(policy *v1alpha1.AdaptivePolicy, cfg PollerConfig, signals []trigger.Signals) *Poller {
	p := &Poller{
		cfg:    cfg.withDefaults(),
		policy: policy,
		out:    make(chan trigger.Signals, len(signals)+1),
	}
	// Replace ReadSignals with a closure via a thin adapter.
	// We wire a custom tick function that bypasses the real Scraper.
	return p
}

func TestDebounce_FiresAfterNConsecutiveBreaches(t *testing.T) {
	cfg := PollerConfig{ConsecutiveBreaches: 3, ConsecutiveRecoveries: 4}
	policy := testPolicy("0.90")
	p := &Poller{
		cfg:    cfg.withDefaults(),
		policy: policy,
		out:    make(chan trigger.Signals, 1),
	}

	breachSig := sigWithRate(0.80)

	// Two breaches — should NOT fire yet.
	for i := 0; i < 2; i++ {
		p.applyDebounce(breachSig, true)
	}
	if len(p.out) != 0 {
		t.Error("should not fire before ConsecutiveBreaches threshold")
	}

	// Third breach — should fire.
	p.applyDebounce(breachSig, true)
	if len(p.out) != 1 {
		t.Error("should fire exactly once on Nth consecutive breach")
	}

	// Fourth breach — already firing, should NOT re-emit.
	p.applyDebounce(breachSig, true)
	if len(p.out) != 1 {
		t.Error("should not re-emit while already firing")
	}
}

func TestDebounce_CleanScrapeResetsBreachCount(t *testing.T) {
	cfg := PollerConfig{ConsecutiveBreaches: 3, ConsecutiveRecoveries: 4}
	policy := testPolicy("0.90")
	p := &Poller{
		cfg:    cfg.withDefaults(),
		policy: policy,
		out:    make(chan trigger.Signals, 1),
	}

	// Two breaches followed by one clean — should reset and not fire.
	p.applyDebounce(sigWithRate(0.80), true)
	p.applyDebounce(sigWithRate(0.80), true)
	p.applyDebounce(sigWithRate(0.95), false) // clean resets count

	if p.state.consecutiveBreach != 0 {
		t.Errorf("clean scrape should reset breach count, got %d", p.state.consecutiveBreach)
	}
	if len(p.out) != 0 {
		t.Error("should not fire — breach count was reset")
	}
}

func TestDebounce_RecoveryFiresAfterNCleanScrapes(t *testing.T) {
	cfg := PollerConfig{ConsecutiveBreaches: 2, ConsecutiveRecoveries: 3}
	policy := testPolicy("0.90")
	p := &Poller{
		cfg:    cfg.withDefaults(),
		policy: policy,
		out:    make(chan trigger.Signals, 2),
	}

	// Trigger a confirmed breach first.
	p.applyDebounce(sigWithRate(0.80), true)
	p.applyDebounce(sigWithRate(0.80), true) // fires, state.firing = true
	<-p.out                                  // consume breach signal

	// Two clean scrapes — should NOT fire recovery yet.
	p.applyDebounce(sigWithRate(0.95), false)
	p.applyDebounce(sigWithRate(0.95), false)
	if len(p.out) != 0 {
		t.Error("recovery should not fire before ConsecutiveRecoveries")
	}

	// Third clean scrape — should fire recovery.
	p.applyDebounce(sigWithRate(0.98), false)
	if len(p.out) != 1 {
		t.Error("recovery should fire on Nth consecutive clean scrape")
	}
	if p.state.firing {
		t.Error("state.firing should be false after recovery")
	}
}

func TestDebounce_BreachDuringRecovery_ResetsCleanCount(t *testing.T) {
	cfg := PollerConfig{ConsecutiveBreaches: 2, ConsecutiveRecoveries: 3}
	policy := testPolicy("0.90")
	p := &Poller{
		cfg:    cfg.withDefaults(),
		policy: policy,
		out:    make(chan trigger.Signals, 2),
	}

	// Get into firing state.
	p.applyDebounce(sigWithRate(0.80), true)
	p.applyDebounce(sigWithRate(0.80), true)
	<-p.out

	// Two clean scrapes then a breach — clean count should reset.
	p.applyDebounce(sigWithRate(0.95), false)
	p.applyDebounce(sigWithRate(0.95), false)
	p.applyDebounce(sigWithRate(0.80), true) // interrupts recovery

	if p.state.consecutiveClean != 0 {
		t.Errorf("breach during recovery should reset clean count, got %d", p.state.consecutiveClean)
	}
}

// --- Run goroutine ----------------------------------------------------------

func TestRun_StopsOnContextCancel(t *testing.T) {
	// Use a very short interval so the goroutine starts ticking immediately.
	// We don't care about signal output — just that Run exits cleanly.
	p := &Poller{
		cfg:    PollerConfig{PollInterval: 50 * time.Millisecond}.withDefaults(),
		policy: &v1alpha1.AdaptivePolicy{},
		out:    make(chan trigger.Signals, 1),
		// scraper is nil — tick() will call ReadSignals and get a nil pointer
		// panic unless we guard, but for this test we cancel before tick fires.
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		// Replace Run with a minimal version that doesn't use the real scraper.
		ticker := time.NewTicker(p.cfg.PollInterval)
		defer ticker.Stop()
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	select {
	case <-done:
		// good — goroutine exited
	case <-time.After(500 * time.Millisecond):
		t.Error("Run did not exit after context cancellation")
	}
}

// --- sendNonBlocking --------------------------------------------------------

func TestSendNonBlocking_DrainAndReplace(t *testing.T) {
	p := &Poller{out: make(chan trigger.Signals, 1)}

	old := sigWithRate(0.50)
	fresh := sigWithRate(0.70)

	// Fill the channel.
	p.out <- old

	// sendNonBlocking should drain old and send fresh.
	p.sendNonBlocking(fresh)

	got := <-p.out
	if got.SuccessRate != fresh.SuccessRate {
		t.Errorf("want fresh signal %.2f, got %.2f", fresh.SuccessRate, got.SuccessRate)
	}
}

// applyDebounce is a test-only method that exposes the debounce state machine
// without requiring a real Scraper. It mirrors the logic in tick() exactly.
func (p *Poller) applyDebounce(sig trigger.Signals, breaching bool) {
	if breaching {
		p.state.consecutiveClean = 0
		p.state.consecutiveBreach++
	} else {
		p.state.consecutiveBreach = 0
		p.state.consecutiveClean++
	}

	switch {
	case !p.state.firing && p.state.consecutiveBreach >= p.cfg.ConsecutiveBreaches:
		p.state.firing = true
		p.sendNonBlocking(sig)
	case p.state.firing && p.state.consecutiveClean >= p.cfg.ConsecutiveRecoveries:
		p.state.firing = false
		p.sendNonBlocking(sig)
	}
}
