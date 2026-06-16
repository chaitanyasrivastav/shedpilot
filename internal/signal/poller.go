// Package signal reads live metrics from Envoy sidecar stats endpoints.
// poller.go owns the tick loop and debounce logic so detection latency
// is explicit and never at the mercy of the controller reconcile interval.
//
// Architecture:
//
//	Scraper.ReadSignals()        — unchanged, called by poller on each tick
//	Poller.Run(ctx)              — goroutine: ticker → scrape → debounce → emit
//	Poller.Signals()             — read-only channel of trigger.Signals
//
// Detection latency:
//
//	worst case = PollInterval + RTDS propagation (<200ms)
//	typical    = PollInterval/2 + RTDS propagation
//
// Flap protection:
//
//	A profile switch only fires after ConsecutiveBreaches consecutive
//	scrapes all confirm the breach. Recovery requires ConsecutiveRecoveries
//	consecutive clean scrapes. Both are configurable via PollerConfig.
package signal

import (
	"context"
	"fmt"
	"time"

	"github.com/chaitanyasrivastav/shedpilot/api/v1alpha1"
	"github.com/chaitanyasrivastav/shedpilot/internal/trigger"
)

const (
	// DefaultPollInterval is the cadence at which each pod's Envoy sidecar
	// stats endpoint is scraped. 500ms gives ~700ms worst-case detection
	// (500ms wait + <200ms RTDS) while keeping per-cluster HTTP load trivial:
	// 100 pods × 2 polls/s = 200 req/s, each a ~10KB filtered GET.
	DefaultPollInterval = 500 * time.Millisecond

	// DefaultConsecutiveBreaches is how many consecutive scrapes must all
	// show a threshold breach before a trigger fires. Prevents reacting to
	// a single noisy reading (pod restart counter reset, momentary GC pause,
	// brief deployment rollout spike).
	//
	// 3 × 500ms = 1.5s real trigger latency — safe against flapping while
	// still 40× faster than the previous 30–60s Prometheus detection path.
	DefaultConsecutiveBreaches = 3

	// DefaultConsecutiveRecoveries is how many consecutive clean scrapes
	// must pass before switching back to normal profile. Higher than breach
	// threshold intentionally — recovery should be conservative to avoid
	// oscillation when a service is borderline healthy.
	DefaultConsecutiveRecoveries = 4

	// minPollInterval guards against misconfiguration that would make the
	// poller a meaningful load source on the cluster API server.
	minPollInterval = 100 * time.Millisecond

	// maxPollInterval caps conservatively configured pollers so they still
	// provide meaningful signal within a reasonable window.
	maxPollInterval = 10 * time.Second
)

// PollerConfig is the tunable surface exposed in the AdaptivePolicy CRD.
// All fields have safe defaults; zero values are replaced at construction.
//
// CRD YAML mapping (under spec.detection):
//
//	spec:
//	  detection:
//	    pollIntervalMs: 500       # range [100, 10000]
//	    consecutiveBreaches: 3   # range [1, 20]
//	    consecutiveRecoveries: 4 # range [1, 20]
type PollerConfig struct {
	// PollInterval controls how often each pod's :15090/stats/prometheus is
	// scraped. Shorter = faster detection, marginally more cluster traffic.
	PollInterval time.Duration

	// ConsecutiveBreaches is the number of back-to-back scrapes that must
	// all confirm a trigger condition before the signal is emitted.
	ConsecutiveBreaches int

	// ConsecutiveRecoveries is the number of back-to-back scrapes that must
	// all show recovery before a "return to normal" signal is emitted.
	ConsecutiveRecoveries int
}

// withDefaults returns a PollerConfig with zero fields replaced by safe defaults
// and out-of-range values clamped. Called at NewPoller construction time.
func (c PollerConfig) withDefaults() PollerConfig {
	if c.PollInterval < minPollInterval || c.PollInterval > maxPollInterval {
		c.PollInterval = DefaultPollInterval
	}
	if c.ConsecutiveBreaches <= 0 || c.ConsecutiveBreaches > 20 {
		c.ConsecutiveBreaches = DefaultConsecutiveBreaches
	}
	if c.ConsecutiveRecoveries <= 0 || c.ConsecutiveRecoveries > 20 {
		c.ConsecutiveRecoveries = DefaultConsecutiveRecoveries
	}
	return c
}

// breachState tracks consecutive breach/recovery counts per policy.
// Kept inside Poller rather than Scraper — it's control-plane logic,
// not signal-collection logic.
type breachState struct {
	// consecutiveBreach counts how many back-to-back scrapes triggered.
	// Reset to 0 on any clean scrape.
	consecutiveBreach int

	// consecutiveClean counts how many back-to-back clean scrapes have passed
	// since the last breach. Reset to 0 on any breaching scrape.
	consecutiveClean int

	// firing is true while a breach has been confirmed and emitted.
	// Used to gate recovery signal emission.
	firing bool
}

// Poller owns the tick loop. It wraps Scraper and adds:
//   - an explicit, configurable tick interval (not dependent on reconcile)
//   - consecutive-breach debouncing to prevent flapping
//   - a typed output channel consumed by the controller
//
// One Poller is created per AdaptivePolicy in the controller's Reconcile.
// The controller calls Run in a goroutine and cancels the context on cleanup.
type Poller struct {
	scraper *Scraper
	cfg     PollerConfig
	policy  *v1alpha1.AdaptivePolicy

	// out is the channel the controller reads for trigger decisions.
	// Buffered 1: if the controller is slow processing a signal, the poller
	// does not block — the next tick overwrites the pending signal.
	out chan trigger.Signals

	// state is per-policy debounce counters. Not concurrent — only the Run
	// goroutine writes it, controller only reads out channel.
	state breachState
}

// NewPoller constructs a Poller for the given policy.
// cfg zero values are replaced by safe defaults.
func NewPoller(scraper *Scraper, policy *v1alpha1.AdaptivePolicy, cfg PollerConfig) *Poller {
	return &Poller{
		scraper: scraper,
		cfg:     cfg.withDefaults(),
		policy:  policy,
		out:     make(chan trigger.Signals, 1),
	}
}

// Signals returns the read-only channel of debounced signals.
// The controller selects on this channel alongside its other watches.
func (p *Poller) Signals() <-chan trigger.Signals {
	return p.out
}

// Run is the poll loop. Call it in a goroutine; cancel ctx to stop it.
//
// Loop:
//  1. Wait for next tick (PollInterval)
//  2. Scrape all pods via Scraper.ReadSignals()
//  3. Evaluate debounce state
//  4. If confirmed breach or confirmed recovery → send on out channel
//
// The channel send is non-blocking (select with default): if the controller
// hasn't consumed the previous signal yet, this tick's result is dropped.
// This is intentional — we want the most recent state, not a queue.
func (p *Poller) Run(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.tick(ctx)
		}
	}
}

// tick performs one scrape + debounce evaluation.
// Separated from Run for testability.
func (p *Poller) tick(ctx context.Context) {
	signals, err := p.scraper.ReadSignals(ctx, p.policy)
	if err != nil {
		// Scrape error (pod unreachable, cluster API down) — do not count
		// toward breach or recovery. State is held as-is until next tick.
		return
	}

	breaching := p.isBreaching(signals)

	if breaching {
		p.state.consecutiveClean = 0
		p.state.consecutiveBreach++
	} else {
		p.state.consecutiveBreach = 0
		p.state.consecutiveClean++
	}

	switch {
	case !p.state.firing && p.state.consecutiveBreach >= p.cfg.ConsecutiveBreaches:
		// Confirmed breach: N consecutive scrapes all triggered.
		// Emit signal and mark firing so we don't re-emit on the next tick.
		p.state.firing = true
		p.sendNonBlocking(signals)

	case p.state.firing && p.state.consecutiveClean >= p.cfg.ConsecutiveRecoveries:
		// Confirmed recovery: N consecutive clean scrapes after a breach.
		// Emit the clean signal so the controller can switch back to normal.
		p.state.firing = false
		p.sendNonBlocking(signals)
	}
}

// isBreaching checks whether the current signals satisfy any trigger condition
// defined in the policy. This mirrors the condition logic in the controller's
// trigger evaluator — kept here for the debounce gate, not for profile selection.
//
// A signal is considered "breaching" if success rate has dropped below the
// lowest threshold across all triggers. The controller's trigger evaluator
// handles the full per-trigger logic including profile selection.
func (p *Poller) isBreaching(sig trigger.Signals) bool {
	if p.policy.Spec.Triggers == nil {
		return false
	}
	for _, t := range p.policy.Spec.Triggers {
		if t.When.SuccessRate == nil {
			continue
		}
		if t.When.SuccessRate.Below != "" {
			threshold := parseFloat(t.When.SuccessRate.Below)
			if threshold > 0 && sig.SuccessRate < threshold {
				return true
			}
		}
	}
	return false
}

// sendNonBlocking attempts to send on the output channel without blocking.
// If the channel already has a pending signal (controller is busy), the
// current signal replaces it — we always want the freshest reading.
func (p *Poller) sendNonBlocking(sig trigger.Signals) {
	select {
	case p.out <- sig:
	default:
		// Drain the stale signal and send the fresh one.
		select {
		case <-p.out:
		default:
		}
		select {
		case p.out <- sig:
		default:
		}
	}
}

// parseFloat is a best-effort float parser for threshold strings from the CRD.
// Returns 0 on any parse failure, which causes the breach check to skip that
// trigger — safe default, consistent with how the controller handles it.
func parseFloat(s string) float64 {
	if s == "" {
		return 0
	}
	var f float64
	_, err := fmt.Sscanf(s, "%f", &f)
	if err != nil {
		return 0
	}
	return f
}
