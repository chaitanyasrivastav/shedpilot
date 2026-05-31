# Profiles and Triggers

Profiles and triggers are the core of shedpilot's value. This document explains how to design them effectively for real production scenarios.

## Designing profiles

A profile is your pre-written response to a specific service state. Think of it as encoding your runbook into YAML. The question to answer for each profile is: "When my service is in this state, what protection do I want?"

### The four standard profiles

Most services need these four:

```yaml
profiles:
  normal:
    # Baseline protection. Service is healthy.
    # Threshold high — only shed when something is genuinely wrong.
    admissionControl:
      successRateThreshold: "95.0"
      sheddingSpeed: "1.5"

  degraded:
    # Service is struggling. Tighten protection.
    # Switch to this when success rate drops — buys time for autoscaling.
    admissionControl:
      successRateThreshold: "85.0"
      sheddingSpeed: "2.0"
      successRateWindow: "20s"   # shorter window = faster reaction
    adaptiveConcurrency:
      latencyPercentile: p75     # more conservative baseline

  critical:
    # Service is barely alive. Protect at all costs.
    # Better to serve 25% of users well than 100% of users badly.
    admissionControl:
      successRateThreshold: "75.0"
      sheddingSpeed: "3.0"
      successRateWindow: "15s"

  flash-sale:
    # Pre-armed for known high-traffic events.
    # Slightly tighter than normal — protection without aggressiveness.
    # Switch proactively via schedule, not reactively via trigger.
    admissionControl:
      successRateThreshold: "90.0"
      sheddingSpeed: "1.8"
      successRateWindow: "15s"
```

### Custom profiles for specific scenarios

Add custom profiles for scenarios specific to your service:

```yaml
profiles:
  db-slow:
    # Database is slow but service itself is healthy.
    # Shed some load to reduce DB pressure.
    admissionControl:
      successRateThreshold: "88.0"
      sheddingSpeed: "1.8"

  post-deploy:
    # Conservative mode immediately after a deployment.
    # Tight threshold catches regressions fast before they spread.
    admissionControl:
      successRateThreshold: "97.0"
      sheddingSpeed: "1.0"        # linear — don't over-shed on a transient

  batch-window:
    # Protect during nightly batch job execution.
    admissionControl:
      successRateThreshold: "92.0"
    streamingProtection:
      maxConcurrentStreams: 100   # limit streaming connections during batch
```

### Tuning thresholds

**`successRateThreshold`** — start conservative. Set it where you know something is wrong, not where you want to get ahead of potential problems.

- `"95.0"` means "shed when >5% of requests are failing." This is the right baseline for most services.
- `"85.0"` for `degraded` means "shed more aggressively when >15% are failing."
- Too tight (e.g. `"99.0"`) causes false positives during normal variance.
- Too loose (e.g. `"70.0"`) means you don't protect until things are very bad.

**`sheddingSpeed`** — controls how steeply the rejection probability rises as success rate falls.

- `1.0` is linear. At 50% below threshold, you reject 50% of traffic.
- `1.5` moderate. At 50% below threshold, you reject more than 50%.
- `2.0` aggressive. At 50% below threshold, you reject significantly more.
- Start at `1.5`. Increase if your service needs faster protection during degradation.

**`successRateWindow`** — shorter is more reactive but noisier.

- `30s` is the standard baseline. Smooths out transient blips.
- `20s` for `degraded` — react faster when already in trouble.
- `15s` for `critical` — fastest reaction when barely surviving.
- `60s` for noisy services with high natural variance.

## Designing triggers

### The standard trigger set

```yaml
triggers:
# 1. Detect degradation — switch to degraded profile
- name: degradation-detected
  when:
    successRate:
      below: "0.90"
      consecutiveSamples: 2      # 2 bad readings = not a blip
  switchTo: degraded
  cooldownSeconds: 60

# 2. Escalate — switch to critical if degraded profile isn't helping
- name: critical-degradation
  when:
    successRate:
      below: "0.80"
      consecutiveSamples: 2
  switchTo: critical
  cooldownSeconds: 60

# 3. Recover — return to normal when healthy again
- name: recovery
  when:
    successRate:
      above: "0.97"
      consecutiveSamples: 3      # 3 good readings before restoring
  fromProfiles: [degraded, critical]   # only fire when actually degraded
  switchTo: normal
  cooldownSeconds: 120           # longer cooldown — don't rush recovery
```

**Why `consecutiveSamples: 2` for degradation but `3` for recovery?**

Degrade quickly, recover carefully. One bad reading could be noise — two consecutive means something real is happening. Recovery should be more conservative: three consecutive good readings before restoring normal thresholds, giving the service time to stabilise.

**Why `fromProfiles: [degraded, critical]` on the recovery trigger?**

Without `fromProfiles`, the recovery trigger could fire from `flash-sale` and switch you back to `normal` during a sale. `fromProfiles` ensures it only fires when you're actually in a degraded state.

### RPS-based pre-arming

For services where RPS is a reliable leading indicator of trouble:

```yaml
- name: rps-spike
  when:
    rpsAbove: 5000     # tune to your normal peak — this is a spike indicator
  switchTo: flash-sale
  cooldownSeconds: 300  # don't re-arm for 5 minutes
```

This fires before success rate degrades — switching to a tighter profile as soon as the spike is detected.

### Trigger ordering matters

Triggers are evaluated in spec order. The first matching trigger wins. Order them from most severe to least:

```yaml
triggers:
- name: critical-degradation    # most severe — check first
  when:
    successRate: {below: "0.80", consecutiveSamples: 2}
  switchTo: critical

- name: degradation-detected    # moderate — check second
  when:
    successRate: {below: "0.90", consecutiveSamples: 2}
  switchTo: degraded

- name: recovery                # recovery — check last
  when:
    successRate: {above: "0.97", consecutiveSamples: 3}
  fromProfiles: [degraded, critical]
  switchTo: normal
```

## Designing schedules

Schedules are for events you know are coming. The key insight: switch proactively, before traffic arrives.

```yaml
schedules:
# Pre-arm 10 minutes before the event
- name: flash-sale-start
  cron: "50 13 * * 5"     # Friday 1:50 PM UTC
  switchTo: flash-sale

# Restore 90 minutes after end — enough time for traffic to normalise
- name: flash-sale-end
  cron: "30 15 * * 5"     # Friday 3:30 PM UTC
  switchTo: normal
  fromProfiles: [flash-sale]   # don't override if we degraded during the sale

# Protect during nightly batch job
- name: batch-start
  cron: "0 2 * * *"       # 2 AM UTC daily
  switchTo: batch-window

- name: batch-end
  cron: "0 4 * * *"       # 4 AM UTC daily
  switchTo: normal
  fromProfiles: [batch-window]
```

**`fromProfiles` on end schedules** prevents the schedule from overriding a degradation trigger. If your service degraded during the flash sale and is in `critical` profile, the `flash-sale-end` schedule won't switch you back to `normal` — it only fires from `flash-sale`.

## Common mistakes

**Threshold too tight.** Setting `successRateThreshold: "99.0"` causes constant false positives. Most services have some natural error rate — 404s, validation failures, expected client errors. Use `successCodes` to exclude those, or set the threshold at a level that means "something is genuinely wrong."

**Recovery too fast.** `consecutiveSamples: 1` on recovery means the first good reading after degradation restores normal thresholds. If the service is still unstable, it'll immediately degrade again — causing rapid oscillation. Use at least 3, preferably with a longer `cooldownSeconds`.

**Missing `fromProfiles` on recovery.** Without it, recovery fires from every profile including `flash-sale` and custom profiles. If you've manually switched to a profile for a specific reason, the recovery trigger will undo it.

**Identical profile and baseline.** If your `normal` profile and baseline config are identical, the normal profile is redundant. Either remove it or make it explicit — having it defined is useful as a named target for recovery triggers even if the values match.

**No schedule for known events.** If you have a flash sale every Friday and you're handling it with reactive triggers, you're always reacting after the spike hits. Add a schedule to pre-arm 10 minutes early.