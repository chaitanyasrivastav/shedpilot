# Profiles and Triggers

## Designing profiles

A profile is your pre-written response to a specific service state. Think of it as encoding your incident runbook into YAML. The question to answer for each profile: "When my service is in this state, what protection level do I want?"

### The four standard profiles

```yaml
profiles:
  normal:
    # Service is healthy. Baseline protection only.
    admissionControl:
      successRateThreshold: "95.0"
      sheddingSpeed: "1.5"

  degraded:
    # Service is struggling. Tighten protection.
    # Switch here when success rate drops — buys time for autoscaling.
    admissionControl:
      successRateThreshold: "85.0"
      sheddingSpeed: "2.0"
      successRateWindow: "20s"     # shorter = reacts faster
    adaptiveConcurrency:
      latencyPercentile: p75       # more conservative baseline

  critical:
    # Service is barely alive. Maximum protection.
    # Better to serve 25% of users well than 100% badly.
    admissionControl:
      successRateThreshold: "75.0"
      sheddingSpeed: "3.0"
      successRateWindow: "15s"

  flash-sale:
    # Pre-armed for known high-traffic events.
    # Slightly tighter than normal. Switch proactively via schedule.
    admissionControl:
      successRateThreshold: "90.0"
      sheddingSpeed: "1.8"
      successRateWindow: "15s"
```

### Threshold tuning

**successRateThreshold** — set where you know something is wrong, not where you want to get ahead of potential problems.

- "95.0" = shed when more than 5% of requests fail. Right baseline for most services.
- "85.0" = shed more aggressively when more than 15% fail. Right for degraded.
- Too tight (e.g. "99.0") causes false positives from normal variance (404s, expected client errors).
- Too loose (e.g. "70.0") means you do not protect until things are very bad.

If your service has a high natural 4xx rate (validation APIs, auth endpoints), configure successCodes to include 4xx — otherwise those healthy responses count as failures and trigger spuriously.

**sheddingSpeed** — how steeply rejection rises as success rate falls below threshold.

- "1.0" is linear. At 50% below threshold, reject ~50% of traffic.
- "1.5" moderate. Sheds proportionally more as degradation worsens.
- "2.0" aggressive. Steeper curve — fast protection, higher impact on clients.
- "3.0" very aggressive. For critical profile only.

Start at "1.5". Increase if your service needs faster protection.

**successRateWindow** — shorter is more reactive but noisier.

- "30s" standard baseline. Smooths transient blips.
- "20s" for degraded — react faster when already in trouble.
- "15s" for critical — fastest reaction when barely surviving.
- "60s" for high-variance services.

### Custom profiles

Add custom profiles for scenarios specific to your service:

```yaml
profiles:
  db-slow:
    # Database is slow. Shed load to reduce DB pressure.
    admissionControl:
      successRateThreshold: "88.0"
      sheddingSpeed: "1.8"

  post-deploy:
    # Conservative immediately after a deployment.
    # Tight threshold catches regressions fast.
    admissionControl:
      successRateThreshold: "97.0"
      sheddingSpeed: "1.0"   # linear — don't over-shed on a transient

  batch-window:
    # Protect during nightly batch job.
    admissionControl:
      successRateThreshold: "92.0"
```

## Designing triggers

### The standard trigger set

```yaml
triggers:
# Most severe first — evaluated in order, first match wins
- name: critical-degradation
  when:
    successRate:
      below: "0.75"
      consecutiveSamples: 2
  switchTo: critical
  cooldownSeconds: 60

- name: degradation-detected
  when:
    successRate:
      below: "0.90"
      consecutiveSamples: 2
  switchTo: degraded
  cooldownSeconds: 60

- name: recovery
  when:
    successRate:
      above: "0.97"
      consecutiveSamples: 3     # 3 good readings before restoring
  fromProfiles: [degraded, critical]   # only fire from degraded states
  switchTo: normal
  cooldownSeconds: 120          # longer — don't rush recovery
```

**Why consecutiveSamples: 2 for breach but 3 for recovery?**

Degrade quickly, recover carefully. Two consecutive bad readings confirm something real is happening. Three consecutive good readings before restoring ensure the service has stabilised. Premature recovery followed by immediate re-degradation is worse than staying in degraded profile a few extra seconds.

**Why fromProfiles on recovery?**

Without it, the recovery trigger fires from any profile — including flash-sale and custom profiles you intentionally set. fromProfiles ensures recovery only fires when you are actually in a degraded state, not during a proactive pre-arm.

**Why longer cooldown on recovery?**

The service may be borderline stable — oscillating just above and below the recovery threshold. A longer cooldown prevents the policy from thrashing between degraded and normal.

### RPS-based pre-arming

For services where RPS is a reliable leading indicator:

```yaml
- name: rps-spike
  when:
    rpsAbove: 5000     # tune to your normal peak
  switchTo: flash-sale
  cooldownSeconds: 300
```

This fires before success rate degrades — switching to tighter protection the moment the spike is detected, not after damage has accumulated.

### Trigger ordering

Triggers are evaluated in spec order. Order from most severe to least:

```yaml
triggers:
- name: critical-degradation   # check first — most severe
  ...
- name: degradation-detected   # check second
  ...
- name: rps-spike              # leading indicator
  ...
- name: recovery               # check last
  ...
```

If you put recovery first, it fires before the degradation trigger gets a chance — the service will never enter a degraded profile.

## Designing schedules

Schedules fire at a specific time regardless of service health. The key: switch proactively before traffic arrives, not reactively after it hits.

```yaml
schedules:
# Pre-arm 10 minutes before the event
- name: flash-sale-start
  cron: "50 13 * * 5"     # Friday 1:50 PM UTC — 10 min before 2 PM sale
  switchTo: flash-sale

# Restore after traffic normalises
- name: flash-sale-end
  cron: "30 15 * * 5"     # Friday 3:30 PM UTC
  switchTo: normal
  fromProfiles: [flash-sale]   # don't override if we degraded during the sale

# Protect during nightly batch
- name: batch-start
  cron: "0 2 * * *"
  switchTo: batch-window

- name: batch-end
  cron: "0 4 * * *"
  switchTo: normal
  fromProfiles: [batch-window]
```

**fromProfiles on end schedules** prevents the schedule from overriding a degradation trigger. If your service degraded during the flash sale, the flash-sale-end schedule will not switch you back to normal — it only fires from the flash-sale profile.

All cron times are UTC.

## Common mistakes

**Threshold too tight** — "99.0" causes false positives. Most services have natural error rates from 404s, client errors, expected validation failures. Use successCodes to exclude expected 4xx, or set the threshold where you know something is genuinely wrong.

**Recovery too fast** — consecutiveSamples: 1 on recovery means the first good reading restores normal thresholds. If the service is still borderline, it immediately degrades again causing oscillation. Use at least 3 consecutive samples and a cooldownSeconds of 2x the breach cooldown.

**Missing fromProfiles on recovery** — without it, recovery fires from flash-sale, batch-window, and any profile you manually set. Use fromProfiles: [degraded, critical] on recovery triggers.

**Identical profile and baseline** — if normal profile values match baseline exactly, the profile is redundant but harmless. Keeping it explicit is actually useful as a named target for recovery triggers.

**No schedules for known events** — if you have a weekly flash sale and handle it with reactive triggers, you are always reacting after the spike hits. Add a schedule to pre-arm 10 minutes early.

**profileBefore and profileAfter both showing wrong values** — verify activeProfile is set in spec before enabling triggers. Without activeProfile, the controller has no current profile to switch from.