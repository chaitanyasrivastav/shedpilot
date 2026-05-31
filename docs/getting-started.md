# Getting Started

This guide takes you from zero to a running adaptive policy in under 10 minutes, and from there to a production-ready policy in a week.

## Prerequisites

- Kubernetes 1.24+
- Istio 1.6+ (sidecar or ambient mode)
- `kubectl` configured for your cluster

## Install

```bash
kubectl apply -f https://github.com/shedpilot-io/operator/releases/latest/download/install.yaml
```

Verify the controller is running:

```bash
kubectl get pods -n shedpilot-system
# NAME                                    READY   STATUS
# shedpilot-controller-5d4b9f8c7-xk2jq   1/1     Running
# shedpilot-controller-5d4b9f8c7-mn8qp   1/1     Running
```

Two replicas with leader election — survives a node failure.

## Step 1 — Start in dryRun mode

**Always start with `dryRun: true`.** Filters are installed but never reject any requests. You observe what would happen before enabling enforcement. This is the recommended path for every new policy.

```yaml
# policy.yaml
apiVersion: resilience.shedpilot.io/v1alpha1
kind: AdaptivePolicy
metadata:
  name: payments
  namespace: production
spec:
  selector:
    app: payments

  dryRun: true        # observe only
  meshBackend: auto   # detects Istio or Cilium

  admissionControl:
    enabled: true
    successRateThreshold: "95.0"

  adaptiveConcurrency:
    enabled: true
```

```bash
kubectl apply -f policy.yaml
kubectl describe adaptivepolicy payments -n production
```

You should see:

```
Status:
  Detected Backend:  istio
  Active Filters:    admission-control, adaptive-concurrency
  Shed Rate Now:     0%
  Ready:             True
  Managed Resources:
    payments-admission-control    EnvoyFilter
    payments-adaptive-concurrency EnvoyFilter
```

## Step 2 — Observe for 1–2 weeks

Watch `status.wouldHaveShedRate` — what the shed rate would have been if enforcement were enabled. This tells you whether your `successRateThreshold` is calibrated correctly for your service.

```bash
# Watch the status update every reconcile cycle
watch kubectl describe adaptivepolicy payments -n production
```

If `wouldHaveShedRate` is non-zero during normal traffic, your threshold is too tight — raise `successRateThreshold` slightly. If it's zero even during known degradation events, your threshold may be too loose.

## Step 3 — Add profiles and triggers

Once you understand your service's signal patterns:

```yaml
apiVersion: resilience.shedpilot.io/v1alpha1
kind: AdaptivePolicy
metadata:
  name: payments
  namespace: production
spec:
  selector:
    app: payments

  dryRun: false   # enforcement enabled

  admissionControl:
    enabled: true
    successRateThreshold: "95.0"
    sheddingSpeed: "1.5"
    maxRejectionPercent: "80.0"   # never reject more than 80%
    minRequestsPerSecond: 5       # inactive below 5 RPS

  adaptiveConcurrency:
    enabled: true
    latencyPercentile: p50

  profiles:
    normal:
      admissionControl:
        successRateThreshold: "95.0"
        sheddingSpeed: "1.5"

    degraded:
      admissionControl:
        successRateThreshold: "85.0"
        sheddingSpeed: "2.0"
        successRateWindow: "20s"  # shorter window = faster reaction
      adaptiveConcurrency:
        latencyPercentile: p75

    critical:
      admissionControl:
        successRateThreshold: "75.0"
        sheddingSpeed: "3.0"
        successRateWindow: "15s"

    flash-sale:
      admissionControl:
        successRateThreshold: "90.0"
        sheddingSpeed: "1.8"
        successRateWindow: "15s"

  activeProfile: normal

  triggers:
  - name: degradation-detected
    when:
      successRate:
        below: "0.90"
        consecutiveSamples: 2
    switchTo: degraded
    cooldownSeconds: 60

  - name: critical-degradation
    when:
      successRate:
        below: "0.80"
        consecutiveSamples: 2
    switchTo: critical
    cooldownSeconds: 60

  - name: recovery
    when:
      successRate:
        above: "0.97"
        consecutiveSamples: 3
    fromProfiles: [degraded, critical]
    switchTo: normal
    cooldownSeconds: 120

  schedules:
  - name: friday-sale-start
    cron: "50 13 * * 5"      # Friday 1:50 PM UTC — 10 min before sale
    switchTo: flash-sale

  - name: friday-sale-end
    cron: "30 15 * * 5"      # Friday 3:30 PM UTC
    switchTo: normal
    fromProfiles: [flash-sale]

  signalConfig:
    evaluationIntervalSeconds: 30
    capacityWarningPercent: 10
    capacityWarningWindowDays: 7
```

```bash
kubectl apply -f policy.yaml
kubectl describe adaptivepolicy payments -n production
```

## Step 4 — Escape hatches

Keep these commands handy. Run them without hesitation during an incident.

```bash
# Freeze all automatic switches — enforcement keeps running
kubectl annotate adaptivepolicy payments shedpilot.io/human-override=true -n production

# Switch to observe mode instantly
kubectl patch adaptivepolicy payments --type merge \
  -p '{"spec":{"dryRun":true}}' -n production

# Manually switch profile
kubectl patch adaptivepolicy payments --type merge \
  -p '{"spec":{"activeProfile":"degraded"}}' -n production

# Remove everything — cascade deletes all EnvoyFilters and DestinationRules
kubectl delete adaptivepolicy payments -n production
```

## What's next

- [Concepts](concepts.md) — understand how load shedding works and when to use it
- [Profiles & Triggers](profiles-triggers.md) — write effective resilience runbooks
- [API Reference](api-reference.md) — every available field
- [Runbook](runbook.md) — what to do during an incident