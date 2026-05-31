# Runbook

This document is written for engineers responding to incidents at 3am. It answers specific questions with specific commands.

## First — read the status

One command. Read this first before doing anything else.

```bash
kubectl describe adaptivepolicy <name> -n <namespace>
```

The status block tells you:
- **Detected Backend** — which mesh is running
- **Active Profile** — which profile is currently enforced
- **Shed Rate Now** — approximate percentage of requests being rejected
- **RTDS Connected** — whether fast profile switching is available
- **Last Decision** — what switched, when, why, and what happened
- **Consecutive Bad Samples** — how close to the next trigger firing
- **Next Trigger Evaluation** — when the controller will evaluate again
- **Ready** — whether the controller is reconciling correctly

---

## Immediate controls

### Freeze automatic switches

Use when you want to stop the controller from making any more autonomous decisions. Enforcement continues — filters keep running at their current profile. Only automatic switches are blocked.

```bash
kubectl annotate adaptivepolicy payments shedpilot.io/human-override=true -n production
```

Remove the freeze:

```bash
kubectl annotate adaptivepolicy payments shedpilot.io/human-override- -n production
```

### Switch to observe mode

Use when you want to keep filters installed but immediately stop all rejection. No traffic will be shed. The controller continues to evaluate triggers and update status.

```bash
kubectl patch adaptivepolicy payments -n production \
  --type merge -p '{"spec":{"dryRun":true}}'
```

### Manually switch profile

```bash
# Switch to degraded
kubectl patch adaptivepolicy payments -n production \
  --type merge -p '{"spec":{"activeProfile":"degraded"}}'

# Switch to critical
kubectl patch adaptivepolicy payments -n production \
  --type merge -p '{"spec":{"activeProfile":"critical"}}'

# Return to normal
kubectl patch adaptivepolicy payments -n production \
  --type merge -p '{"spec":{"activeProfile":"normal"}}'
```

### Remove everything

Nuclear option. Removes the policy and cascade-deletes all EnvoyFilters and DestinationRules. Envoy sidecars return to zero shedpilot configuration.

```bash
kubectl delete adaptivepolicy payments -n production
```

---

## Scenario playbook

### Scenario: Service is degraded, controller switched profile automatically

**Signs:** `Active Profile: degraded`, `Shed Rate Now: ~40%`, `Last Decision` shows trigger fired.

**What to check:**

```bash
# 1. Read the status — what triggered the switch?
kubectl describe adaptivepolicy payments -n production

# 2. Is success rate recovering?
kubectl logs -n shedpilot-system -l app=shedpilot --tail=50

# 3. Are pods healthy?
kubectl get pods -n production -l app=payments

# 4. Is there a downstream issue?
kubectl describe adaptivepolicy <downstream-service> -n production
```

**If the service is recovering:** wait. The recovery trigger will fire when success rate stays above 97% for 3 consecutive evaluations. Normal is restored automatically.

**If the service is not recovering:** escalate manually.

```bash
kubectl patch adaptivepolicy payments -n production \
  --type merge -p '{"spec":{"activeProfile":"critical"}}'
```

**If you need to stop shedding immediately** (e.g. the degradation was a false positive):

```bash
kubectl patch adaptivepolicy payments -n production \
  --type merge -p '{"spec":{"dryRun":true}}'
```

---

### Scenario: Controller switched profile but service is still degraded

**Signs:** `Active Profile: degraded`, high error rate continues, `Shed Rate Now: ~40%` but success rate not recovering.

**Possible causes:**
1. The threshold is too loose — degraded profile isn't shedding enough
2. The problem is downstream — shedding inbound load doesn't help a slow database
3. Pods are OOMKilled or crashing — shedding doesn't fix broken pods

**Check downstream:**

```bash
# Is the database slow?
kubectl describe adaptivepolicy <db-proxy-service> -n production

# Are pods crashing?
kubectl get pods -n production -l app=payments
kubectl describe pod <crashing-pod> -n production
```

**If downstream is the issue:** shedpilot reduces load on your service, which reduces load on the database. Give it 2–3 minutes. If not recovering, the database needs direct intervention.

**If pods are crashing:** shedpilot cannot fix broken pods. Address the root cause directly.

**If threshold is too loose:** manually escalate to critical.

```bash
kubectl patch adaptivepolicy payments -n production \
  --type merge -p '{"spec":{"activeProfile":"critical"}}'
```

---

### Scenario: Service is over-shedding (too much traffic being rejected)

**Signs:** `Shed Rate Now: ~70%`, success rate is high (99%+), legitimate traffic is being rejected.

Over-shedding happens when the service recovers but the protection is still too aggressive.

**Quick fix — relax by switching to a less aggressive profile:**

```bash
kubectl patch adaptivepolicy payments -n production \
  --type merge -p '{"spec":{"activeProfile":"normal"}}'
```

**If triggers keep switching back to degraded:** the threshold may be miscalibrated. Temporarily freeze:

```bash
kubectl annotate adaptivepolicy payments shedpilot.io/human-override=true -n production
```

Then review your threshold settings and update them:

```bash
kubectl edit adaptivepolicy payments -n production
# Increase successRateThreshold in the degraded profile
# or increase consecutiveSamples in the trigger
```

---

### Scenario: Controller shows Ready: False

**Signs:** `Ready: False`, `Conditions` shows `Degraded: True`.

```bash
# Read the detailed error message
kubectl describe adaptivepolicy payments -n production | grep -A5 "Degraded"

# Common causes:
# MeshDetectionFailed — istiod not running?
kubectl get pods -n istio-system -l app=istiod

# ApplyFailed — Istio webhook rejected the EnvoyFilter config
kubectl get events -n production --sort-by='.lastTimestamp' | tail -20

# RenderFailed — invalid policy configuration
kubectl describe adaptivepolicy payments -n production
```

---

### Scenario: RTDS Connected: false

**Signs:** `RTDS Connected: false`. Profile switches still work but use the EnvoyFilter path (5–30s instead of <200ms).

```bash
# Check if istiod is healthy
kubectl get pods -n istio-system -l app=istiod

# Check controller logs for RTDS errors
kubectl logs -n shedpilot-system -l app=shedpilot --tail=100 | grep -i rtds
```

The controller reconnects automatically when Istiod is available. During disconnection, profile switches use the EnvoyFilter path — slower but still functional.

---

### Scenario: ScalabilityWarning condition is True

**Signs:** `ScalabilityWarning: true`, detail says something like "service was in non-normal profile for 18% of the last 7 days."

**This is not an incident — it's a signal.** Your service is shedding load chronically. The correct response is not to tune shedpilot further — it's to scale up the service permanently.

```bash
# See the detail
kubectl describe adaptivepolicy payments -n production | grep -A3 "Scalability Warning"

# Check what the normal load looks like vs what your replicas can handle
kubectl top pods -n production -l app=payments
kubectl get hpa -n production
```

Options:
1. Increase the replica count permanently
2. Increase pod resource limits
3. Optimise the service to handle more load per pod
4. Reduce `capacityWarningPercent` if the warning threshold is miscalibrated

---

## Decision log

The status keeps the last 10 decisions. Read them to understand what happened during an incident:

```bash
kubectl get adaptivepolicy payments -n production \
  -o jsonpath='{.status.decisionHistory}' | python3 -m json.tool
```

Each decision shows: timestamp, trigger name, signal values, profile before/after, delivery method, and outcome. This is your incident timeline without needing to query logs.

---

## Controller logs

```bash
# Stream controller logs
kubectl logs -n shedpilot-system -l app=shedpilot -f

# Filter for a specific policy
kubectl logs -n shedpilot-system -l app=shedpilot | grep "payments"

# Filter for errors only
kubectl logs -n shedpilot-system -l app=shedpilot | grep '"level":"error"'
```

---

## Verify EnvoyFilters are present

```bash
kubectl get envoyfilter -n production

# Inspect the actual filter config Istio accepted
kubectl get envoyfilter payments-admission-control -n production -o yaml
kubectl get envoyfilter payments-adaptive-concurrency -n production -o yaml
```

If EnvoyFilters are missing but the policy exists and is Ready, the controller should recreate them on the next reconcile (within 30s). If they stay missing, check controller logs for apply errors.