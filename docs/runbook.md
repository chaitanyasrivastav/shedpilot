# Runbook

Written for engineers responding to incidents. Read status first. Then act.

## Read the status first

```bash
kubectl describe adaptivepolicy <name> -n <namespace>
```

This one command tells you:

| Field | What it means |
|---|---|
| **Active Profile** | Which profile is currently enforced |
| **Shed Rate Now** | Approximate % of requests being rejected |
| **RTDS Connected** | Whether fast delivery (<200ms) is available |
| **Last Decision** | What switched, when, why, signal values, delivery method |
| **Consecutive Bad Samples** | How close to the next trigger firing |
| **Next Trigger Evaluation** | When the controller next evaluates |
| **Ready** | Whether the controller is reconciling correctly |

Read Last Decision and Decision History before taking any action. They tell you what shedpilot has already done and why.

---

## Immediate controls

### Freeze automatic switches

Use when you want to stop all autonomous decisions. Enforcement continues — filters keep running at the current profile. Only automatic profile switches are blocked.

```bash
kubectl annotate adaptivepolicy payments shedpilot.io/human-override=true -n production
```

Remove the freeze:

```bash
kubectl annotate adaptivepolicy payments shedpilot.io/human-override- -n production
```

### Switch to observe-only mode

Filters stay installed. All request rejection stops immediately. Trigger evaluation and status updates continue — you can still see what would happen.

```bash
kubectl patch adaptivepolicy payments -n production \
  --type merge -p '{"spec":{"dryRun":true}}'
```

### Force a profile switch

```bash
kubectl patch adaptivepolicy payments -n production \
  --type merge -p '{"spec":{"activeProfile":"degraded"}}'

kubectl patch adaptivepolicy payments -n production \
  --type merge -p '{"spec":{"activeProfile":"critical"}}'

kubectl patch adaptivepolicy payments -n production \
  --type merge -p '{"spec":{"activeProfile":"normal"}}'
```

### Remove everything

Removes the policy and cascade-deletes all EnvoyFilters and DestinationRules. Envoy sidecars return to zero shedpilot configuration.

```bash
kubectl delete adaptivepolicy payments -n production
```

---

## Scenario playbook

### Service is degraded — controller switched profile automatically

Signs: `Active Profile: degraded`, `Shed Rate Now: ~40%`, Last Decision shows trigger fired.

```bash
# 1. Read the full status
kubectl describe adaptivepolicy payments -n production

# 2. Is success rate recovering?
kubectl logs -n shedpilot-system \
  deployment/shedpilot-controller-manager | grep "signal read" | tail -5

# 3. Are pods healthy?
kubectl get pods -n production -l app=payments

# 4. Any downstream service also degraded?
kubectl get adaptivepolicies -n production
```

If the service is recovering: wait. The recovery trigger fires when success rate stays above 97% for 3 consecutive evaluations. Normal restores automatically.

If the service is not recovering and you need more aggressive protection:

```bash
kubectl patch adaptivepolicy payments -n production \
  --type merge -p '{"spec":{"activeProfile":"critical"}}'
```

If the degradation was a false positive and you need to stop shedding immediately:

```bash
kubectl patch adaptivepolicy payments -n production \
  --type merge -p '{"spec":{"dryRun":true}}'
```

---

### Service still degraded after profile switch

Signs: Active Profile is degraded or critical, high error rate continues, success rate not recovering.

Possible causes:

**Downstream dependency is the problem.** shedpilot sheds inbound load which reduces downstream call volume — helpful but may not be sufficient if the dependency is completely unavailable. Check the dependency directly.

**Pods are crashing or OOMKilled.** shedpilot cannot fix broken pods. Address the root cause.

**Threshold too loose.** The degraded profile may not be shedding enough. Escalate manually to critical.

```bash
kubectl patch adaptivepolicy payments -n production \
  --type merge -p '{"spec":{"activeProfile":"critical"}}'
```

---

### Service is over-shedding — too many 503s

Signs: Shed Rate Now is high (60–80%), success rate is actually fine (99%+), legitimate clients getting rejected.

Over-shedding happens when the service recovered but protection is still aggressive.

Immediate fix:

```bash
kubectl patch adaptivepolicy payments -n production \
  --type merge -p '{"spec":{"activeProfile":"normal"}}'
```

If triggers keep switching back to degraded:

```bash
# Freeze first
kubectl annotate adaptivepolicy payments -n production \
  shedpilot.io/human-override=true

# Then review and raise thresholds
kubectl edit adaptivepolicy payments -n production
# Increase successRateThreshold in the degraded profile
# or increase consecutiveSamples in the trigger

# Unfreeze after adjusting
kubectl annotate adaptivepolicy payments -n production \
  shedpilot.io/human-override-
```

---

### Ready: False

```bash
kubectl describe adaptivepolicy payments -n production | grep -A5 "Degraded"
```

| Reason | Cause | Fix |
|---|---|---|
| MeshDetectionFailed | istiod pod not running | kubectl get pods -n istio-system -l app=istiod |
| ApplyFailed | Istio webhook rejected EnvoyFilter | kubectl get events -n production; check successRateThreshold format |
| RenderFailed | Invalid policy config | Check the message field for specifics |

The most common ApplyFailed cause: `successRateThreshold` sent as decimal (`0.95`) instead of percentage string (`"95.0"`). Ensure all threshold fields use percentage strings.

---

### RTDS Connected: false

Profile switches still work but use the EnvoyFilter path (5–30s instead of <200ms).

```bash
# Check if fast delivery RBAC is granted
kubectl auth can-i create pods/exec \
  --namespace production \
  --as system:serviceaccount:shedpilot-system:shedpilot-controller-manager

# Check controller logs
kubectl logs -n shedpilot-system \
  deployment/shedpilot-controller-manager | grep -i "fast delivery\|forbidden"
```

If `no` from auth check, apply the pods/exec permission — see README RBAC requirements.

---

### ScalabilityWarning condition is True

This is not an incident — it is a signal.

```bash
kubectl describe adaptivepolicy payments -n production | grep -A3 "Scalability Warning"
```

The service is chronically shedding load. This means capacity is permanently insufficient. The correct response is not to tune shedpilot further.

Options:
- Increase replica count permanently via HPA minReplicas
- Increase pod resource limits
- Optimise the service to handle more load per pod

---

### Trigger oscillation — profile switches back and forth

Signs: Decision history shows alternating degraded/normal every 30s.

Most common cause: recovery trigger firing on zero-sample safe defaults immediately after a breach.

```bash
# Check the signal values in decision history
kubectl get adaptivepolicy payments -n production \
  -o jsonpath='{.status.decisionHistory}' | python3 -m json.tool | grep signalValues
```

If signalValues shows `successRate=0.990` on recovery decisions, that is the safe default (0.99) not a real reading. Ensure you are on v0.2+ and that trigger evaluation is skipped on zero-sample reads.

Also check cooldownSeconds — if shorter than evaluationIntervalSeconds, triggers can re-fire on consecutive reconciles.

---

## Decision history

Last 10 decisions with full context:

```bash
kubectl get adaptivepolicy payments -n production \
  -o jsonpath='{.status.decisionHistory}' | python3 -m json.tool
```

Each decision shows: timestamp, trigger name, signal values that caused the decision, profiles before/after, delivery method, and outcome. This is your incident timeline without needing to grep logs.

---

## Controller logs

```bash
# Stream all logs
kubectl logs -n shedpilot-system \
  deployment/shedpilot-controller-manager -f

# Filter for a specific policy
kubectl logs -n shedpilot-system \
  deployment/shedpilot-controller-manager | grep '"name":"payments"'

# Errors only
kubectl logs -n shedpilot-system \
  deployment/shedpilot-controller-manager | grep '"level":"error"'

# Signal reads — verify scraping is working
kubectl logs -n shedpilot-system \
  deployment/shedpilot-controller-manager | grep "signal read"

# Fast delivery results
kubectl logs -n shedpilot-system \
  deployment/shedpilot-controller-manager | grep "fast delivery"
```

---

## Verify EnvoyFilters are present

```bash
kubectl get envoyfilter -n production

kubectl get envoyfilter payments-admission-control -n production -o yaml
kubectl get envoyfilter payments-adaptive-concurrency -n production -o yaml
```

If EnvoyFilters are missing but the policy is Ready, the controller recreates them on the next reconcile (within evaluationIntervalSeconds). If they stay missing, check controller logs for apply errors.