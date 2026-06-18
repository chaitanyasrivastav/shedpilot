# API Reference

## AdaptivePolicy

```
apiVersion: resilience.shedpilot.io/v1alpha1
kind: AdaptivePolicy
```

Short name: `adp` (use `adaptivepolicy` to avoid conflicts with Istio's `AuthorizationPolicy` which also has `ap`).

---

### Top-level spec fields

| Field | Type | Default | Description |
|---|---|---|---|
| `selector` | `map[string]string` | required | Pod label selector. Must not be empty. Must match at least one running pod. |
| `meshBackend` | `auto \| istio \| cilium` | `auto` | Which mesh to target. `auto` detects Istio first, then Cilium. |
| `meshMode` | `sidecar \| ambient` | `sidecar` | Istio data plane mode. |
| `dryRun` | `bool` | `false` | Install filters but never reject requests. Use this for at least two weeks before enabling enforcement. |
| `admissionControl` | object | — | Success-rate-based shedding filter. Omit to disable. |
| `adaptiveConcurrency` | object | — | Gradient-based concurrency filter. Omit to disable. |
| `streamingProtection` | object | — | Connection limits for gRPC streaming and WebSocket. Omit to disable. |
| `profiles` | `map[string]ProfileConfig` | — | Named resilience configurations. |
| `activeProfile` | `string` | — | Currently enforced profile. Must match a key in `profiles`. |
| `triggers` | `[]TriggerConfig` | — | Conditions that automatically switch profiles. |
| `schedules` | `[]ScheduleConfig` | — | Time-based proactive profile switches. |
| `signalConfig` | object | — | Signal collection and evaluation configuration. |
| `detection` | object | — | Fast-poll detection loop configuration. |

---

### admissionControl

Controls the `envoy.filters.http.admission_control` filter. The primary load shedding mechanism.

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | `bool` | `true` | Toggle the filter. When false, the EnvoyFilter is deleted from the cluster. |
| `successRateThreshold` | `string` | `"95.0"` | Shed when fewer than this percentage of requests succeed. `"95.0"` = shed when more than 5% fail. Profile overrides take precedence when a profile is active. **Must be a percentage string (e.g. `"95.0"`), not a decimal (e.g. `"0.95"`).**|
| `sheddingSpeed` | `string` | `"1.5"` | Shape of the rejection probability curve. `"1.0"`=linear, `"1.5"`=moderate, `"2.0"`=aggressive, `"3.0"`=very aggressive. Higher = sheds more traffic faster as degradation worsens. |
| `successRateWindow` | `string` | `"30s"` | Rolling time window for success rate calculation. Shorter = reacts faster but noisier. |
| `maxRejectionPercent` | `string` | `"80.0"` | Hard cap on rejection rate. Never rejects more than this percentage regardless of degradation. Non-negotiable safety valve. Do not set above `"95.0"`. |
| `minRequestsPerSecond` | `int32` | `5` | Filter is inactive below this RPS. Prevents false positives during startup and very low traffic. |
| `successCodes` | `[]HTTPStatusRange` | 100–399 | HTTP status ranges counted as successful. gRPC status 0 (OK) is always counted as success. **If your service legitimately returns 4xx (validation, auth), add `{start: 400, end: 499}` here or shedpilot will count healthy 4xx as failures.** |

**HTTPStatusRange:**

| Field | Type | Description |
|---|---|---|
| `start` | `int32` | Inclusive start of the HTTP status range (100–599). |
| `end` | `int32` | Inclusive end of the HTTP status range (100–599). |

**Protocol support:** HTTP/1.1 and unary gRPC only. Does not intercept gRPC streaming or WebSocket.

---

### adaptiveConcurrency

Controls the `envoy.filters.http.adaptive_concurrency` filter. Limits in-flight requests using a gradient controller.

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | `bool` | `true` | Toggle the filter. When false, the EnvoyFilter is deleted. |
| `latencyPercentile` | `p50 \| p75 \| p90 \| p99` | `p50` | Latency percentile used as the ideal baseline. p50 recommended for most services — responds quickly without over-reacting to tail latency. Use p99 only for tail-latency-critical services. |
| `latencyBaselineInterval` | `string` | `"60s"` | How often Envoy recalculates the minimum RTT baseline. Shorter = adapts faster but causes brief elevated 503s during recalculation. Configure client-side retries. |
| `latencyBaselineSampleSize` | `int32` | `50` | Requests sampled per baseline window. |
| `concurrencyAdjustInterval` | `string` | `"100ms"` | How often the concurrency limit is recomputed. |
| `maxLoadIncrease` | `string` | `"2.0"` | Maximum multiplier between adjustment intervals. Prevents runaway scaling. |
| `concurrencyLimit` | `int32` | `0` | Optional hard cap on computed limit. `0` = no cap. |
| `measurementJitter` | `int32` | `10` | Percentage jitter on measurement timing across replicas (0–50). Prevents synchronised measurement. |

**Protocol support:** HTTP/1.1 and unary gRPC only. Does not intercept gRPC streaming or WebSocket.

---

### streamingProtection

Rendered as `DestinationRule.trafficPolicy.connectionPool`. Use for gRPC streaming and WebSocket connections.

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | `bool` | `true` | Toggle. When false, no DestinationRule is created. |
| `maxConcurrentStreams` | `int32` | `200` | Maximum concurrent active streams. Maps to `http2MaxRequests`. |
| `streamTimeoutSeconds` | `int32` | `300` | Maximum stream duration before force-close. Prevents stalled streams holding slots. Maps to `idleTimeout`. |
| `maxPendingRequests` | `int32` | `1024` | Queue depth before immediate rejection. Maps to `http1MaxPendingRequests`. |

---

### profiles

A map of named profile configurations. Keys are arbitrary — name them to describe the service state (e.g. `normal`, `degraded`, `critical`, `flash-sale`).

Each profile specifies **only what changes from the baseline**. Unspecified fields inherit from the top-level `admissionControl` and `adaptiveConcurrency` config.

**ProfileConfig:**

| Field | Type | Description |
|---|---|---|
| `admissionControl` | AdmissionControlOverride | Override admission control fields for this profile. |
| `adaptiveConcurrency` | AdaptiveConcurrencyOverride | Override adaptive concurrency fields for this profile. |

**AdmissionControlOverride** — all fields optional:

| Field | Type | Description |
|---|---|---|
| `successRateThreshold` | `string` | Override the shedding threshold. |
| `sheddingSpeed` | `string` | Override the shedding curve aggressiveness. |
| `successRateWindow` | `string` | Override the calculation window duration. |

**AdaptiveConcurrencyOverride** — all fields optional:

| Field | Type | Description |
|---|---|---|
| `latencyPercentile` | `p50 \| p75 \| p90 \| p99` | Override the gradient baseline percentile. |

---

### triggers

Evaluated every `signalConfig.evaluationIntervalSeconds`. First matching trigger wins. All conditions within a trigger are ANDed.

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | `string` | required | Unique identifier. Pattern: `^[a-z0-9][a-z0-9-]*[a-z0-9]$`. Used in status and logs. |
| `when` | TriggerCondition | required | Conditions that must all be true to fire the trigger. |
| `switchTo` | `string` | required | Profile to activate. Must match a key in `profiles`. |
| `fromProfiles` | `[]string` | — | Only fire when currently in one of these profiles. Empty = fire from any profile. Use on recovery triggers to prevent them from firing during a `flash-sale` profile. |
| `cooldownSeconds` | `int32` | `60` | Minimum seconds between consecutive firings of this trigger. Prevents oscillation. |

**TriggerCondition:**

| Field | Type | Description |
|---|---|---|
| `successRate` | RateCondition | Trigger based on request success rate (0.0–1.0). |
| `serviceLatencyMs` | ThresholdCondition | Trigger based on latency in milliseconds. In v1, this uses total request latency from Envoy stats. |
| `rpsAbove` | `int32` | Trigger when RPS exceeds this value. |

**RateCondition:**

| Field | Type | Default | Description |
|---|---|---|---|
| `below` | `string` | — | Fire when rate falls below this value. Use decimal notation: `"0.90"` (range 0.0001–1.0). |
| `above` | `string` | — | Fire when rate rises above this value. `"0.97"` fires when more than 97% succeed. |
| `consecutiveSamples` | `int32` | `2` | Required consecutive matching evaluations before firing (1–10). Use 2 for breach, 3+ for recovery. |

**ThresholdCondition:**

| Field | Type | Default | Description |
|---|---|---|---|
| `above` | `string` | — | Fire when value exceeds this threshold. |
| `below` | `string` | — | Fire when value falls below this threshold. |
| `consecutiveSamples` | `int32` | `2` | Required consecutive matching evaluations (1–10). |

---

### schedules

Proactive time-based profile switches. Fire at a specific time regardless of service health.

| Field | Type | Description |
|---|---|---|
| `name` | `string` | Unique identifier. |
| `cron` | `string` | Standard 5-field cron expression. Times are UTC. Minimum: `* * * * *`. |
| `switchTo` | `string` | Profile to activate. Must match a key in `profiles`. |
| `fromProfiles` | `[]string` | Only fire when currently in one of these profiles. Empty = fire from any profile. |

---

### signalConfig

| Field | Type | Default | Description |
|---|---|---|---|
| `evaluationIntervalSeconds` | `int32` | `30` | How often trigger conditions are evaluated against live signals (5–300). Also controls drift correction reconcile frequency. |
| `capacityWarningPercent` | `int32` | `10` | Emit `ScalabilityWarning` condition when non-normal profile time exceeds this percentage over the warning window. Set to `0` to disable. |
| `capacityWarningWindowDays` | `int32` | `7` | Rolling window for scalability warning calculation (1–30 days). |

---

### detection

Controls the fast-poll detection loop — a background goroutine per policy that runs independently of the reconcile interval.

| Field | Type | Default | Description |
|---|---|---|---|
| `pollIntervalMs` | `int32` | `500` | How often each pod's stats endpoint is scraped, in milliseconds (100–10000). Shorter = faster detection. 100 pods × 500ms = 200 scrapes/second — negligible cluster traffic. |
| `consecutiveBreaches` | `int32` | `3` | Consecutive scrapes that must all confirm a breach before the signal fires (1–20). Default 3 × 500ms = 1.5s breach confirmation window. |
| `consecutiveRecoveries` | `int32` | `4` | Consecutive clean scrapes required to confirm recovery (1–20). Intentionally higher than `consecutiveBreaches` — recovery should be conservative. |

**Changes to `detection` take effect on the next reconcile — no operator restart required.**

---

## Status fields

| Field | Type | Description |
|---|---|---|
| `detectedBackend` | `string` | Mesh detected: `istio` \| `cilium` \| `none`. |
| `activeProfile` | `string` | Profile currently being enforced. Empty = baseline config. |
| `activeFilters` | `[]string` | Envoy filter names currently installed. |
| `managedResources` | `[]ManagedResource` | All resources owned by this policy. Cascade-deleted when the policy is deleted. |
| `shedRateNow` | `string` | Approximate current rejection percentage. Approximation only — do not alert on this field. `"0%"` when dryRun is true or no shedding is active. |
| `lastDecision` | DecisionRecord | Most recent profile switch with full reasoning. |
| `decisionHistory` | `[]DecisionRecord` | Last 10 decisions in reverse chronological order. |
| `consecutiveBadSamples` | `int32` | How many consecutive evaluations have met degradation conditions. Shows how close a trigger is to firing. |
| `rtdsConnected` | `bool` | Whether fast delivery via Envoy admin API is available. When true, profile switches deliver in <200ms. When false, falls back to EnvoyFilter path (5–30s). |
| `scalabilityWarning` | `bool` | True when the service is chronically shedding — indicates a capacity problem, not a traffic spike. |
| `scalabilityWarningDetail` | `string` | Human-readable explanation of the scalability warning. |
| `nextTriggerEvaluation` | `time` | When the next trigger evaluation will run. |
| `lastReconcileTime` | `time` | When the controller last successfully reconciled. Stale value indicates a stuck controller. |
| `observedGeneration` | `int64` | Spec generation last reconciled. If less than `metadata.generation`, spec changes are pending. |
| `conditions` | `[]Condition` | Standard Kubernetes conditions. |

**Conditions:**

| Type | Meaning |
|---|---|
| `Ready` | All managed resources reconciled correctly. |
| `Degraded` | Reconciliation error. Check `message` field for details. |
| `MeshDetected` | A supported mesh was found. |
| `ScalabilityWarning` | Chronic shedding detected — capacity problem. |

**DecisionRecord:**

| Field | Type | Description |
|---|---|---|
| `timestamp` | `time` | When this decision was made. |
| `triggerName` | `string` | Which trigger fired. Empty for schedule-triggered or manual switches. |
| `scheduleName` | `string` | Which schedule fired. Empty for trigger or manual switches. |
| `profileBefore` | `string` | Profile active before the switch. |
| `profileAfter` | `string` | Profile switched to. |
| `signalValues` | `string` | Plain-English description of the signal values: `"successRate=0.622 < 0.75 (2 consecutive samples)"`. |
| `deliveryMethod` | `string` | `"rtds"` (<200ms via admin API) or `"envoyfilter"` (5–30s via xDS). |
| `deliveryLatencyMs` | `int64` | Delivery duration in milliseconds. |
| `outcome` | `string` | Set ~5 minutes after the decision: `service_recovered` \| `partially_recovered` \| `no_change` \| `over_shed` \| `worsened` \| `pending`. |

---

## Annotations

| Annotation | Value | Effect |
|---|---|---|
| `shedpilot.io/human-override` | `"true"` | Freeze all automatic profile switches. Enforcement continues at current profile. Remove with `kubectl annotate ... shedpilot.io/human-override-`. |
| `shedpilot.io/previous-profile` | profile name | Set by controller on every profile switch. Read-only — do not set manually. |
| `shedpilot.io/managed-by` | string | Records what last modified thresholds (`"human"` or brain version). |

---

## kubectl quick reference

```bash
# List all policies across all namespaces
kubectl get adaptivepolicies -A

# Describe a policy — primary 3am interface
kubectl describe adaptivepolicy payments -n production

# Enable enforcement (disable dryRun)
kubectl patch adaptivepolicy payments -n production \
  --type merge -p '{"spec":{"dryRun":false}}'

# Manual profile switch
kubectl patch adaptivepolicy payments -n production \
  --type merge -p '{"spec":{"activeProfile":"degraded"}}'

# Freeze automatic switches
kubectl annotate adaptivepolicy payments -n production \
  shedpilot.io/human-override=true

# Unfreeze
kubectl annotate adaptivepolicy payments -n production \
  shedpilot.io/human-override-

# View decision history
kubectl get adaptivepolicy payments -n production \
  -o jsonpath='{.status.decisionHistory}' | python3 -m json.tool

# Remove policy and all generated EnvoyFilters
kubectl delete adaptivepolicy payments -n production
```