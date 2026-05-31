# API Reference

## AdaptivePolicy

```
apiVersion: resilience.shedpilot.io/v1alpha1
kind: AdaptivePolicy
```

### Top-level spec fields

| Field | Type | Default | Description |
|---|---|---|---|
| `selector` | `map[string]string` | required | Pod label selector. Must not be empty. |
| `meshBackend` | `auto \| istio \| cilium` | `auto` | Which mesh to target. `auto` detects automatically. |
| `meshMode` | `sidecar \| ambient` | `sidecar` | Istio data plane mode. |
| `dryRun` | `bool` | `false` | Install filters but never reject. Observe mode. |
| `admissionControl` | object | — | Success-rate-based shedding filter. Omit to disable. |
| `adaptiveConcurrency` | object | — | Gradient-based concurrency filter. Omit to disable. |
| `streamingProtection` | object | — | Connection limits for gRPC streaming / WebSocket. Omit to disable. |
| `profiles` | `map[string]ProfileConfig` | — | Named resilience configurations. |
| `activeProfile` | `string` | — | Currently enforced profile. Must match a key in `profiles`. |
| `triggers` | `[]TriggerConfig` | — | Conditions that automatically switch profiles. |
| `schedules` | `[]ScheduleConfig` | — | Time-based proactive profile switches. |
| `signalConfig` | object | — | Signal collection and evaluation configuration. |

---

### admissionControl

Controls the `envoy.filters.http.admission_control` filter. This is the primary load shedding mechanism.

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | `bool` | `true` | Toggle the filter. When false, the EnvoyFilter is deleted. |
| `successRateThreshold` | `string` | `"95.0"` | Shed when fewer than this percentage of requests succeed. `"95.0"` = shed when >5% fail. Profile overrides take precedence. |
| `sheddingSpeed` | `string` | `"1.5"` | Shape of the rejection probability curve. `1.0`=linear, `1.5`=moderate, `2.0`=aggressive. Higher = sheds more traffic faster as degradation worsens. |
| `successRateWindow` | `string` | `"30s"` | Rolling time window for success rate calculation. Shorter = reacts faster but noisier. Longer = smoother but slower. |
| `maxRejectionPercent` | `string` | `"80.0"` | Hard cap on rejection rate. Never rejects more than this regardless of degradation. Non-negotiable safety valve. Do not set above 95. |
| `minRequestsPerSecond` | `int32` | `5` | Filter is inactive below this RPS. Prevents false positives during startup and very low traffic periods. |
| `successCodes` | `[]HTTPStatusRange` | 100–399 | HTTP status codes counted as successful. gRPC status 0 (OK) is always counted as success. |

**HTTPStatusRange:**

| Field | Type | Description |
|---|---|---|
| `start` | `int32` | Inclusive start of the HTTP status range (100–599). |
| `end` | `int32` | Inclusive end of the HTTP status range (100–599). |

---

### adaptiveConcurrency

Controls the `envoy.filters.http.adaptive_concurrency` filter. Limits concurrent in-flight requests using a gradient controller.

**Protocol support:** HTTP/1.1 and unary gRPC only. Does not intercept gRPC streaming or WebSocket — use `streamingProtection` for those.

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | `bool` | `true` | Toggle the filter. |
| `latencyPercentile` | `p50 \| p75 \| p90 \| p99` | `p50` | Latency percentile used as the ideal baseline. p50 recommended for most services. Use p99 only for tail-latency-critical services. |
| `latencyBaselineInterval` | `string` | `"60s"` | How often Envoy recalculates the minimum RTT baseline. Shorter adapts faster but causes brief measurement disruption (elevated 503s). |
| `latencyBaselineSampleSize` | `int32` | `50` | Number of requests sampled per baseline calculation window. |
| `concurrencyAdjustInterval` | `string` | `"100ms"` | How often the concurrency limit is recomputed. |
| `maxLoadIncrease` | `string` | `"2.0"` | Maximum multiplier between adjustment intervals. Prevents runaway scaling. |
| `concurrencyLimit` | `int32` | `0` | Optional hard cap on the computed concurrency limit. `0` = no cap. |
| `measurementJitter` | `int32` | `10` | Percentage jitter on measurement timing. Prevents synchronised measurement across replicas (0–50). |

---

### streamingProtection

Rendered as `DestinationRule.trafficPolicy.connectionPool`. Use for services with gRPC streaming or WebSocket connections.

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | `bool` | `true` | Toggle. When false, no DestinationRule is created. |
| `maxConcurrentStreams` | `int32` | `200` | Maximum concurrent active streams. Maps to `http2MaxRequests`. |
| `streamTimeoutSeconds` | `int32` | `300` | Maximum stream duration before force-close. Prevents stalled streams holding connection slots. Maps to `idleTimeout`. |
| `maxPendingRequests` | `int32` | `1024` | Queue size before immediate rejection. Maps to `http1MaxPendingRequests`. |

---

### profiles

A map of named profile configurations. Keys are arbitrary — name them to describe the service state.

Each profile specifies **only what changes from the baseline**. Unspecified fields inherit from the top-level `admissionControl` and `adaptiveConcurrency` config.

**ProfileConfig:**

| Field | Type | Description |
|---|---|---|
| `admissionControl` | AdmissionControlOverride | Override admission control fields for this profile. |
| `adaptiveConcurrency` | AdaptiveConcurrencyOverride | Override adaptive concurrency fields for this profile. |

**AdmissionControlOverride:**

| Field | Type | Description |
|---|---|---|
| `successRateThreshold` | `string` | Override the shedding threshold. |
| `sheddingSpeed` | `string` | Override the shedding curve. |
| `successRateWindow` | `string` | Override the calculation window. |

**AdaptiveConcurrencyOverride:**

| Field | Type | Description |
|---|---|---|
| `latencyPercentile` | `p50 \| p75 \| p90 \| p99` | Override the gradient baseline percentile. |

---

### triggers

Evaluated every `signalConfig.evaluationIntervalSeconds` against live Envoy metrics. First matching trigger wins.

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | `string` | required | Unique identifier. Used in status and logs. Pattern: `^[a-z0-9][a-z0-9-]*[a-z0-9]$`. |
| `when` | TriggerCondition | required | Conditions that must be met to fire. All conditions are ANDed. |
| `switchTo` | `string` | required | Profile to activate. Must match a key in `profiles`. |
| `fromProfiles` | `[]string` | — | Only fire when currently in one of these profiles. Empty = fire from any profile. |
| `cooldownSeconds` | `int32` | `60` | Minimum seconds between consecutive firings. Prevents oscillation. |

**TriggerCondition:**

| Field | Type | Description |
|---|---|---|
| `successRate` | RateCondition | Trigger based on request success rate (0.0–1.0). |
| `serviceLatencyMs` | ThresholdCondition | Trigger based on service latency in milliseconds. |
| `rpsAbove` | `int32` | Trigger when RPS exceeds this value. |

**RateCondition:**

| Field | Type | Default | Description |
|---|---|---|---|
| `below` | `string` | — | Fire when rate falls below this value. Format: `"0.90"` (range 0.0–1.0). |
| `above` | `string` | — | Fire when rate rises above this value. Format: `"0.97"` (range 0.0–1.0). |
| `consecutiveSamples` | `int32` | `2` | Required consecutive matching evaluations before firing (1–10). |

**ThresholdCondition:**

| Field | Type | Default | Description |
|---|---|---|---|
| `above` | `string` | — | Fire when value exceeds this threshold. Format: `"500.0"` (milliseconds). |
| `below` | `string` | — | Fire when value falls below this threshold. |
| `consecutiveSamples` | `int32` | `2` | Required consecutive matching evaluations before firing (1–10). |

---

### schedules

Proactive time-based profile switches. Fire before traffic arrives, not after.

| Field | Type | Description |
|---|---|---|
| `name` | `string` | Unique identifier. |
| `cron` | `string` | Standard 5-field cron expression. Times are UTC. |
| `switchTo` | `string` | Profile to activate. Must match a key in `profiles`. |
| `fromProfiles` | `[]string` | Only fire when currently in one of these profiles. Empty = fire from any profile. |

---

### signalConfig

| Field | Type | Default | Description |
|---|---|---|---|
| `metricsEndpoint` | `string` | auto | Override the Envoy stats endpoint URL. Defaults to pod-IP-based auto-discovery. |
| `evaluationIntervalSeconds` | `int32` | `30` | How often trigger conditions are evaluated (5–300). |
| `capacityWarningPercent` | `int32` | `10` | Fire `ScalabilityWarning` condition when non-normal profile time exceeds this percentage over the warning window (0 = disabled). |
| `capacityWarningWindowDays` | `int32` | `7` | Rolling window for scalability warning calculation (1–30 days). |

---

## Status fields

| Field | Type | Description |
|---|---|---|
| `detectedBackend` | `string` | Mesh detected: `istio` \| `cilium` \| `none`. |
| `activeProfile` | `string` | Profile currently being enforced. Empty = baseline. |
| `activeFilters` | `[]string` | Envoy filter names currently installed. |
| `managedResources` | `[]ManagedResource` | All resources owned by this policy. Cascade-deleted with the policy. |
| `shedRateNow` | `string` | Approximate current rejection rate. Approximation only — do not alert on this. |
| `wouldHaveShedRate` | `string` | Shed rate if `dryRun` were false. Only populated when `dryRun: true`. |
| `lastDecision` | DecisionRecord | Most recent profile switch decision with full reasoning. |
| `decisionHistory` | `[]DecisionRecord` | Last 10 decisions in reverse chronological order. |
| `consecutiveBadSamples` | `int32` | How many consecutive evaluations have met degradation conditions. |
| `rtdsConnected` | `bool` | Whether RTDS gRPC stream to Istiod is active. False = falls back to EnvoyFilter path (5–30s). |
| `scalabilityWarning` | `bool` | True when shedding chronically — indicates capacity problem. |
| `scalabilityWarningDetail` | `string` | Human-readable explanation of the scalability warning. |
| `nextTriggerEvaluation` | `time` | When the next trigger evaluation will run. |
| `lastReconcileTime` | `time` | When the controller last successfully reconciled. |
| `conditions` | `[]Condition` | Standard Kubernetes conditions: `Ready`, `Degraded`, `MeshDetected`, `ScalabilityWarning`. |

**DecisionRecord:**

| Field | Type | Description |
|---|---|---|
| `timestamp` | `time` | When this decision was made. |
| `triggerName` | `string` | Which trigger fired (empty for schedule or manual). |
| `scheduleName` | `string` | Which schedule fired (empty for trigger or manual). |
| `profileBefore` | `string` | Profile active before the switch. |
| `profileAfter` | `string` | Profile switched to. |
| `signalValues` | `string` | Plain-English description of the signal values that caused the decision. |
| `deliveryMethod` | `string` | `rtds` (<200ms) or `envoyfilter` (5–30s). |
| `deliveryLatencyMs` | `int64` | How long the delivery took. |
| `outcome` | `string` | `service_recovered` \| `partially_recovered` \| `no_change` \| `over_shed` \| `worsened` \| `pending`. |
| `outcomeDetail` | `string` | Human-readable outcome explanation. |

---

## Annotations

| Annotation | Value | Effect |
|---|---|---|
| `shedpilot.io/human-override` | `"true"` | Prevents all automatic profile switches. Enforcement continues. |
| `shedpilot.io/previous-profile` | profile name | Set by controller on every profile switch. Read-only. |
| `shedpilot.io/brain-mode` | `observe \| assisted \| autonomous` | V2 brain operating mode. |
| `shedpilot.io/managed-by` | string | What last modified thresholds. |

---

## kubectl quick reference

```bash
# List all policies
kubectl get adaptivepolicies -A
kubectl get shp -A                    # short name

# Describe a policy
kubectl describe adaptivepolicy payments -n production

# Enable enforcement
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

# Remove policy and all generated resources
kubectl delete adaptivepolicy payments -n production
```