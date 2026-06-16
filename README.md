# shedpilot

**Adaptive load shedding for Envoy-based service meshes. One CRD. Sub-200ms profile switches.**

When traffic spikes, autoscaling takes 2–5 minutes to provision new capacity. During that gap, your service queues requests, latency climbs, and callers cascade into failure. shedpilot bridges that gap — switching your service into a protective profile in under 200ms when conditions are met, then automatically restoring when the service recovers.

At 8,000 RPS, 30 seconds unprotected = **240,000 failed requests**. 200ms = **1,600**.

---

## Why shedpilot?

**Circuit breakers protect your outbound calls. shedpilot protects your inbound traffic. You need both.**

| Scenario | Why it breaks | shedpilot response |
|---|---|---|
| Flash sale: 50k unique users, each within quota | Rate limits don't help — no individual is over quota | Pre-arm a `flash-sale` profile via schedule |
| Slow database | CPU looks fine, autoscaling doesn't trigger | Success rate drops → trigger fires → `degraded` profile engaged |
| Retry storm | Callers amplify a 10s blip into a 10-minute outage | Admission control rejects probabilistically, not all-or-nothing |

| | Raw EnvoyFilter | shedpilot |
|---|---|---|
| Profile switch speed | 5–30s (CRD → Istiod → xDS) | **<200ms via RTDS** |
| Signal collection | Manual Prometheus query | Built-in Envoy stats scrape — no cluster Prometheus needed |
| Fast detection | Reconcile interval (≥30s) | Configurable 500ms poll with consecutive-breach debouncing |
| Drift correction | Manual | Level-triggered reconcile loop |
| Cascade delete | Manual cleanup | Owner references on all generated resources |
| Observability | Check logs | `kubectl describe ap payments` — 3am-legible status |

---

## How it works

shedpilot configures two Envoy filters already present in every Istio and Cilium sidecar. No sidecar changes, no custom builds.

```
Incoming request
  │
  ▼
admission_control  (outer)
  Tracks a rolling window of request outcomes. When success rate drops
  below your threshold, probabilistically rejects requests using Google's
  Client-Side Throttling formula:
    P(reject) = max(0, (requests − K×successes) / (requests + 1))
  where K = 1/sheddingSpeed. Reacts to historical evidence.
  │
  ▼
adaptive_concurrency  (inner)
  Measures minimum RTT continuously inside Envoy at 100ms granularity.
  Caps in-flight requests with a gradient controller:
    gradient = minRTT / sampleRTT
    newLimit = currentLimit × gradient
  Requests that arrive when the limit is full get immediate 503.
  No queueing. No external dependency.
  │
  ▼
  your service
```

**Signal collection** — the controller scrapes Envoy's built-in stats endpoint on every pod (`http://<pod-ip>:15090/stats/prometheus`) directly. No cluster Prometheus required. A dedicated background poller runs at 500ms per policy (configurable), debounces noise with configurable consecutive-breach counting, and emits confirmed signals to the reconcile loop in under 200ms.

**Profile switching** — when a trigger fires, the profile change is delivered via RTDS (Envoy's Runtime Discovery Service) directly to Istiod in under 200ms. If RTDS is unavailable, the operator falls back to re-rendering the EnvoyFilter (5–30s delivery). Both paths are always in play — RTDS is the fast lane on top of a reliable base.

**Reconcile loop** — controller-runtime's level-triggered reconcile ensures the cluster never drifts from spec. It prunes EnvoyFilters and DestinationRules that were previously rendered but are no longer needed (e.g. when a filter is disabled mid-lifecycle).

---

## Quick start

```bash
# 1. Install the operator
kubectl apply -f https://github.com/chaitanyasrivastav/shedpilot/releases/latest/download/install.yaml

# 2. Apply a policy in dryRun mode — observes traffic without enforcing
kubectl apply -f https://github.com/chaitanyasrivastav/shedpilot/raw/main/config/samples/basic.yaml

# 3. Watch what would happen
kubectl describe adaptivepolicy payments

# 4. Enable enforcement when ready
kubectl patch adaptivepolicy payments --type merge -p '{"spec":{"dryRun":false}}'
```

The recommended adoption path is always: **observe first, enforce second**. Run with `dryRun: true` for at least two weeks. Watch `status.lastDecision` and `status.shedRateNow` to validate your profiles and triggers before enabling enforcement.

---

## The AdaptivePolicy CRD

```yaml
apiVersion: resilience.shedpilot.io/v1alpha1
kind: AdaptivePolicy
metadata:
  name: payments
  namespace: production
spec:
  selector:
    app: payments             # matches pods — must not be empty

  dryRun: false               # true = enforce nothing, observe only
  meshBackend: auto           # auto | istio | cilium
  meshMode: sidecar           # sidecar | ambient (Istio only)

  admissionControl:           # success-rate-based shedding
    enabled: true
    successRateThreshold: "95.0"     # shed when <95% of requests succeed
    sheddingSpeed: "1.5"             # 1.0=gentle, 1.5=moderate, 2.0=aggressive
    successRateWindow: "30s"         # rolling window for success/failure history
    minRequestsPerSecond: 5          # inactive below this RPS (cold start safety)
    maxRejectionPercent: "80.0"      # hard cap — always let some traffic through
    successCodes:                    # defaults to 100-399 if omitted
      - {start: 100, end: 399}       # customize for APIs with healthy 4xx traffic

  adaptiveConcurrency:        # gradient-based concurrency control
    enabled: true
    latencyPercentile: p50           # gradient baseline: p50 | p75 | p90 | p99
    latencyBaselineInterval: 60s     # how often to recalculate minRTT
    latencyBaselineSampleSize: 50    # requests sampled per baseline window
    concurrencyAdjustInterval: 100ms # how often Envoy recomputes the limit
    maxLoadIncrease: "2.0"           # cap on limit multiplier between intervals
    concurrencyLimit: 0              # hard cap (0 = no hard cap)
    measurementJitter: 10            # % jitter — prevents synchronized measurement

  streamingProtection:        # connection-level limits for gRPC/WebSocket
    enabled: true
    maxConcurrentStreams: 200        # maps to http2MaxRequests
    maxPendingRequests: 1024         # queue size before immediate 503
    streamTimeoutSeconds: 300        # max stream duration before force-close

  profiles:
    normal:
      admissionControl:
        successRateThreshold: "95.0"
        sheddingSpeed: "1.5"

    degraded:
      admissionControl:
        successRateThreshold: "85.0"
        sheddingSpeed: "2.0"
        successRateWindow: "20s"
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

  activeProfile: normal       # switched by triggers, schedules, or manual patch

  triggers:
    - name: degradation-detected
      when:
        successRate:
          below: "0.90"
          consecutiveSamples: 2     # require 2 consecutive bad readings
      switchTo: degraded
      cooldownSeconds: 60           # minimum seconds between re-fires

    - name: critical-degradation
      when:
        successRate:
          below: "0.80"
          consecutiveSamples: 2
      switchTo: critical
      cooldownSeconds: 60

    - name: rps-spike            # flash-sale leading indicator
      when:
        rpsAbove: 5000
      switchTo: flash-sale
      cooldownSeconds: 300

    - name: recovery
      when:
        successRate:
          above: "0.97"
          consecutiveSamples: 3     # more confirmation needed to recover
      fromProfiles: [degraded, critical]
      switchTo: normal
      cooldownSeconds: 120

  schedules:
    - name: friday-flash-sale-start
      cron: "50 13 * * 5"           # Friday 1:50 PM UTC — fires BEFORE traffic
      switchTo: flash-sale

    - name: friday-flash-sale-end
      cron: "30 15 * * 5"
      switchTo: normal
      fromProfiles: [flash-sale]    # don't override if we degraded during the sale

  signalConfig:
    evaluationIntervalSeconds: 30   # reconcile loop frequency
    capacityWarningPercent: 10      # ScalabilityWarning if shedding >10% of time
    capacityWarningWindowDays: 7

  detection:                  # fast-poll detection loop (runs between reconciles)
    pollIntervalMs: 500             # scrape each pod every 500ms [100–10000]
    consecutiveBreaches: 3          # scrapes confirming breach before signal fires
    consecutiveRecoveries: 4        # clean scrapes confirming recovery
```

---

## Filters in depth

### admission_control

Tracks a rolling window of request outcomes. Uses Google's Client-Side Throttling formula to probabilistically reject requests when success rate drops below the configured threshold.

- **successRateThreshold** — the rate (0–100) below which the filter begins shedding. Matched to what Envoy itself counts, which is controlled by `successCodes`.
- **sheddingSpeed** — controls the aggressiveness of the shedding curve. At 1.0, shedding is linear. At 2.0 or 3.0, the filter sheds more aggressively once a breach is confirmed.
- **successCodes** — which HTTP status ranges count as success. Defaults to 100–399. If your API returns healthy 4xx (validation services, authentication endpoints), configure this to include 400 so the filter does not false-positive on normal traffic.
- **maxRejectionPercent** — hard safety valve. Never reject more than this percentage regardless of how degraded the service is. Default 80%. Do not set above 95.
- **minRequestsPerSecond** — the filter is inactive below this RPS. Prevents false positives during cold start and very low traffic periods.

Protocol support: HTTP/1.1 and unary gRPC. Not applicable to gRPC streaming or WebSocket (use `streamingProtection` for those).

### adaptive_concurrency

Runs a gradient controller inside Envoy at sub-second intervals. Measures the minimum RTT over a configurable window and computes a concurrency limit from the ratio of observed RTT to minimum RTT. Requests that arrive when the in-flight count exceeds the limit receive 503 immediately.

- **latencyPercentile** — p50 is recommended for most services. It reflects the median experience and responds quickly to load changes without over-reacting to tail latency. Use p99 only for latency-critical services where tail latency must be specifically controlled.
- **latencyBaselineInterval** — how often Envoy recalculates the minimum RTT. Shorter = adapts faster but causes more measurement disruption (brief spike in 503s during measurement windows). Configure client-side retries.
- **measurementJitter** — adds random jitter to baseline measurement timing across replicas. Prevents a fleet from measuring simultaneously and amplifying the measurement disruption.

Protocol support: HTTP/1.1 and unary gRPC. Not applicable to gRPC streaming or WebSocket.

### streamingProtection

Rendered as an Istio `DestinationRule.connectionPool`. Unlike `admission_control` and `adaptive_concurrency`, this applies to long-lived connections (gRPC streaming, WebSocket) where per-request interception is not possible. It provides static limits — not adaptive.

- **maxConcurrentStreams** — caps active concurrent streams. Maps to `http2MaxRequests`.
- **maxPendingRequests** — queue depth before immediate 503. Maps to `http1MaxPendingRequests`.
- **streamTimeoutSeconds** — force-closes stalled streams holding connection slots.

---

## Profiles and triggers

**Profiles** are named configurations you define once and switch between. Each profile specifies only what changes from the baseline — unspecified fields inherit from `spec.admissionControl` and `spec.adaptiveConcurrency`.

Recommended profile set:

| Profile | Intent | Typical config |
|---|---|---|
| `normal` | Baseline protection, healthy service | successRateThreshold: "95.0" |
| `degraded` | Service struggling, tighten gates | successRateThreshold: "85.0", sheddingSpeed: "2.0" |
| `critical` | Service barely alive, protect at all costs | successRateThreshold: "75.0", sheddingSpeed: "3.0" |
| `flash-sale` | Pre-armed for known high-traffic events | successRateThreshold: "90.0", sheddingSpeed: "1.8" |

**Triggers** fire when conditions are met on live signals. Signal conditions:

| Field | Fires when | Example |
|---|---|---|
| `successRate.below` | Success rate drops below threshold | `below: "0.90"` |
| `successRate.above` | Success rate rises above threshold | `above: "0.97"` |
| `serviceLatencyMs.above` | Latency exceeds threshold in ms | `above: "500.0"` |
| `rpsAbove` | Requests per second exceeds value | `rpsAbove: 5000` |

Multiple conditions on one trigger are **ANDed** — all must be true simultaneously.

**consecutiveSamples** controls how many consecutive evaluations must satisfy the condition before the trigger fires. Use at least 2 for breach triggers to avoid reacting to a single noisy reading. Use 3 or more for recovery triggers — you want confident evidence before restoring.

**cooldownSeconds** prevents rapid oscillation. After a trigger fires, it cannot re-fire until the cooldown window has elapsed.

**fromProfiles** limits which source profiles a trigger can fire from. Use this on recovery triggers to ensure they only restore when currently degraded, not when running a scheduled `flash-sale` profile.

**Schedules** are proactive — they fire before traffic arrives, not after. This is the key advantage over purely reactive tools: you know a flash sale starts at 2pm, so arm at 1:50pm. Schedules run in UTC. Each schedule can specify `fromProfiles` to prevent overriding an already-active protective profile if conditions degrade during the event.

---

## Fast detection loop

The signal collection loop runs as a background goroutine per policy, independent of the controller's reconcile interval. It scrapes each pod's Envoy stats endpoint at `pollIntervalMs` cadence (default 500ms) and applies consecutive-breach debouncing before emitting a confirmed signal.

```yaml
detection:
  pollIntervalMs: 500           # default: 500ms per pod
  consecutiveBreaches: 3        # 3 × 500ms = 1.5s breach latency
  consecutiveRecoveries: 4      # 4 × 500ms = 2s recovery latency
```

**Detection latency** = `consecutiveBreaches × pollIntervalMs` + RTDS propagation (<200ms).
Default configuration: **1.5s worst-case detection latency**, 40× faster than a 30s reconcile interval.

**Flap protection** — a breach must be confirmed by N consecutive scrapes before the signal fires. Recovery requires M consecutive clean scrapes. This prevents reacting to a single pod restart counter reset, momentary GC pause, or brief deployment rollout spike. The recovery threshold is intentionally higher than the breach threshold — oscillation on a borderline-healthy service is worse than delayed recovery.

Scrape errors (pod unreachable, cluster API down) do not count toward breach or recovery — state is held as-is until the next tick.

---

## Sub-200ms profile switching via RTDS

When a trigger fires, the profile change is delivered via RTDS:

1. The controller opens a `StreamRuntime` gRPC stream to Istiod
2. It subscribes to the runtime layer, ACKs the current state, then pushes the updated runtime keys
3. Istiod distributes the update to all matching Envoy proxies via xDS in <200ms

Runtime keys controlled:

| Key | Controls |
|---|---|
| `admission_control.enabled` | Toggle enforcement on/off (dryRun switch) |
| `admission_control.sr_threshold` | Success rate threshold (0.0–1.0) |
| `admission_control.aggression` | Shedding speed |
| `adaptive_concurrency.enabled` | Toggle concurrency limiting |

If RTDS is unavailable (connection lost, Istiod unreachable), updates are queued in memory and replayed when the connection is re-established. The RTDS client reconnects with exponential backoff (1s base, 30s cap). The EnvoyFilter re-render path (5–30s delivery via standard xDS) is always the reliable fallback — RTDS is the fast lane on top of it.

RTDS connectivity is reflected in `status.rtdsConnected`. If false, profile switches still work — they just take longer.

---

## Stats freshness

shedpilot renders a `BOOTSTRAP` EnvoyFilter patch (`stats_flush_on_admin: true`) alongside every admission control or adaptive concurrency filter. Without this, Envoy buffers stats internally for up to 5 seconds before they appear on the stats endpoint. With it, every GET to `:15090/stats/prometheus` bypasses the flush timer and returns genuinely current counts — critical for the 500ms detection loop.

New pods pick up the bootstrap config at startup. Existing pods need one rolling restart to apply it. The operator tracks this EnvoyFilter in `status.managedResources` and prunes it if all filters are later disabled.

---

## Observability

`kubectl describe adaptivepolicy payments -n production` is the primary 3am interface:

```
Status:
  Detected Backend:        istio
  Active Profile:          degraded
  Active Filters:          admission-control, adaptive-concurrency
  Shed Rate Now:           ~42%
  RTDS Connected:          true
  Last Reconcile Time:     2026-01-15T14:34:46Z
  Next Trigger Evaluation: 2026-01-15T14:35:16Z
  Consecutive Bad Samples: 0

  Last Decision:
    Trigger Name:    degradation-detected
    Profile Before:  normal
    Profile After:   degraded
    Signal Values:   successRate=0.882 < 0.90 (2 consecutive samples)
    Delivery Method: rtds
    Timestamp:       2026-01-15T14:31:02Z
    Outcome:         service_recovered

  Decision History:        (last 10, reverse chronological)
    ...

  Managed Resources:
    - EnvoyFilter/payments-stats-flush
    - EnvoyFilter/payments-admission-control
    - EnvoyFilter/payments-adaptive-concurrency

  Conditions:
    Ready:   True  — 3 resources applied via rtds (<200ms), backend: istio
    Degraded: False
```

**status.shedRateNow** — approximate current rejection percentage using the Client-Side Throttling formula inverse. Suitable for dashboards and operational awareness; do not alert on this field. Reads `"0%"` when no shedding is active.

**status.consecutiveBadSamples** — how many consecutive evaluations have met degradation conditions. Useful for understanding how close a trigger is to firing before it actually does.

**status.scalabilityWarning** — fires when the service has been in a non-normal profile for more than `capacityWarningPercent` of the time over `capacityWarningWindowDays`. If this fires regularly, the correct response is to provision more capacity — not to tune shedpilot further.

**status.decisionHistory** — last 10 decisions in reverse chronological order, each with full signal values, delivery method, and outcome. Provides complete incident context without needing to query logs.

---

## Escape hatches

```bash
# Freeze all automatic switches — keeps enforcement running at current profile
kubectl annotate adaptivepolicy payments shedpilot.io/human-override=true -n production

# Switch to observe-only mode — filters stay installed but never reject requests
kubectl patch adaptivepolicy payments --type merge \
  -p '{"spec":{"dryRun":true}}' -n production

# Force a manual profile switch
kubectl patch adaptivepolicy payments --type merge \
  -p '{"spec":{"activeProfile":"degraded"}}' -n production

# Remove human-override — resumes automatic switching
kubectl annotate adaptivepolicy payments shedpilot.io/human-override- -n production

# Remove everything — owner references cascade-delete all EnvoyFilters
kubectl delete adaptivepolicy payments -n production
```

---

## Supported meshes

| Mesh | Filters | Streaming | RTDS | Status |
|---|---|---|---|---|
| Istio sidecar | ✅ admission_control + adaptive_concurrency | ✅ DestinationRule | ✅ <200ms | Primary target |
| Istio ambient | ✅ (via waypoint proxy) | ✅ | ✅ | Supported |
| Cilium (≥1.14) | ✅ CiliumEnvoyConfig | ✗ | ✗ (planned v1.1) | Supported — RTDS in v1.1 |
| Linkerd | ✗ | ✗ | ✗ | Not planned — no Envoy filters |

**Mesh auto-detection** runs in priority order when `meshBackend: auto`:
1. Istio — checks for a running `istiod` pod in `istio-system`
2. Cilium — checks for a ready `cilium-envoy` DaemonSet in `kube-system`

Set `meshBackend: istio` or `meshBackend: cilium` explicitly to skip auto-detection. Set `meshMode: ambient` for Istio ambient (waypoint) deployments.

---

## Installation

### kubectl

```bash
kubectl apply -f https://github.com/chaitanyasrivastav/shedpilot/releases/latest/download/install.yaml
```

The install bundle includes the CRD, RBAC, and the controller Deployment in the `shedpilot-system` namespace.

### From source

```bash
# Build and push the image
export IMG=your-registry/shedpilot:latest
make docker-build docker-push IMG=$IMG

# Generate CRDs and deploy
make deploy IMG=$IMG
```

### Operator flags

| Flag | Default | Description |
|---|---|---|
| `--enable-rtds` | `true` | Enable RTDS for sub-200ms profile switching |
| `--istiod-rtds-address` | `istiod.istio-system.svc.cluster.local:15010` | Istiod RTDS gRPC address |
| `--leader-elect` | `true` | Enable leader election (required for HA, ≥2 replicas) |
| `--metrics-bind-address` | `:8080` | Prometheus metrics endpoint |
| `--health-probe-bind-address` | `:8081` | Liveness/readiness probe endpoint |

RTDS failure at startup is **non-fatal** — the operator logs a warning and continues using the EnvoyFilter path. RTDS reconnects automatically when Istiod becomes reachable.

---

## RBAC requirements

The operator requires:

```
resilience.shedpilot.io/adaptivepolicies         get, list, watch, create, update, patch, delete
resilience.shedpilot.io/adaptivepolicies/status   get, update, patch
networking.istio.io/envoyfilters                  get, list, watch, create, update, patch, delete
networking.istio.io/destinationrules              get, list, watch, create, update, patch, delete
core/pods                                         get, list, watch
```

---

## Known limitations

- `admission_control` and `adaptive_concurrency` are HTTP/1.1-scoped. They cannot intercept long-lived gRPC streaming or WebSocket connections. Use `streamingProtection` for those.
- xDS consistency window: EnvoyFilter updates propagate across the proxy fleet in 1–3s. RTDS is faster but still has a brief window during which different proxies may enforce different configs. Inherent to xDS v3.
- The `adaptive_concurrency` filter introduces brief measurement disruption (elevated 503s) during latency baseline recalculation. Configure client-side retries with appropriate backoff.
- The fast-poll detection loop is created with the config at creation time. If `spec.detection` is changed after the policy exists, the operator must be restarted to pick up new poll parameters. (v0.2 fix.)
- Cilium RTDS support is planned for v1.1. Currently, Cilium profile switches use CiliumEnvoyConfig re-render (5–30s delivery).
- `serviceLatencyMs` triggers in v1 fall back to total request latency from Envoy stats. Causal attribution (service latency excluding downstream dependency time) requires the v2 brain with OTLP trace ingestion.
- Chronic shedding (daily, predictable) is a capacity problem. shedpilot surfaces this via `status.scalabilityWarning`. The right response is to provision more capacity — not to tune thresholds further.

---

## License

MIT. See [LICENSE](./LICENSE).