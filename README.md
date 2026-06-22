# shedpilot

**Adaptive load shedding for Envoy-based service meshes. One CRD. Sub-200ms profile switches.**

When traffic spikes, autoscaling takes 2–5 minutes to provision new capacity. During that gap, your service queues requests, latency climbs, and callers cascade into failure. shedpilot bridges that gap — detecting degradation in ~3 seconds, switching your service into a protective profile, then automatically restoring when the service recovers.

At 8,000 RPS, 30 seconds unprotected = **240,000 failed requests**. 3 seconds = **24,000**. With fast delivery to all sidecars in under 200ms.

---

## Why shedpilot?

**Circuit breakers protect your outbound calls. shedpilot protects your inbound traffic. You need both.**

Circuit breakers eject individual bad hosts from the load balancing pool. They work perfectly when one pod is misbehaving. But in a flash sale or retry storm, every pod is equally overloaded — so no single host looks worse than any other, and the circuit breaker never trips. That is the scenario where you need protection most.

shedpilot detects the aggregate cluster-wide signal and applies a coordinated response across all sidecars simultaneously. No host ejection needed. Graduated profiles — normal → degraded → critical — give you proportional response rather than binary in/out.

| Scenario | Why it breaks | shedpilot response |
|---|---|---|
| Flash sale: 50k unique users, each within quota | Rate limits don't help — no individual is over quota | Pre-arm a `flash-sale` profile via schedule |
| Slow database | CPU looks fine, autoscaling doesn't trigger | Success rate drops → trigger fires → `degraded` profile engaged |
| Retry storm | Callers amplify a 10s blip into a 10-minute outage | Admission control rejects probabilistically, not all-or-nothing |

| | Raw EnvoyFilter | Istio circuit breaker | shedpilot |
|---|---|---|---|
| Detection latency | Manual | 0–10s (per-host) | **~3s cluster-wide** |
| Profile switch speed | 5–30s | In-process | **<10ms via RTDS gRPC** |
| Cluster-wide overload | ✗ | ✗ | ✅ |
| Graduated response | ✗ | ✗ | ✅ normal→degraded→critical |
| Signal collection | Manual Prometheus | Built-in | Built-in — no Prometheus needed |
| Drift correction | Manual | N/A | Level-triggered reconcile loop |
| Observability | Check logs | Check logs | `kubectl describe` — 3am-legible |

---

## How it works

shedpilot configures two Envoy filters already present in every Istio sidecar. No sidecar changes, no custom builds.

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

### Signal collection

The controller scrapes `istio_requests_total` from each pod's Envoy sidecar stats endpoint (`http://<pod-ip>:15090/stats/prometheus`) directly. No cluster-level Prometheus required — it is just the sidecar itself.

A dedicated background goroutine runs per policy at a configurable interval (default 500ms). It applies consecutive-breach debouncing before acting — 3 consecutive scrapes must all confirm a breach before a trigger fires. This prevents reacting to a single pod restart, GC pause, or deployment rollout spike.

Detection latency in testing on Istio 1.30: **~3 seconds end-to-end** (3 × 500ms breach confirmation + reconcile trigger + profile switch delivery).

### Profile switching

When a trigger fires, the profile change is delivered via shedpilot's built-in RTDS (Runtime Discovery Service) gRPC server. Each Envoy sidecar opens a persistent gRPC stream to shedpilot at startup — one push from shedpilot reaches all connected sidecars simultaneously in under 10ms. No kube-apiserver involvement, no per-pod connection setup.

The EnvoyFilter re-render path (5–30s via standard xDS) runs in parallel as the reliable fallback — if a sidecar is not yet connected to RTDS (e.g. during startup), the EnvoyFilter ensures eventual consistency.

### Reconcile loop

Controller-runtime's level-triggered reconcile ensures the cluster never drifts from spec. It prunes EnvoyFilters that were previously rendered but are no longer needed — for example, when a filter is disabled mid-lifecycle without deleting the policy.

---

## Quick start

```bash
# 1. Install the operator
kubectl apply -f https://github.com/chaitanyasrivastav/shedpilot/releases/latest/download/install.yaml

# 2. Apply a policy in dryRun mode — observes traffic without enforcing
kubectl apply -f - <<'EOF'
apiVersion: resilience.shedpilot.io/v1alpha1
kind: AdaptivePolicy
metadata:
  name: payments
  namespace: production
spec:
  selector:
    app: payments
  dryRun: true
  admissionControl:
    enabled: true
    successRateThreshold: "95.0"
  adaptiveConcurrency:
    enabled: true
EOF

# 3. Watch what would happen
kubectl describe adaptivepolicy payments -n production

# 4. Enable enforcement when ready
kubectl patch adaptivepolicy payments -n production \
  --type merge -p '{"spec":{"dryRun":false}}'
```

**Always observe before enforcing.** Run with `dryRun: true` for at least two weeks. Watch `status.lastDecision` and `status.shedRateNow` to validate your profiles and triggers against real traffic patterns before enabling enforcement.

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
    app: payments             # matches pods by label — must not be empty

  dryRun: false               # true = install filters but never reject requests
  meshBackend: auto           # auto | istio | cilium
  meshMode: sidecar           # sidecar | ambient (Istio only)

  # ── Success-rate-based shedding ───────────────────────────────────────────
  admissionControl:
    enabled: true
    successRateThreshold: "95.0"  # start shedding when <95% of requests succeed
    sheddingSpeed: "1.5"          # 1.0=gentle, 1.5=moderate, 2.0=aggressive
    successRateWindow: "30s"      # rolling window for success/failure history
    minRequestsPerSecond: 5       # inactive below this RPS (cold start safety)
    maxRejectionPercent: "80.0"   # hard cap — always let some traffic through
    successCodes:                 # defaults to 100-399 if omitted
      - {start: 100, end: 399}    # add {start: 400, end: 499} for validation APIs

  # ── Gradient-based concurrency control ────────────────────────────────────
  adaptiveConcurrency:
    enabled: true
    latencyPercentile: p50        # p50 recommended — p99 for latency-critical
    latencyBaselineInterval: 60s  # how often to recalculate minRTT
    latencyBaselineSampleSize: 50
    concurrencyAdjustInterval: 100ms
    maxLoadIncrease: "2.0"
    measurementJitter: 10         # % jitter — prevents synchronised measurement

  # ── Connection limits for gRPC streaming and WebSocket ───────────────────
  streamingProtection:
    enabled: true
    maxConcurrentStreams: 200
    maxPendingRequests: 1024
    streamTimeoutSeconds: 300

  # ── Named resilience profiles ─────────────────────────────────────────────
  # Each profile overrides specific fields from the baseline config.
  # Unspecified fields inherit from admissionControl / adaptiveConcurrency above.
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

  # ── Trigger conditions ────────────────────────────────────────────────────
  # Triggers fire in spec order — first matching trigger wins.
  # Multiple conditions on one trigger are ANDed.
  triggers:
    - name: degradation-detected
      when:
        successRate:
          below: "0.90"
          consecutiveSamples: 2   # require 2 consecutive bad readings
      switchTo: degraded
      cooldownSeconds: 60         # minimum seconds between re-fires

    - name: critical-degradation
      when:
        successRate:
          below: "0.75"
          consecutiveSamples: 2
      switchTo: critical
      cooldownSeconds: 60

    - name: rps-spike             # pre-arm before traffic arrives
      when:
        rpsAbove: 5000
      switchTo: flash-sale
      cooldownSeconds: 300

    - name: recovery
      when:
        successRate:
          above: "0.97"
          consecutiveSamples: 3   # more confirmation needed to recover
      fromProfiles: [degraded, critical]  # only fire from these profiles
      switchTo: normal
      cooldownSeconds: 120

  # ── Proactive schedules ───────────────────────────────────────────────────
  # Schedules fire before traffic arrives — not after. Times are UTC.
  schedules:
    - name: friday-flash-sale-start
      cron: "50 13 * * 5"         # Friday 1:50 PM UTC — 10 min before 2 PM sale
      switchTo: flash-sale

    - name: friday-flash-sale-end
      cron: "30 15 * * 5"
      switchTo: normal
      fromProfiles: [flash-sale]  # don't override if we degraded during the sale

  # ── Signal collection config ──────────────────────────────────────────────
  signalConfig:
    evaluationIntervalSeconds: 30  # how often the reconcile loop reads signals
    capacityWarningPercent: 10     # warn if shedding >10% of time
    capacityWarningWindowDays: 7

  # ── Fast detection loop ───────────────────────────────────────────────────
  # Runs between reconciles. Controls how quickly breaches are detected.
  detection:
    pollIntervalMs: 500            # scrape each pod every 500ms [100–10000]
    consecutiveBreaches: 3         # scrapes confirming breach before signal fires
    consecutiveRecoveries: 4       # clean scrapes confirming recovery
```

---

## Filters in depth

### admission_control

Tracks a rolling window of request outcomes. When success rate drops below `successRateThreshold`, it probabilistically rejects requests using Google's Client-Side Throttling formula. The rejection probability starts low and rises as the service degrades further — it never goes all-or-nothing.

**successCodes** is the most important field to configure correctly. It controls which HTTP status codes the scraper counts as success — matching what Envoy's filter counts internally. The default is 100–399. If your service legitimately returns 400s (validation endpoints, auth APIs), add `{start: 400, end: 499}` to `successCodes` or shedpilot will treat healthy 400s as failures and trigger spuriously.

**maxRejectionPercent** is a non-negotiable safety valve. Default 80%. Never set above 95 — you must always let some traffic through so the service can recover.

Protocol support: HTTP/1.1 and unary gRPC. Not applicable to gRPC streaming or WebSocket — use `streamingProtection` for those.

### adaptive_concurrency

Runs a gradient controller inside Envoy at 100ms granularity. It measures the minimum RTT over a configurable window and continuously adjusts the concurrency limit based on the ratio of observed RTT to minimum RTT. When the in-flight request count hits the limit, new requests receive 503 immediately — no queueing.

**latencyPercentile p50** is recommended for most services. It reflects the median experience and responds quickly to load changes. p99 is appropriate only for services where tail latency specifically must be controlled — it reacts more slowly and may not catch load spikes fast enough.

During the baseline measurement window, there will be brief elevated 503s — configure client-side retries with backoff on the callers.

Protocol support: HTTP/1.1 and unary gRPC only.

### streamingProtection

Rendered as an Istio `DestinationRule.connectionPool`. Provides static connection-level limits for gRPC streaming and WebSocket connections where per-request interception is not possible. Not adaptive — just hard limits.

---

## Detection and delivery in practice

### What was measured on Istio 1.30 (kind cluster)

| Step | Time |
|---|---|
| Spike starts | T+0s |
| 3 consecutive breach confirmations (3 × 500ms) | T+1.5s |
| Reconcile triggered | T+1.5–2s |
| Fast delivery to all sidecars via RTDS | T+2s |
| **Total: normal → critical** | **~3 seconds** |
| Recovery: critical → normal after traffic restored | **~10 seconds** |

These are real numbers from end-to-end testing, not theoretical claims.

### How fast delivery works

shedpilot runs its own RTDS (Runtime Discovery Service) gRPC server on port 15050. Each Envoy sidecar connects to it at startup via a `BOOTSTRAP` EnvoyFilter patch installed automatically by the operator. The connection is a persistent bidirectional gRPC stream.

When a profile switch fires:
1. shedpilot updates the runtime layer in memory
2. Pushes the new values to all connected streams simultaneously — one operation, all pods
3. Envoy applies the new runtime keys inline, no restart needed

Delivery time is bounded by a single gRPC send, not by the number of pods. At 100 pods the delivery time is the same as at 1 pod: under 10ms inside a cluster.

The EnvoyFilter re-render (5–30s via standard xDS) runs in parallel as a reliable fallback. Pods that have not yet connected to RTDS (e.g. during initial startup) receive the correct config via this path.

### Why shedpilot implements its own RTDS server

Istiod 1.23+ does not implement `RuntimeDiscoveryService` as a server — it is a consumer of xDS, not a provider of RTDS. To get sub-10ms delivery without `kubectl exec` on every pod, shedpilot runs its own minimal RTDS gRPC server. Envoy sidecars connect to it directly — Istiod is not involved in profile switches at all.

---

## Observability

`kubectl describe adaptivepolicy payments -n production` is the primary 3am interface. One command answers: what profile is active, what triggered it, what the signal values were, and what delivery method was used.

```
Status:
  Detected Backend:        istio
  Active Profile:          degraded
  Active Filters:          admission-control, adaptive-concurrency
  Shed Rate Now:           ~42%
  RTDS Connected:          true
  Last Reconcile Time:     2026-06-18T15:23:00Z
  Next Trigger Evaluation: 2026-06-18T15:23:30Z
  Consecutive Bad Samples: 0

  Last Decision:
    Trigger Name:    degradation-detected
    Profile Before:  normal
    Profile After:   degraded
    Signal Values:   successRate=0.622 < 0.90 (2 consecutive samples)
    Delivery Method: rtds
    Timestamp:       2026-06-18T15:23:00Z
    Outcome:         pending

  Decision History: (last 10, reverse chronological)
    ...

  Managed Resources:
    - EnvoyFilter/payments-stats-flush
    - EnvoyFilter/payments-admission-control
    - EnvoyFilter/payments-adaptive-concurrency

  Conditions:
    Ready:                      True  — 3 resources applied via rtds (<200ms), backend: istio
    Degraded:                   False
    SignalCollectionAvailable:  True  — sidecar stats endpoints reachable on TCP 15090
```

**status.shedRateNow** — approximate current rejection percentage. Suitable for dashboards; do not alert on this field. Reads `"0%"` when no shedding is active or dryRun is true.

**status.consecutiveBadSamples** — how many consecutive evaluations have met degradation conditions. Useful for understanding how close to firing a trigger is before it actually fires.

**status.scalabilityWarning** — fires when the service has been in a non-normal profile for more than `capacityWarningPercent` of the time over `capacityWarningWindowDays`. If this fires regularly, the correct response is to provision more capacity — not to tune thresholds further.

**status.decisionHistory** — last 10 decisions in reverse chronological order, each with full signal values, delivery method, and outcome. Provides complete incident context without needing to query logs.

---

## Escape hatches

```bash
# Freeze all automatic switches — enforcement stays running at current profile
# Use during incidents to prevent shedpilot from interfering with manual intervention
kubectl annotate adaptivepolicy payments -n production \
  shedpilot.io/human-override=true

# Switch to observe-only mode — filters installed but never reject requests
kubectl patch adaptivepolicy payments -n production \
  --type merge -p '{"spec":{"dryRun":true}}'

# Force a manual profile switch
kubectl patch adaptivepolicy payments -n production \
  --type merge -p '{"spec":{"activeProfile":"degraded"}}'

# Remove human-override — automatic switching resumes
kubectl annotate adaptivepolicy payments -n production \
  shedpilot.io/human-override-

# Remove everything — owner references cascade-delete all EnvoyFilters
kubectl delete adaptivepolicy payments -n production
```

---

## Supported meshes

| Mesh | Filters | Streaming | Fast delivery | Status |
|---|---|---|---|---|
| Istio sidecar (≥1.23) | ✅ | ✅ DestinationRule | ✅ RTDS gRPC <10ms | Primary target |
| Istio ambient | ✅ via waypoint | ✅ | ✅ | Supported |
| Cilium (≥1.14) | ✅ CiliumEnvoyConfig | ✗ | ✗ planned v1.1 | Supported |
| Linkerd | ✗ | ✗ | ✗ | Not planned — no Envoy filters |

Mesh auto-detection runs in priority order when `meshBackend: auto`:
1. Istio — checks for a running `istiod` pod in `istio-system`
2. Cilium — checks for a ready `cilium-envoy` DaemonSet in `kube-system`

---

## Installation

### kubectl

```bash
kubectl apply -f https://github.com/chaitanyasrivastav/shedpilot/releases/latest/download/install.yaml
```

The install bundle includes the CRD, RBAC, and the controller Deployment in the `shedpilot-system` namespace.

### From source

```bash
export IMG=your-registry/shedpilot:latest
make docker-build docker-push IMG=$IMG
make deploy IMG=$IMG
```

### Operator flags

| Flag | Default | Description |
|---|---|---|
| `--enable-rtds` | `true` | Enable the RTDS gRPC server for sub-10ms profile switching |
| `--rtds-port` | `15050` | Port the RTDS gRPC server listens on |
| `--leader-elect` | `true` | Enable leader election (required for HA) |
| `--metrics-bind-address` | `:8080` | Prometheus metrics endpoint |
| `--health-probe-bind-address` | `:8081` | Liveness/readiness probe endpoint |

### RBAC requirements

```
resilience.shedpilot.io/adaptivepolicies         get, list, watch, create, update, patch, delete
resilience.shedpilot.io/adaptivepolicies/status   get, update, patch
networking.istio.io/envoyfilters                  get, list, watch, create, update, patch, delete
networking.istio.io/destinationrules              get, list, watch, create, update, patch, delete
core/pods                                         get, list, watch
```

**No `pods/exec` required.** Fast delivery is handled by the RTDS gRPC server — Envoy sidecars connect to shedpilot, not the other way around.

**NetworkPolicy note**: the operator pod must be able to reach pod IPs on TCP 15090 (Envoy sidecar stats endpoint) for signal collection. If your cluster uses strict NetworkPolicy, add an egress rule allowing the `shedpilot-system` namespace to reach pods on port 15090. If this is blocked, the `SignalCollectionAvailable` condition will be False and triggers will not fire.

---

## Known limitations

- `admission_control` and `adaptive_concurrency` intercept HTTP/1.1 and unary gRPC only. They cannot intercept long-lived gRPC streaming or WebSocket connections. Use `streamingProtection` for those.

- Signal collection reads `istio_requests_total` from the destination pod's sidecar. When all pods are down simultaneously (scaled to zero), there are no sidecars to scrape — detection cannot fire. This is the correct behaviour: if there are no pods, there is no traffic to shed.

- Signal collection requires the operator pod to reach pod IPs on TCP 15090. If NetworkPolicy blocks this, triggers will not fire and the `SignalCollectionAvailable` condition will be False. The policy still installs filters and protects the service via the EnvoyFilter config — only automatic trigger-based profile switches are affected.

- The `adaptive_concurrency` filter introduces brief elevated 503s during latency baseline recalculation. Configure client-side retries with appropriate backoff on callers.

- `serviceLatencyMs` triggers use total request latency in v1. Causal attribution (service latency excluding downstream dependency time) requires the v2 brain with OTLP trace ingestion.

- `spec.detection` changes take effect on the next reconcile — no operator restart required.

- Cilium fast delivery is planned for v1.1. Currently, Cilium profile switches use CiliumEnvoyConfig re-render (5–30s delivery).

- Chronic shedding (daily, predictable) is a capacity problem, not a traffic spike. shedpilot surfaces this via `status.scalabilityWarning`. The correct response is to provision more capacity — not to tune thresholds further.

---

## License

MIT. See [LICENSE](./LICENSE).