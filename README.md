# shedpilot

**Adaptive load shedding for Istio. One CRD. Sub-200ms profile switches.**

When traffic spikes, autoscaling takes 2–5 minutes to provision new capacity. During that gap, your service queues requests, latency climbs, and callers cascade into failure. shedpilot bridges that gap — switching your service into a protective profile in under 200ms when conditions are met, then automatically restoring when the service recovers.

```yaml
profiles:
  degraded:
    admissionControl:
      successRateThreshold: "85.0"
      sheddingSpeed: "2.0"

triggers:
- name: degradation-detected
  when:
    successRate: {below: "0.90", consecutiveSamples: 2}
  switchTo: degraded

- name: recovery
  when:
    successRate: {above: "0.97", consecutiveSamples: 3}
  switchTo: normal
```

---

## Why shedpilot?

**Circuit breakers protect your outbound calls. shedpilot protects your inbound traffic. You need both.**

A flash sale with 50,000 unique users each sending one request bypasses every rate limit — they're all within quota. A slow database drops your success rate — autoscaling doesn't help because CPU looks fine. A retry storm from callers amplifies a 10-second blip into a 10-minute outage.

shedpilot protects against all three. It uses Envoy's built-in `admission_control` and `adaptive_concurrency` filters, already running in every Istio sidecar, configured by a single CRD with profiles, triggers, and schedules.

| | Raw EnvoyFilter | shedpilot |
|---|---|---|
| Profile switch speed | 5–30s (CRD → Istiod → xDS) | **<200ms via RTDS** |
| Drift correction | Manual | Level-triggered reconcile |
| Cascade delete | Manual cleanup | Owner references |
| Fleet management | Per-service scripts | One reconcile loop |
| Status at 3am | Check logs | `kubectl describe adaptivepolicy payments` |

At 8,000 RPS, 30 seconds unprotected = 240,000 failed requests. 200ms = 1,600.

---

## Quick start

```bash
# Install
kubectl apply -f https://github.com/shedpilot-io/operator/releases/latest/download/install.yaml

# Apply a policy — dryRun: true observes without enforcing
kubectl apply -f https://github.com/shedpilot-io/operator/raw/main/config/samples/basic.yaml

# Watch what would happen
kubectl describe adaptivepolicy payments

# Enable enforcement when ready
kubectl patch adaptivepolicy payments --type merge -p '{"spec":{"dryRun":false}}'
```

See [docs/getting-started.md](docs/getting-started.md) for the full walkthrough.

---

## How it works

Two Envoy filters, applied on every inbound request:

```
Incoming request
  → admission_control (outer)
      Tracks rolling success rate. When it drops below your threshold,
      probabilistically rejects requests using Google's Client-Side
      Throttling formula. Reacts to historical evidence.

  → adaptive_concurrency (inner)
      Measures minimum RTT continuously. Caps concurrent in-flight
      requests using a gradient controller. Reacts in 100ms inside
      Envoy. No external dependency.

  → your service
```

The controller evaluates your trigger conditions against live Envoy stats (`http://<pod-ip>:15090/stats/prometheus`) — no cluster Prometheus needed. When a trigger fires, it delivers the profile change via RTDS in under 200ms.

---

## Status at 3am

One command tells the complete story:

```bash
kubectl describe adaptivepolicy payments -n production
```

```
Status:
  Detected Backend:        istio
  Active Profile:          degraded
  Active Filters:          admission-control, adaptive-concurrency
  Shed Rate Now:           ~42%
  RTDS Connected:          true
  Last Decision:
    Trigger Name:          degradation-detected
    Signal Values:         successRate=0.882 below 0.90 (2 consecutive samples)
    Profile Before:        normal
    Profile After:         degraded
    Delivery Method:       rtds
    Outcome:               pending
  Consecutive Bad Samples: 0
  Next Trigger Evaluation: 2026-01-15T14:35:16Z
  Conditions:
    Ready: True — 2 resources applied via rtds (<200ms), backend: istio
```

---

## Escape hatches

```bash
# Freeze all automatic switches — enforcement keeps running
kubectl annotate adaptivepolicy payments shedpilot.io/human-override=true -n production

# Switch to observe mode — keep filters installed, disable rejection
kubectl patch adaptivepolicy payments --type merge \
  -p '{"spec":{"dryRun":true}}' -n production

# Manual profile switch
kubectl patch adaptivepolicy payments --type merge \
  -p '{"spec":{"activeProfile":"degraded"}}' -n production

# Remove everything — cascade deletes all EnvoyFilters
kubectl delete adaptivepolicy payments -n production
```

---

## Supported meshes

| Mesh | Filters | RTDS | Notes |
|---|---|---|---|
| Istio sidecar | ✅ | ✅ <200ms | Primary target |
| Istio ambient | ✅ | ✅ | Waypoints are Envoy |
| Cilium | v1.1 | v1.1 | CiliumEnvoyConfig renderer |
| Linkerd | ✗ | ✗ | linkerd2-proxy has no Envoy filters |

---

## Documentation

| | |
|---|---|
| [Getting Started](docs/getting-started.md) | Install, first policy, dryRun adoption path |
| [Concepts](docs/concepts.md) | Load shedding, death spirals, when to use shedpilot |
| [API Reference](docs/api-reference.md) | Every field documented |
| [Profiles & Triggers](docs/profiles-triggers.md) | Writing effective resilience runbooks |
| [Runbook](docs/runbook.md) | 3am incident response |
| [Architecture](docs/architecture.md) | Internals, RTDS, signal collection |

---

## Known limitations

- `admission_control` and `adaptive_concurrency` are HTTP/1.1-scoped. Use `streamingProtection` for gRPC streaming and WebSocket — these filters cannot intercept long-lived connections.
- xDS consistency window: EnvoyFilter updates take 1–3s to propagate across the fleet. RTDS is faster but still has a brief window. Inherent to xDS v3.
- During latency baseline recalculation (`latencyBaselineInterval`), Envoy introduces brief measurement delays. Configure client-side retries.
- Chronic shedding (daily, predictable) is a capacity problem. shedpilot surfaces this via `ScalabilityWarning` — the right response is to scale up, not to tune shedpilot further.

---

## License

Apache 2.0. See [LICENSE](./LICENSE).