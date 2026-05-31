# Architecture

## Overview

shedpilot is a Kubernetes operator — it watches `AdaptivePolicy` CRDs and keeps cluster state in sync with the desired state in those CRDs. It does not run in the data path. It configures Envoy sidecars, which run in the data path.

```
┌─────────────────────────────────────────────────────────────────┐
│  shedpilot controller                                           │
│                                                                 │
│  1. Watch AdaptivePolicy CRDs                                   │
│  2. Detect mesh backend (Istio / Cilium)                        │
│  3. Read signals from Envoy stats endpoint                      │
│  4. Evaluate trigger conditions                                 │
│  5. Render EnvoyFilter + DestinationRule resources              │
│  6. Apply via server-side apply                                 │
│  7. Push profile switches via RTDS (<200ms)                     │
│  8. Update status block                                         │
└─────────────────┬───────────────────────────────────────────────┘
                  │ generates
                  ▼
┌─────────────────────────────────────────────────────────────────┐
│  Kubernetes API                                                 │
│  • EnvoyFilter resources      (one per filter per policy)       │
│  • DestinationRule resources  (one per streaming policy)        │
└─────────────────┬───────────────────────────────────────────────┘
                  │ pushed by Istiod via xDS
                  ▼
┌─────────────────────────────────────────────────────────────────┐
│  Envoy sidecar (in every pod)                                   │
│  • admission_control filter   (outer — history-based shedding)  │
│  • adaptive_concurrency filter(inner — gradient-based limiting) │
│  • connection pool limits     (for streaming connections)       │
└─────────────────────────────────────────────────────────────────┘
```

## Reconcile loop

The controller uses a level-triggered reconcile loop — it continuously compares desired state (CRD spec) to actual state (cluster) and converges them. It does not matter what changed or how many times the controller crashed. The next reconcile always fixes everything.

```
Reconcile(policy):
  1. Get AdaptivePolicy — if deleted, GC handles cleanup via owner refs
  2. Detect mesh backend
  3. Read signals from Envoy stats endpoint
  4. Evaluate triggers → patch spec.activeProfile if trigger fires
  5. Render EnvoyFilter resources from current spec + active profile
  6. Server-side apply all rendered resources (idempotent)
  7. If profile switched and RTDS connected → push RTDS update
  8. Update status
  9. Requeue after evaluationIntervalSeconds
```

The controller only ever writes `spec.activeProfile` to the CRD spec. Everything else in spec is human-authored or written by the v2 brain. This is intentional — the controller is deterministic and does not make autonomous decisions beyond trigger evaluation.

## Two-component design

### Muscle (v1 — this operator)

Deterministic. Watches CRDs, renders filter config, evaluates triggers, delivers profile switches. The evaluation signal in v1 is Envoy stats from the sidecar endpoint. Simple, fast, zero external dependencies.

### Brain (v2 — separate process, not yet built)

Intelligent. Reads OTLP trace spans from an OpenTelemetry Collector processor. Provides causal attribution — distinguishing "own service slow" from "downstream dependency slow." Evaluates richer trigger conditions. Patches `spec.activeProfile` autonomously in the same way a human would via kubectl. The controller doesn't know or care whether the patch came from a human or the brain.

The brain is deliberately deferred until v1 has production users. The trigger spec is identical in both versions — the same YAML works with both the simple (Envoy stats) and rich (OTLP trace) signal sources.

## Signal collection

### V1 — Envoy sidecar stats

```
Controller → HTTP GET → http://<pod-ip>:15090/stats/prometheus
                       (Envoy sidecar stats endpoint)
```

Pod discovery: the controller lists pods matching the policy `selector` in the same namespace, then scrapes each running pod.

Counter handling: Envoy exposes cumulative counters. The scraper tracks deltas between consecutive scrapes to compute per-interval rates. If a counter decreases (pod restart), the interval is skipped.

Aggregation: success rate and RPS are aggregated across all pods in the fleet for the policy.

### V2 — OpenTelemetry Collector processor

An OTel Collector processor reads OTLP trace spans and computes:
- Own service latency (span duration minus downstream call durations)
- Downstream service latency per dependency
- Fleet-wide success rate from span status codes

This provides causal attribution: if success rate drops and own service latency is fine but downstream latency is high, the brain knows not to trigger load shedding on the service — the problem is downstream.

## RTDS delivery

RTDS (Runtime Discovery Service) is one of Envoy's xDS sub-protocols. It allows runtime key-value pairs to be pushed to Envoy sidecars without restarting Envoy or rebuilding the full filter configuration.

The admission_control and adaptive_concurrency filters expose runtime keys that control their behaviour:

```
admission_control.enabled        → toggle filter on/off
admission_control.sr_threshold   → success rate threshold (0.0-1.0)
admission_control.aggression     → shedding speed
adaptive_concurrency.enabled     → toggle filter on/off
```

When a profile switch occurs:
1. Controller has already applied the EnvoyFilter (step 6) — this is the reliable fallback
2. Controller pushes updated runtime key values via RTDS to Istiod port 15010
3. Istiod propagates to relevant Envoy proxies in <200ms
4. Envoy applies new values to new connections immediately

The RTDS push updates only the runtime keys — it does not re-render the full filter configuration. The EnvoyFilter stays in place; only the threshold values change.

If RTDS fails, the EnvoyFilter applied in step 6 already reflects the new profile. It just takes 5–30s to propagate via the normal xDS path instead of <200ms via RTDS.

## Resource ownership

Every resource generated by shedpilot carries owner references pointing to the AdaptivePolicy:

```yaml
ownerReferences:
- apiVersion: resilience.shedpilot.io/v1alpha1
  kind: AdaptivePolicy
  name: payments
  uid: 8347d93a-39a4-4b89-a7ba-d43cffecb65a
  controller: true
  blockOwnerDeletion: true
```

When the AdaptivePolicy is deleted, Kubernetes garbage collection automatically deletes all owned resources — EnvoyFilters, DestinationRules. No explicit cleanup is needed in the controller.

## Server-side apply

Resources are applied using server-side apply with field ownership `shedpilot`. This means:

- Apply is idempotent — calling it twice with the same spec produces no change
- Field ownership prevents conflicts with other controllers managing the same resource
- The controller will not overwrite fields it does not own

## High availability

The controller runs as a 2-replica Deployment with leader election. Only the leader reconciles. If the leader pod is evicted or crashes, one of the other replicas becomes leader within seconds and resumes reconciliation.

During leader transition, Envoy sidecars continue enforcing the last applied configuration. No traffic impact from controller restarts.

## Istio-specific details

### EnvoyFilter context

Filters are applied in `SIDECAR_INBOUND` context — they intercept traffic arriving at the service, not traffic the service sends out. This is the correct context for inbound load shedding.

In ambient mode, waypoint proxies also use `SIDECAR_INBOUND` context for now. This may change as Istio ambient matures.

### Filter ordering

The admission_control and adaptive_concurrency filters are inserted `INSERT_BEFORE` the router filter in the HTTP filter chain. Admission control is applied first (outer), adaptive concurrency second (inner).

```
HTTP filter chain:
  [admission_control]         ← inserted by shedpilot
  [adaptive_concurrency]      ← inserted by shedpilot
  [router]                    ← existing Istio filter
```

### xDS consistency window

EnvoyFilter changes propagate across the proxy fleet as Istiod pushes xDS updates. This takes 1–3 seconds and during this window different pods may enforce slightly different thresholds. This is inherent to xDS v3 and cannot be fixed at this layer.

RTDS updates have a shorter consistency window (<200ms per proxy) because they target specific runtime keys rather than rebuilding the full listener configuration.