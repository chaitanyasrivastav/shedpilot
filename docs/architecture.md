# Architecture

## Overview

shedpilot is a Kubernetes operator. It watches `AdaptivePolicy` CRDs and keeps cluster state in sync with the desired state defined in those CRDs. It does not run in the request data path — it configures Envoy sidecars, which do.

```
┌─────────────────────────────────────────────────────────────────┐
│  shedpilot controller (runs as a pod in shedpilot-system)       │
│                                                                 │
│  1. Watch AdaptivePolicy CRDs                                   │
│  2. Detect mesh backend (Istio / Cilium)                        │
│  3. Read signals from istio_requests_total (pod stats endpoint) │
│  4. Evaluate trigger conditions                                 │
│  5. Render EnvoyFilter + DestinationRule resources              │
│  6. Apply via server-side apply                                 │
│  7. Fast delivery to sidecars via Envoy admin API (<200ms)      │
│  8. Update status block                                         │
└──────────┬─────────────────────────────────┬───────────────────┘
           │ generates                        │ fast delivery
           ▼                                  ▼
┌────────────────────┐            ┌───────────────────────────────┐
│  Kubernetes API    │            │  Envoy sidecar admin API      │
│  • EnvoyFilter     │            │  localhost:15000/runtime_modify│
│  • DestinationRule │            │  (called via pod exec on each  │
└─────────┬──────────┘            │   matching pod concurrently)  │
          │ Istiod xDS push       └───────────────────────────────┘
          ▼
┌─────────────────────────────────────────────────────────────────┐
│  Envoy sidecar (in every pod matching the policy selector)      │
│  • admission_control filter   (outer — history-based shedding)  │
│  • adaptive_concurrency filter(inner — gradient-based limiting) │
│  • connection pool limits     (DestinationRule — for streaming) │
└─────────────────────────────────────────────────────────────────┘
```

## Reconcile loop

The controller uses a level-triggered reconcile loop — it continuously converges desired state (CRD spec) to actual state (cluster). It does not matter how the system got into its current state or how many times the controller has restarted. The next reconcile always corrects everything.

```
Reconcile(policy):
  1. Fetch AdaptivePolicy — if deleted, Kubernetes GC handles cleanup via owner refs
  2. Ensure Poller goroutine is running for this policy (start or restart on generation change)
  3. Detect mesh backend
  4. Read signals — from Poller channel (fast path) or direct scrape (drift correction)
  5. Evaluate triggers → skip if SampleCount == 0 (no real data yet)
                       → skip if human-override annotation is set
                       → patch spec.activeProfile if trigger fires
  6. Render EnvoyFilter resources from current spec + active profile
  7. Server-side apply all rendered resources (idempotent)
  7b.Prune orphaned resources (filters disabled mid-lifecycle)
  8. Fast delivery via Envoy admin API to all matching pods concurrently
  9. Update status
  10.RequeueAfter(evaluationIntervalSeconds) for drift correction
```

The controller only ever writes `spec.activeProfile` to the CRD spec. Everything else in spec is human-authored. This is intentional — the controller is deterministic and does not make autonomous decisions beyond trigger evaluation.

## Two-speed detection

The controller uses two parallel detection paths with different latencies and purposes:

### Fast path — Poller goroutine (detection)

One Poller goroutine runs per AdaptivePolicy, started on first reconcile, restarted automatically when `spec.detection` changes (generation bump).

```
Poller.Run():
  every pollIntervalMs (default 500ms):
    scrape all pod stats endpoints → compute success rate delta
    if consecutiveBreaches threshold met → emit signal on channel
    if consecutiveRecoveries threshold met → emit recovery signal

Controller.Reconcile():
  select {
  case sig := <-poller.Signals():
      // confirmed breach — evaluate triggers immediately
  default:
      // no signal — scrape directly for status updates
  }
```

The Poller's debounce prevents reacting to a single noisy reading — pod restart counter resets, momentary GC pauses, brief deployment rollout spikes.

### Slow path — RequeueAfter (drift correction)

Every `evaluationIntervalSeconds` (default 30s), the controller reconciles regardless of Poller state. This catches:
- EnvoyFilters manually deleted from the cluster
- Istiod restarts that affect filter config
- Pod label changes shifting workload selector matches
- Spec changes applied while the Poller was mid-tick

## Signal collection

### What is scraped

The controller scrapes `istio_requests_total` from each pod's Envoy sidecar stats endpoint:

```
http://<pod-ip>:15090/stats/prometheus
```

This endpoint is the sidecar itself — no cluster-level Prometheus installation required. The metric has a `response_code` label from which the scraper computes success rate by status class.

**Important:** On Istio 1.30+, request counts are in `istio_requests_total`, not `envoy_cluster_upstream_rq_*`. The scraper reads the Istio telemetry plugin metric.

### Counter handling

Envoy exposes cumulative counters. The scraper stores the previous snapshot per pod and computes per-interval deltas. If a counter decreases (pod restart), the interval is skipped. If the interval between two scrapes is less than 100ms, it is skipped (too short for a reliable delta).

### Fleet aggregation

Success rate and RPS are aggregated across all running pods matching the policy selector. A pod that fails to scrape is skipped and does not affect the aggregate.

### Zero-sample guard

When `SampleCount == 0` (no previous snapshot yet, or interval too short), the scraper returns safe defaults (`successRate=0.99`). Trigger evaluation is skipped entirely on zero-sample readings. This prevents the recovery trigger from firing on the synthetic 0.99 value immediately after a breach.

### stats_flush_on_admin

shedpilot renders a `BOOTSTRAP` EnvoyFilter patch that sets `stats_flush_on_admin: true` on every pod matching the policy selector. Without this, Envoy buffers stats internally for up to 5 seconds before they appear on the endpoint. With it, every scrape returns genuinely current counts.

New pods pick this up at startup. Existing pods need one rolling restart to apply it.

## Fast delivery

### Why not Istiod RTDS?

Istiod 1.23+ does not implement `RuntimeDiscoveryService` as a server. Istiod is a consumer of xDS, not a provider of RTDS. Attempting to push Runtime resources to Istiod results in `Unimplemented: unknown service envoy.service.runtime.v3.RuntimeDiscoveryService`.

### How fast delivery actually works

Every Envoy sidecar exposes an admin API at `localhost:15000`. The `/runtime_modify` endpoint accepts POST requests that immediately change runtime flag values — no Istiod, no xDS propagation, no CRD watch latency.

```bash
# Example — what shedpilot calls on each pod
POST localhost:15000/runtime_modify?admission_control.sr_threshold=0.85&admission_control.aggression=2.0
→ OK
```

The controller calls this endpoint on every matching pod concurrently via the Kubernetes exec API (equivalent to `kubectl exec -c istio-proxy`). For N pods, all calls run in parallel — total delivery is bounded by the slowest single pod, typically under 200ms inside a cluster.

Runtime keys controlled:

| Key | Controls |
|---|---|
| `admission_control.enabled` | Toggle enforcement on/off |
| `admission_control.sr_threshold` | Success rate threshold (0.0–1.0) |
| `admission_control.aggression` | Shedding speed |
| `adaptive_concurrency.enabled` | Toggle concurrency limiting |

### Reliable fallback

The EnvoyFilter re-render (step 7 in the reconcile loop) runs on every profile switch regardless of fast delivery. If fast delivery fails for any pod, the EnvoyFilter ensures eventual consistency via the standard xDS path (5–30s). Both paths are always in play — fast delivery is the accelerator on top of a reliable base.

## Resource ownership

Every resource generated by shedpilot carries an owner reference pointing to the AdaptivePolicy:

```yaml
ownerReferences:
- apiVersion: resilience.shedpilot.io/v1alpha1
  kind: AdaptivePolicy
  name: payments
  controller: true
  blockOwnerDeletion: true
```

When the AdaptivePolicy is deleted, Kubernetes garbage collection automatically deletes all owned resources. No explicit cleanup logic is needed.

## Orphan pruning

Owner references handle deletion of the parent policy. They do not handle mid-lifecycle changes — for example, disabling `adaptiveConcurrency` while the policy still exists leaves the EnvoyFilter in place.

The controller tracks previously rendered resources in `status.managedResources` and explicitly deletes any that are no longer in the current render result on every reconcile. This closes the gap between owner-reference GC (parent deleted) and mid-lifecycle config changes.

## Server-side apply

Resources are applied using `kubectl apply --server-side` with field ownership `shedpilot`. This means:
- Apply is idempotent — calling it twice with identical spec produces no change and no conflict
- Field conflicts with other controllers are surfaced as errors rather than silently overwritten
- The controller will not modify fields it does not own

## High availability

The controller runs as a Deployment with `--leader-elect=true` (default). Only the leader reconciles. Followers are hot-standby. If the leader pod crashes, a follower becomes leader within seconds via the Kubernetes lease mechanism.

During leader transition, Envoy sidecars continue enforcing the last applied configuration. Profile switches resume as soon as the new leader completes its first reconcile.

## Poller lifecycle

```
First reconcile for a policy:
  → ensurePoller() creates Poller with DetectionConfig from spec
  → go poller.Run(ctx)  — goroutine starts

spec.detection changes (generation bumps):
  → ensurePoller() detects generation mismatch
  → cancels old Poller goroutine
  → starts new Poller with updated config
  → no operator restart required

Policy deleted:
  → stopPoller() cancels goroutine
  → Poller exits cleanly
  → owner refs clean up EnvoyFilters
```

Two separate Scraper instances are used — one for the Poller goroutine, one for the reconcile loop. This prevents them from corrupting each other's per-pod counter history (the `previous` snapshot map is protected by a mutex but shared state causes incorrect delta computation when both scrapers race on the same pod in the same interval).

## Istio-specific details

### Filter context

Filters are applied in `SIDECAR_INBOUND` context — they intercept traffic arriving at the service, not traffic the service sends out. Correct for inbound load shedding.

### Filter ordering

Filters are inserted `INSERT_BEFORE` the router filter in the HTTP filter chain:

```
HTTP filter chain:
  [admission_control]      ← shedpilot (outer — shed first)
  [adaptive_concurrency]   ← shedpilot (inner — limit concurrency)
  [router]                 ← existing Istio filter
```

### xDS consistency window

EnvoyFilter changes propagate across the proxy fleet in 1–3 seconds as Istiod pushes xDS updates. During this window, different pods may enforce slightly different thresholds. This is inherent to xDS v3. Fast delivery via the admin API is not subject to this window — it reaches each pod directly and immediately.