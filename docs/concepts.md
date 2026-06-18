# Concepts

## The death spiral

Every distributed service has the same failure pattern:

1. Service receives more requests than it can handle
2. Requests queue, latency climbs
3. Callers time out and retry
4. Retry volume amplifies the overload
5. More timeouts, more retries
6. Total service failure

This is a death spiral. Once it starts, it is very hard to stop. The correct intervention is to shed load early — reject some requests immediately with a clear 503 — before the queue builds and the spiral begins. Some users get a fast error. Most users complete successfully. This is categorically better than all users experiencing cascading failure.

## What shedpilot is not

**Not a replacement for autoscaling.** Autoscaling manages capacity. shedpilot manages what happens during the 2–5 minute gap between "spike detected" and "new pods ready." Both are needed.

**Not a circuit breaker.** Circuit breakers protect outbound calls — your service stops calling a slow dependency. shedpilot protects inbound traffic — your service stops accepting more requests than it can handle. A flash sale with 50,000 unique users each sending one request bypasses every rate limit and looks fine to a circuit breaker. shedpilot detects the cluster-wide overload signal and responds.

**Not a rate limiter.** Rate limiting asks "who is this caller and have they sent too many requests?" Load shedding asks "is my service healthy right now?" Different questions, different tools.

## The two Envoy filters

### admission_control (outer layer)

Tracks a rolling window of request outcomes. When success rate drops below the threshold, it probabilistically rejects incoming requests using Google's Client-Side Throttling formula:

```
P(reject) = max(0, (requests − K × successes) / (requests + 1))
```

where K is derived from sheddingSpeed. At 1.0 (linear), each 1% drop in success rate causes roughly 1% more rejection. At 2.0 (aggressive), the rejection curve rises faster — the service sheds proportionally more traffic as it degrades further.

This filter sheds based on historical evidence. It does not react to individual requests.

### adaptive_concurrency (inner layer)

Continuously measures the minimum RTT — the ideal latency your service achieves under low load. It computes a gradient:

```
gradient    = minRTT / sampleRTT
concurrency = currentLimit × gradient
```

When in-flight requests exceed the computed limit, new requests get immediate 503. No queueing. This filter reacts to current load at 100ms granularity with zero external dependencies.

### How they compose

Admission control sheds based on recent history. Adaptive concurrency prevents the service from being overwhelmed before that history accumulates. Together they form a layered defence: one historical, one current, neither dependent on the other.

## Profiles

A profile is a named set of filter overrides. When a profile is active, its fields replace the corresponding baseline values. Unspecified fields inherit from baseline.

```yaml
admissionControl:
  successRateThreshold: "95.0"   # baseline

profiles:
  degraded:
    admissionControl:
      successRateThreshold: "85.0"   # overrides baseline
      sheddingSpeed: "2.0"           # overrides baseline
      # successRateWindow not set — inherits "30s" from baseline
```

Name profiles to describe the service state: normal, degraded, critical, flash-sale, db-slow, post-deploy.

## Triggers

A trigger evaluates a condition against live signals and switches profiles when the condition is met for consecutiveSamples consecutive evaluations.

consecutiveSamples prevents false positives from transient noise — a single bad reading does not fire the trigger. cooldownSeconds prevents oscillation after firing. fromProfiles prevents a recovery trigger from firing when you intentionally set a non-standard profile (like flash-sale).

Triggers are evaluated in spec order. First match wins. Order from most severe to least severe.

## Signal collection

The controller reads istio_requests_total from each pod's Envoy sidecar stats endpoint (port 15090) directly. This metric is the Istio telemetry plugin counter, not the raw Envoy cluster stats. It has a response_code label from which the scraper computes success rate by status class.

The scraper stores per-pod cumulative counter snapshots and computes per-interval deltas. A Poller goroutine runs per policy at configurable interval (default 500ms) and applies consecutive-breach debouncing before emitting a signal. Only after N consecutive breach confirmations does the signal fire — preventing reaction to single-reading noise.

**Important for validation APIs:** if your service legitimately returns 4xx (input validation, auth checks), configure successCodes to include status 400-499. Without this, shedpilot counts healthy 4xx responses as failures and may trigger spuriously.

## Fast delivery via Envoy admin API

When a trigger fires, shedpilot calls POST localhost:15000/runtime_modify on every matching pod's istio-proxy container concurrently via the Kubernetes exec API. This changes Envoy's runtime flags immediately — no Istiod, no xDS propagation, no CRD watch latency. All pod calls run in parallel and complete in under 200ms inside a cluster.

The EnvoyFilter re-render runs in parallel as a reliable fallback. If admin API delivery fails for any pod, the EnvoyFilter ensures eventual consistency via standard xDS (5–30s).

## Scalability warning

If your service spends more than capacityWarningPercent of the time in a non-normal profile over capacityWarningWindowDays, the ScalabilityWarning condition fires.

This is not an incident — it is a signal that the service is chronically overwhelmed. The correct response is permanent capacity increase: more replicas, larger pods, or service optimisation. Tuning shedpilot thresholds tighter is not the right response to a capacity problem.