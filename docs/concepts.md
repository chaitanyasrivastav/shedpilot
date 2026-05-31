# Concepts

## The death spiral

Every distributed system has the same failure pattern:

1. Service receives more requests than it can handle
2. Requests queue up, latency climbs
3. Callers time out and retry
4. Retry volume amplifies the load
5. More timeouts, more retries
6. Total service failure

This is a **death spiral**. Once it starts, it is very hard to stop. The correct intervention is to shed load early — reject some requests immediately with a clear 503 — before the queue builds and the spiral begins.

## What load shedding is

Load shedding is **controlled degradation** that prevents total failure. When a service is overwhelmed:

- Without shedding: all requests queue, latency climbs for everyone, eventually all requests fail
- With shedding: some requests are rejected immediately with 503, the remaining requests complete successfully

Some users are affected. Most are not. This is categorically better than all users being affected.

## What shedpilot is not

**Not a replacement for autoscaling.** HPA and KEDA manage capacity. shedpilot manages what happens when current capacity is temporarily insufficient — the 2–5 minute gap between "spike detected" and "new pods ready." Both are needed.

**Not a circuit breaker.** Circuit breakers protect outbound calls — your service stops calling a slow dependency. shedpilot protects inbound traffic — your service stops accepting more requests than it can handle. They solve different problems. Use both.

**Not a rate limiter.** Rate limiting asks: "who is this caller and have they sent too many requests?" Load shedding asks: "is my service healthy right now?" A flash sale with 50,000 unique users each sending one request bypasses every rate limit. shedpilot protects regardless of caller identity.

## The two Envoy filters

### admission_control (outer layer)

Tracks a rolling window of request outcomes (`successRateWindow`). When the success rate drops below `successRateThreshold`, it probabilistically rejects incoming requests using Google's Client-Side Throttling formula:

```
P(reject) = max(0, (requests − K × successes) / (requests + 1))
```

where K is derived from `sheddingSpeed`. At 1.0, the rejection curve is linear. At 2.0, it rises faster — more traffic is shed as the service degrades further.

This filter sheds based on **historical evidence**. It does not react to individual requests.

### adaptive_concurrency (inner layer)

Continuously measures the minimum RTT — the ideal latency your service achieves under low load. It computes a gradient:

```
gradient    = minRTT / sampleRTT
concurrency = currentLimit × gradient
```

When in-flight requests exceed the computed limit, new requests receive an immediate 503. No queueing. This filter reacts to **current conditions** inside Envoy at 100ms granularity with zero external dependencies.

### How they compose

```
Incoming request
  → admission_control says: "recent history is bad — probabilistically reject"
  → adaptive_concurrency says: "too many in-flight — reject immediately"
  → your service handles the rest
```

Admission control sheds based on history. Adaptive concurrency prevents the service from being overwhelmed before history accumulates. Together they create a layered defence.

## Profiles

A profile is a named set of overrides applied on top of the baseline filter configuration. When a profile is active, its fields replace the corresponding baseline values. Unspecified fields inherit from the baseline.

```yaml
admissionControl:
  successRateThreshold: "95.0"   # baseline

profiles:
  degraded:
    admissionControl:
      successRateThreshold: "85.0"   # overrides baseline
      sheddingSpeed: "2.0"           # overrides baseline
      # successRateWindow not specified — inherits "30s" from baseline
```

Profile names are arbitrary. Name them to describe the service state: `normal`, `degraded`, `critical`, `flash-sale`, `post-deploy`, `db-slow`.

## Triggers

A trigger evaluates a condition against live signals and switches the active profile when the condition is met for `consecutiveSamples` consecutive evaluations.

```yaml
triggers:
- name: degradation-detected
  when:
    successRate:
      below: "0.90"          # condition
      consecutiveSamples: 2  # must be true 2 evaluations in a row
  switchTo: degraded
  cooldownSeconds: 60        # minimum time between re-firings
```

**`consecutiveSamples`** prevents false positives from transient blips. A single bad evaluation does not fire the trigger.

**`cooldownSeconds`** prevents rapid oscillation between profiles after the trigger fires.

**`fromProfiles`** limits which profiles the trigger can fire from. Recovery triggers should only fire when already in a degraded state — not when normal.

Triggers are evaluated in spec order. The first matching trigger wins.

## Schedules

A schedule fires at a specific time regardless of service health. Use for events you know about in advance.

```yaml
schedules:
- name: friday-sale
  cron: "50 13 * * 5"   # Friday 1:50 PM UTC
  switchTo: flash-sale
```

The key advantage over reactive tools: the schedule pre-arms the service 10 minutes before traffic arrives, not after it hits.

## Signal collection

The controller discovers pods matching the policy `selector` and scrapes the Envoy sidecar stats endpoint directly at `http://<pod-ip>:15090/stats/prometheus`. This endpoint is the sidecar itself — no cluster-level Prometheus installation required.

The scraper computes per-interval rates from cumulative Envoy counters by tracking deltas between consecutive scrapes. It aggregates across all replicas to give fleet-wide signals.

In v2, an OpenTelemetry Collector processor replaces this. It provides richer signals including per-request trace data with causal attribution — distinguishing "own service slow" from "downstream dependency slow."

## RTDS delivery

When a trigger fires, shedpilot delivers the profile change via Istio's RTDS endpoint at port 15010. This pushes new runtime key values directly to Envoy proxies, bypassing the full CRD → EnvoyFilter → Istiod → xDS chain:

```
EnvoyFilter path:  CRD change → Istiod xDS push → Envoy   5–30 seconds
RTDS path:         trigger fires → RTDS push → Envoy        < 200ms
```

EnvoyFilters are still applied at startup to install the filters. RTDS updates the runtime keys (threshold, sheddingSpeed) on profile switches without re-rendering the filter.

## Scalability warning

If your service spends more than `capacityWarningPercent` of the time in a non-normal profile over `capacityWarningWindowDays`, shedpilot sets a `ScalabilityWarning` condition on the status.

This means the service is chronically overwhelmed — a capacity problem, not a spike problem. The correct response is to scale up permanently. Shedpilot is a gap-filler. If the gap is permanent, you need more capacity.