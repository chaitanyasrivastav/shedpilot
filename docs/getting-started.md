# Getting Started

This guide takes you from zero to a working adaptive policy following the recommended adoption path: observe first, enforce second.

## Prerequisites

- Kubernetes 1.24+
- Istio 1.23+ with sidecar injection enabled on your target namespace
- kubectl configured for your cluster

## Install

```bash
kubectl apply -f https://github.com/chaitanyasrivastav/shedpilot/releases/latest/download/install.yaml
kubectl get pods -n shedpilot-system
# NAME                                          READY   STATUS
# shedpilot-controller-manager-5d4b9f-xk2jq     1/1     Running
# shedpilot-controller-manager-5d4b9f-mn8qp     1/1     Running
```

Two replicas with leader election. One reconciles, one is hot standby.

## Verify Istio sidecar injection

Without sidecars, shedpilot cannot scrape metrics or deliver config.

```bash
kubectl get namespace production --show-labels | grep istio-injection
kubectl label namespace production istio-injection=enabled --overwrite

kubectl rollout restart deployment payments -n production
kubectl rollout status deployment payments -n production

# Each pod must show 2/2 (app + istio-proxy)
kubectl get pods -n production -l app=payments
```

## Step 1 — Start in dryRun mode

Always start with dryRun: true. Filters are installed but never reject requests.

```bash
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
  meshBackend: auto
  admissionControl:
    enabled: true
    successRateThreshold: "95.0"
  adaptiveConcurrency:
    enabled: true
EOF
```

Verify:

```bash
kubectl describe adaptivepolicy payments -n production
```

Expected status:

```
Detected Backend:  istio
Active Filters:    admission-control, adaptive-concurrency
Shed Rate Now:     0%
Ready:             True
Managed Resources:
  EnvoyFilter/payments-admission-control
  EnvoyFilter/payments-adaptive-concurrency
  EnvoyFilter/payments-stats-flush
```

Common failures:
- Ready: False with MeshDetectionFailed — check kubectl get pods -n istio-system
- Ready: False with ApplyFailed — check kubectl get events -n production

## Step 2 — Verify signal collection

Signal collection must be working before enforcement means anything.

```bash
kubectl logs -n shedpilot-system \
  deployment/shedpilot-controller-manager | grep "signal read"
```

Expected:

```
{"msg":"signal read","successRate":"1.0000","rps":"45.2","samples":1356}
```

If samples is always 0: verify pods show 2/2 READY and the selector matches your pods.

## Step 3 — Observe for 1-2 weeks

Watch status.shedRateNow — what would be shed if enforcement were enabled. If non-zero during normal traffic, your threshold may be too tight. If zero even during known degradation events, too loose.

## Step 4 — Add profiles and triggers

```bash
kubectl apply -f - <<'EOF'
apiVersion: resilience.shedpilot.io/v1alpha1
kind: AdaptivePolicy
metadata:
  name: payments
  namespace: production
spec:
  selector:
    app: payments
  dryRun: false
  admissionControl:
    enabled: true
    successRateThreshold: "95.0"
    sheddingSpeed: "1.5"
    maxRejectionPercent: "80.0"
    minRequestsPerSecond: 5
  adaptiveConcurrency:
    enabled: true
    latencyPercentile: p50
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
  activeProfile: normal
  triggers:
  - name: degradation-detected
    when:
      successRate:
        below: "0.90"
        consecutiveSamples: 2
    switchTo: degraded
    cooldownSeconds: 60
  - name: critical-degradation
    when:
      successRate:
        below: "0.75"
        consecutiveSamples: 2
    switchTo: critical
    cooldownSeconds: 60
  - name: recovery
    when:
      successRate:
        above: "0.97"
        consecutiveSamples: 3
    fromProfiles: [degraded, critical]
    switchTo: normal
    cooldownSeconds: 120
  detection:
    pollIntervalMs: 500
    consecutiveBreaches: 3
    consecutiveRecoveries: 4
  signalConfig:
    evaluationIntervalSeconds: 30
    capacityWarningPercent: 10
    capacityWarningWindowDays: 7
EOF
```

## Step 5 — Escape hatches

Keep these in your runbook:

```bash
# Freeze automatic switches (enforcement keeps running)
kubectl annotate adaptivepolicy payments -n production \
  shedpilot.io/human-override=true

# Switch to observe-only (no rejections)
kubectl patch adaptivepolicy payments -n production \
  --type merge -p '{"spec":{"dryRun":true}}'

# Force a profile switch
kubectl patch adaptivepolicy payments -n production \
  --type merge -p '{"spec":{"activeProfile":"critical"}}'

# Unfreeze
kubectl annotate adaptivepolicy payments -n production \
  shedpilot.io/human-override-

# Remove everything
kubectl delete adaptivepolicy payments -n production
```

## Common setup issues

| Symptom | Cause | Fix |
|---|---|---|
| samples always 0 | Operator running outside cluster, or pods missing sidecar | Run operator as pod inside cluster; verify 2/2 READY |
| MeshDetectionFailed | istiod not found | Check kubectl get pods -n istio-system; set meshBackend: istio explicitly |
| ApplyFailed | Threshold as decimal (0.95) not percentage (95.0) | Use string percentage format in all threshold fields |
| deliveryMethod: envoyfilter | Missing pods/exec RBAC | Apply the pods/exec ClusterRole — see README RBAC requirements |
| Trigger oscillation | Recovery firing on zero-sample defaults | Upgrade to v0.2+; ensure fromProfiles on recovery triggers |