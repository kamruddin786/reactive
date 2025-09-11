# Prometheus HPA for SSE Active User Metrics

This directory contains the necessary configurations and scripts to set up Horizontal Pod Autoscaling (HPA) based on custom active user metrics for the Reactive SSE Application. The autoscaling is configured to scale the application based on the number of active SSE users.

## Files Overview

- **prometheus-adapter-config.yaml**: Configuration for the Prometheus Adapter to expose the `sse_active_users` metric to the Kubernetes custom metrics API.
- **prometheus-scrape-config.yaml**: Additional scrape configuration for Prometheus to collect metrics from the application.
- **hpa-config.yaml**: HPA configuration that defines scaling rules based on the number of active users.
- **setup-metrics-hpa.ps1**: PowerShell script to set up the metrics and HPA configuration.
- **setup-metrics-hpa.sh**: Bash script to set up the metrics and HPA configuration.
- **test-hpa-scaling.ps1**: PowerShell script to test the HPA by generating simulated load.
- **test-hpa-scaling.sh**: Bash script to test the HPA by generating simulated load.

## Changes Made to Enable Metrics-based Autoscaling

1. **Custom Metric Registration**: The application registers the `sse_active_users` metric via Micrometer/Prometheus in `SseMetricsConfiguration.java`.

2. **Prometheus Adapter Configuration**: 
   - Added a specific rule for the `sse_active_users` metric.
   - Configured proper resource mapping (namespaces and pods).
   - Configured the metrics query for proper aggregation.

3. **Prometheus Scrape Configuration**: 
   - Added a specific scrape job for the application.
   - Configured labels to properly identify the application pods.

4. **HPA Configuration**: 
   - Set up autoscaling based on the custom `sse_active_users` metric.
   - Defined scaling behavior with appropriate stabilization windows.

## Understanding the Scaling Calculations

The HPA is configured to scale based on the number of active users with the following parameters:

- **Metric**: `sse_active_users` (average value per pod)
- **Target Value**: 5000 users per pod
- **Min Replicas**: 1
- **Max Replicas**: 4

### How Scaling Works

1. **Metric Collection**:
   - The application exposes the `sse_active_users` metric via the `/actuator/prometheus` endpoint.
   - Prometheus scrapes this metric from all application pods.
   - The Prometheus adapter exposes this metric to the Kubernetes custom metrics API.

2. **Scaling Decision**:
   - The HPA controller periodically (default: 15 seconds) checks the current value of the metric.
   - It calculates the ratio: `current_value / target_value`
   - It determines the desired number of replicas based on this ratio.

3. **Scaling Calculation Example**:
   - If the current average is 7500 users per pod with 1 pod:
     - Ratio: 7500 / 5000 = 1.5
     - Desired replicas: 1 × 1.5 = 1.5 (rounded up to 2)
   - If the current average is 15000 users with 2 pods (7500 per pod):
     - Ratio: 7500 / 5000 = 1.5
     - Desired replicas: 2 × 1.5 = 3
   - If the current average is 5000 users with 3 pods (1666 per pod):
     - Ratio: 1666 / 5000 = 0.33
     - Desired replicas: 3 × 0.33 = 0.99 (rounded up to 1)

4. **Stabilization Windows**:
   - Scale up: 60 seconds (will scale up only if the condition persists for 60 seconds)
   - Scale down: 120 seconds (will scale down only if the condition persists for 120 seconds)

## Setup Instructions

### Prerequisites

- Kubernetes cluster with Prometheus and Prometheus Adapter installed
- `kubectl` configured to access your cluster

### Setup Steps

1. **PowerShell (Windows)**:
   ```powershell
   .\setup-metrics-hpa.ps1
   ```

2. **Bash (Linux/macOS)**:
   ```bash
   chmod +x setup-metrics-hpa.sh
   ./setup-metrics-hpa.sh
   ```

### Testing the Autoscaling

1. **PowerShell (Windows)**:
   ```powershell
   .\test-hpa-scaling.ps1 -Users 20000 -DurationSeconds 600
   ```

2. **Bash (Linux/macOS)**:
   ```bash
   chmod +x test-hpa-scaling.sh
   ./test-hpa-scaling.sh 20000 600
   ```

## Monitoring the HPA

To monitor the HPA status:

```bash
kubectl get hpa -w
```

To see detailed information about the HPA:

```bash
kubectl describe hpa reactive-sse-local-hpa
```

## Troubleshooting

If the HPA shows `<unknown>` for the metric:

1. Check if the metric is being correctly exposed:
   ```bash
   kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1/namespaces/default/pods/*/sse_active_users"
   ```

2. Check if Prometheus is scraping the application:
   ```bash
   # Port-forward Prometheus
   kubectl port-forward -n monitoring svc/prom-prometheus-server 9090:80
   # Then open http://localhost:9090 in a browser and check the targets
   ```

3. Verify the application is exposing metrics:
   ```bash
   # Get a pod name
   POD=$(kubectl get pods -l app=reactive-sse-local -o jsonpath='{.items[0].metadata.name}')
   # Check metrics endpoint
   kubectl exec $POD -- curl -s localhost:8080/actuator/prometheus | grep sse_active_users
   ```
