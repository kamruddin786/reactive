# Prometheus HPA for SSE Active User Metrics

This directory contains the necessary configurations and scripts to set up Horizontal Pod Autoscaling (HPA) based on custom active user metrics for the Reactive SSE Application. The autoscaling is configured to scale the application based on the number of active SSE users.

## Files Overview

### Configuration Files
- **prometheus-adapter-config.yaml**: Configuration for the Prometheus Adapter to expose the `sse_active_users` metric to the Kubernetes custom metrics API.
- **prometheus-scrape-config.yaml**: Additional scrape configuration for Prometheus to collect metrics from the application.
- **hpa-config.yaml**: HPA configuration that defines scaling rules based on the number of active users.

### Scripts

#### Installation Scripts
- **install-complete-hpa-setup.sh**: **[NEW]** Complete installation script for fresh setups. Installs Prometheus, Prometheus Adapter, and configures everything from scratch on a new machine/cluster.
- **setup-metrics-hpa.sh**: **[UPDATED]** Configuration update script for existing installations. Use this to update configurations when Prometheus and Prometheus Adapter are already installed.

#### Testing Scripts  
- **test-hpa-scaling.sh**: Bash script to test the HPA by generating simulated load.

#### Cleanup Scripts
- **cleanup-hpa-setup.sh**: **[NEW]** Complete cleanup script that removes all Prometheus, Prometheus Adapter components, and HPA configurations. Use this to completely uninstall everything.

## Quick Start

### For New Machines/Clusters (Fresh Installation)
```bash
# Run the complete installation script
./install-complete-hpa-setup.sh
```

### For Existing Prometheus Installations (Update Only)
```bash
# Run the configuration update script
./setup-metrics-hpa.sh
```

### To Remove Everything
```bash
# Run the cleanup script
./cleanup-hpa-setup.sh
```

## What Each Script Does

### install-complete-hpa-setup.sh
This script performs a complete installation from scratch:
1. Creates monitoring namespace
2. Adds Helm repositories (prometheus-community)
3. Installs Prometheus server via Helm
4. Installs Prometheus Adapter via Helm
5. Applies custom configurations for SSE metrics
6. Updates Prometheus scrape configs
7. Applies HPA configuration
8. Verifies installation and metric availability

**Requirements**: 
- Helm installed and configured
- kubectl configured to connect to your cluster
- No existing Prometheus installation

### setup-metrics-hpa.sh
This script updates existing installations:
1. Checks for existing Prometheus and Prometheus Adapter installations
2. Updates Prometheus Adapter configuration for SSE metrics
3. Adds SSE application scrape configs
4. Updates Prometheus ConfigMap
5. Restarts services to apply changes
6. Applies HPA configuration

**Requirements**:
- Existing Prometheus server deployment named `prom-prometheus-server`
- Existing Prometheus Adapter deployment named `prom-adapter-prometheus-adapter`
- Both must be in the `monitoring` namespace

### cleanup-hpa-setup.sh
This script completely removes all components:
1. Removes HPA configurations
2. Removes additional ConfigMaps created
3. Uninstalls Prometheus Adapter via Helm
4. Uninstalls Prometheus Server via Helm
5. Cleans up remaining resources
6. Optionally removes monitoring namespace
7. Optionally removes Helm repositories

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

### Check Metric Availability
```bash
# Check if custom metrics API is available
kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1"

# Check specific SSE metric
kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1/namespaces/default/pods/*/sse_active_users"
```

### Check HPA Status
```bash
kubectl get hpa
kubectl describe hpa reactive-sse-local-hpa
```

### Check Prometheus Targets
```bash
# Port-forward to Prometheus and check targets
kubectl port-forward -n monitoring svc/prom-prometheus-server 9090:80
# Then visit http://localhost:9090/targets
```

### Common Issues
1. **Metric not available**: Ensure your application is running and exposing metrics
2. **HPA shows "unknown" metrics**: Wait a few minutes for metrics to be scraped
3. **Scaling not working**: Check HPA events with `kubectl describe hpa`

## Notes

- The setup assumes your reactive-sse-local deployment is in the `default` namespace
- Metrics collection may take 1-2 minutes to start working after setup
- The `sse_active_users` metric should be exposed by your Spring Boot application via Micrometer
- For production environments, adjust the target values and replica limits as needed
