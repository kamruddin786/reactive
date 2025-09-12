#!/bin/bash
# Complete Installation Script for Prometheus Adapter and SSE User Metrics Autoscaling
# This script installs everything from scratch on a new machine/cluster
# Author: Kamruddin
# Date: September 11, 2025

set -e  # Exit on any error

echo -e "\033[0;34m============================================================\033[0m"
echo -e "\033[0;34m    Complete HPA Setup with Prometheus and Custom Metrics\033[0m"
echo -e "\033[0;34m============================================================\033[0m"

# Step 1: Create monitoring namespace
echo -e "\033[0;32mStep 1: Creating monitoring namespace...\033[0m"
kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -
echo -e "\033[0;32mMonitoring namespace created/verified!\033[0m"

# Step 2: Add Helm repositories
echo -e "\033[0;32mStep 2: Adding required Helm repositories...\033[0m"
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
echo -e "\033[0;32mHelm repositories added and updated!\033[0m"

# Step 3: Install Prometheus server
echo -e "\033[0;32mStep 3: Installing Prometheus server...\033[0m"
helm install prom prometheus-community/prometheus \
  --namespace monitoring \
  --set server.service.type=ClusterIP \
  --set server.persistentVolume.enabled=false \
  --set alertmanager.enabled=false \
  --set pushgateway.enabled=false \
  --set nodeExporter.enabled=true \
  --set kubeStateMetrics.enabled=true \
  --set server.retention=1h \
  --wait --timeout=300s

echo -e "\033[0;32mPrometheus server installed successfully!\033[0m"

# Step 4: Wait for Prometheus to be ready
echo -e "\033[0;33mWaiting for Prometheus server to be ready...\033[0m"
kubectl wait --for=condition=available --timeout=300s deployment/prom-prometheus-server -n monitoring
echo -e "\033[0;32mPrometheus server is ready!\033[0m"

# Step 5: Install Prometheus Adapter
echo -e "\033[0;32mStep 5: Installing Prometheus Adapter...\033[0m"
helm install prom-adapter prometheus-community/prometheus-adapter \
  --namespace monitoring \
  --set prometheus.url=http://prom-prometheus-server.monitoring.svc \
  --set prometheus.port=80 \
  --wait --timeout=300s

echo -e "\033[0;32mPrometheus Adapter installed successfully!\033[0m"

# Step 6: Wait for Prometheus adapter to be ready
echo -e "\033[0;33mWaiting for Prometheus adapter to be ready...\033[0m"
kubectl wait --for=condition=available --timeout=300s deployment/prom-adapter-prometheus-adapter -n monitoring
echo -e "\033[0;32mPrometheus adapter is ready!\033[0m"

# Step 7: Apply the prometheus adapter configuration
echo -e "\033[0;32mStep 7: Applying Prometheus adapter configuration...\033[0m"
kubectl apply -f prometheus-adapter-config.yaml
echo -e "\033[0;32mPrometheus adapter configuration applied!\033[0m"

# Step 8: Add scrape config for SSE application
echo -e "\033[0;32mStep 8: Creating additional scrape configs for Prometheus...\033[0m"
kubectl apply -f prometheus-scrape-config.yaml
echo -e "\033[0;32mAdditional scrape configs created!\033[0m"

# Step 9: Update Prometheus ConfigMap with the additional scrape config
echo -e "\033[0;32mStep 9: Updating Prometheus ConfigMap with scrape configs...\033[0m"
kubectl get configmap -n monitoring prom-prometheus-server -o yaml > prom-config.yaml
sed -i 's/scrape_configs:/scrape_configs:\n    - job_name: '\''reactive-sse-app'\''\n      metrics_path: '\''\/actuator\/prometheus'\''\n      kubernetes_sd_configs:\n      - role: endpoints\n        namespaces:\n          names:\n          - default\n      relabel_configs:\n      - source_labels: [__meta_kubernetes_service_label_app]\n        action: keep\n        regex: reactive-sse-local\n      - source_labels: [__meta_kubernetes_pod_name]\n        action: replace\n        target_label: pod\n      - source_labels: [__meta_kubernetes_namespace]\n        action: replace\n        target_label: namespace/g' prom-config.yaml
kubectl replace -f prom-config.yaml
rm prom-config.yaml
echo -e "\033[0;32mPrometheus ConfigMap updated successfully!\033[0m"

# Step 10: Restart Prometheus server to pick up the new configuration
echo -e "\033[0;32mStep 10: Restarting Prometheus server...\033[0m"
kubectl rollout restart deployment/prom-prometheus-server -n monitoring
kubectl rollout status deployment/prom-prometheus-server -n monitoring --timeout=300s
echo -e "\033[0;32mPrometheus server restarted and ready!\033[0m"

# Step 11: Restart Prometheus adapter to pick up the new configuration
echo -e "\033[0;32mStep 11: Restarting Prometheus adapter...\033[0m"
kubectl rollout restart deployment/prom-adapter-prometheus-adapter -n monitoring
kubectl rollout status deployment/prom-adapter-prometheus-adapter -n monitoring --timeout=300s
echo -e "\033[0;32mPrometheus adapter restarted and ready!\033[0m"

# Step 12: Apply the HPA configuration
echo -e "\033[0;32mStep 12: Applying HPA configuration...\033[0m"
kubectl apply -f hpa-config.yaml
echo -e "\033[0;32mHPA configuration applied!\033[0m"

# Step 13: Wait a bit for metrics to be available
echo -e "\033[0;33mWaiting 60 seconds for metrics to be available...\033[0m"
sleep 60

# Step 14: Verify that the metric is available
echo -e "\033[0;32mStep 14: Verifying that the custom metric is available...\033[0m"
echo "Checking custom metrics API..."
kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1" || echo "Custom metrics API not yet ready, this is normal during initial setup"

echo -e "\033[0;32mStep 15: Checking if sse_active_users metric is available...\033[0m"
kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1/namespaces/default/pods/*/sse_active_users" || echo "SSE metric not yet available, may need application to be running and exposing metrics"

echo -e "\033[0;32m============================================================\033[0m"
echo -e "\033[0;32m    INSTALLATION COMPLETE!\033[0m"
echo -e "\033[0;32m============================================================\033[0m"

# Show installation status
echo -e "\033[0;32mInstallation Summary:\033[0m"
echo "1. Monitoring namespace: $(kubectl get namespace monitoring -o jsonpath='{.status.phase}')"
echo "2. Prometheus server: $(kubectl get deployment prom-prometheus-server -n monitoring -o jsonpath='{.status.readyReplicas}')/$(kubectl get deployment prom-prometheus-server -n monitoring -o jsonpath='{.spec.replicas}') ready"
echo "3. Prometheus adapter: $(kubectl get deployment prom-adapter-prometheus-adapter -n monitoring -o jsonpath='{.status.readyReplicas}')/$(kubectl get deployment prom-adapter-prometheus-adapter -n monitoring -o jsonpath='{.spec.replicas}') ready"

echo -e "\033[0;32mCurrent HPA Status:\033[0m"
kubectl get hpa

echo -e "\033[0;33mNotes:\033[0m"
echo "- Make sure your reactive-sse-local deployment is running and exposing metrics at /actuator/prometheus"
echo "- The sse_active_users metric should be available in your application"
echo "- It may take a few minutes for all metrics to be scraped and available"
echo "- Use 'kubectl get hpa' to monitor HPA status"
echo "- Use 'kubectl get --raw \"/apis/custom.metrics.k8s.io/v1beta1/namespaces/default/pods/*/sse_active_users\"' to check metric availability"
