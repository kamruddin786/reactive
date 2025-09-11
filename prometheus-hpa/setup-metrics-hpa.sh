#!/bin/bash
# Script to configure Prometheus Adapter for SSE User Metrics Autoscaling
# Author: Kamruddin
# Date: September 11, 2025

# Apply the prometheus adapter configuration
echo -e "\033[0;32mStep 1: Applying Prometheus adapter configuration...\033[0m"
kubectl apply -f prometheus-adapter-config.yaml
echo -e "\033[0;32mPrometheus adapter configuration applied!\033[0m"

# Add scrape config for SSE application
echo -e "\033[0;32mStep 2: Creating additional scrape configs for Prometheus...\033[0m"
kubectl apply -f prometheus-scrape-config.yaml
echo -e "\033[0;32mAdditional scrape configs created!\033[0m"

# Update Prometheus ConfigMap with the additional scrape config
echo -e "\033[0;32mStep 3: Updating Prometheus ConfigMap with scrape configs...\033[0m"
kubectl get configmap -n monitoring prom-prometheus-server -o yaml > prom-config.yaml
sed -i 's/scrape_configs:/scrape_configs:\n    - job_name: '\''reactive-sse-app'\''\n      metrics_path: '\''\/actuator\/prometheus'\''\n      kubernetes_sd_configs:\n      - role: endpoints\n        namespaces:\n          names:\n          - default\n      relabel_configs:\n      - source_labels: [__meta_kubernetes_service_label_app]\n        action: keep\n        regex: reactive-sse-local\n      - source_labels: [__meta_kubernetes_pod_name]\n        action: replace\n        target_label: pod\n      - source_labels: [__meta_kubernetes_namespace]\n        action: replace\n        target_label: namespace/g' prom-config.yaml
kubectl replace -f prom-config.yaml
rm prom-config.yaml
echo -e "\033[0;32mPrometheus ConfigMap updated successfully!\033[0m"

# Restart Prometheus server to pick up the new configuration
echo -e "\033[0;32mStep 4: Restarting Prometheus server...\033[0m"
kubectl rollout restart deployment/prom-prometheus-server -n monitoring
echo -e "\033[0;32mPrometheus server restarting...\033[0m"

# Wait for Prometheus server to be ready
echo -e "\033[0;33mWaiting for Prometheus server to be ready...\033[0m"
sleep 30

# Restart Prometheus adapter to pick up the new configuration
echo -e "\033[0;32mStep 5: Restarting Prometheus adapter...\033[0m"
kubectl rollout restart deployment/prom-adapter-prometheus-adapter -n monitoring
echo -e "\033[0;32mPrometheus adapter restarting...\033[0m"

# Wait for Prometheus adapter to be ready
echo -e "\033[0;33mWaiting for Prometheus adapter to be ready...\033[0m"
sleep 30

# Apply the HPA configuration
echo -e "\033[0;32mStep 6: Applying HPA configuration...\033[0m"
kubectl apply -f hpa-config.yaml
echo -e "\033[0;32mHPA configuration applied!\033[0m"

# Verify that the metric is available
echo -e "\033[0;32mStep 7: Verifying that the metric is available...\033[0m"
kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1/namespaces/default/pods/*/sse_active_users"
echo -e "\033[0;32mSetup complete! HPA should now be configured for SSE active users metric.\033[0m"

# Show HPA status
echo -e "\033[0;32mCurrent HPA Status:\033[0m"
kubectl get hpa
