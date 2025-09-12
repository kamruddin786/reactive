#!/bin/bash
# Cleanup Script for Prometheus Adapter and SSE User Metrics Autoscaling
# This script removes all components installed for HPA with custom metrics
# Author: Kamruddin
# Date: September 11, 2025

set -e  # Exit on any error

echo -e "\033[0;31m============================================================\033[0m"
echo -e "\033[0;31m    CLEANUP: Removing HPA Setup with Prometheus\033[0m"
echo -e "\033[0;31m============================================================\033[0m"

# Function to safely delete resources
safe_delete() {
    local resource_type=$1
    local resource_name=$2
    local namespace=$3

    if [ -n "$namespace" ]; then
        if kubectl get $resource_type $resource_name -n $namespace &>/dev/null; then
            echo "Deleting $resource_type/$resource_name in namespace $namespace..."
            kubectl delete $resource_type $resource_name -n $namespace
        else
            echo "$resource_type/$resource_name not found in namespace $namespace, skipping..."
        fi
    else
        if kubectl get $resource_type $resource_name &>/dev/null; then
            echo "Deleting $resource_type/$resource_name..."
            kubectl delete $resource_type $resource_name
        else
            echo "$resource_type/$resource_name not found, skipping..."
        fi
    fi
}

# Step 1: Remove HPA
echo -e "\033[0;33mStep 1: Removing HPA configuration...\033[0m"
safe_delete "hpa" "reactive-sse-local-hpa" ""
echo -e "\033[0;32mHPA removed!\033[0m"

# Step 2: Remove additional ConfigMaps we created
echo -e "\033[0;33mStep 2: Removing additional scrape configs...\033[0m"
safe_delete "configmap" "prometheus-additional-scrape-configs" "monitoring"
echo -e "\033[0;32mAdditional scrape configs removed!\033[0m"

# Step 3: Uninstall Prometheus Adapter via Helm
echo -e "\033[0;33mStep 3: Uninstalling Prometheus Adapter...\033[0m"
if helm list -n monitoring | grep -q prom-adapter; then
    helm uninstall prom-adapter -n monitoring
    echo -e "\033[0;32mPrometheus Adapter uninstalled!\033[0m"
else
    echo "Prometheus Adapter not found, skipping..."
fi

# Step 4: Uninstall Prometheus Server via Helm
echo -e "\033[0;33mStep 4: Uninstalling Prometheus Server...\033[0m"
if helm list -n monitoring | grep -q prom; then
    helm uninstall prom -n monitoring
    echo -e "\033[0;32mPrometheus Server uninstalled!\033[0m"
else
    echo "Prometheus Server not found, skipping..."
fi

# Step 5: Wait for resources to be terminated
echo -e "\033[0;33mStep 5: Waiting for resources to be terminated...\033[0m"
sleep 30

# Step 6: Clean up any remaining resources in monitoring namespace
echo -e "\033[0;33mStep 6: Cleaning up remaining resources in monitoring namespace...\033[0m"

# Remove any remaining ConfigMaps
kubectl get configmap -n monitoring --no-headers 2>/dev/null | grep -E "(prom-|prometheus-)" | awk '{print $1}' | xargs -r kubectl delete configmap -n monitoring || true

# Remove any remaining Secrets
kubectl get secret -n monitoring --no-headers 2>/dev/null | grep -E "(prom-|prometheus-)" | awk '{print $1}' | xargs -r kubectl delete secret -n monitoring || true

# Remove any remaining Services
kubectl get service -n monitoring --no-headers 2>/dev/null | grep -E "(prom-|prometheus-)" | awk '{print $1}' | xargs -r kubectl delete service -n monitoring || true

# Remove any remaining PVCs
kubectl get pvc -n monitoring --no-headers 2>/dev/null | grep -E "(prom-|prometheus-)" | awk '{print $1}' | xargs -r kubectl delete pvc -n monitoring || true

# Remove any remaining Deployments
kubectl get deployment -n monitoring --no-headers 2>/dev/null | grep -E "(prom-|prometheus-)" | awk '{print $1}' | xargs -r kubectl delete deployment -n monitoring || true

# Remove any remaining ReplicaSets
kubectl get replicaset -n monitoring --no-headers 2>/dev/null | grep -E "(prom-|prometheus-)" | awk '{print $1}' | xargs -r kubectl delete replicaset -n monitoring || true

# Remove any remaining Pods
kubectl get pod -n monitoring --no-headers 2>/dev/null | grep -E "(prom-|prometheus-)" | awk '{print $1}' | xargs -r kubectl delete pod -n monitoring --force --grace-period=0 || true

echo -e "\033[0;32mRemaining resources cleaned up!\033[0m"

# Step 7: Remove monitoring namespace (optional - ask user)
echo -e "\033[0;33mStep 7: Monitoring namespace cleanup...\033[0m"
read -p "Do you want to remove the 'monitoring' namespace entirely? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    if kubectl get namespace monitoring &>/dev/null; then
        echo "Removing monitoring namespace..."
        kubectl delete namespace monitoring --timeout=300s
        echo -e "\033[0;32mMonitoring namespace removed!\033[0m"
    else
        echo "Monitoring namespace not found, skipping..."
    fi
else
    echo "Keeping monitoring namespace..."
fi

# Step 8: Clean up any CustomResourceDefinitions related to Prometheus (if any)
echo -e "\033[0;33mStep 8: Checking for Prometheus-related CRDs...\033[0m"
kubectl get crd 2>/dev/null | grep -E "(prometheus|monitoring)" | awk '{print $1}' || echo "No Prometheus-related CRDs found"

# Step 9: Remove Helm repositories (optional)
echo -e "\033[0;33mStep 9: Helm repository cleanup...\033[0m"
read -p "Do you want to remove the prometheus-community Helm repository? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    if helm repo list | grep -q prometheus-community; then
        helm repo remove prometheus-community
        echo -e "\033[0;32mPrometheus-community Helm repository removed!\033[0m"
    else
        echo "Prometheus-community repository not found, skipping..."
    fi
else
    echo "Keeping prometheus-community Helm repository..."
fi

echo -e "\033[0;31m============================================================\033[0m"
echo -e "\033[0;31m    CLEANUP COMPLETE!\033[0m"
echo -e "\033[0;31m============================================================\033[0m"

# Show cleanup status
echo -e "\033[0;32mCleanup Summary:\033[0m"
echo "1. HPA: $(kubectl get hpa reactive-sse-local-hpa 2>/dev/null && echo "Still exists" || echo "Removed")"
echo "2. Monitoring namespace: $(kubectl get namespace monitoring 2>/dev/null && echo "Still exists" || echo "Removed")"
echo "3. Helm releases in monitoring: $(helm list -n monitoring 2>/dev/null | wc -l) releases remaining"

echo -e "\033[0;33mNotes:\033[0m"
echo "- All Prometheus and Prometheus Adapter components have been removed"
echo "- Custom metrics API is no longer available"
echo "- Your application deployment (reactive-sse-local) was not affected"
echo "- Use 'kubectl get all -n monitoring' to verify namespace is clean"
echo "- Use 'helm list -A' to verify no Prometheus-related releases remain"
