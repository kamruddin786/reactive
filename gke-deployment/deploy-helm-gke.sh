#!/bin/bash

# Helm Deployment Script for Reactive SSE Application on GKE
# This script uses Helm to deploy the reactive SSE application with all configurations

set -e

NAMESPACE="${NAMESPACE:-default}"
RELEASE_NAME="${RELEASE_NAME:-reactive-sse-app}"
CHART_PATH="./helm-chart/reactive-sse-app"

echo "🚀 Deploying Reactive SSE Application using Helm..."
echo "📍 Namespace: $NAMESPACE"
echo "🏷️  Release Name: $RELEASE_NAME"

# Check if Helm is installed
if ! command -v helm &> /dev/null; then
    echo "❌ Helm is not installed. Please install Helm first."
    echo "   Visit: https://helm.sh/docs/intro/install/"
    exit 1
fi

# Check if kubectl is configured
if ! kubectl cluster-info &> /dev/null; then
    echo "❌ kubectl is not configured or cluster is not accessible."
    exit 1
fi

# Create namespace if it doesn't exist
echo "📋 Creating namespace if it doesn't exist..."
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# Validate the Helm chart
echo "🔍 Validating Helm chart..."
helm lint "$CHART_PATH"

# Check if release already exists
if helm list -n "$NAMESPACE" | grep -q "$RELEASE_NAME"; then
    echo "🔄 Upgrading existing release..."
    helm upgrade "$RELEASE_NAME" "$CHART_PATH" \
        --namespace "$NAMESPACE" \
        --wait \
        --timeout=600s \
        --history-max=5
else
    echo "🆕 Installing new release..."
    helm install "$RELEASE_NAME" "$CHART_PATH" \
        --namespace "$NAMESPACE" \
        --wait \
        --timeout=600s \
        --create-namespace
fi

echo "✅ Deployment complete!"

# Display status
echo ""
echo "📊 Deployment Status:"
echo "====================="
kubectl get all -l app.kubernetes.io/name=reactive-sse-app -n "$NAMESPACE"

echo ""
echo "📈 Autoscaling Status:"
echo "======================"
kubectl get hpa -n "$NAMESPACE"

echo ""
echo "🌐 Ingress Status:"
echo "=================="
kubectl get ingress -n "$NAMESPACE"

echo ""
echo "🔍 Useful Commands:"
echo "==================="
echo "  View pods:           kubectl get pods -n $NAMESPACE"
echo "  View services:       kubectl get services -n $NAMESPACE"
echo "  View logs:           kubectl logs -f deployment/$RELEASE_NAME -n $NAMESPACE"
echo "  View HPA:            kubectl get hpa -n $NAMESPACE --watch"
echo "  Monitor resources:   kubectl top pods -n $NAMESPACE"
echo ""
echo "  Helm status:         helm status $RELEASE_NAME -n $NAMESPACE"
echo "  Helm history:        helm history $RELEASE_NAME -n $NAMESPACE"
echo "  Helm values:         helm get values $RELEASE_NAME -n $NAMESPACE"
echo ""
echo "🎯 To customize deployment:"
echo "  Create a custom values file and use: helm upgrade $RELEASE_NAME $CHART_PATH -f custom-values.yaml -n $NAMESPACE"
echo ""
echo "📈 Autoscaling Configuration:"
echo "  Initial pods: 1, Max pods: 2, Memory threshold: 90%"
echo ""
echo "⚠️  Important: Wait 5-10 minutes for GKE load balancer to fully configure"
