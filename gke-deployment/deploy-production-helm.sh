#!/bin/bash

# Production Deployment Script for Reactive SSE Application on GKE
# This script deploys to production with optimized configurations

set -e

# Production-specific defaults
NAMESPACE="${NAMESPACE:-production}"
RELEASE_NAME="${RELEASE_NAME:-reactive-sse-app}"
CHART_PATH="./helm-chart/reactive-sse-app"
VALUES_FILE="${VALUES_FILE:-values-prod.yaml}"
IMAGE_TAG="${IMAGE_TAG:-v2.1.11}"

echo "🏭 Deploying Reactive SSE Application to PRODUCTION using Helm..."
echo "📍 Namespace: $NAMESPACE"
echo "🏷️  Release Name: $RELEASE_NAME"
echo "📋 Values File: $VALUES_FILE"
echo "🏗️  Image Tag: $IMAGE_TAG"

# Production safety check
if [ "$NAMESPACE" == "production" ]; then
    echo "⚠️  WARNING: You are deploying to PRODUCTION!"
    echo "   This will affect live traffic and users."
    read -p "❓ Are you sure you want to continue? (yes/no): " -r
    if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
        echo "🚫 Production deployment cancelled."
        exit 0
    fi
fi

# Validate prerequisites
if ! command -v helm &> /dev/null; then
    echo "❌ Helm is not installed. Please install Helm first."
    exit 1
fi

if ! kubectl cluster-info &> /dev/null; then
    echo "❌ kubectl is not configured or cluster is not accessible."
    exit 1
fi

# Verify production values file exists
if [ ! -f "$CHART_PATH/$VALUES_FILE" ]; then
    echo "❌ Production values file not found: $CHART_PATH/$VALUES_FILE"
    exit 1
fi

# Create production namespace
echo "📋 Creating production namespace..."
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# Validate the Helm chart
echo "🔍 Validating Helm chart..."
helm lint "$CHART_PATH"

# Show what will be deployed
echo "🔍 Preview of production configuration:"
helm template "$RELEASE_NAME" "$CHART_PATH" \
    -f "$CHART_PATH/$VALUES_FILE" \
    --set image.tag="$IMAGE_TAG" \
    --namespace "$NAMESPACE" | head -50

echo ""
read -p "❓ Proceed with this configuration? (yes/no): " -r
if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
    echo "🚫 Deployment cancelled."
    exit 0
fi

# Deploy with production values
if helm list -n "$NAMESPACE" | grep -q "$RELEASE_NAME"; then
    echo "🔄 Upgrading existing production release..."
    helm upgrade "$RELEASE_NAME" "$CHART_PATH" \
        -f "$CHART_PATH/$VALUES_FILE" \
        --set image.tag="$IMAGE_TAG" \
        --namespace "$NAMESPACE" \
        --wait \
        --timeout=900s \
        --history-max=10 \
        --atomic
else
    echo "🆕 Installing new production release..."
    helm install "$RELEASE_NAME" "$CHART_PATH" \
        -f "$CHART_PATH/$VALUES_FILE" \
        --set image.tag="$IMAGE_TAG" \
        --namespace "$NAMESPACE" \
        --wait \
        --timeout=900s \
        --create-namespace \
        --atomic
fi

echo "✅ Production deployment complete!"

# Display production status
echo ""
echo "📊 Production Deployment Status:"
echo "================================="
kubectl get all -l app.kubernetes.io/name=reactive-sse-app -n "$NAMESPACE"

echo ""
echo "📈 Production Autoscaling Status:"
echo "================================="
kubectl get hpa -n "$NAMESPACE"

echo ""
echo "🌐 Production Ingress Status:"
echo "============================="
kubectl get ingress -n "$NAMESPACE"

echo ""
echo "🔍 Production Monitoring Commands:"
echo "=================================="
echo "  Monitor pods:        kubectl get pods -n $NAMESPACE -w"
echo "  View logs:           kubectl logs -f deployment/$RELEASE_NAME -n $NAMESPACE"
echo "  Monitor HPA:         kubectl get hpa -n $NAMESPACE --watch"
echo "  Resource usage:      kubectl top pods -n $NAMESPACE"
echo "  Service endpoints:   kubectl get endpoints -n $NAMESPACE"
echo ""
echo "  Helm status:         helm status $RELEASE_NAME -n $NAMESPACE"
echo "  Rollback if needed:  helm rollback $RELEASE_NAME -n $NAMESPACE"
echo ""
echo "📈 Production Configuration Applied:"
echo "===================================="
echo "  Min replicas: 2, Max replicas: 5, Memory threshold: 80%"
echo "  Resources: 512Mi-2Gi memory, 200m-1000m CPU"
echo "  Health checks: Faster detection intervals"
echo ""
echo "🎯 Production Health Checks:"
echo "============================"
echo "  Wait 2-3 minutes for all pods to be ready"
echo "  Monitor HPA scaling based on load"
echo "  Check ingress IP assignment (may take 5-10 minutes)"
