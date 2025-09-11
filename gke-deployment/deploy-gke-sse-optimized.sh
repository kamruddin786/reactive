#!/bin/bash

# GKE SSE Streaming Deployment Script
# This script deploys the reactive SSE application with optimized configurations for stable streaming

set -e

echo "🚀 Deploying Reactive SSE Application to GKE with streaming optimizations..."

# Apply BackendConfig first (required for service)
echo "📋 Applying BackendConfig..."
kubectl apply -f k8s-gke-backendconfig.yaml

# Wait for configs to be ready
echo "⏳ Waiting for configs to be processed..."
sleep 10

# Apply Service (references BackendConfig)
echo "🔧 Applying Service..."
kubectl apply -f k8s-gke-service.yaml

# Apply Deployment
echo "🚀 Applying Deployment..."
kubectl apply -f k8s-gke-deployment.yaml

# Wait for deployment to be ready
echo "⏳ Waiting for deployment to be ready..."
kubectl wait --for=condition=available --timeout=300s deployment/reactive-sse-app

# Apply HPA for autoscaling
#echo "📈 Applying Horizontal Pod Autoscaler..."
#kubectl apply -f k8s-gke-hpa.yaml

# Apply Ingress (references FrontendConfig and BackendConfig)
echo "🌐 Applying Ingress..."
kubectl apply -f k8s-gke-ingress.yaml

echo "✅ Deployment complete!"

# Display status
echo "📊 Deployment Status:"
kubectl get pods -l app=reactive-sse-app
kubectl get services -l app=reactive-sse-app
#kubectl get hpa reactive-sse-app-hpa
kubectl get ingress reactive-sse-app-ingress

echo ""
echo "🔍 To monitor the deployment:"
echo "  kubectl logs -f deployment/reactive-sse-app"
echo "  kubectl describe ingress reactive-sse-app-ingress"
echo "  kubectl describe hpa reactive-sse-app-hpa"
echo ""
echo "📈 To monitor autoscaling:"
echo "  kubectl get hpa reactive-sse-app-hpa --watch"
echo "  kubectl top pods -l app=reactive-sse-app"
echo ""
echo "🩺 Health check endpoints:"
echo "  /api/notifications/health - General health check"
echo "  /api/notifications/ready - Kubernetes readiness probe"
echo "  /api/notifications/stats - Connection statistics"
echo ""
echo "🌊 SSE Stream endpoint:"
echo "  /api/notifications/user/{userId}/stream"
echo ""
echo "⚙️  Autoscaling Configuration:"
echo "  Initial pods: 1, Max pods: 2, Memory threshold: 90%"
echo ""
echo "⚠️  Important: Wait 5-10 minutes for GKE load balancer to fully configure"
echo "    The ingress IP will be available once the load balancer is ready."
