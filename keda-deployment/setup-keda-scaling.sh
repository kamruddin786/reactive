#!/bin/bash

# KEDA Setup Script for Reactive SSE Application
# This script sets up KEDA autoscaling based on Redis pub/sub metrics

set -e

echo "=== Setting up KEDA for Reactive SSE Application ==="

# Check if KEDA is installed
if ! kubectl get crd scaledobjects.keda.sh > /dev/null 2>&1; then
    echo "Installing KEDA..."
    kubectl apply -f https://github.com/kedacore/keda/releases/download/v2.12.0/keda-2.12.0.yaml
    echo "Waiting for KEDA to be ready..."
    kubectl wait --for=condition=ready pod -l app=keda-operator -n keda --timeout=300s
else
    echo "KEDA is already installed"
fi

# Create namespace if it doesn't exist
kubectl create namespace reactive-sse --dry-run=client -o yaml | kubectl apply -f -

# Apply Redis authentication secret
echo "Creating Redis authentication secret..."
kubectl apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: redis-auth
  namespace: reactive-sse
type: Opaque
data:
  password: bXlwYXNz  # base64 encoded "mypass"
EOF

# Apply the main KEDA ScaledObject for Redis pub/sub scaling
echo "Applying KEDA ScaledObject for Redis pub/sub scaling..."
kubectl apply -f keda-redis-pubsub-optimized.yaml -n reactive-sse

# Apply custom metrics based scaling
echo "Applying KEDA ScaledObject for custom metrics scaling..."
kubectl apply -f keda-custom-metrics-scaler.yaml -n reactive-sse

# Wait for ScaledObjects to be ready
echo "Waiting for KEDA ScaledObjects to be ready..."
kubectl wait --for=condition=ready scaledobject reactive-sse-redis-pubsub-scaler -n reactive-sse --timeout=60s

echo "=== KEDA Setup Complete ==="
echo ""
echo "Scaling triggers configured:"
echo "1. Redis pub/sub subscriber count for 'user:messages' topic"
echo "2. Redis pub/sub subscriber count for 'broadcast:messages' topic"
echo "3. Redis memory usage (indicates message backlog)"
echo "4. Custom application metrics via /metrics/keda endpoints"
echo ""
echo "Monitor scaling with:"
echo "kubectl get hpa -n reactive-sse"
echo "kubectl get scaledobject -n reactive-sse"
echo "kubectl describe scaledobject reactive-sse-redis-pubsub-scaler -n reactive-sse"
