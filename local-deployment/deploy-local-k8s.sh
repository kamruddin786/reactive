#!/bin/bash

# Local Kubernetes Deployment Script for Reactive SSE Application with KEDA Scaling
# This script works with Docker Desktop Kubernetes or Minikube

set -e

echo "🚀 Starting Local Kubernetes Deployment for Reactive SSE Application with KEDA"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if kubectl is available
if ! command -v kubectl &> /dev/null; then
    print_error "kubectl is not installed or not in PATH"
    exit 1
fi

# Check if we're connected to a Kubernetes cluster
if ! kubectl cluster-info &> /dev/null; then
    print_error "Not connected to a Kubernetes cluster"
    print_warning "Please start Docker Desktop Kubernetes or Minikube"
    exit 1
fi

print_success "Connected to Kubernetes cluster"

# Step 1: Install KEDA (if not already installed)
print_status "Checking for KEDA installation..."
if ! kubectl get crd scaledobjects.keda.sh &> /dev/null; then
    print_status "Installing KEDA..."
    kubectl apply -f https://github.com/kedacore/keda/releases/download/v2.12.0/keda-2.12.0.yaml

    print_status "Waiting for KEDA to be ready..."
    kubectl wait --for=condition=ready pod -l app=keda-operator -n keda --timeout=300s
    kubectl wait --for=condition=ready pod -l app=keda-metrics-apiserver -n keda --timeout=300s
    print_success "KEDA installed and ready"
else
    print_success "KEDA is already installed"
fi

# Step 2: Build Docker image
print_status "Building Docker image..."
docker build -t reactive-sse-app:latest ../. || {
    print_error "Failed to build Docker image"
    exit 1
}
print_success "Docker image built successfully"

# Step 3: Load image into Minikube (if using Minikube)
if kubectl config current-context | grep -q "minikube"; then
    print_status "Detected Minikube, loading image..."
    minikube image load reactive-sse-app:latest || {
        print_warning "Failed to load image into Minikube, continuing anyway..."
    }
fi

# Step 4: Install NGINX Ingress Controller (if not already installed)
print_status "Checking for NGINX Ingress Controller..."
if ! kubectl get ingressclass nginx &> /dev/null; then
    print_status "Installing NGINX Ingress Controller..."

    if kubectl config current-context | grep -q "minikube"; then
        # For Minikube
        minikube addons enable ingress
    else
        # For Docker Desktop or other
        kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.8.2/deploy/static/provider/cloud/deploy.yaml

        # Wait for ingress controller to be ready
        print_status "Waiting for NGINX Ingress Controller to be ready..."
        kubectl wait --namespace ingress-nginx \
            --for=condition=ready pod \
            --selector=app.kubernetes.io/component=controller \
            --timeout=300s
    fi
    print_success "NGINX Ingress Controller installed"
else
    print_success "NGINX Ingress Controller already available"
fi

# Step 5: Deploy Redis
print_status "Deploying Redis..."
kubectl apply -f redis-local-deployment.yaml
kubectl wait --for=condition=available --timeout=300s deployment/redis-local
print_success "Redis deployed successfully"

# Step 5.1: Deploy Redis Commander
print_status "Deploying Redis Commander..."
kubectl apply -f redis-commander-local.yaml
kubectl wait --for=condition=available --timeout=300s deployment/redis-commander-local
print_success "Redis Commander deployed successfully"

# Step 6: Create KEDA Redis authentication secret (required for KEDA to connect to Redis)
print_status "Creating Redis authentication secret for KEDA..."
kubectl apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: redis-auth
  namespace: default
type: Opaque
data:
  password: bXlwYXNz  # base64 encoded "mypass"
---
apiVersion: keda.sh/v1alpha1
kind: TriggerAuthentication
metadata:
  name: redis-auth-trigger
  namespace: default
spec:
  secretTargetRef:
  - parameter: password
    name: redis-auth
    key: password
EOF
print_success "Redis authentication secret and TriggerAuthentication created for KEDA"

# Step 7: Deploy Application
print_status "Deploying Reactive SSE Application..."
kubectl apply -f k8s-local-deployment.yaml
print_status "Waiting for application to be ready..."
kubectl wait --for=condition=available --timeout=300s deployment/reactive-sse-local
print_success "Application deployed successfully"

# Step 8: Deploy KEDA Scaling Configuration
print_status "Deploying KEDA scaling configuration..."

# Apply the KEDA ScaledObject from file
kubectl apply -f keda-local-scaledobject.yaml

print_success "KEDA scaling configuration deployed"

# Step 9: Deploy Ingress
print_status "Deploying Ingress..."
kubectl apply -f ingress-local.yaml
print_success "Ingress deployed successfully"

# Step 10: Display deployment information
echo ""
echo "📊 Deployment Summary:"
echo "====================="
kubectl get deployments
echo ""
kubectl get services
echo ""
kubectl get ingress
echo ""
echo "🔄 KEDA Scaling Information:"
echo "=========================="
kubectl get scaledobject
echo ""
kubectl get hpa
echo ""

# Step 11: Get access URLs
print_status "Getting access information..."

if kubectl config current-context | grep -q "minikube"; then
    MINIKUBE_IP=$(minikube ip)
    echo ""
    print_success "🌐 Application Access URLs:"
    echo "   Main App: http://reactive-sse.local"
    echo "   Add to /etc/hosts: $MINIKUBE_IP reactive-sse.local"
    echo ""
    echo "   Alternative access: http://$MINIKUBE_IP/reactive-sse"
    echo ""
    print_status "📡 Available Endpoints:"
    echo "   Reactive Notifications: http://reactive-sse.local/reactive-notifications.html"
    echo "   SSE Stream: http://reactive-sse.local/api/notifications/user/{userId}/stream"
    echo "   Import CSV: POST http://reactive-sse.local/api/messages/import/csv"
    echo "   Import JSON: POST http://reactive-sse.local/api/messages/import/json"
    echo "   Health Check: http://reactive-sse.local/actuator/health"
    echo "   KEDA Metrics: http://reactive-sse.local/metrics/keda/scaling-metrics"
    echo ""
    print_status "🗄️  Database Management:"
    echo "   Redis Commander: http://reactive-sse.local/redis-commander"
else
    echo ""
    print_success "🌐 Application Access URLs:"
    echo "   Main App: http://reactive-sse.local"
    echo "   Add to /etc/hosts (Windows: C:\\Windows\\System32\\drivers\\etc\\hosts):"
    echo "   127.0.0.1 reactive-sse.local"
    echo ""
    echo "   Alternative access: http://localhost/reactive-sse"
    echo ""
    print_status "📡 Available Endpoints:"
    echo "   Reactive Notifications: http://reactive-sse.local/reactive-notifications.html"
    echo "   SSE Stream: http://reactive-sse.local/api/notifications/user/{userId}/stream"
    echo "   Import CSV: POST http://reactive-sse.local/api/messages/import/csv"
    echo "   Import JSON: POST http://reactive-sse.local/api/messages/import/json"
    echo "   Health Check: http://reactive-sse.local/actuator/health"
    echo "   KEDA Metrics: http://reactive-sse.local/metrics/keda/scaling-metrics"
    echo ""
    print_status "🗄️  Database Management:"
    echo "   Redis Commander: http://reactive-sse.local/redis-commander"
fi

echo ""
print_status "🔧 Management Commands:"
echo "   View KEDA scaling: kubectl describe scaledobject reactive-sse-local-scaler"
echo "   View HPA status: kubectl get hpa"
echo "   Check KEDA logs: kubectl logs -n keda deployment/keda-operator"
echo "   View app logs: kubectl logs -f deployment/reactive-sse-local"
echo "   Port forward app: kubectl port-forward service/reactive-sse-local-service 8080:8080"
echo ""

print_status "🎯 KEDA Scaling Information:"
echo "   Scaling Strategy: USER CONNECTION-BASED (NEW)"
echo "   Scaling Triggers:"
echo "   - Total connected users across all pods (target: 80 users per pod)"
echo "   - Average connections per pod load balancing (target: 120 connections per pod)"
echo "   - Intelligent scaling algorithm with built-in recommendations"
echo "   - Min replicas: 1, Max replicas: 5"
echo "   - Polling interval: 30s, Cooldown: 120s"
echo ""
echo "   User Connection Endpoints:"
echo "   - Primary metrics: /metrics/keda/user-connections"
echo "   - Scaling algorithm: /metrics/keda/scaling-metrics"
echo ""

print_success "🎉 Local Kubernetes deployment with KEDA scaling completed successfully!"
echo ""
print_status "To test USER CONNECTION-BASED KEDA scaling:"
echo "1. Open multiple browser tabs: http://reactive-sse.local/reactive-notifications.html"
echo "2. Connect with different User IDs (1561, 1562, 1563, etc.) to simulate multiple users"
echo "3. Monitor user connections: curl http://reactive-sse.local/metrics/keda/user-connections"
echo "4. Check scaling metrics: curl http://reactive-sse.local/metrics/keda/scaling-metrics"
echo "5. Watch scaling in action: kubectl get hpa -w"
echo "6. View connection tracking: curl http://reactive-sse.local/api/notifications/connections/redis"
echo "7. Import messages to generate activity: curl -X POST http://reactive-sse.local/api/messages/import/csv"
echo ""
print_status "📊 New Monitoring Endpoints:"
echo "   User Connections: http://reactive-sse.local/api/notifications/connections/user/{userId}"
echo "   Redis Tracking: http://reactive-sse.local/api/notifications/connections/redis"
echo "   Comprehensive Stats: http://reactive-sse.local/api/notifications/stats"
echo ""
print_status "🔍 Scaling Verification:"
echo "   Expected scaling behavior:"
echo "   - 1-80 users: 1 pod"
echo "   - 81-160 users: 2 pods"
echo "   - >120 connections/pod: additional scaling"
echo "   - Intelligent algorithm adjusts based on connection patterns"
