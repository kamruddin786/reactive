#!/bin/bash

# Local Kubernetes Deployment Script for Reactive SSE Application
# This script works with Docker Desktop Kubernetes or Minikube

set -e

echo "🚀 Starting Local Kubernetes Deployment for Reactive SSE Application"

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

# Step 1: Build Docker image
print_status "Building Docker image..."
docker build -t reactive-sse-app:latest ../. || {
    print_error "Failed to build Docker image"
    exit 1
}
print_success "Docker image built successfully"

# Step 2: Load image into Minikube (if using Minikube)
if kubectl config current-context | grep -q "minikube"; then
    print_status "Detected Minikube, loading image..."
    minikube image load reactive-sse-app:latest || {
        print_warning "Failed to load image into Minikube, continuing anyway..."
    }
fi

# Step 3: Install NGINX Ingress Controller (if not already installed)
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

# Step 4: Deploy Redis
print_status "Deploying Redis..."
kubectl apply -f redis-local-deployment.yaml
kubectl wait --for=condition=available --timeout=300s deployment/redis-local
print_success "Redis deployed successfully"

# Step 4.1: Deploy Redis Commander
print_status "Deploying Redis Commander..."
kubectl apply -f redis-commander-local.yaml
kubectl wait --for=condition=available --timeout=300s deployment/redis-commander-local
print_success "Redis Commander deployed successfully"

# Step 5: Deploy Application
print_status "Deploying Reactive SSE Application..."
kubectl apply -f k8s-local-deployment.yaml
print_status "Waiting for application to be ready..."
kubectl wait --for=condition=available --timeout=300s deployment/reactive-sse-local
print_success "Application deployed successfully"

# Step 6: Deploy Ingress
print_status "Deploying Ingress..."
kubectl apply -f ingress-local.yaml
print_success "Ingress deployed successfully"

# Step 7: Display deployment information
echo ""
echo "📊 Deployment Summary:"
echo "====================="
kubectl get deployments
echo ""
kubectl get services
echo ""
kubectl get ingress
echo ""

# Step 9: Get access URLs
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
    echo ""
    print_status "🗄️  Database Management:"
    echo "   Redis Commander: http://reactive-sse.local/redis-commander"
fi

echo ""
print_status "🔧 Management Commands:"
echo "   Scale up: kubectl scale deployment reactive-sse-local --replicas=3"
echo "   View logs: kubectl logs -f deployment/reactive-sse-local"
echo "   Port forward app: kubectl port-forward service/reactive-sse-local-service 8080:8080"
echo ""

print_success "🎉 Local Kubernetes deployment with MongoDB completed successfully!"
echo ""
print_status "To test the deployment:"
echo "1. Open: http://reactive-sse.local/reactive-notifications.html"
echo "2. Connect with User ID: 1561"
echo "3. Import messages: curl -X POST http://reactive-sse.local/api/messages/import/csv"
echo "4. Watch real-time notifications!"
