#!/bin/bash

# Cleanup script for Local Kubernetes Deployment
# Removes all resources created for the Reactive SSE Application

echo "🧹 Cleaning up Local Kubernetes Deployment for Reactive SSE Application"

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Remove Ingress
print_status "Removing Ingress..."
kubectl delete -f ingress-local.yaml --ignore-not-found=true

# Remove Application Deployment
print_status "Removing Application Deployment..."
kubectl delete -f k8s-local-deployment.yaml --ignore-not-found=true

# Remove Redis Deployment
print_status "Removing Redis Deployment..."
kubectl delete -f redis-local-deployment.yaml --ignore-not-found=true

# Remove Redis Commander
print_status "Removing Redis Commander..."
kubectl delete -f redis-commander-local.yaml --ignore-not-found=true

# Remove Docker image (optional)
read -p "Do you want to remove the Docker image? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    print_status "Removing Docker image..."
    docker rmi reactive-sse-app:latest 2>/dev/null || print_warning "Docker image not found"
fi

print_success "Cleanup completed!"

# Show remaining resources (if any)
echo ""
print_status "Remaining deployments:"
kubectl get deployments | grep -E "(reactive-sse|redis)" || echo "None found"

echo ""
print_status "Remaining services:"
kubectl get services | grep -E "(reactive-sse|redis)" || echo "None found"

echo ""
print_status "Remaining PVCs (if any):"
kubectl get pvc | grep -E "(redis)" || echo "None found"
