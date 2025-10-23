#!/bin/bash

# Cleanup script for Local Kubernetes Deployment with KEDA
# Removes all resources created for the Reactive SSE Application including KEDA scaling

echo "🧹 Cleaning up Local Kubernetes Deployment for Reactive SSE Application with KEDA"

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

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Step 1: Remove KEDA ScaledObject
print_status "Removing KEDA ScaledObject..."
kubectl delete -f keda-local-scaledobject.yaml --ignore-not-found=true
print_success "KEDA ScaledObject removed"

# Step 2: Remove KEDA Redis authentication secret
print_status "Removing KEDA Redis authentication secret..."
kubectl delete secret redis-auth --ignore-not-found=true
print_success "Redis authentication secret removed"

# Step 3: Remove Ingress
print_status "Removing Ingress..."
kubectl delete -f ingress-local.yaml --ignore-not-found=true
print_success "Ingress removed"

# Step 4: Remove Application Deployment
print_status "Removing Application Deployment..."
kubectl delete -f k8s-local-deployment.yaml --ignore-not-found=true
print_success "Application deployment removed"

# Step 5: Remove Redis Commander
print_status "Removing Redis Commander..."
kubectl delete -f redis-commander-local.yaml --ignore-not-found=true
print_success "Redis Commander removed"

# Step 6: Remove Redis Deployment
print_status "Removing Redis Deployment..."
kubectl delete -f redis-local-deployment.yaml --ignore-not-found=true
print_success "Redis deployment removed"

# Step 7: Wait for pods to terminate
print_status "Waiting for pods to terminate..."
kubectl wait --for=delete pod -l app=reactive-sse-local --timeout=60s --ignore-not-found=true
kubectl wait --for=delete pod -l app=redis-local --timeout=60s --ignore-not-found=true
kubectl wait --for=delete pod -l app=redis-commander-local --timeout=60s --ignore-not-found=true

# Step 8: Check and optionally remove KEDA
echo ""
read -p "Do you want to remove KEDA completely? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    print_status "Removing KEDA..."
    kubectl delete -f https://github.com/kedacore/keda/releases/download/v2.12.0/keda-2.12.0.yaml --ignore-not-found=true
    print_status "Waiting for KEDA pods to terminate..."
    kubectl wait --for=delete pod -l app=keda-operator -n keda --timeout=120s --ignore-not-found=true
    kubectl wait --for=delete pod -l app=keda-metrics-apiserver -n keda --timeout=120s --ignore-not-found=true
    print_success "KEDA removed"
else
    print_warning "KEDA left installed for future use"
fi

# Step 9: Check and optionally remove NGINX Ingress Controller
echo ""
read -p "Do you want to remove NGINX Ingress Controller? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    print_status "Removing NGINX Ingress Controller..."

    if kubectl config current-context | grep -q "minikube"; then
        # For Minikube
        minikube addons disable ingress
        print_success "Minikube ingress addon disabled"
    else
        # For Docker Desktop or other
        kubectl delete -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.8.2/deploy/static/provider/cloud/deploy.yaml --ignore-not-found=true
        print_success "NGINX Ingress Controller removed"
    fi
else
    print_warning "NGINX Ingress Controller left installed for future use"
fi

# Step 10: Remove Docker image (optional)
echo ""
read -p "Do you want to remove the Docker image? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    print_status "Removing Docker image..."
    docker rmi reactive-sse-app:latest 2>/dev/null || print_warning "Docker image not found"
    print_success "Docker image removed"
fi

# Step 11: Clean up any remaining HPA resources
print_status "Cleaning up any remaining HPA resources..."
kubectl delete hpa reactive-sse-local-hpa --ignore-not-found=true
kubectl delete hpa keda-hpa-reactive-sse-local-scaler --ignore-not-found=true

print_success "🎉 Cleanup completed!"

# Show remaining resources (if any)
echo ""
echo "📊 Cleanup Summary:"
echo "=================="

print_status "Remaining deployments:"
REMAINING_DEPLOYMENTS=$(kubectl get deployments 2>/dev/null | grep -E "(reactive-sse|redis)" || echo "")
if [ -z "$REMAINING_DEPLOYMENTS" ]; then
    print_success "✅ No application deployments remaining"
else
    print_warning "⚠️  Found remaining deployments:"
    echo "$REMAINING_DEPLOYMENTS"
fi

echo ""
print_status "Remaining services:"
REMAINING_SERVICES=$(kubectl get services 2>/dev/null | grep -E "(reactive-sse|redis)" || echo "")
if [ -z "$REMAINING_SERVICES" ]; then
    print_success "✅ No application services remaining"
else
    print_warning "⚠️  Found remaining services:"
    echo "$REMAINING_SERVICES"
fi

echo ""
print_status "Remaining ingress:"
REMAINING_INGRESS=$(kubectl get ingress 2>/dev/null | grep -E "(reactive-sse|redis)" || echo "")
if [ -z "$REMAINING_INGRESS" ]; then
    print_success "✅ No application ingress remaining"
else
    print_warning "⚠️  Found remaining ingress:"
    echo "$REMAINING_INGRESS"
fi

echo ""
print_status "KEDA resources:"
REMAINING_SCALEDOBJECTS=$(kubectl get scaledobject 2>/dev/null | grep -v "NAME" || echo "")
if [ -z "$REMAINING_SCALEDOBJECTS" ]; then
    print_success "✅ No ScaledObjects remaining"
else
    print_warning "⚠️  Found remaining ScaledObjects:"
    echo "$REMAINING_SCALEDOBJECTS"
fi

REMAINING_HPA=$(kubectl get hpa 2>/dev/null | grep -v "NAME" || echo "")
if [ -z "$REMAINING_HPA" ]; then
    print_success "✅ No HPA resources remaining"
else
    print_warning "⚠️  Found remaining HPA resources:"
    echo "$REMAINING_HPA"
fi

echo ""
print_status "KEDA installation status:"
if kubectl get crd scaledobjects.keda.sh &> /dev/null; then
    print_warning "⚠️  KEDA is still installed"
    echo "   To remove: kubectl delete -f https://github.com/kedacore/keda/releases/download/v2.12.0/keda-2.12.0.yaml"
else
    print_success "✅ KEDA is not installed"
fi

echo ""
print_status "NGINX Ingress Controller status:"
if kubectl get ingressclass nginx &> /dev/null; then
    print_warning "⚠️  NGINX Ingress Controller is still installed"
else
    print_success "✅ NGINX Ingress Controller is not installed"
fi

echo ""
print_status "🔧 Manual cleanup commands (if needed):"
echo "   Remove any stuck pods: kubectl delete pod --all --force --grace-period=0"
echo "   Remove any stuck PVCs: kubectl delete pvc --all"
echo "   Remove KEDA manually: kubectl delete -f https://github.com/kedacore/keda/releases/download/v2.12.0/keda-2.12.0.yaml"
echo "   Check for stuck resources: kubectl get all"
