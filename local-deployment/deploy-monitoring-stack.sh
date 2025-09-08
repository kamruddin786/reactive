#!/bin/bash
# Shell script to deploy the complete local monitoring stack

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

function info() {
    echo -e "${GREEN}INFO: $1${NC}"
}

function warn() {
    echo -e "${YELLOW}WARNING: $1${NC}"
}

function error() {
    echo -e "${RED}ERROR: $1${NC}"
}

function deploy_monitoring_stack() {
    info "Deploying local monitoring stack..."

    print_status "Building Docker image..."
    docker build -t reactive-sse-app:latest ../. || {
        print_error "Failed to build Docker image"
        exit 1
    }
    print_success "Docker image built successfully"

    # Deploy Prometheus configuration
    info "Deploying Prometheus configuration..."
    kubectl apply -f prometheus-config.yaml

    # Deploy Prometheus
    info "Deploying Prometheus..."
    kubectl apply -f prometheus-local-deployment.yaml

    # Deploy Grafana configuration
    info "Deploying Grafana configuration..."
    kubectl apply -f grafana-config.yaml

    # Deploy Grafana
    info "Deploying Grafana..."
    kubectl apply -f grafana-local-deployment.yaml

    # Deploy Redis (if not already deployed)
    if kubectl get deployment redis-local >/dev/null 2>&1; then
        info "Redis deployment already exists, skipping..."
    else
        info "Deploying Redis..."
        kubectl apply -f redis-local-deployment.yaml
    fi

    # Deploy the reactive application
    info "Deploying reactive SSE application..."
    kubectl apply -f k8s-local-deployment.yaml

    info "Waiting for deployments to be ready..."
    kubectl wait --for=condition=available --timeout=300s deployment/prometheus-local
    kubectl wait --for=condition=available --timeout=300s deployment/grafana-local
    kubectl wait --for=condition=available --timeout=300s deployment/reactive-sse-local

    info "Monitoring stack deployment completed!"
    echo ""
    info "Access URLs:"
    info "- Reactive App: http://localhost:30080"
    info "- Prometheus: http://localhost:30090"
    info "- Grafana: http://localhost:30300 (admin/admin123)"
    info "- App Metrics: http://localhost:30080/actuator/prometheus"
    info "- App Health: http://localhost:30080/actuator/health"
}

function cleanup_monitoring_stack() {
    warn "Cleaning up monitoring stack..."

    # Delete deployments
    kubectl delete -f k8s-local-deployment.yaml --ignore-not-found=true
    kubectl delete -f grafana-local-deployment.yaml --ignore-not-found=true
    kubectl delete -f prometheus-local-deployment.yaml --ignore-not-found=true
    kubectl delete -f redis-local-deployment.yaml --ignore-not-found=true

    # Delete configurations
    kubectl delete -f grafana-config.yaml --ignore-not-found=true
    kubectl delete -f prometheus-config.yaml --ignore-not-found=true

    # Remove Docker image (optional)
    read -p "Do you want to remove the Docker image? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        print_status "Removing Docker image..."
        docker rmi reactive-sse-app:latest 2>/dev/null || print_warning "Docker image not found"
    fi

    info "Cleanup completed!"
}

function show_status() {
    info "Current deployment status:"

    echo ""
    info "Deployments:"
    kubectl get deployments -l environment=local

    echo ""
    info "Services:"
    kubectl get services -l environment=local

    echo ""
    info "Pods:"
    kubectl get pods -l environment=local

    echo ""
    info "ConfigMaps:"
    kubectl get configmaps | grep -E "(prometheus|grafana)" || echo "No monitoring ConfigMaps found"
}

function show_help() {
    info "Local Monitoring Stack Deployment Script"
    echo "Usage:"
    echo "  $0 deploy   : Deploy the complete monitoring stack"
    echo "  $0 cleanup  : Clean up all monitoring resources"
    echo "  $0 status   : Show current deployment status"
    echo "  $0 help     : Show this help message"
    echo ""
    info "Prerequisites:"
    echo "- kubectl configured for local Kubernetes cluster"
    echo "- Docker images built locally"
    echo "- Sufficient cluster resources"
}

# Main execution
case "$1" in
    deploy)
        deploy_monitoring_stack
        ;;
    cleanup)
        cleanup_monitoring_stack
        ;;
    status)
        show_status
        ;;
    help)
        show_help
        ;;
    *)
        show_help
        ;;
esac
