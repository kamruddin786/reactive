# Local Kubernetes Deployment Script for Reactive SSE Application (Windows PowerShell)
# This script works with Docker Desktop Kubernetes or Minikube

Write-Host "🚀 Starting Local Kubernetes Deployment for Reactive SSE Application" -ForegroundColor Blue

# Function to print colored output
function Write-Status {
    param([string]$Message)
    Write-Host "[INFO] $Message" -ForegroundColor Cyan
}

function Write-Success {
    param([string]$Message)
    Write-Host "[SUCCESS] $Message" -ForegroundColor Green
}

function Write-Warning {
    param([string]$Message)
    Write-Host "[WARNING] $Message" -ForegroundColor Yellow
}

function Write-Error {
    param([string]$Message)
    Write-Host "[ERROR] $Message" -ForegroundColor Red
}

# Check if kubectl is available
try {
    kubectl version --client --output=yaml | Out-Null
    Write-Success "kubectl is available"
} catch {
    Write-Error "kubectl is not installed or not in PATH"
    exit 1
}

# Check if we're connected to a Kubernetes cluster
try {
    kubectl cluster-info | Out-Null
    Write-Success "Connected to Kubernetes cluster"
} catch {
    Write-Error "Not connected to a Kubernetes cluster"
    Write-Warning "Please start Docker Desktop Kubernetes or Minikube"
    exit 1
}

# Step 1: Build Docker image
Write-Status "Building Docker image..."
try {
    docker build -t reactive-sse-app:latest .
    Write-Success "Docker image built successfully"
} catch {
    Write-Error "Failed to build Docker image"
    exit 1
}

# Step 2: Load image into Minikube (if using Minikube)
$currentContext = kubectl config current-context
if ($currentContext -like "*minikube*") {
    Write-Status "Detected Minikube, loading image..."
    try {
        minikube image load reactive-sse-app:latest
    } catch {
        Write-Warning "Failed to load image into Minikube, continuing anyway..."
    }
}

# Step 3: Install NGINX Ingress Controller (if not already installed)
Write-Status "Checking for NGINX Ingress Controller..."
try {
    kubectl get ingressclass nginx | Out-Null
    Write-Success "NGINX Ingress Controller already available"
} catch {
    Write-Status "Installing NGINX Ingress Controller..."

    if ($currentContext -like "*minikube*") {
        # For Minikube
        minikube addons enable ingress
    } else {
        # For Docker Desktop or other
        kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.8.2/deploy/static/provider/cloud/deploy.yaml

        # Wait for ingress controller to be ready
        Write-Status "Waiting for NGINX Ingress Controller to be ready..."
        kubectl wait --namespace ingress-nginx --for=condition=ready pod --selector=app.kubernetes.io/component=controller --timeout=300s
    }
    Write-Success "NGINX Ingress Controller installed"
}

# Step 4: Deploy Redis
Write-Status "Deploying Redis..."
kubectl apply -f redis-local-deployment.yaml
kubectl wait --for=condition=available --timeout=300s deployment/redis-local
Write-Success "Redis deployed successfully"

# Step 5: Deploy Application
Write-Status "Deploying Reactive SSE Application..."
kubectl apply -f k8s-local-deployment.yaml
kubectl wait --for=condition=available --timeout=300s deployment/reactive-sse-local
Write-Success "Application deployed successfully"

# Step 6: Deploy Ingress
Write-Status "Deploying Ingress..."
kubectl apply -f ingress-local.yaml
Write-Success "Ingress deployed successfully"

# Step 7: Display deployment information
Write-Host ""
Write-Host "📊 Deployment Summary:" -ForegroundColor Blue
Write-Host "=====================" -ForegroundColor Blue
kubectl get deployments
Write-Host ""
kubectl get services
Write-Host ""
kubectl get ingress
Write-Host ""

# Step 8: Get access URLs
Write-Status "Getting access information..."

if ($currentContext -like "*minikube*") {
    $minikubeIp = minikube ip
    Write-Host ""
    Write-Success "🌐 Application Access URLs:"
    Write-Host "   With custom hostname: http://reactive-sse.local"
    Write-Host "   Add to /etc/hosts: $minikubeIp reactive-sse.local"
    Write-Host ""
    Write-Host "   Alternative access: http://$minikubeIp/reactive-sse"
} else {
    Write-Host ""
    Write-Success "🌐 Application Access URLs:"
    Write-Host "   With custom hostname: http://reactive-sse.local"
    Write-Host "   Add to C:\Windows\System32\drivers\etc\hosts:"
    Write-Host "   127.0.0.1 reactive-sse.local"
    Write-Host ""
    Write-Host "   Alternative access: http://localhost/reactive-sse"
}

Write-Host ""
Write-Status "📡 Available Endpoints:"
Write-Host "   SSE Stream: http://reactive-sse.local/sse/connect/{clientId}"
Write-Host "   Push Message: POST http://reactive-sse.local/sse/push/{clientId}"
Write-Host "   Broadcast: POST http://reactive-sse.local/sse/broadcast"
Write-Host "   Health Check: http://reactive-sse.local/actuator/health"

Write-Host ""
Write-Status "🔧 Management Commands:"
Write-Host "   Scale up: kubectl scale deployment reactive-sse-local --replicas=3"
Write-Host "   View logs: kubectl logs -f deployment/reactive-sse-local"
Write-Host "   Port forward: kubectl port-forward service/reactive-sse-local-service 8080:8080"
Write-Host ""

Write-Success "🎉 Local Kubernetes deployment completed successfully!"
Write-Host ""
Write-Status "To test the deployment:"
Write-Host "Invoke-WebRequest -Uri http://reactive-sse.local/sse/connect/testuser -Method Get"
