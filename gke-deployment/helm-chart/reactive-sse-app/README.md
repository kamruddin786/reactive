# Reactive SSE App Helm Chart

This Helm chart deploys the Reactive SSE Application on Google Kubernetes Engine (GKE) with autoscaling capabilities.

## Prerequisites

- Kubernetes cluster (GKE recommended)
- Helm 3.x installed
- kubectl configured to access your cluster
- Redis instance available (default: 10.139.0.107:6379)

## Installation

### Quick Start
```bash
# Deploy with default values
helm install reactive-sse-app ./helm-chart/reactive-sse-app

# Or use the deployment script
./deploy-helm-gke.sh
```

### Custom Installation
```bash
# Install with custom values
helm install reactive-sse-app ./helm-chart/reactive-sse-app \
  --set image.tag=v2.1.12 \
  --set autoscaling.maxReplicas=3 \
  --set redis.host=your-redis-host

# Install in specific namespace
helm install reactive-sse-app ./helm-chart/reactive-sse-app \
  --namespace production \
  --create-namespace

# Install with production values
helm install reactive-sse-app ./helm-chart/reactive-sse-app \
  -f values-prod.yaml \
  --namespace production
```

## Configuration

### Key Configuration Values

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Initial number of pods | `1` |
| `image.repository` | Docker image repository | `us-central1-docker.pkg.dev/rfx-eng-tm-poc-d/poc-images/reactive-sse-app` |
| `image.tag` | Docker image tag | `v2.1.11` |
| `service.port` | Service port | `80` |
| `service.targetPort` | Container port | `8080` |
| `autoscaling.enabled` | Enable HPA | `true` |
| `autoscaling.minReplicas` | Minimum pods | `1` |
| `autoscaling.maxReplicas` | Maximum pods | `2` |
| `autoscaling.targetMemoryUtilizationPercentage` | Memory threshold | `90` |
| `redis.host` | Redis host | `10.139.0.107` |
| `redis.port` | Redis port | `6379` |

### Environment-Specific Deployments

**Development:**
```bash
helm install reactive-sse-app ./helm-chart/reactive-sse-app \
  --set autoscaling.maxReplicas=1 \
  --set resources.limits.memory=1Gi
```

**Production:**
```bash
helm install reactive-sse-app ./helm-chart/reactive-sse-app \
  -f values-prod.yaml
```

## Monitoring and Management

### View Status
```bash
# Check deployment status
kubectl get all -l app.kubernetes.io/name=reactive-sse-app

# Monitor autoscaling
kubectl get hpa --watch

# View logs
kubectl logs -f deployment/reactive-sse-app
```

### Helm Operations
```bash
# Upgrade deployment
helm upgrade reactive-sse-app ./helm-chart/reactive-sse-app

# View current values
helm get values reactive-sse-app

# View deployment history
helm history reactive-sse-app

# Rollback to previous version
helm rollback reactive-sse-app 1
```

## Features

- **Autoscaling**: HPA configured for memory-based scaling (1-2 pods by default)
- **GKE Integration**: BackendConfig for load balancer optimization
- **Health Checks**: Kubernetes liveness and readiness probes
- **Resource Management**: CPU and memory limits/requests
- **Session Affinity**: Sticky sessions for SSE connections
- **Streaming Optimization**: Custom headers and timeout settings

## Troubleshooting

### Common Issues

1. **Pods not starting**: Check image pull policy and repository access
   ```bash
   kubectl describe pod <pod-name>
   ```

2. **HPA not scaling**: Verify metrics server is running
   ```bash
   kubectl get hpa
   kubectl top pods
   ```

3. **Ingress not accessible**: Check load balancer provisioning (takes 5-10 minutes)
   ```bash
   kubectl describe ingress reactive-sse-app-ingress
   ```

### Cleanup
```bash
# Remove deployment
helm uninstall reactive-sse-app

# Or use cleanup script
./cleanup-helm-gke.sh
```

## Architecture

The chart deploys:
- **Deployment**: Main application pods with configurable replicas
- **Service**: ClusterIP service with GKE annotations
- **HPA**: Memory-based horizontal pod autoscaler
- **BackendConfig**: GKE-specific load balancer configuration
- **Ingress**: HTTP(S) load balancer with SSL termination support

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review Helm and Kubernetes logs
3. Consult the original GKE deployment documentation
