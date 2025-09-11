#!/bin/bash
# Script to test HPA by generating load
# Author: Kamruddin
# Date: September 11, 2025

USERS=${1:-10000}
DURATION=${2:-300}

# Check if curl is installed
if ! command -v curl &> /dev/null; then
    echo -e "\033[0;31mcurl is not installed. Please install curl and try again.\033[0m"
    exit 1
fi

# Get the service URL
SERVICE_PORT=$(kubectl get svc reactive-sse-local-service -o jsonpath='{.spec.ports[0].nodePort}')
SERVICE_IP="localhost"
SERVICE_URL="http://${SERVICE_IP}:${SERVICE_PORT}/api/notifications/user"

echo -e "\033[0;32mStarting load test with $USERS users for $DURATION seconds against $SERVICE_URL\033[0m"

# Generate connections in parallel
for ((i=0; i<$USERS; i++)); do
    userId="user-$i"
    
    # Using curl to make an SSE connection
    (curl -N -H "Accept: text/event-stream" "$SERVICE_URL/$userId/stream" > /dev/null 2>&1) &
    
    # Display progress every 100 users
    if ((i % 100 == 0)); then
        echo -e "\033[0;33mStarted $i connections...\033[0m"
        sleep 0.1 # Brief pause to avoid overwhelming the system
    fi
done

echo -e "\033[0;32mAll $USERS connections started. Monitoring HPA scaling...\033[0m"

# Monitor HPA status every 10 seconds during the test
END_TIME=$(($(date +%s) + $DURATION))
while [[ $(date +%s) -lt $END_TIME ]]; do
    HPA_STATUS=$(kubectl get hpa reactive-sse-local-hpa -o json)
    CURRENT_REPLICAS=$(echo "$HPA_STATUS" | jq -r '.status.currentReplicas')
    DESIRED_REPLICAS=$(echo "$HPA_STATUS" | jq -r '.status.desiredReplicas')
    
    echo -e "\033[0;36mCurrent Replicas: $CURRENT_REPLICAS, Desired Replicas: $DESIRED_REPLICAS\033[0m"
    
    CURRENT_VALUE=$(echo "$HPA_STATUS" | jq -r '.status.currentMetrics[] | select(.pods.metricName=="sse_active_users") | .pods.current.averageValue')
    TARGET_VALUE=$(echo "$HPA_STATUS" | jq -r '.status.currentMetrics[] | select(.pods.metricName=="sse_active_users") | .pods.target.averageValue')
    
    if [[ -n "$CURRENT_VALUE" && -n "$TARGET_VALUE" ]]; then
        echo -e "\033[0;36mActive Users Metric: Current: $CURRENT_VALUE, Target: $TARGET_VALUE\033[0m"
    fi
    
    sleep 10
done

echo -e "\033[0;32mLoad test completed. Cleaning up connections...\033[0m"

# Kill all curl processes
pkill -f "curl -N -H Accept: text/event-stream $SERVICE_URL/"

echo -e "\033[0;32mLoad test finished. Check the HPA metrics to see how it scaled.\033[0m"
kubectl get hpa reactive-sse-local-hpa
