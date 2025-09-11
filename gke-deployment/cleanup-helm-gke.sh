#!/bin/bash

# Helm Cleanup Script for Reactive SSE Application
# This script removes the Helm release and associated resources

set -e

NAMESPACE="${NAMESPACE:-default}"
RELEASE_NAME="${RELEASE_NAME:-reactive-sse-app}"

echo "🗑️  Cleaning up Reactive SSE Application Helm deployment..."
echo "📍 Namespace: $NAMESPACE"
echo "🏷️  Release Name: $RELEASE_NAME"

# Check if Helm is installed
if ! command -v helm &> /dev/null; then
    echo "❌ Helm is not installed."
    exit 1
fi

# Check if release exists
if ! helm list -n "$NAMESPACE" | grep -q "$RELEASE_NAME"; then
    echo "ℹ️  Release '$RELEASE_NAME' not found in namespace '$NAMESPACE'"
    echo "   Nothing to clean up."
    exit 0
fi

# Show what will be deleted
echo ""
echo "📋 Resources that will be deleted:"
helm get all "$RELEASE_NAME" -n "$NAMESPACE" | head -20

echo ""
read -p "❓ Are you sure you want to delete the release '$RELEASE_NAME'? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "🚫 Cleanup cancelled."
    exit 0
fi

# Uninstall the Helm release
echo "🗑️  Uninstalling Helm release..."
helm uninstall "$RELEASE_NAME" -n "$NAMESPACE"

# Wait for pods to terminate
echo "⏳ Waiting for pods to terminate..."
kubectl wait --for=delete pods -l app.kubernetes.io/name=reactive-sse-app -n "$NAMESPACE" --timeout=120s || true

# Optionally clean up persistent volumes if any
echo "🧹 Checking for persistent volumes..."
PVS=$(kubectl get pv -o jsonpath='{.items[?(@.spec.claimRef.namespace=="'$NAMESPACE'")].metadata.name}' || true)
if [ -n "$PVS" ]; then
    echo "⚠️  Found persistent volumes: $PVS"
    echo "   You may want to manually delete them if no longer needed."
fi

echo "✅ Cleanup completed!"
echo ""
echo "🔍 Verification - remaining resources in namespace:"
kubectl get all -n "$NAMESPACE" | grep reactive-sse-app || echo "   No reactive-sse-app resources found."
