# KEDA Scaling Strategy for Redis-based Reactive SSE Application

## Overview
This document outlines the KEDA scaling metrics and strategies specifically designed for your Spring Boot reactive application using Redis pub/sub with the MessagePublisher service.

## Your MessagePublisher Topics and KEDA Integration

### Current Topics in MessagePublisher
1. **`user:messages`** - Individual user messages
2. **`broadcast:messages`** - Broadcast messages to all users

### Recommended KEDA Metrics for Scaling

## 1. Redis Pub/Sub Subscriber Count (Primary Metric)
**Best for:** Scaling based on actual demand (number of connected clients)

```yaml
- type: redis
  metadata:
    address: redis-service:6379
    command: PUBSUB
    commandArgs: NUMSUB user:messages
    targetValue: '5'  # Scale up when >5 subscribers per pod
```

**Why this works for your topics:**
- Your `USER_MESSAGES_TOPIC` and `BROADCAST_MESSAGES_TOPIC` have subscriber counts
- More subscribers = more SSE connections = need more pods
- Your MessagePublisher already tracks subscriber counts via `updateSubscriberCount()`

## 2. Custom Application Metrics (Recommended)
**Best for:** Application-specific scaling decisions

Your enhanced MessagePublisher now exposes these metrics:
- `redis_pending_messages` - Messages waiting to be processed
- `redis_active_subscribers` - Current subscriber count
- `messages_published_total` - Publishing rate by topic
- `redis_connection_status` - Redis health

**KEDA Configuration:**
```yaml
- type: metrics-api
  metadata:
    url: http://reactive-sse-app:8080/metrics/keda/scaling-metrics
    valueLocation: 'totalPendingMessages'
    targetValue: '100'
```

## 3. Message Publishing Rate
**Best for:** Scaling based on message throughput

```yaml
- type: prometheus
  metadata:
    query: rate(redis_messages_published_total[2m])
    threshold: '20'
```

## 4. Redis Memory Usage
**Best for:** Preventing Redis overload from message backlogs

```yaml
- type: redis
  metadata:
    command: INFO
    commandArgs: memory
    targetValue: '104857600'  # 100MB
```

## Implementation Strategy

### Step 1: Use Your Existing MessagePublisher Metrics
Your MessagePublisher class already provides:
```java
// These metrics are perfect for KEDA scaling
private final AtomicLong pendingMessageCount = new AtomicLong(0);
private final AtomicLong activeSubscribersCount = new AtomicLong(0);

// Call this when messages are queued
public void updatePendingMessageCount(long delta)

// Tracks subscribers for your topics
private void updateSubscriberCount(String topic)
```

### Step 2: Custom Metrics Endpoint
The new `/metrics/keda/scaling-metrics` endpoint provides:
```json
{
  "topics": {
    "userMessages": {"subscribers": 25, "published": 1500},
    "broadcastMessages": {"subscribers": 100, "published": 300}
  },
  "totalPendingMessages": 50,
  "totalActiveSubscribers": 125,
  "recommendedPods": 3
}
```

### Step 3: Topic-Specific Scaling Logic

**For `user:messages` topic:**
- Scale based on subscriber count (1 pod per 50 subscribers)
- Monitor pending message count
- Consider message publishing rate

**For `broadcast:messages` topic:**
- Scale more aggressively (broadcasts affect all users)
- Monitor Redis memory usage
- Consider connection count

## Scaling Scenarios

### Scenario 1: High User Traffic
**Trigger:** Increase in `user:messages` subscribers
**Metrics:** 
- `redis_active_subscribers` > 50 per pod
- `messages_published_total{topic="user:messages"}` rate increasing

**KEDA Response:** Scale up pods to handle more SSE connections

### Scenario 2: Broadcast Storm
**Trigger:** Many `broadcast:messages` being published
**Metrics:**
- `redis_pending_messages` increasing
- Redis memory usage growing
- `messages_published_total{topic="broadcast:messages"}` spike

**KEDA Response:** Rapid scale-up to process broadcast backlog

### Scenario 3: Message Processing Lag
**Trigger:** Messages published faster than consumed
**Metrics:**
- Custom metric: `rate(published) - rate(consumed) > threshold`
- `redis_pending_messages` accumulating

**KEDA Response:** Scale up to reduce processing lag

## Deployment Commands

```bash
# Set up KEDA with your Redis scaling
./keda-deployment/setup-keda-scaling.sh

# Monitor your topics specifically
kubectl logs -f deployment/reactive-sse-app | grep "user:messages\|broadcast:messages"

# Check KEDA scaling decisions
kubectl describe scaledobject reactive-sse-redis-pubsub-scaler
```

## Best Practices for Your Topics

1. **Monitor Both Topics Separately:**
   ```yaml
   # Scale differently for user vs broadcast messages
   - targetValue: '5'   # user:messages
   - targetValue: '10'  # broadcast:messages
   ```

2. **Use Composite Metrics:**
   ```yaml
   # Combine subscriber count + pending messages
   query: redis_active_subscribers + (redis_pending_messages / 10)
   ```

3. **Topic-Aware Scaling:**
   ```java
   // In your MessagePublisher, call this when publishing
   updatePendingMessageCount(1);  // Increment when publishing
   updatePendingMessageCount(-1); // Decrement when processed
   ```

## Monitoring Your Topic Metrics

```bash
# Check subscriber counts for your topics
redis-cli PUBSUB NUMSUB user:messages broadcast:messages

# Monitor your custom metrics
curl http://your-app:8080/metrics/keda/scaling-metrics

# View Prometheus metrics
curl http://your-app:8080/actuator/prometheus | grep messages_published_total
```

## Configuration Files Created

1. `keda-redis-pubsub-optimized.yaml` - Redis pub/sub scaling for your topics
2. `keda-custom-metrics-scaler.yaml` - Custom application metrics
3. `KedaMetricsController.java` - Exposes your topic metrics to KEDA
4. Enhanced `MessagePublisher.java` - Additional KEDA-friendly metrics

This strategy leverages your existing Redis pub/sub architecture and MessagePublisher topics to provide intelligent, responsive scaling based on actual application demand.
