package com.kamruddin.reactive.controllers;

import com.kamruddin.reactive.services.MessagePublisher;
import com.kamruddin.reactive.services.MessageNotificationConsumer;
import com.kamruddin.reactive.services.UserConnectionTracker;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping("/metrics/keda")
public class KedaMetricsController {

    @Autowired
    private MessagePublisher messagePublisher;

    @Autowired
    private MessageNotificationConsumer messageNotificationConsumer;

    @Autowired
    private UserConnectionTracker userConnectionTracker;

    @Autowired
    private MeterRegistry meterRegistry;

    /**
     * Endpoint for KEDA to get Redis topic metrics
     * Usage: /metrics/keda/redis/topic/{topicName}
     */
    @GetMapping("/redis/topic/{topicName}")
    public ResponseEntity<Map<String, Object>> getRedisTopicMetrics(@PathVariable String topicName) {
        MessagePublisher.RedisTopicMetrics metrics = messagePublisher.getTopicMetrics(topicName);

        Map<String, Object> response = new HashMap<>();
        response.put("topic", metrics.getTopic());
        response.put("subscriberCount", metrics.getSubscriberCount());
        response.put("memoryUsage", metrics.getMemoryUsage());
        response.put("timestamp", metrics.getTimestamp());
        response.put("pendingMessages", messagePublisher.getPendingMessageCount());
        response.put("redisConnected", messagePublisher.isRedisConnected());

        return ResponseEntity.ok(response);
    }

    /**
     * Endpoint for KEDA to get message publishing rate
     * This provides the rate of messages published in the last minute
     */
    @GetMapping("/redis/publish-rate")
    public ResponseEntity<Map<String, Object>> getMessagePublishRate() {
        Map<String, Object> response = new HashMap<>();

        // Get counters for both topics
        double userMessages = meterRegistry.counter("messages_published_total", "topic", MessagePublisher.USER_MESSAGES_TOPIC).count();
        double broadcastMessages = meterRegistry.counter("messages_published_total", "topic", MessagePublisher.BROADCAST_MESSAGES_TOPIC).count();
        double totalMessages = meterRegistry.counter("redis_messages_published_total").count();
        double failures = meterRegistry.counter("messages_publish_failures_total").count();

        response.put("userMessagesTotal", userMessages);
        response.put("broadcastMessagesTotal", broadcastMessages);
        response.put("totalMessagesPublished", totalMessages);
        response.put("publishFailures", failures);
        response.put("pendingMessages", messagePublisher.getPendingMessageCount());
        response.put("activeSubscribers", messagePublisher.getActiveSubscribersCount());
        response.put("successRate", totalMessages > 0 ? (totalMessages - failures) / totalMessages : 1.0);

        return ResponseEntity.ok(response);
    }

    /**
     * NEW: Endpoint for KEDA to get user connection metrics for scaling
     * This is the primary endpoint for user-based scaling decisions
     */
    @GetMapping("/user-connections")
    public ResponseEntity<Map<String, Object>> getUserConnectionMetrics() {
        Map<String, Object> response = new HashMap<>();

        try {
            // Get connection statistics from our Redis tracking system
            Map<String, Object> connectionStats = messageNotificationConsumer.getConnectionStats();
            Map<String, Object> redisStats = userConnectionTracker.getConnectionStats();

            // Extract key metrics for KEDA scaling
            @SuppressWarnings("unchecked")
            Map<String, Object> localMemory = (Map<String, Object>) connectionStats.get("localMemory");

            int totalUsersInMemory = (Integer) localMemory.getOrDefault("totalUsers", 0);
            int activeHeartbeats = (Integer) localMemory.getOrDefault("activeHeartbeats", 0);
            int activeSubscriptions = messageNotificationConsumer.getActiveSubscriptionCount();

            // Redis-based metrics
            int totalConnectedUsers = (Integer) redisStats.getOrDefault("totalConnectedUsers", 0);
            int totalConnections = (Integer) redisStats.getOrDefault("totalConnections", 0);

            @SuppressWarnings("unchecked")
            Map<String, Long> connectionsByPod = (Map<String, Long>) redisStats.getOrDefault("connectionsByPod", new HashMap<>());

            // Current pod information
            String currentPodId = getCurrentPodId();
            long currentPodConnections = connectionsByPod.getOrDefault(currentPodId, 0L);

            // Build response for KEDA
            response.put("totalConnectedUsers", totalConnectedUsers);
            response.put("totalActiveConnections", totalConnections);
            response.put("activeSubscriptions", activeSubscriptions);
            response.put("currentPodConnections", currentPodConnections);
            response.put("currentPodId", currentPodId);
            response.put("activePods", connectionsByPod.size());
            response.put("connectionsByPod", connectionsByPod);
            response.put("timestamp", System.currentTimeMillis());

            // Calculate average connections per pod
            double avgConnectionsPerPod = connectionsByPod.isEmpty() ? 0 :
                connectionsByPod.values().stream().mapToLong(Long::longValue).average().orElse(0);
            response.put("avgConnectionsPerPod", avgConnectionsPerPod);

            // Calculate scaling recommendation based on connected users
            int recommendedPods = calculateUserBasedScaling(totalConnectedUsers, totalConnections, connectionsByPod);
            response.put("recommendedPods", recommendedPods);

            // Add scaling reason for debugging
            response.put("scalingReason", getScalingReason(totalConnectedUsers, totalConnections, avgConnectionsPerPod));

        } catch (Exception e) {
            response.put("error", "Failed to get user connection metrics: " + e.getMessage());
            response.put("totalConnectedUsers", 0);
            response.put("recommendedPods", 1);
        }

        return ResponseEntity.ok(response);
    }

    /**
     * NEW: Enhanced scaling metrics endpoint focused on user connections
     * This replaces the old topic-based scaling logic with user connection-based scaling
     */
    @GetMapping("/scaling-metrics")
    public ResponseEntity<Map<String, Object>> getScalingMetrics() {
        Map<String, Object> response = new HashMap<>();

        try {
            // Get user connection metrics (primary scaling factor)
            Map<String, Object> userConnectionStats = messageNotificationConsumer.getConnectionStats();
            Map<String, Object> redisConnectionStats = userConnectionTracker.getConnectionStats();

            // Extract user connection data
            int totalConnectedUsers = (Integer) redisConnectionStats.getOrDefault("totalConnectedUsers", 0);
            int totalConnections = (Integer) redisConnectionStats.getOrDefault("totalConnections", 0);

            @SuppressWarnings("unchecked")
            Map<String, Long> connectionsByPod = (Map<String, Long>) redisConnectionStats.getOrDefault("connectionsByPod", new HashMap<>());

            // Current pod metrics
            String currentPodId = getCurrentPodId();
            long currentPodConnections = connectionsByPod.getOrDefault(currentPodId, 0L);
            int activeSubscriptions = messageNotificationConsumer.getActiveSubscriptionCount();

            // Build primary metrics for KEDA
            response.put("primaryMetrics", Map.of(
                "totalConnectedUsers", totalConnectedUsers,
                "totalActiveConnections", totalConnections,
                "activeSubscriptions", activeSubscriptions,
                "activePods", connectionsByPod.size()
            ));

            // Pod distribution metrics
            response.put("podMetrics", Map.of(
                "currentPodId", currentPodId,
                "currentPodConnections", currentPodConnections,
                "connectionsByPod", connectionsByPod,
                "avgConnectionsPerPod", connectionsByPod.isEmpty() ? 0 :
                    connectionsByPod.values().stream().mapToLong(Long::longValue).average().orElse(0)
            ));

            // Calculate user-based scaling recommendation
            int recommendedPods = calculateUserBasedScaling(totalConnectedUsers, totalConnections, connectionsByPod);
            response.put("recommendedPods", recommendedPods);
            response.put("scalingStrategy", "USER_CONNECTION_BASED");
            response.put("scalingReason", getScalingReason(totalConnectedUsers, totalConnections,
                connectionsByPod.isEmpty() ? 0 : connectionsByPod.values().stream().mapToLong(Long::longValue).average().orElse(0)));

            // Add secondary metrics (Redis/messaging) for context
            response.put("secondaryMetrics", Map.of(
                "redisConnected", messagePublisher.isRedisConnected(),
                "pendingMessages", messagePublisher.getPendingMessageCount(),
                "messagePublishFailures", meterRegistry.counter("messages_publish_failures_total").count()
            ));

            response.put("timestamp", System.currentTimeMillis());

        } catch (Exception e) {
            // Fallback metrics if connection tracking fails
            response.put("error", "Failed to get scaling metrics: " + e.getMessage());
            response.put("recommendedPods", 1);
            response.put("scalingStrategy", "FALLBACK");
            response.put("primaryMetrics", Map.of(
                "totalConnectedUsers", 0,
                "totalActiveConnections", 0,
                "activeSubscriptions", 0,
                "activePods", 1
            ));
        }

        return ResponseEntity.ok(response);
    }

    /**
     * Calculate recommended pod count based on user connections
     */
    private int calculateUserBasedScaling(int totalUsers, int totalConnections, Map<String, Long> connectionsByPod) {
        // Base scaling: 1 pod per 100 connected users
        int baseScale = Math.max(1, (int) Math.ceil(totalUsers / 100.0));

        // Load balancing consideration: if any pod has >150 connections, scale up
        long maxPodConnections = connectionsByPod.values().stream().mapToLong(Long::longValue).max().orElse(0);
        int loadBalanceScale = maxPodConnections > 150 ? baseScale + 1 : baseScale;

        // High activity scaling: if total connections > users (multiple connections per user), scale more aggressively
        int activityScale = totalConnections > totalUsers * 1.5 ? (int) Math.ceil(totalConnections / 120.0) : baseScale;

        // Take the maximum of all scaling factors, but cap at 10 pods
        return Math.min(10, Math.max(Math.max(baseScale, loadBalanceScale), activityScale));
    }

    /**
     * Get human-readable scaling reason for debugging
     */
    private String getScalingReason(int totalUsers, int totalConnections, double avgConnectionsPerPod) {
        if (totalUsers == 0) return "No active users";
        if (totalUsers <= 50) return "Low user count (≤50)";
        if (totalUsers <= 100) return "Moderate user count (≤100)";
        if (avgConnectionsPerPod > 150) return "High load per pod (>150 connections/pod)";
        if (totalConnections > totalUsers * 1.5) return "High activity (multiple connections per user)";
        return "Standard scaling based on user count";
    }

    /**
     * Get current pod ID
     */
    private String getCurrentPodId() {
        try {
            return System.getenv("HOSTNAME") != null ? System.getenv("HOSTNAME") :
                   java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown-pod";
        }
    }
}
