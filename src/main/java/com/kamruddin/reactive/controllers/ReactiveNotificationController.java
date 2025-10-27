package com.kamruddin.reactive.controllers;

import com.kamruddin.reactive.models.MessageNotification;
import com.kamruddin.reactive.services.MessageNotificationConsumer;
import com.kamruddin.reactive.services.UserConnectionTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping("/api/notifications")
@CrossOrigin(origins = "*", allowedHeaders = "*")
public class ReactiveNotificationController {

    private static final Logger logger = LoggerFactory.getLogger(ReactiveNotificationController.class);

//    @Autowired
//    private UserNotificationConsumer userNotificationConsumer;

//    @Autowired
//    private StreamNotificationConsumer streamNotificationConsumer;

    @Autowired
    private MessageNotificationConsumer messageNotificationConsumer;

    @Autowired
    private UserConnectionTracker userConnectionTracker;

    /**
     * Reactive SSE endpoint for user-specific message notifications
     * Returns Flux<ServerSentEvent> for reactive streaming
     */
    @GetMapping(value = "/user/{userId}/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<MessageNotification>> streamNotifications(
            @PathVariable Long userId) {

        logger.info("Client connecting to reactive SSE stream for user: {}", userId);

        return messageNotificationConsumer.createUserStream(userId)
            .timeout(Duration.ofHours(1))
            .retryWhen(Retry.backoff(3, Duration.ofSeconds(1))
                .maxBackoff(Duration.ofSeconds(10))
                .filter(throwable -> !(throwable instanceof java.util.concurrent.CancellationException)))
            .doOnSubscribe(subscription ->
                logger.info("Started SSE stream for user: {}", userId))
            .doOnCancel(() ->
                logger.info("SSE stream cancelled for user: {}", userId))
            .doOnTerminate(() ->
                logger.info("SSE stream terminated for user: {}", userId))
            .doOnError(error -> {
                logger.error("Error in SSE stream for user {}: {}", userId, error.getMessage());
            })
            .onErrorResume(error -> {
                logger.warn("Recovering from SSE stream error for user {}: {}", userId, error.getMessage());
                return Flux.just(ServerSentEvent.<MessageNotification>builder()
                    .id(String.valueOf(System.currentTimeMillis()))
                    .event("error")
                    .comment("Stream error occurred, client should reconnect")
                    .build());
            });
    }

    /**
     * Get connection statistics
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getConnectionStats() {
        try {
            Map<String, Object> stats = messageNotificationConsumer.getConnectionStats();
            return ResponseEntity.ok(stats);
        } catch (Exception e) {
            logger.error("Error getting connection stats: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get Redis connection tracking statistics
     */
    @GetMapping("/connections/redis")
    public ResponseEntity<Map<String, Object>> getRedisConnections() {
        try {
            Map<String, Object> redisStats = userConnectionTracker.getConnectionStats();
            return ResponseEntity.ok(redisStats);
        } catch (Exception e) {
            logger.error("Error getting Redis connection stats: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get connections for a specific user
     */
    @GetMapping("/connections/user/{userId}")
    public ResponseEntity<Map<String, Object>> getUserConnections(@PathVariable Long userId) {
        try {
            Set<String> connections = userConnectionTracker.getUserConnections(userId);
            boolean isConnected = userConnectionTracker.isUserConnected(userId);

            Map<String, Object> result = Map.of(
                    "userId", userId,
                    "isConnected", isConnected,
                    "connectedPods", connections,
                    "connectionCount", connections.size(),
                    "timestamp", System.currentTimeMillis()
            );

            return ResponseEntity.ok(result);
        } catch (Exception e) {
            logger.error("Error getting connections for user {}: {}", userId, e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Administrative endpoint to cleanup connections for current pod
     */
    @PostMapping("/admin/cleanup-pod")
    public ResponseEntity<Map<String, Object>> cleanupCurrentPod() {
        try {
            String podId = System.getenv("HOSTNAME") != null ? System.getenv("HOSTNAME") : "localhost";
            userConnectionTracker.cleanupPodConnections(podId);

            Map<String, Object> result = Map.of(
                    "status", "success",
                    "message", "Cleaned up connections for pod: " + podId,
                    "podId", podId,
                    "timestamp", System.currentTimeMillis()
            );

            logger.info("Manual cleanup performed for pod: {}", podId);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            logger.error("Error during pod cleanup: {}", e.getMessage());
            return ResponseEntity.status(500).body(Map.of(
                    "status", "error",
                    "message", "Failed to cleanup pod connections",
                    "error", e.getMessage()
            ));
        }
    }

    /**
     * Administrative endpoint to cleanup connections for a specific pod
     */
    @PostMapping("/admin/cleanup-pod/{podId}")
    public ResponseEntity<Map<String, Object>> cleanupSpecificPod(@PathVariable String podId) {
        try {
            userConnectionTracker.cleanupPodConnections(podId);

            Map<String, Object> result = Map.of(
                    "status", "success",
                    "message", "Cleaned up connections for pod: " + podId,
                    "podId", podId,
                    "timestamp", System.currentTimeMillis()
            );

            logger.info("Manual cleanup performed for pod: {}", podId);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            logger.error("Error during pod cleanup for {}: {}", podId, e.getMessage());
            return ResponseEntity.status(500).body(Map.of(
                    "status", "error",
                    "message", "Failed to cleanup pod connections",
                    "podId", podId,
                    "error", e.getMessage()
            ));
        }
    }

    /**
     * Health check endpoint
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        String podId = System.getenv("HOSTNAME") != null ? System.getenv("HOSTNAME") : "localhost";
        try {
            // Check Redis connectivity and other dependencies
            Map<String, Object> stats = messageNotificationConsumer.getConnectionStats();

            Map<String, Object> health = Map.of(
                    "status", "UP",
                    "service", "ReactiveNotificationController",
                    "timestamp", System.currentTimeMillis(),
                    "environment", "GKE",
                    "activeUsers", stats.getOrDefault("totalUsers", 0),
                    "podId", podId
            );
            return ResponseEntity.ok(health);
        } catch (Exception e) {
            logger.error("Health check failed: {}", e.getMessage());
            Map<String, Object> health = Map.of(
                    "podId", podId,
                    "status", "DOWN",
                    "service", "ReactiveNotificationController",
                    "timestamp", System.currentTimeMillis(),
                    "error", e.getMessage()
            );
            return ResponseEntity.status(503).body(health);
        }
    }

    /**
     * Load-balancing health check endpoint for NGINX routing decisions
     * Returns different HTTP status codes based on current pod's connection load only
     */
    @GetMapping("/health/load-balance")
    public ResponseEntity<Map<String, Object>> loadBalanceHealthCheck() {
        String podId = System.getenv("HOSTNAME") != null ? System.getenv("HOSTNAME") : "localhost";
        
        try {
            // Get current pod's connection metrics only
            Map<String, Object> connectionStats = messageNotificationConsumer.getConnectionStats();
            int activeSubscriptions = messageNotificationConsumer.getActiveSubscriptionCount();
            
            // Extract local connection count from memory stats
            @SuppressWarnings("unchecked")
            Map<String, Object> localMemory = (Map<String, Object>) connectionStats.get("localMemory");
            int currentPodConnections = localMemory != null ? 
                (Integer) localMemory.getOrDefault("totalUsers", 0) : 0;
            
            // Use active subscriptions as more accurate current load
            int actualLoad = Math.max(currentPodConnections, activeSubscriptions);
            
            // Determine load status based on current pod's load only
            String loadStatus;
            int httpStatus;
            double loadPercentage = (actualLoad / 6000.0) * 100; // Max capacity as reference
            
            if (actualLoad < 5000) {
                loadStatus = "low";
                httpStatus = 200; // OK - Preferred for routing
            } else if (actualLoad <= 6000) {
                loadStatus = "medium"; 
                httpStatus = 201; // Created - Secondary choice
            } else {
                loadStatus = "high";
                httpStatus = 202; // Accepted - Last resort
            }
            
            // Build response with current pod's metrics only
            Map<String, Object> health = Map.of(
                "status", "UP",
                "loadStatus", loadStatus,
                "podId", podId,
                "currentConnections", actualLoad,
                "activeSubscriptions", activeSubscriptions,
                "localUsers", currentPodConnections,
                "loadPercentage", Math.round(loadPercentage * 100.0) / 100.0,
                "timestamp", System.currentTimeMillis(),
                "capacity", Map.of(
                    "low", "< 5000",
                    "medium", "5000-6000", 
                    "high", "> 6000"
                )
            );
            
            // Create response with custom headers for NGINX
            return ResponseEntity.status(httpStatus)
                .header("X-Connection-Count", String.valueOf(actualLoad))
                .header("X-Load-Status", loadStatus)
                .header("X-Load-Percentage", String.valueOf(Math.round(loadPercentage)))
                .header("X-Pod-Capacity", "6000")
                .header("X-Pod-Id", podId)
                .body(health);
                
        } catch (Exception e) {
            logger.error("Load balance health check failed: {}", e.getMessage());
            
            Map<String, Object> health = Map.of(
                "podId", podId,
                "status", "DOWN",
                "loadStatus", "unknown",
                "timestamp", System.currentTimeMillis(),
                "error", e.getMessage()
            );
            
            // Return 503 Service Unavailable for failed health checks
            return ResponseEntity.status(503)
                .header("X-Load-Status", "error")
                .body(health);
        }
    }
}
