package com.kamruddin.reactive.controllers;

import com.kamruddin.reactive.models.MessageNotification;
import com.kamruddin.reactive.services.MessageNotificationConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

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

    /**
     * Reactive SSE endpoint for user-specific message notifications
     * Returns Flux<ServerSentEvent> for reactive streaming
     */
    @GetMapping(value = "/user/{userId}/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<MessageNotification>> streamNotifications(
            @PathVariable Long userId) {

        logger.info("Client connecting to reactive SSE stream for user: {}", userId);
//        Long id, String type, String message, String notificationTime
        Flux<ServerSentEvent<MessageNotification>> heartBeat = Flux.interval(Duration.ofSeconds(15))
                .map(tick -> ServerSentEvent.<MessageNotification>builder()
                        .id(String.valueOf(System.currentTimeMillis()))
                        .event("heartbeat")
                        .comment("ping")
                        .data(new MessageNotification(100L, "heartbeat", "ping to keep connection alive",
                                LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)))
                        .build());

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
            }).mergeWith(heartBeat);
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
     * Health check endpoint
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        try {
            // Check Redis connectivity and other dependencies
            Map<String, Object> stats = messageNotificationConsumer.getConnectionStats();

            Map<String, Object> health = Map.of(
                    "status", "UP",
                    "service", "ReactiveNotificationController",
                    "timestamp", System.currentTimeMillis(),
                    "environment", "GKE",
                    "activeConnections", stats.getOrDefault("activeConnections", 0),
                    "redisConnected", stats.containsKey("redisHost")
            );
            return ResponseEntity.ok(health);
        } catch (Exception e) {
            logger.error("Health check failed: {}", e.getMessage());
            Map<String, Object> health = Map.of(
                    "status", "DOWN",
                    "service", "ReactiveNotificationController",
                    "timestamp", System.currentTimeMillis(),
                    "error", e.getMessage()
            );
            return ResponseEntity.status(503).body(health);
        }
    }

    /**
     * Readiness probe endpoint for Kubernetes
     */
    @GetMapping("/ready")
    public ResponseEntity<Map<String, Object>> readinessCheck() {
        try {
            // Verify service dependencies are ready
            Map<String, Object> stats = messageNotificationConsumer.getConnectionStats();
            boolean isReady = stats.containsKey("redisHost");

            if (isReady) {
                return ResponseEntity.ok(Map.of(
                    "status", "READY",
                    "timestamp", System.currentTimeMillis()
                ));
            } else {
                return ResponseEntity.status(503).body(Map.of(
                    "status", "NOT_READY",
                    "timestamp", System.currentTimeMillis()
                ));
            }
        } catch (Exception e) {
            return ResponseEntity.status(503).body(Map.of(
                "status", "NOT_READY",
                "error", e.getMessage(),
                "timestamp", System.currentTimeMillis()
            ));
        }
    }
}
