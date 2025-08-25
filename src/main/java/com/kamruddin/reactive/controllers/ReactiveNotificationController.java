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
import reactor.util.retry.Retry;

import java.time.Duration;
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
                    "activeConnections", stats.getOrDefault("activeStreams", 0),
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
}
