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
            .doOnSubscribe(subscription ->
                logger.info("Started SSE stream for user: {}", userId))
            .doOnCancel(() ->
                logger.info("SSE stream cancelled for user: {}", userId))
            .doOnTerminate(() ->
                logger.info("SSE stream terminated for user: {}", userId))
            .onErrorResume(error -> {
                logger.error("Error in SSE stream for user {}: {}", userId, error.getMessage());
                return Flux.empty();
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
        Map<String, Object> health = Map.of(
                "status", "UP",
                "service", "ReactiveNotificationController",
                "timestamp", System.currentTimeMillis()
        );
        return ResponseEntity.ok(health);
    }


    // ========== REDIS STREAMS ENDPOINTS (Guaranteed Delivery) ==========

    /**
     * Redis Streams SSE endpoint with guaranteed message delivery and replay
     * This uses Redis Streams for reliable message delivery with acknowledgments
     */
//    @GetMapping(value = "/user/{userId}/streams-2", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
//    public Flux<ServerSentEvent<MessageNotification>> streamNotificationsViaStreams(
//            @PathVariable Long userId,
//            @RequestParam(value = "fromMessageId", required = false) String fromMessageId,
//            @RequestParam(value = "maxReplayMessages", defaultValue = "50") int maxReplayMessages) {
//
//        logger.info("Client connecting to Redis Streams SSE for user: {} (from: {}, max: {})",
//            userId, fromMessageId, maxReplayMessages);
//
//        return streamNotificationConsumer.createUserStreamWithReplay(userId, fromMessageId, maxReplayMessages)
//            .timeout(Duration.ofHours(1))
//            .doOnSubscribe(subscription ->
//                logger.info("Started Redis Streams SSE for user: {}", userId))
//            .doOnCancel(() ->
//                logger.info("Redis Streams SSE cancelled for user: {}", userId))
//            .doOnTerminate(() ->
//                logger.info("Redis Streams SSE terminated for user: {}", userId))
//            .onErrorResume(error -> {
//                logger.error("Error in Redis Streams SSE for user {}: {}", userId, error.getMessage());
//                return Flux.empty();
//            });
//    }

    /**
     * Get the last processed message ID for a user from Redis Streams
     */
//    @GetMapping("/user/{userId}/streams/last-message-id")
//    public ResponseEntity<Map<String, String>> getLastStreamMessageId(@PathVariable Long userId) {
//        String lastMessageId = streamNotificationConsumer.getLastStreamProcessedMessageId(userId);
//        Map<String, String> response = new HashMap<>();
//        response.put("userId", String.valueOf(userId));
//        response.put("lastStreamMessageId", lastMessageId);
//        response.put("source", "Redis Streams");
//        return ResponseEntity.ok(response);
//    }

    /**
     * Get Redis Streams consumption statistics
     */
//    @GetMapping("/streams/consumption-stats")
//    public ResponseEntity<Map<String, Object>> getStreamConsumptionStats() {
//        try {
//            Map<String, Object> stats = streamNotificationConsumer.getStreamConsumptionStats();
//            return ResponseEntity.ok(stats);
//        } catch (Exception e) {
//            logger.error("Error getting stream consumption stats: {}", e.getMessage());
//            return ResponseEntity.internalServerError().build();
//        }
//    }

    /**
     * Get Redis Streams connection statistics
     */
//    @GetMapping("/streams/stats")
//    public ResponseEntity<Map<String, Object>> getStreamConnectionStats() {
//        try {
//            Map<String, Object> stats = streamNotificationConsumer.getStreamConnectionStats();
//            return ResponseEntity.ok(stats);
//        } catch (Exception e) {
//            logger.error("Error getting stream connection stats: {}", e.getMessage());
//            return ResponseEntity.internalServerError().build();
//        }
//    }

    /**
     * Get pending messages for a specific user from Redis Streams
     */
//    @GetMapping("/user/{userId}/streams/pending-messages")
//    public ResponseEntity<Map<String, Object>> getStreamPendingMessages(@PathVariable Long userId) {
//        try {
//            java.util.List<MessageNotification> pendingMessages =
//                streamNotificationConsumer.getStreamPendingMessages(userId);
//
//            Map<String, Object> response = new HashMap<>();
//            response.put("userId", userId);
//            response.put("pendingCount", pendingMessages.size());
//            response.put("pendingMessages", pendingMessages);
//            response.put("source", "Redis Streams");
//
//            return ResponseEntity.ok(response);
//        } catch (Exception e) {
//            logger.error("Error getting stream pending messages for user {}: {}", userId, e.getMessage());
//            return ResponseEntity.internalServerError().build();
//        }
//    }

    /**
     * Manually acknowledge a Redis Streams message
     */
//    @PostMapping("/streams/acknowledge/{messageId}")
//    public ResponseEntity<Map<String, String>> acknowledgeStreamMessage(@PathVariable String messageId) {
//        try {
//            streamNotificationConsumer.acknowledgeStreamMessage(messageId);
//
//            Map<String, String> response = new HashMap<>();
//            response.put("status", "acknowledged");
//            response.put("messageId", messageId);
//            response.put("source", "Redis Streams");
//
//            return ResponseEntity.ok(response);
//        } catch (Exception e) {
//            logger.error("Error acknowledging stream message {}: {}", messageId, e.getMessage());
//            return ResponseEntity.badRequest().build();
//        }
//    }

    // ========== COMPARISON AND MONITORING ENDPOINTS ==========

    /**
     * Get comprehensive statistics comparing both pub/sub and streams
     */
//    @GetMapping("/all-stats")
//    public ResponseEntity<Map<String, Object>> getAllStats() {
//        try {
//            Map<String, Object> allStats = new HashMap<>();
//
//            // Pub/Sub stats
//            Map<String, Object> pubsubStats = userNotificationConsumer.getConnectionStats();
//
//            // Streams stats
//            Map<String, Object> streamStats = streamNotificationConsumer.getStreamConnectionStats();
//            Map<String, Object> streamConsumption = streamNotificationConsumer.getStreamConsumptionStats();
//
//            allStats.put("pubsub", Map.of(
//                "connections", pubsubStats,
//                "type", "Redis Pub/Sub (Fire-and-forget)"
//            ));
//
//            allStats.put("streams", Map.of(
//                "connections", streamStats,
//                "consumption", streamConsumption,
//                "type", "Redis Streams (Guaranteed delivery)"
//            ));
//
//            allStats.put("timestamp", System.currentTimeMillis());
//
//            return ResponseEntity.ok(allStats);
//        } catch (Exception e) {
//            logger.error("Error getting all stats: {}", e.getMessage());
//            return ResponseEntity.internalServerError().build();
//        }
//    }
}
