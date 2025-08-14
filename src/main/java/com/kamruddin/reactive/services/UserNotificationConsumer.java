//package com.kamruddin.reactive.services;
//
//import com.kamruddin.reactive.models.MessageNotification;
//import jakarta.annotation.PostConstruct;
//import org.bson.Document;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.http.codec.ServerSentEvent;
//import org.springframework.stereotype.Service;
//import reactor.core.publisher.Flux;
//import reactor.core.publisher.Sinks;
//
//import java.time.LocalDateTime;
//import java.time.format.DateTimeFormatter;
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.atomic.AtomicBoolean;
//import java.util.function.Consumer;
//
//@Service
//public class UserNotificationConsumer implements Consumer<Document> {
//
//    private static final Logger logger = LoggerFactory.getLogger(UserNotificationConsumer.class);
//
//    @Autowired
//    private MongoStreamEvents mongoStreamEvents;
//
//    // Map to store user-specific Flux sinks
//    private final Map<Long, Sinks.Many<ServerSentEvent<MessageNotification>>> userSinks = new ConcurrentHashMap<>();
//
//    // Track termination state to prevent duplicate logs
//    private final Map<Long, AtomicBoolean> userTerminationState = new ConcurrentHashMap<>();
//
//    @PostConstruct
//    public void initialize() {
//        logger.info("Initializing reactive user notification system");
//
//        // Subscribe to MongoDB document events via Redis pub/sub
//        mongoStreamEvents.subscribeToDocumentEvents("messages")
//            .doOnNext(doc -> logger.debug("Received document event: {}", doc.toJson()))
//            .filter(this::isValidMessageDocument)
//            .map(this::documentToMessageNotification)
//            .filter(notification -> notification.getUserId() != null)
//            .subscribe(
//                this::routeNotificationToUser,
//                error -> logger.error("Error in notification stream: {}", error.getMessage(), error),
//                () -> logger.info("Notification stream completed")
//            );
//    }
//
//    /**
//     * Legacy accept method for backward compatibility
//     */
//    @Override
//    public void accept(Document document) {
//        logger.debug("Received notification via legacy consumer: {}", document.toJson());
//        // This will be handled by the reactive stream above
//    }
//
//    /**
//     * Create user-specific Flux stream for SSE
//     */
//    public Flux<ServerSentEvent<MessageNotification>> createUserStream(Long userId) {
//        logger.info("Creating SSE stream for user: {}", userId);
//
//        // Reset termination state for this user
//        userTerminationState.put(userId, new AtomicBoolean(false));
//
//        // Create or get existing sink for this user
//        Sinks.Many<ServerSentEvent<MessageNotification>> sink = userSinks.computeIfAbsent(userId,
//            k -> Sinks.many().multicast().onBackpressureBuffer());
//
//        return sink.asFlux()
//            .doOnSubscribe(subscription -> {
//                logger.info("User {} subscribed to notification stream", userId);
//                // Send initial connection event
//                sendConnectionEvent(userId);
//            })
//            .doOnCancel(() -> {
//                AtomicBoolean terminated = userTerminationState.get(userId);
//                if (terminated != null && terminated.compareAndSet(false, true)) {
//                    logger.info("User {} cancelled notification stream", userId);
//                    cleanupUserSink(userId);
//                }
//            })
//            .doOnTerminate(() -> {
//                AtomicBoolean terminated = userTerminationState.get(userId);
//                if (terminated != null && terminated.compareAndSet(false, true)) {
//                    logger.info("User {} notification stream terminated", userId);
//                    cleanupUserSink(userId);
//                }
//            })
//            .doFinally(signalType -> {
//                // Clean up termination state when stream is completely done
//                userTerminationState.remove(userId);
//            })
//            .onErrorResume(error -> {
//                logger.error("Error in user {} stream: {}", userId, error.getMessage());
//                return Flux.empty();
//            });
//    }
//
//    /**
//     * Route notification to specific user's sink
//     */
//    private void routeNotificationToUser(MessageNotification notification) {
//        Long userId = notification.getUserId();
//        Sinks.Many<ServerSentEvent<MessageNotification>> sink = userSinks.get(userId);
//
//        if (sink != null) {
//            ServerSentEvent<MessageNotification> event = ServerSentEvent.<MessageNotification>builder()
//                    .event("new-message")
//                    .id(String.valueOf(System.currentTimeMillis()))
//                    .data(notification)
//                    .build();
//
//            Sinks.EmitResult result = sink.tryEmitNext(event);
//
//            if (result.isSuccess()) {
//                logger.debug("Sent notification to user {}: {}", userId, notification.getMessage());
//            } else {
//                logger.warn("Failed to send notification to user {}: {}", userId, result);
//                if (result == Sinks.EmitResult.FAIL_NON_SERIALIZED) {
//                    // Clean up failed sink
//                    cleanupUserSink(userId);
//                }
//            }
//        } else {
//            logger.debug("No active stream for user {}, notification not sent", userId);
//        }
//    }
//
//    /**
//     * Send initial connection confirmation event
//     */
//    private void sendConnectionEvent(Long userId) {
//        Sinks.Many<ServerSentEvent<MessageNotification>> sink = userSinks.get(userId);
//        if (sink != null) {
//            MessageNotification connectionNotification = new MessageNotification();
//
//            // Set all required fields to avoid null values in logs
//            connectionNotification.setId(System.currentTimeMillis()); // Generate unique ID
//            connectionNotification.setType("connection");
//            connectionNotification.setMessage("Connected to real-time notifications");
//            connectionNotification.setUserId(userId);
//            connectionNotification.setTimestamp(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
//            connectionNotification.setSeverity("INFO"); // Set appropriate severity
//            connectionNotification.setSource("SSE_CONNECTION"); // Set source identifier
//            connectionNotification.setNotificationTime(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)); // Set notification time
//
//            ServerSentEvent<MessageNotification> event = ServerSentEvent.<MessageNotification>builder()
//                    .event("connection-established")
//                    .id(String.valueOf(System.currentTimeMillis()))
//                    .data(connectionNotification)
//                    .build();
//
//            sink.tryEmitNext(event);
//        }
//    }
//
//    /**
//     * Cleanup user sink when stream ends
//     */
//    private void cleanupUserSink(Long userId) {
//        Sinks.Many<ServerSentEvent<MessageNotification>> sink = userSinks.remove(userId);
//        if (sink != null) {
//            sink.tryEmitComplete();
//            logger.info("Cleaned up notification sink for user: {}", userId);
//        }
//    }
//
//    /**
//     * Check if document is a valid message document
//     */
//    private boolean isValidMessageDocument(Document document) {
//        return document.containsKey("user_id") &&
//               document.containsKey("message") &&
//               document.containsKey("type");
//    }
//
//    /**
//     * Convert MongoDB document to MessageNotification
//     */
//    private MessageNotification documentToMessageNotification(Document document) {
//        MessageNotification notification = new MessageNotification();
//
//        try {
//            // Extract fields from document
//            notification.setId(document.getLong("id"));
//            notification.setType(document.getString("type"));
//            notification.setMessage(document.getString("message"));
//            notification.setSeverity(document.getString("severity"));
//            notification.setSource(document.getString("source"));
//
//            // Handle userId - could be Long or String
//            Object userIdObj = document.get("user_id");
//            if (userIdObj instanceof Number) {
//                notification.setUserId(((Number) userIdObj).longValue());
//            } else if (userIdObj instanceof String) {
//                notification.setUserId(Long.parseLong((String) userIdObj));
//            }
//
//            // Handle timestamp
//            Object timestampObj = document.get("timestamp");
//            if (timestampObj != null) {
//                notification.setTimestamp(timestampObj.toString());
//            }
//
//            notification.setNotificationTime(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
//
//        } catch (Exception e) {
//            logger.error("Error converting document to notification: {}", e.getMessage(), e);
//        }
//
//        return notification;
//    }
//
//    /**
//     * Get connection statistics
//     */
//    public Map<String, Object> getConnectionStats() {
//        Map<String, Object> stats = new ConcurrentHashMap<>();
//        stats.put("totalUsers", userSinks.size());
//        stats.put("activeStreams", userSinks.entrySet().stream()
//                .collect(java.util.stream.Collectors.toMap(
//                        e -> e.getKey().toString(),
//                        e -> e.getValue().currentSubscriberCount() > 0
//                )));
//        return stats;
//    }
//}
