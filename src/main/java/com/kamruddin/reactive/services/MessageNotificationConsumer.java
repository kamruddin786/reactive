package com.kamruddin.reactive.services;

import com.kamruddin.reactive.models.Message;
import com.kamruddin.reactive.models.MessageNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@Service
public class MessageNotificationConsumer implements Consumer<Message> {

    private static final Logger logger = LoggerFactory.getLogger(MessageNotificationConsumer.class);

    // Map to store user-specific Flux sinks
    private final Map<Long, Sinks.Many<ServerSentEvent<MessageNotification>>> userSinks = new ConcurrentHashMap<>();
    // Track termination state to prevent duplicate logs
    private final Map<Long, AtomicBoolean> userTerminationState = new ConcurrentHashMap<>();

    @Override
    public void accept(Message message) {
        logger.info("Processing user message - ID: {}, Type: {}, User: {}, Severity: {}",
                message.getId(), message.getType(), message.getUserId(), message.getSeverity());
        routeNotificationToUser(messageToMessageNotification(message));
        logger.debug("Message processed and routed to user: {}", message.getUserId());
    }

    @Override
    public Consumer<Message> andThen(Consumer<? super Message> after) {
        return Consumer.super.andThen(after);
    }

    public Flux<ServerSentEvent<MessageNotification>> createUserStream(Long userId) {
        logger.info("Creating SSE stream for user: {}", userId);

        // Reset termination state for this user
        userTerminationState.put(userId, new AtomicBoolean(false));

        // Create or get existing sink for this user
        Sinks.Many<ServerSentEvent<MessageNotification>> sink = userSinks.computeIfAbsent(userId,
                k -> Sinks.many().multicast().onBackpressureBuffer());

        return sink.asFlux()
                .doOnSubscribe(subscription -> {
                    logger.info("User {} subscribed to notification stream", userId);
                    // Send initial connection event
                    sendConnectionEvent(userId);
                })
                .doOnCancel(() -> {
                    AtomicBoolean terminated = userTerminationState.get(userId);
                    if (terminated != null && terminated.compareAndSet(false, true)) {
                        logger.info("User {} cancelled notification stream", userId);
                        cleanupUserSink(userId);
                    }
                })
                .doOnTerminate(() -> {
                    AtomicBoolean terminated = userTerminationState.get(userId);
                    if (terminated != null && terminated.compareAndSet(false, true)) {
                        logger.info("User {} notification stream terminated", userId);
                        cleanupUserSink(userId);
                    }
                })
                .doFinally(signalType -> {
                    // Clean up termination state when stream is completely done
                    userTerminationState.remove(userId);
                })
                .onErrorResume(error -> {
                    logger.error("Error in user {} stream: {}", userId, error.getMessage());
                    return Flux.empty();
                });
    }

    /**
     * Send initial connection confirmation event
     */
    private void sendConnectionEvent(Long userId) {
        Sinks.Many<ServerSentEvent<MessageNotification>> sink = userSinks.get(userId);
        if (sink != null) {
            MessageNotification connectionNotification = new MessageNotification();

            // Set all required fields to avoid null values in logs
            connectionNotification.setId(System.currentTimeMillis()); // Generate unique ID
            connectionNotification.setType("connection");
            connectionNotification.setMessage("Connected to real-time notifications");
            connectionNotification.setUserId(userId);
            connectionNotification.setTimestamp(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            connectionNotification.setSeverity("INFO"); // Set appropriate severity
            connectionNotification.setSource("SSE_CONNECTION"); // Set source identifier
            connectionNotification.setNotificationTime(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)); // Set notification time

            ServerSentEvent<MessageNotification> event = ServerSentEvent.<MessageNotification>builder()
                    .event("connection-established")
                    .id(String.valueOf(System.currentTimeMillis()))
                    .data(connectionNotification)
                    .build();

            sink.tryEmitNext(event);
        }
    }

    /**
     * Cleanup user sink when stream ends
     */
    private void cleanupUserSink(Long userId) {
        Sinks.Many<ServerSentEvent<MessageNotification>> sink = userSinks.remove(userId);
        if (sink != null) {
            sink.tryEmitComplete();
            logger.info("Cleaned up notification sink for user: {}", userId);
        }
    }

    /**
     * Route notification to specific user's sink
     */
    private void routeNotificationToUser(MessageNotification notification) {
        Long userId = notification.getUserId();
        Sinks.Many<ServerSentEvent<MessageNotification>> sink = userSinks.get(userId);

        if (sink != null) {
            logger.info("Current subscriber count for user {}: {}", userId, sink.currentSubscriberCount());
            ServerSentEvent<MessageNotification> event = ServerSentEvent.<MessageNotification>builder()
                    .event("new-message")
                    .id(String.valueOf(System.currentTimeMillis()))
                    .data(notification)
                    .build();

            Sinks.EmitResult result = sink.tryEmitNext(event);

            if (result.isSuccess()) {
                logger.debug("Sent notification to user {}: {}", userId, notification.getMessage());
            } else {
                logger.warn("Failed to send notification to user {}: {}", userId, result);
                if (result == Sinks.EmitResult.FAIL_NON_SERIALIZED) {
                    // Clean up failed sink
                    cleanupUserSink(userId);
                }
            }
        } else {
            logger.debug("No active stream for user {}, notification not sent", userId);
        }
    }

    private MessageNotification messageToMessageNotification(Message message) {
        MessageNotification notification = new MessageNotification();

        try {
            // Extract fields from document
            notification.setId(message.getId());
            notification.setType(message.getType());
            notification.setMessage(message.getMessage());
            notification.setSeverity(message.getSeverity());
            notification.setSource(message.getSource());
            notification.setUserId(message.getUserId());

            // Handle timestamp
            Object timestampObj = message.getTimestamp();
            if (timestampObj != null) {
                notification.setTimestamp(timestampObj.toString());
            }
            notification.setPodId(getHostname()); // Assuming getHostname() returns the pod ID
            notification.setNotificationTime(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

        } catch (Exception e) {
            logger.error("Error converting document to notification: {}", e.getMessage(), e);
        }

        return notification;
    }

    private String getHostname() {
        try {
            return System.getenv("HOSTNAME") != null ? System.getenv("HOSTNAME") : java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            logger.error("Failed to get hostname: {}", e.getMessage(), e);
            return "unknown-host";
        }
    }

    /**
     * Get connection statistics
     */
    public Map<String, Object> getConnectionStats() {
        Map<String, Object> stats = new ConcurrentHashMap<>();
        stats.put("totalUsers", userSinks.size());
        stats.put("activeStreams", userSinks.entrySet().stream()
                .collect(java.util.stream.Collectors.toMap(
                        e -> e.getKey().toString(),
                        e -> e.getValue().currentSubscriberCount() > 0
                )));
        return stats;
    }
}
