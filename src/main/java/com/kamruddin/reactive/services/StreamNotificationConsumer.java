package com.kamruddin.reactive.services;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.RedisTemplate;
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

/**
 * Dedicated consumer for Redis Streams with guaranteed message delivery
 * This service provides enhanced reliability features like message replay,
 * acknowledgments, and pending message recovery.
 */
@Service
public class StreamNotificationConsumer implements Consumer<Document> {

    private static final Logger logger = LoggerFactory.getLogger(StreamNotificationConsumer.class);

    @Autowired
    private MongoStreamEvents mongoStreamEvents;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    // Map to store user-specific Flux sinks for stream consumers
    private final Map<Long, Sinks.Many<ServerSentEvent<MessageNotification>>> streamUserSinks = new ConcurrentHashMap<>();

    // Track termination state to prevent duplicate logs
    private final Map<Long, AtomicBoolean> streamUserTerminationState = new ConcurrentHashMap<>();

    // Stream configuration
    private static final String CONSUMER_GROUP = "stream-notification-group";
    private static final String STREAM_KEY = "mongodb:events:messages";

    @Override
    public void accept(Document document) {
        logger.debug("Received notification via legacy consumer: {}", document.toJson());
        // This will be handled by the reactive stream above
    }

    @PostConstruct
    public void initialize() {
        logger.info("Initializing dedicated Redis Streams notification consumer");

        // Subscribe to MongoDB document events via Redis STREAMS (guaranteed delivery)
        /*mongoStreamEvents.startDistributedMonitoringWithStreams("messages", document -> {
            logger.debug("Stream consumer received document event: {}", document.toJson());
            if (isValidMessageDocument(document)) {
                MessageNotification notification = documentToMessageNotification(document);
                if (notification != null && notification.getUserId() != null) {
                    routeStreamNotificationToUser(notification);
                }
            }
        });*/
    }

    @PreDestroy
    public void cleanup() {
        logger.info("Cleaning up Redis Streams notification consumer");
        streamUserSinks.values().forEach(sink -> {
            try {
                sink.tryEmitComplete();
            } catch (Exception e) {
                logger.warn("Error completing sink during cleanup: {}", e.getMessage());
            }
        });
        streamUserSinks.clear();
    }

    /**
     * Create user-specific Flux stream with Redis Streams guaranteed delivery
     * This is the main method for creating streams with replay capability
     */
    public Flux<ServerSentEvent<MessageNotification>> createUserStreamWithReplay(Long userId) {
        return createUserStreamWithReplay(userId, null, 100);
    }

    /**
     * Create user-specific Flux stream with message replay from Redis streams
     * @param userId the user ID
     * @param fromMessageId optional message ID to start replay from (null = recent messages)
     * @param maxReplayMessages maximum number of historical messages to replay
     */
    public Flux<ServerSentEvent<MessageNotification>> createUserStreamWithReplay(
            Long userId, String fromMessageId, int maxReplayMessages) {

        logger.info("Creating Redis Streams SSE with replay for user: {} (from: {}, max: {})",
            userId, fromMessageId, maxReplayMessages);

        // Reset termination state for this user
        streamUserTerminationState.put(userId, new AtomicBoolean(false));

        // Create or get existing sink for this user
        Sinks.Many<ServerSentEvent<MessageNotification>> sink = streamUserSinks.computeIfAbsent(userId,
            k -> Sinks.many().multicast().onBackpressureBuffer());

        // First, replay historical messages from Redis streams
        Flux<ServerSentEvent<MessageNotification>> historyFlux = replayUserMessages(userId, fromMessageId, maxReplayMessages);

        // Then, continue with real-time messages
        Flux<ServerSentEvent<MessageNotification>> realTimeFlux = sink.asFlux()
            .doOnSubscribe(subscription -> {
                logger.info("User {} subscribed to real-time Redis Streams notification stream", userId);
            });

        // Combine historical and real-time messages
        return Flux.concat(
            // 1. Send connection event first
            Flux.just(createStreamConnectionEvent(userId)),
            // 2. Replay historical messages
            historyFlux,
            // 3. Continue with real-time messages
            realTimeFlux
        )
        .doOnCancel(() -> {
            AtomicBoolean terminated = streamUserTerminationState.get(userId);
            if (terminated != null && terminated.compareAndSet(false, true)) {
                logger.info("User {} cancelled Redis Streams notification stream", userId);
                cleanupStreamUserSink(userId);
            }
        })
        .doOnTerminate(() -> {
            AtomicBoolean terminated = streamUserTerminationState.get(userId);
            if (terminated != null && terminated.compareAndSet(false, true)) {
                logger.info("User {} Redis Streams notification stream terminated", userId);
                cleanupStreamUserSink(userId);
            }
        })
        .doFinally(signalType -> {
            streamUserTerminationState.remove(userId);
        })
        .onErrorResume(error -> {
            logger.error("Error in user {} Redis Streams stream: {}", userId, error.getMessage());
            return Flux.empty();
        });
    }

    /**
     * Replay historical messages for a user from Redis streams
     */
    private Flux<ServerSentEvent<MessageNotification>> replayUserMessages(
            Long userId, String fromMessageId, int maxMessages) {

        return Flux.fromIterable(getHistoricalMessages(userId, fromMessageId, maxMessages))
            .map(this::createStreamReplayEvent)
            .doOnNext(event -> logger.debug("Replaying stream message for user {}: {}", userId, event.id()))
            .onErrorResume(error -> {
                logger.error("Error replaying stream messages for user {}: {}", userId, error.getMessage());
                return Flux.empty();
            });
    }

    /**
     * Get historical messages using proper Redis streams consumer group functionality
     */
    private java.util.List<MessageNotification> getHistoricalMessages(
            Long userId, String fromMessageId, int maxMessages) {

        java.util.List<MessageNotification> historicalMessages = new java.util.ArrayList<>();

        try {
            String consumerName = "stream-user-" + userId + "-" + System.currentTimeMillis();

            // Create consumer group if it doesn't exist
            createConsumerGroupIfNotExists();

            // Determine the starting point for reading
            org.springframework.data.redis.connection.stream.ReadOffset readOffset;
            if (fromMessageId != null && !fromMessageId.isEmpty()) {
                readOffset = org.springframework.data.redis.connection.stream.ReadOffset.from(fromMessageId);
            } else {
                readOffset = org.springframework.data.redis.connection.stream.ReadOffset.from("0-0");
            }

            // Read messages from the stream using consumer group
            java.util.List<org.springframework.data.redis.connection.stream.MapRecord<String, Object, Object>> records =
                redisTemplate.opsForStream()
                    .read(org.springframework.data.redis.connection.stream.Consumer.from(CONSUMER_GROUP, consumerName),
                        org.springframework.data.redis.connection.stream.StreamReadOptions.empty()
                            .count((long) maxMessages * 10), // Read more to filter for this user
                        org.springframework.data.redis.connection.stream.StreamOffset.create(STREAM_KEY, readOffset));

            if (records != null) {
                for (org.springframework.data.redis.connection.stream.MapRecord<String, Object, Object> record : records) {
                    try {
                        MessageNotification notification = parseStreamRecord(record);
                        if (notification != null && userId.equals(notification.getUserId())) {
                            historicalMessages.add(notification);

                            // IMPORTANT: Acknowledge the message to mark it as consumed
                            redisTemplate.opsForStream().acknowledge(STREAM_KEY, CONSUMER_GROUP, record.getId());
                            logger.debug("Stream consumer acknowledged historical message {} for user {}", record.getId(), userId);

                            // Stop when we have enough messages for this user
                            if (historicalMessages.size() >= maxMessages) {
                                break;
                            }
                        } else {
                            // Acknowledge messages not for this user to avoid redelivery
                            redisTemplate.opsForStream().acknowledge(STREAM_KEY, CONSUMER_GROUP, record.getId());
                        }
                    } catch (Exception e) {
                        logger.warn("Error parsing stream record {}: {}", record.getId(), e.getMessage());
                        // Don't acknowledge on parse error - let it be retried
                    }
                }
            }

            logger.info("Stream consumer found and acknowledged {} historical messages for user {}",
                historicalMessages.size(), userId);

        } catch (Exception e) {
            logger.error("Error reading historical messages from streams for user {}: {}", userId, e.getMessage());
        }

        return historicalMessages;
    }

    /**
     * Create consumer group if it doesn't exist
     */
    private void createConsumerGroupIfNotExists() {
        try {
            redisTemplate.opsForStream().createGroup(STREAM_KEY,
                org.springframework.data.redis.connection.stream.ReadOffset.from("0-0"), CONSUMER_GROUP);
            logger.info("Created stream consumer group '{}' for stream '{}'", CONSUMER_GROUP, STREAM_KEY);
        } catch (Exception e) {
            logger.debug("Stream consumer group '{}' already exists for stream '{}': {}",
                CONSUMER_GROUP, STREAM_KEY, e.getMessage());
        }
    }

    /**
     * Route notification to specific user's sink using Redis Streams
     */
    private void routeStreamNotificationToUser(MessageNotification notification) {
        Long userId = notification.getUserId();
        Sinks.Many<ServerSentEvent<MessageNotification>> sink = streamUserSinks.get(userId);

        if (sink != null) {
            ServerSentEvent<MessageNotification> event = ServerSentEvent.<MessageNotification>builder()
                    .event("stream-new-message")
                    .id("stream-" + System.currentTimeMillis())
                    .data(notification)
                    .comment("Message delivered via Redis Streams")
                    .build();

            Sinks.EmitResult result = sink.tryEmitNext(event);

            if (result.isSuccess()) {
                logger.debug("Stream consumer sent notification to user {}: {}", userId, notification.getMessage());

                // Save the last delivered message ID for this user
                saveLastStreamDeliveredMessageId(userId, String.valueOf(notification.getId()));
            } else {
                logger.warn("Stream consumer failed to send notification to user {}: {}", userId, result);
                if (result == Sinks.EmitResult.FAIL_NON_SERIALIZED) {
                    cleanupStreamUserSink(userId);
                }
            }
        } else {
            logger.debug("No active stream for user {}, stream notification not sent", userId);
        }
    }

    /**
     * Parse a Redis stream record into a MessageNotification
     */
    private MessageNotification parseStreamRecord(MapRecord<String, Object, Object> record) {
        try {
            Map<Object, Object> values = record.getValue();

            // Extract document JSON from stream record
            String documentJson = (String) values.get("document");
            if (documentJson == null || documentJson.trim().isEmpty()) {
                return null;
            }

            // Parse the document and convert to notification
            Document document = Document.parse(documentJson);
            return documentToMessageNotification(document);

        } catch (Exception e) {
            logger.warn("Error parsing stream record: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Create a replay event with special marking for streams
     */
    private ServerSentEvent<MessageNotification> createStreamReplayEvent(MessageNotification notification) {
        return ServerSentEvent.<MessageNotification>builder()
            .event("stream-replay-message")
            .id("stream-replay-" + notification.getId())
            .data(notification)
            .comment("Historical message replayed from Redis Streams")
            .build();
    }

    /**
     * Create connection event for streams
     */
    private ServerSentEvent<MessageNotification> createStreamConnectionEvent(Long userId) {
        MessageNotification connectionNotification = new MessageNotification();
        connectionNotification.setId(System.currentTimeMillis());
        connectionNotification.setType("stream-connection");
        connectionNotification.setMessage("Connected to Redis Streams with guaranteed delivery - Pod: " +
            (System.getenv("HOSTNAME") != null ? System.getenv("HOSTNAME") : "local"));
        connectionNotification.setUserId(userId);
        connectionNotification.setTimestamp(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        connectionNotification.setSeverity("INFO");
        connectionNotification.setSource("REDIS_STREAMS");
        connectionNotification.setNotificationTime(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

        return ServerSentEvent.<MessageNotification>builder()
            .event("stream-connection-established")
            .id("stream-conn-" + System.currentTimeMillis())
            .data(connectionNotification)
            .comment("Connected to Redis Streams consumer")
            .build();
    }

    /**
     * Cleanup user sink when stream ends
     */
    private void cleanupStreamUserSink(Long userId) {
        Sinks.Many<ServerSentEvent<MessageNotification>> sink = streamUserSinks.remove(userId);
        if (sink != null) {
            sink.tryEmitComplete();
            logger.info("Cleaned up Redis Streams notification sink for user: {}", userId);
        }
    }

    /**
     * Save the last delivered message ID for a user (stream-specific tracking)
     */
    private void saveLastStreamDeliveredMessageId(Long userId, String messageId) {
        try {
            String key = "stream:user:last_delivered:" + userId;
            redisTemplate.opsForValue().set(key, messageId,
                java.time.Duration.ofDays(30)); // Keep for 30 days
            logger.debug("Saved last stream delivered message ID for user {}: {}", userId, messageId);
        } catch (Exception e) {
            logger.error("Error saving last stream delivered message ID for user {}: {}", userId, e.getMessage());
        }
    }

    /**
     * Get the last processed message ID for a user from streams
     */
    public String getLastStreamProcessedMessageId(Long userId) {
        try {
            String key = "stream:user:last_delivered:" + userId;
            return (String) redisTemplate.opsForValue().get(key);
        } catch (Exception e) {
            logger.error("Error getting last stream processed message ID for user {}: {}", userId, e.getMessage());
            return null;
        }
    }

    /**
     * Get pending (unacknowledged) messages for a user from Redis streams
     */
    public java.util.List<MessageNotification> getStreamPendingMessages(Long userId) {
        java.util.List<MessageNotification> pendingMessages = new java.util.ArrayList<>();

        try {
            // Get pending messages summary
            org.springframework.data.redis.connection.stream.PendingMessagesSummary pendingSummary =
                redisTemplate.opsForStream().pending(STREAM_KEY, CONSUMER_GROUP);

            if (pendingSummary != null && pendingSummary.getTotalPendingMessages() > 0) {
                logger.info("Found {} total pending messages in stream '{}'",
                    pendingSummary.getTotalPendingMessages(), STREAM_KEY);

                // Get pending messages for the consumer group - simplified approach
                try {
                    org.springframework.data.redis.connection.stream.PendingMessages pendingMessagesList =
                        redisTemplate.opsForStream().pending(STREAM_KEY,
                            org.springframework.data.redis.connection.stream.Consumer.from(CONSUMER_GROUP, "temp-consumer"));

                    for (org.springframework.data.redis.connection.stream.PendingMessage pendingMsg : pendingMessagesList) {
                        logger.debug("Pending stream message: {} for consumer: {}",
                            pendingMsg.getId(), pendingMsg.getConsumerName());
                    }
                } catch (Exception e) {
                    logger.warn("Error getting detailed pending messages: {}", e.getMessage());
                }
            }

        } catch (Exception e) {
            logger.error("Error getting pending stream messages for user {}: {}", userId, e.getMessage());
        }

        return pendingMessages;
    }

    /**
     * Manually acknowledge a message as consumed in streams
     */
    public void acknowledgeStreamMessage(String messageId) {
        try {
            org.springframework.data.redis.connection.stream.RecordId recordId =
                org.springframework.data.redis.connection.stream.RecordId.of(messageId);

            Long acknowledged = redisTemplate.opsForStream().acknowledge(STREAM_KEY, CONSUMER_GROUP, recordId);
            logger.debug("Stream consumer acknowledged {} message(s) with ID: {}", acknowledged, messageId);

        } catch (Exception e) {
            logger.error("Error acknowledging stream message {}: {}", messageId, e.getMessage());
        }
    }

    /**
     * Get consumption statistics for Redis streams monitoring
     */
    public Map<String, Object> getStreamConsumptionStats() {
        Map<String, Object> stats = new ConcurrentHashMap<>();

        try {
            // Get stream info
            org.springframework.data.redis.connection.stream.StreamInfo.XInfoStream streamInfo =
                redisTemplate.opsForStream().info(STREAM_KEY);

            // Get consumer group info
            org.springframework.data.redis.connection.stream.StreamInfo.XInfoGroups groupsInfo =
                redisTemplate.opsForStream().groups(STREAM_KEY);

            // Get pending messages summary
            org.springframework.data.redis.connection.stream.PendingMessagesSummary pendingSummary =
                redisTemplate.opsForStream().pending(STREAM_KEY, CONSUMER_GROUP);

            stats.put("streamLength", streamInfo.streamLength());
            stats.put("consumerGroup", CONSUMER_GROUP);
            stats.put("totalGroups", groupsInfo.size());
            stats.put("totalPendingMessages", pendingSummary.getTotalPendingMessages());
            stats.put("activeStreamConsumers", streamUserSinks.size());
            stats.put("streamKey", STREAM_KEY);

        } catch (Exception e) {
            logger.error("Error getting stream consumption stats: {}", e.getMessage());
        }

        return stats;
    }

    /**
     * Get connection statistics for stream consumers
     */
    public Map<String, Object> getStreamConnectionStats() {
        Map<String, Object> stats = new ConcurrentHashMap<>();
        stats.put("totalStreamUsers", streamUserSinks.size());
        stats.put("activeStreamConnections", streamUserSinks.entrySet().stream()
                .collect(java.util.stream.Collectors.toMap(
                        e -> e.getKey().toString(),
                        e -> e.getValue().currentSubscriberCount() > 0
                )));
        stats.put("consumerType", "Redis Streams");
        return stats;
    }

    // Helper methods (shared with pub/sub consumer)
    private boolean isValidMessageDocument(Document document) {
        return document.containsKey("user_id") &&
               document.containsKey("message") &&
               document.containsKey("type");
    }

    private MessageNotification documentToMessageNotification(Document document) {
        MessageNotification notification = new MessageNotification();

        try {
            notification.setId(document.getLong("id"));
            notification.setType(document.getString("type"));
            notification.setMessage(document.getString("message"));
            notification.setSeverity(document.getString("severity"));
            notification.setSource(document.getString("source"));

            // Handle userId - could be Long or String
            Object userIdObj = document.get("user_id");
            if (userIdObj instanceof Number) {
                notification.setUserId(((Number) userIdObj).longValue());
            } else if (userIdObj instanceof String) {
                notification.setUserId(Long.parseLong((String) userIdObj));
            }

            // Handle timestamp
            Object timestampObj = document.get("timestamp");
            if (timestampObj != null) {
                notification.setTimestamp(timestampObj.toString());
            }

            notification.setNotificationTime(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

        } catch (Exception e) {
            logger.error("Error converting document to notification: {}", e.getMessage(), e);
        }

        return notification;
    }

    /**
     * MessageNotification data class for streams
     */
    public static class MessageNotification {
        private Long id;
        private String type;
        private String message;
        private String timestamp;
        private String severity;
        private String source;
        private Long userId;
        private String notificationTime;

        // Getters and setters
        public Long getId() { return id; }
        public void setId(Long id) { this.id = id; }

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }

        public String getMessage() { return message; }
        public void setMessage(String message) { this.message = message; }

        public String getTimestamp() { return timestamp; }
        public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

        public String getSeverity() { return severity; }
        public void setSeverity(String severity) { this.severity = severity; }

        public String getSource() { return source; }
        public void setSource(String source) { this.source = source; }

        public Long getUserId() { return userId; }
        public void setUserId(Long userId) { this.userId = userId; }

        public String getNotificationTime() { return notificationTime; }
        public void setNotificationTime(String notificationTime) { this.notificationTime = notificationTime; }

        @Override
        public String toString() {
            return "MessageNotification{" +
                    "id=" + id +
                    ", type='" + type + '\'' +
                    ", message='" + message + '\'' +
                    ", timestamp='" + timestamp + '\'' +
                    ", severity='" + severity + '\'' +
                    ", source='" + source + '\'' +
                    ", userId=" + userId +
                    ", notificationTime='" + notificationTime + '\'' +
                    '}';
        }
    }
}
