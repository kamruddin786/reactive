package com.kamruddin.reactive.services;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class RedisStreamService {
    private static final Logger logger = LoggerFactory.getLogger(RedisStreamService.class);

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    private final Map<String, AtomicBoolean> activeConsumers = new ConcurrentHashMap<>();
    private final String consumerGroup = "mongodb-events-group";
    private final String consumerName;

    // Stream configuration
    private static final int MAX_STREAM_LENGTH = 10000;
    private static final Duration CONSUMER_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);
    private static final int BATCH_SIZE = 10;

    public RedisStreamService() {
        // Use pod name or hostname as consumer name for uniqueness
        this.consumerName = getPodName() + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    @PostConstruct
    public void init() {
        logger.info("Initializing Redis Stream Service with consumer: {}", consumerName);
    }

    @PreDestroy
    public void cleanup() {
        logger.info("Cleaning up Redis Stream Service");
        activeConsumers.values().forEach(active -> active.set(false));
    }

    /**
     * Publish a document event to Redis stream with guaranteed persistence
     * @param streamKey the stream name (e.g., "mongodb:events:users")
     * @param documentData the document data to publish
     * @return the message ID
     */
    public String publishToStream(String streamKey, Map<String, Object> documentData) {
        try {
            // Convert all values to strings for Redis streams compatibility
            Map<String, String> messageData = new HashMap<>();

            for (Map.Entry<String, Object> entry : documentData.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                // Convert value to string, handling null values
                String stringValue;
                if (value == null) {
                    stringValue = "";
                } else if (value instanceof String) {
                    stringValue = (String) value;
                } else {
                    stringValue = value.toString();
                }
                messageData.put(key, stringValue);
            }

            // Add metadata as strings
            messageData.put("timestamp", String.valueOf(System.currentTimeMillis()));
            messageData.put("producer", getPodName());
            messageData.put("messageId", UUID.randomUUID().toString());

            // Publish to stream with automatic ID generation
            RecordId recordId = redisTemplate.opsForStream()
                .add(StreamRecords.newRecord()
                    .ofMap(messageData)
                    .withStreamKey(streamKey));

            // Trim stream to prevent unbounded growth
            redisTemplate.opsForStream().trim(streamKey, MAX_STREAM_LENGTH);

            String messageId = recordId != null ? recordId.getValue() : "unknown";
            logger.debug("Published message to stream '{}' with ID: {}", streamKey, messageId);
            return messageId;

        } catch (Exception e) {
            logger.error("Error publishing to Redis stream '{}': {}", streamKey, e.getMessage(), e);
            throw new RuntimeException("Failed to publish to Redis stream", e);
        }
    }

    /**
     * Subscribe to Redis stream with consumer group for guaranteed delivery
     * @param streamKey the stream name
     * @param messageProcessor function to process each message
     * @return Flux of processed messages
     */
    public Flux<MapRecord<String, Object, Object>> subscribeToStream(
            String streamKey,
            java.util.function.Function<MapRecord<String, Object, Object>, Boolean> messageProcessor) {

        return Flux.<MapRecord<String, Object, Object>>create(sink -> {
            try {
                // Create consumer group if it doesn't exist
                createConsumerGroupIfNotExists(streamKey);

                AtomicBoolean isActive = new AtomicBoolean(true);
                activeConsumers.put(streamKey + ":" + consumerName, isActive);

                // Start consuming messages
                while (isActive.get()) {
                    try {
                        // First, process any pending messages
                        processPendingMessages(streamKey, sink, messageProcessor);

                        // Then consume new messages
                        List<MapRecord<String, Object, Object>> messages = redisTemplate.opsForStream()
                            .read(Consumer.from(consumerGroup, consumerName),
                                StreamReadOptions.empty()
                                    .count(BATCH_SIZE)
                                    .block(POLL_TIMEOUT),
                                StreamOffset.create(streamKey, ReadOffset.lastConsumed()));

                        for (MapRecord<String, Object, Object> message : messages) {
                            try {
                                // Process the message
                                boolean processed = messageProcessor.apply(message);

                                if (processed) {
                                    // Acknowledge successful processing
                                    redisTemplate.opsForStream().acknowledge(streamKey, consumerGroup, message.getId());
                                    sink.next(message);
                                    logger.debug("Successfully processed and acknowledged message: {}", message.getId());
                                } else {
                                    logger.warn("Message processing failed, will retry: {}", message.getId());
                                }
                            } catch (Exception e) {
                                logger.error("Error processing message {}: {}", message.getId(), e.getMessage(), e);
                                // Don't acknowledge failed messages - they'll be retried
                            }
                        }

                    } catch (Exception e) {
                        if (isActive.get()) {
                            logger.error("Error in stream consumer for '{}': {}", streamKey, e.getMessage(), e);
                            // Brief pause before retrying
                            Thread.sleep(1000);
                        }
                    }
                }

            } catch (Exception e) {
                logger.error("Fatal error in stream subscription for '{}': {}", streamKey, e.getMessage(), e);
                sink.error(e);
            } finally {
                activeConsumers.remove(streamKey + ":" + consumerName);
                sink.complete();
            }
        })
        .subscribeOn(Schedulers.boundedElastic())
        .doOnCancel(() -> {
            AtomicBoolean isActive = activeConsumers.get(streamKey + ":" + consumerName);
            if (isActive != null) {
                isActive.set(false);
            }
        });
    }

    /**
     * Process any pending (unacknowledged) messages for this consumer
     */
    private void processPendingMessages(
            String streamKey,
            reactor.core.publisher.FluxSink<MapRecord<String, Object, Object>> sink,
            java.util.function.Function<MapRecord<String, Object, Object>, Boolean> messageProcessor) {

        try {
            // Get pending messages for this consumer
            PendingMessagesSummary pendingSummary = redisTemplate.opsForStream()
                .pending(streamKey, consumerGroup);

            if (pendingSummary.getTotalPendingMessages() > 0) {
                logger.info("Found {} pending messages for consumer '{}' in stream '{}'",
                    pendingSummary.getTotalPendingMessages(), consumerName, streamKey);

                // Claim old pending messages (older than timeout)
                List<MapRecord<String, Object, Object>> claimedMessages = redisTemplate.opsForStream()
                    .claim(streamKey, consumerGroup, consumerName, CONSUMER_TIMEOUT,
                        RecordId.of("0-0"));

                for (MapRecord<String, Object, Object> message : claimedMessages) {
                    try {
                        boolean processed = messageProcessor.apply(message);
                        if (processed) {
                            redisTemplate.opsForStream().acknowledge(streamKey, consumerGroup, message.getId());
                            sink.next(message);
                            logger.info("Successfully reprocessed pending message: {}", message.getId());
                        }
                    } catch (Exception e) {
                        logger.error("Error reprocessing pending message {}: {}", message.getId(), e.getMessage(), e);
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("Error processing pending messages for stream '{}': {}", streamKey, e.getMessage());
        }
    }

    /**
     * Create consumer group if it doesn't exist
     */
    private void createConsumerGroupIfNotExists(String streamKey) {
        try {
            redisTemplate.opsForStream().createGroup(streamKey, consumerGroup);
            logger.info("Created consumer group '{}' for stream '{}'", consumerGroup, streamKey);
        } catch (Exception e) {
            // Group likely already exists
            logger.debug("Consumer group '{}' already exists for stream '{}'", consumerGroup, streamKey);
        }
    }

    /**
     * Get consumer group information
     */
    public void logConsumerGroupInfo(String streamKey) {
        try {
            StreamInfo.XInfoGroups groups = redisTemplate.opsForStream().groups(streamKey);
            logger.info("Consumer groups for stream '{}': {}", streamKey, groups);

            StreamInfo.XInfoConsumers consumers = redisTemplate.opsForStream()
                .consumers(streamKey, consumerGroup);
            logger.info("Consumers in group '{}': {}", consumerGroup, consumers);
        } catch (Exception e) {
            logger.warn("Error getting consumer info for stream '{}': {}", streamKey, e.getMessage());
        }
    }

    /**
     * Manual acknowledgment of a message (if needed for custom processing)
     */
    public void acknowledgeMessage(String streamKey, RecordId messageId) {
        try {
            redisTemplate.opsForStream().acknowledge(streamKey, consumerGroup, messageId);
            logger.debug("Manually acknowledged message: {}", messageId);
        } catch (Exception e) {
            logger.error("Error acknowledging message {}: {}", messageId, e.getMessage(), e);
        }
    }

    /**
     * Get stream information for monitoring
     */
    public Map<String, Object> getStreamInfo(String streamKey) {
        try {
            StreamInfo.XInfoStream info = redisTemplate.opsForStream().info(streamKey);
            Map<String, Object> streamInfo = new HashMap<>();
            streamInfo.put("streamLength", info.streamLength());
            streamInfo.put("lastGeneratedId", info.lastGeneratedId());
            streamInfo.put("firstEntry", info.getFirstEntry());
            streamInfo.put("lastEntry", info.getLastEntry());
            streamInfo.put("radixTreeKeys", info.radixTreeKeySize());
            streamInfo.put("radixTreeNodes", info.radixTreeNodesSize());
            streamInfo.put("groups", info.groupCount());
            return streamInfo;
        } catch (Exception e) {
            logger.error("Error getting stream info for '{}': {}", streamKey, e.getMessage(), e);
            return Collections.emptyMap();
        }
    }

    private String getPodName() {
        String podName = System.getenv("HOSTNAME");
        return podName != null ? podName : "local-instance";
    }
}
