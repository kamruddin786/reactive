package com.kamruddin.reactive.services;

import com.kamruddin.reactive.models.Message;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class MessagePublisher {

    private static final Logger logger = LoggerFactory.getLogger(MessagePublisher.class);
    public static final String USER_MESSAGES_TOPIC = "user:messages";
    public static final String BROADCAST_MESSAGES_TOPIC = "broadcast:messages";

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Autowired
    private MeterRegistry meterRegistry;

    // Metrics for KEDA scaling
    private Counter userMessagesPublishedCounter;
    private Counter broadcastMessagesPublishedCounter;
    private Counter totalMessagesPublishedCounter;
    private Counter publishFailuresCounter;
    private Timer messagePublishTimer;
    private final AtomicLong pendingMessageCount = new AtomicLong(0);
    private final AtomicLong activeSubscribersCount = new AtomicLong(0);

    @PostConstruct
    public void initializeMetrics() {
        // Counters for message publishing rates
        userMessagesPublishedCounter = Counter.builder("messages_published_total")
                .description("Total number of messages published")
                .tag("topic", USER_MESSAGES_TOPIC)
                .register(meterRegistry);

        broadcastMessagesPublishedCounter = Counter.builder("messages_published_total")
                .description("Total number of broadcast messages published")
                .tag("topic", BROADCAST_MESSAGES_TOPIC)
                .register(meterRegistry);

        totalMessagesPublishedCounter = Counter.builder("redis_messages_published_total")
                .description("Total messages published to Redis")
                .register(meterRegistry);

        publishFailuresCounter = Counter.builder("messages_publish_failures_total")
                .description("Total number of message publish failures")
                .register(meterRegistry);

        // Timer for message publishing latency
        messagePublishTimer = Timer.builder("message_publish_duration_seconds")
                .description("Time taken to publish messages to Redis")
                .register(meterRegistry);

        // Gauges for real-time metrics that KEDA can use
        Gauge.builder("redis_pending_messages", this, MessagePublisher::getPendingMessageCount)
                .description("Number of pending messages waiting to be processed")
                .register(meterRegistry);

        Gauge.builder("redis_active_subscribers", this, MessagePublisher::getActiveSubscribersCount)
                .description("Number of active subscribers to Redis channels")
                .register(meterRegistry);

        Gauge.builder("redis_connection_status", this, mp -> mp.isRedisConnected() ? 1.0 : 0.0)
                .description("Redis connection status (1=connected, 0=disconnected)")
                .register(meterRegistry);

        logger.info("MessagePublisher metrics initialized for KEDA scaling");
    }

    /**
     * Publishes a message to the user messages Redis topic
     * @param message The message to publish
     * @return true if published successfully, false otherwise
     */
    public boolean publishMessage(Message message) {
        Timer.Sample sample = Timer.start(meterRegistry);
        try {
            boolean result = publishMessageToTopic(USER_MESSAGES_TOPIC, message);
            if (result) {
                userMessagesPublishedCounter.increment();
                totalMessagesPublishedCounter.increment();
                updateSubscriberCount(USER_MESSAGES_TOPIC);
            } else {
                publishFailuresCounter.increment();
            }
            return result;
        } finally {
            sample.stop(messagePublishTimer);
        }
    }

    /**
     * Publishes a broadcast message to all users
     * @param message The message to broadcast
     * @return true if published successfully, false otherwise
     */
    public boolean broadcastMessage(Message message) {
        Timer.Sample sample = Timer.start(meterRegistry);
        try {
            boolean result = publishMessageToTopic(BROADCAST_MESSAGES_TOPIC, message);
            if (result) {
                broadcastMessagesPublishedCounter.increment();
                totalMessagesPublishedCounter.increment();
                updateSubscriberCount(BROADCAST_MESSAGES_TOPIC);
            } else {
                publishFailuresCounter.increment();
            }
            return result;
        } finally {
            sample.stop(messagePublishTimer);
        }
    }

    /**
     * Publishes a message to a specific Redis topic
     * @param topic The topic to publish to
     * @param message The message to publish
     * @return true if published successfully, false otherwise
     */
    public boolean publishMessageToTopic(String topic, Message message) {
        try {
            if (message == null || topic == null || topic.trim().isEmpty()) {
                logger.warn("Invalid parameters - message: {}, topic: {}", message, topic);
                return false;
            }

            redisTemplate.convertAndSend(topic, message);

            logger.info("Successfully published message with ID {} to custom topic {}",
                       message.getId(), topic);
            return true;

        } catch (Exception e) {
            logger.error("Failed to publish message with ID {} to topic {}: {}",
                        message != null ? message.getId() : "null", topic, e.getMessage(), e);
            return false;
        }
    }

    /**
     * Checks if Redis connection is available
     * @return true if Redis is connected, false otherwise
     */
    public boolean isRedisConnected() {
        try {
            redisTemplate.getConnectionFactory().getConnection().ping();
            return true;
        } catch (Exception e) {
            logger.error("Redis connection check failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Updates the subscriber count for a given topic
     * This metric is crucial for KEDA scaling decisions
     */
    private void updateSubscriberCount(String topic) {
        try {
            // Get number of subscribers for the topic using RedisCallback
            Long subscribers = redisTemplate.execute((RedisCallback<Long>) connection -> {
                try {
                    // Use PUBSUB NUMSUB command to get subscriber count
                    Object result = connection.execute("PUBSUB", "NUMSUB".getBytes(), topic.getBytes());
                    if (result instanceof Object[]) {
                        Object[] array = (Object[]) result;
                        if (array.length >= 2) {
                            return Long.parseLong(new String((byte[]) array[1]));
                        }
                    }
                    return 0L;
                } catch (Exception e) {
                    logger.warn("Failed to get subscriber count for topic {}: {}", topic, e.getMessage());
                    return 0L;
                }
            });

            if (subscribers != null) {
                activeSubscribersCount.set(subscribers);
                logger.debug("Updated subscriber count for topic {}: {}", topic, subscribers);
            }
        } catch (Exception e) {
            logger.warn("Failed to update subscriber count for topic {}: {}", topic, e.getMessage());
        }
    }

    /**
     * Gets the current pending message count - useful for KEDA scaling
     */
    public double getPendingMessageCount() {
        return pendingMessageCount.get();
    }

    /**
     * Gets the current active subscribers count - useful for KEDA scaling
     */
    public double getActiveSubscribersCount() {
        return activeSubscribersCount.get();
    }

    /**
     * Updates pending message count based on queue depth
     * Call this method when messages are queued or processed
     */
    public void updatePendingMessageCount(long delta) {
        long newCount = pendingMessageCount.addAndGet(delta);
        logger.debug("Updated pending message count by {}, new total: {}", delta, newCount);
    }

    /**
     * Gets detailed metrics about Redis topics for KEDA scaling decisions
     */
    public RedisTopicMetrics getTopicMetrics(String topic) {
        try {
            // Get subscriber count using RedisCallback
            Long subscribers = redisTemplate.execute((RedisCallback<Long>) connection -> {
                try {
                    Object result = connection.execute("PUBSUB", "NUMSUB".getBytes(), topic.getBytes());
                    if (result instanceof Object[]) {
                        Object[] array = (Object[]) result;
                        if (array.length >= 2) {
                            return Long.parseLong(new String((byte[]) array[1]));
                        }
                    }
                    return 0L;
                } catch (Exception e) {
                    return 0L;
                }
            });

            // Get memory usage info using RedisCallback
            String memoryInfo = redisTemplate.execute((RedisCallback<String>) connection -> {
                try {
                    Object result = connection.execute("INFO", "memory".getBytes());
                    return new String((byte[]) result);
                } catch (Exception e) {
                    return "";
                }
            });

            return new RedisTopicMetrics(
                topic,
                subscribers != null ? subscribers : 0,
                extractMemoryUsage(memoryInfo),
                System.currentTimeMillis()
            );
        } catch (Exception e) {
            logger.error("Failed to get topic metrics for {}: {}", topic, e.getMessage());
            return new RedisTopicMetrics(topic, 0, 0, System.currentTimeMillis());
        }
    }

    private long extractMemoryUsage(String memoryInfo) {
        try {
            // Extract used_memory_rss from Redis INFO memory output
            String[] lines = memoryInfo.split("\n");
            for (String line : lines) {
                if (line.startsWith("used_memory_rss:")) {
                    return Long.parseLong(line.split(":")[1].trim());
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to parse memory usage: {}", e.getMessage());
        }
        return 0;
    }

    /**
     * Data class for Redis topic metrics used by KEDA
     */
    public static class RedisTopicMetrics {
        private final String topic;
        private final long subscriberCount;
        private final long memoryUsage;
        private final long timestamp;

        public RedisTopicMetrics(String topic, long subscriberCount, long memoryUsage, long timestamp) {
            this.topic = topic;
            this.subscriberCount = subscriberCount;
            this.memoryUsage = memoryUsage;
            this.timestamp = timestamp;
        }

        // Getters
        public String getTopic() { return topic; }
        public long getSubscriberCount() { return subscriberCount; }
        public long getMemoryUsage() { return memoryUsage; }
        public long getTimestamp() { return timestamp; }
    }
}
