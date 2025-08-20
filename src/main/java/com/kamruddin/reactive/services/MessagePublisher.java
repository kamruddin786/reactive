package com.kamruddin.reactive.services;

import com.kamruddin.reactive.models.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

@Service
public class MessagePublisher {

    private static final Logger logger = LoggerFactory.getLogger(MessagePublisher.class);
    public static final String USER_MESSAGES_TOPIC = "user:messages";
    public static final String BROADCAST_MESSAGES_TOPIC = "broadcast:messages";

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    /**
     * Publishes a message to the user messages Redis topic
     * @param message The message to publish
     * @return true if published successfully, false otherwise
     */
    public boolean publishMessage(Message message) {
        return publishMessageToTopic(USER_MESSAGES_TOPIC, message);
    }

    /**
     * Publishes a broadcast message to all users
     * @param message The message to broadcast
     * @return true if published successfully, false otherwise
     */
    public boolean broadcastMessage(Message message) {
        return publishMessageToTopic(BROADCAST_MESSAGES_TOPIC, message);
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
}
