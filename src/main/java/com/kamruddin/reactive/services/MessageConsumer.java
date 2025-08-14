package com.kamruddin.reactive.services;

import com.kamruddin.reactive.models.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@Service
public class MessageConsumer {

    private static final Logger logger = LoggerFactory.getLogger(MessageConsumer.class);

    @Autowired
    private RedisMessageListenerContainer redisMessageListenerContainer;

    @Autowired
    private MessageNotificationConsumer messageNotificationConsumer;

    private final ObjectMapper objectMapper;
    private final ConcurrentHashMap<String, Consumer<Message>> messageHandlers;
    private final AtomicLong processedMessageCount = new AtomicLong(0);

    public MessageConsumer() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        this.messageHandlers = new ConcurrentHashMap<>();
    }

    @PostConstruct
    public void initialize() {
        logger.info("Initializing MessageConsumer and subscribing to default topics");

        // Subscribe to default topics
        subscribeToUserMessages();
        subscribeToBroadcastMessages();

        logger.info("MessageConsumer initialized successfully");
    }

    /**
     * Subscribes to the general user messages topic
     */
    public void subscribeToUserMessages() {
        subscribeToTopic(MessagePublisher.USER_MESSAGES_TOPIC, this.messageNotificationConsumer);
    }

    /**
     * Subscribes to broadcast messages topic
     */
    public void subscribeToBroadcastMessages() {
        subscribeToTopic("broadcast:messages", this::handleBroadcastMessage);
    }

    /**
     * Subscribes to user-specific messages topic
     * @param userId The user ID to subscribe to
     */
    public void subscribeToUserSpecificMessages(Long userId) {
        if (userId == null) {
            logger.warn("Cannot subscribe to user-specific messages: userId is null");
            return;
        }

        String userTopic = "user:" + userId + ":messages";
        subscribeToTopic(userTopic, message -> handleUserSpecificMessage(userId, message));
    }

    /**
     * Subscribes to severity-based messages
     * @param severity The severity level (e.g., "error", "warning", "info")
     */
    public void subscribeToSeverityMessages(String severity) {
        if (severity == null || severity.trim().isEmpty()) {
            logger.warn("Cannot subscribe to severity messages: severity is null or empty");
            return;
        }

        String severityTopic = "messages:severity:" + severity.toLowerCase();
        subscribeToTopic(severityTopic, message -> handleSeverityMessage(severity, message));
    }

    /**
     * Subscribes to a custom Redis topic with a custom message handler
     * @param topic The topic to subscribe to
     * @param messageHandler The handler function to process messages
     */
    public void subscribeToTopic(String topic, Consumer<Message> messageHandler) {
        if (topic == null || topic.trim().isEmpty()) {
            logger.warn("Cannot subscribe to topic: topic is null or empty");
            return;
        }

        if (messageHandler == null) {
            logger.warn("Cannot subscribe to topic {}: messageHandler is null", topic);
            return;
        }

        try {
            // Store the handler for this topic
            messageHandlers.put(topic, messageHandler);

            // Create the message listener
            MessageListener listener = (message, pattern) -> {
                try {
                    String messageBody = new String(message.getBody());
                    String channel = new String(message.getChannel());

                    logger.debug("Received message on channel {}: {}", channel, messageBody);

                    // Deserialize the message
                    Message msg = objectMapper.readValue(messageBody, Message.class);

                    // Process the message using the registered handler
                    Consumer<Message> handler = messageHandlers.get(topic);
                    if (handler != null) {
                        handler.accept(msg);
                        processedMessageCount.incrementAndGet();
                    } else {
                        logger.warn("No handler found for topic: {}", topic);
                    }

                } catch (Exception e) {
                    logger.error("Error processing message from topic {}: {}", topic, e.getMessage(), e);
                }
            };

            // Add the listener to the container
            redisMessageListenerContainer.addMessageListener(listener, new ChannelTopic(topic));

            logger.info("Successfully subscribed to topic: {}", topic);

        } catch (Exception e) {
            logger.error("Failed to subscribe to topic {}: {}", topic, e.getMessage(), e);
        }
    }

    /**
     * Unsubscribes from a specific topic
     * @param topic The topic to unsubscribe from
     */
    public void unsubscribeFromTopic(String topic) {
        if (topic == null || topic.trim().isEmpty()) {
            logger.warn("Cannot unsubscribe from topic: topic is null or empty");
            return;
        }

        try {
            // Remove the handler
            messageHandlers.remove(topic);

            // Remove all listeners for this topic
            redisMessageListenerContainer.removeMessageListener(null, new ChannelTopic(topic));

            logger.info("Successfully unsubscribed from topic: {}", topic);

        } catch (Exception e) {
            logger.error("Failed to unsubscribe from topic {}: {}", topic, e.getMessage(), e);
        }
    }

    /**
     * Handles general user messages
     * @param message The received message
     */
    private void handleUserMessage(Message message) {
        logger.info("Processing user message - ID: {}, Type: {}, User: {}, Severity: {}",
                   message.getId(), message.getType(), message.getUserId(), message.getSeverity());

        // Add your custom business logic here
        // For example: save to database, send notifications, etc.

        logMessageDetails("USER_MESSAGE", message);
    }

    /**
     * Handles broadcast messages
     * @param message The received message
     */
    private void handleBroadcastMessage(Message message) {
        logger.info("Processing broadcast message - ID: {}, Type: {}, Message: {}",
                   message.getId(), message.getType(), message.getMessage());

        // Add your custom business logic here
        // For example: notify all connected clients, update global state, etc.

        logMessageDetails("BROADCAST_MESSAGE", message);
    }

    /**
     * Handles user-specific messages
     * @param userId The user ID
     * @param message The received message
     */
    private void handleUserSpecificMessage(Long userId, Message message) {
        logger.info("Processing user-specific message for user {} - ID: {}, Type: {}",
                   userId, message.getId(), message.getType());

        // Add your custom business logic here
        // For example: send push notification to specific user, update user preferences, etc.

        logMessageDetails("USER_SPECIFIC_MESSAGE", message);
    }

    /**
     * Handles severity-based messages
     * @param severity The severity level
     * @param message The received message
     */
    private void handleSeverityMessage(String severity, Message message) {
        logger.info("Processing {} severity message - ID: {}, Type: {}, Message: {}",
                   severity.toUpperCase(), message.getId(), message.getType(), message.getMessage());

        // Add your custom business logic here
        // For example: alert administrators for error messages, log warnings, etc.

        switch (severity.toLowerCase()) {
            case "error":
                handleErrorMessage(message);
                break;
            case "warning":
                handleWarningMessage(message);
                break;
            case "info":
                handleInfoMessage(message);
                break;
            default:
                logger.debug("Processing message with unknown severity: {}", severity);
        }

        logMessageDetails("SEVERITY_MESSAGE_" + severity.toUpperCase(), message);
    }

    /**
     * Handles error severity messages
     * @param message The error message
     */
    private void handleErrorMessage(Message message) {
        logger.error("ERROR MESSAGE RECEIVED - ID: {}, Source: {}, Message: {}",
                    message.getId(), message.getSource(), message.getMessage());

        // Add error-specific handling here
        // For example: send alerts, create tickets, etc.
    }

    /**
     * Handles warning severity messages
     * @param message The warning message
     */
    private void handleWarningMessage(Message message) {
        logger.warn("WARNING MESSAGE RECEIVED - ID: {}, Source: {}, Message: {}",
                   message.getId(), message.getSource(), message.getMessage());

        // Add warning-specific handling here
    }

    /**
     * Handles info severity messages
     * @param message The info message
     */
    private void handleInfoMessage(Message message) {
        logger.info("INFO MESSAGE RECEIVED - ID: {}, Source: {}, Message: {}",
                   message.getId(), message.getSource(), message.getMessage());

        // Add info-specific handling here
    }

    /**
     * Logs detailed message information
     * @param messageType The type of message processing
     * @param message The message object
     */
    private void logMessageDetails(String messageType, Message message) {
        logger.debug("{} - Full Details: ID={}, Type={}, Message='{}', Timestamp={}, " +
                    "Severity={}, Source={}, UserId={}",
                    messageType, message.getId(), message.getType(), message.getMessage(),
                    message.getTimestamp(), message.getSeverity(), message.getSource(),
                    message.getUserId());
    }

    /**
     * Gets the number of processed messages
     * @return The count of processed messages
     */
    public long getProcessedMessageCount() {
        return processedMessageCount.get();
    }

    /**
     * Gets the list of currently subscribed topics
     * @return Set of subscribed topic names
     */
    public java.util.Set<String> getSubscribedTopics() {
        return messageHandlers.keySet();
    }

    /**
     * Checks if the consumer is subscribed to a specific topic
     * @param topic The topic to check
     * @return true if subscribed, false otherwise
     */
    public boolean isSubscribedToTopic(String topic) {
        return messageHandlers.containsKey(topic);
    }

    /**
     * Resets the processed message counter
     */
    public void resetProcessedMessageCount() {
        processedMessageCount.set(0);
        logger.info("Processed message count reset to 0");
    }

    @PreDestroy
    public void cleanup() {
        logger.info("Cleaning up MessageConsumer - unsubscribing from all topics");

        messageHandlers.keySet().forEach(this::unsubscribeFromTopic);
        messageHandlers.clear();

        logger.info("MessageConsumer cleanup completed. Total messages processed: {}",
                   processedMessageCount.get());
    }
}
