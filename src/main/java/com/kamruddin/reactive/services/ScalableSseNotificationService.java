package com.kamruddin.reactive.services;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.concurrent.ConcurrentHashMap;

@Service
@Profile("k8s")
public class ScalableSseNotificationService implements ISseNotificationService {
    private static final Logger logger = LoggerFactory.getLogger(ScalableSseNotificationService.class);

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Autowired
    private RedisMessageListenerContainer redisMessageListenerContainer;

    // Local client connections for this pod instance
    private final ConcurrentHashMap<String, Sinks.Many<ServerSentEvent<String>>> localClientSinks = new ConcurrentHashMap<>();

    private static final String CLIENT_REGISTRY_KEY = "sse:clients";
    private static final String SSE_CHANNEL_PREFIX = "sse:channel:";
    private static final String BROADCAST_CHANNEL = "sse:broadcast";

    @PostConstruct
    public void init() {
        logger.info("=== Initializing ScalableSseNotificationService ===");
        logger.info("Pod name: {}", System.getenv("HOSTNAME"));
        logger.info("Redis template: {}", redisTemplate != null ? "Available" : "NULL");
        logger.info("Redis message listener container: {}", redisMessageListenerContainer != null ? "Available" : "NULL");

        try {
            // Subscribe to Redis pub/sub for client-specific messages using PatternTopic for wildcards
            String channelPattern = SSE_CHANNEL_PREFIX + "*";
            logger.info("Setting up Redis pub/sub listener for pattern: {}", channelPattern);

            redisMessageListenerContainer.addMessageListener(
                (message, pattern) -> {
                    try {
                        // The pattern parameter contains "sse:channel:*", not the actual channel
                        // We need to get the actual channel name from the message
                        String channelName = new String(message.getChannel());
                        String messageBody = message.toString();
                        logger.info("=== REDIS MESSAGE RECEIVED ===");
                        logger.info("Channel Name: {}", channelName);
                        logger.info("Subscribed Pattern: {}", new String(pattern));
                        logger.info("Message body: {}", messageBody);
                        logger.info("Message body length: {}", messageBody.length());

                        String clientId = extractClientIdFromChannel(channelName);
                        logger.info("Extracted clientId: {}", clientId);
                        logger.info("Local clients available: {}", localClientSinks.keySet());

                        handleRedisMessage(clientId, messageBody);
                    } catch (Exception e) {
                        logger.error("Error processing Redis message", e);
                    }
                },
                new PatternTopic(channelPattern)
            );
            logger.info("Successfully set up client-specific message listener with pattern topic");

            // Subscribe to broadcast messages using ChannelTopic (exact match)
            logger.info("Setting up Redis pub/sub listener for broadcast channel: {}", BROADCAST_CHANNEL);
            redisMessageListenerContainer.addMessageListener(
                (message, pattern) -> {
                    try {
                        String messageBody = message.toString();
                        logger.info("=== BROADCAST MESSAGE RECEIVED ===");
                        logger.info("Broadcast message: {}", messageBody);
                        handleBroadcastMessage(messageBody);
                    } catch (Exception e) {
                        logger.error("Error processing broadcast message", e);
                    }
                },
                new ChannelTopic(BROADCAST_CHANNEL)
            );
            logger.info("Successfully set up broadcast message listener");

            // Test Redis connection
            try {
                redisTemplate.opsForValue().set("test:connection", "test-value");
                String testValue = (String) redisTemplate.opsForValue().get("test:connection");
                logger.info("Redis connection test successful. Test value: {}", testValue);
                redisTemplate.delete("test:connection");
            } catch (Exception e) {
                logger.error("Redis connection test failed", e);
            }

            // Test pub/sub setup by publishing a test message
            try {
                logger.info("Testing pub/sub setup...");
                Long testSubscribers = redisTemplate.convertAndSend("test:pubsub", "test-message");
                logger.info("Test pub/sub message sent. Subscribers: {}", testSubscribers);
            } catch (Exception e) {
                logger.error("Test pub/sub failed", e);
            }

        } catch (Exception e) {
            logger.error("Failed to initialize Redis pub/sub listeners", e);
        }

        logger.info("=== ScalableSseNotificationService initialization complete ===");
    }

    public Flux<ServerSentEvent<String>> getClientEventStream(String clientId) {
        logger.info("=== GET CLIENT EVENT STREAM ===");
        logger.info("Creating event stream for clientId: {}", clientId);

        // Register client in Redis with pod instance info
        String podName = System.getenv("HOSTNAME");
        if (podName == null) {
            podName = "localhost-" + System.currentTimeMillis(); // Fallback for local development
        }

        logger.info("Registering client {} on pod {} in Redis", clientId, podName);

        try {
            redisTemplate.opsForHash().put(CLIENT_REGISTRY_KEY, clientId, podName);
            logger.info("Successfully registered client {} in Redis registry", clientId);

            // Verify registration
            String registeredPod = (String) redisTemplate.opsForHash().get(CLIENT_REGISTRY_KEY, clientId);
            logger.info("Verification: client {} registered to pod {}", clientId, registeredPod);
        } catch (Exception e) {
            logger.error("Failed to register client {} in Redis", clientId, e);
        }

        return localClientSinks.computeIfAbsent(clientId, id -> {
            logger.info("Creating new SSE sink for client {}", clientId);
            Sinks.Many<ServerSentEvent<String>> sink = Sinks.many().multicast().onBackpressureBuffer();
            logger.info("Created sink for client {}: {}", clientId, sink);
            return sink;
        }).asFlux()
        .doOnSubscribe(subscription -> {
            logger.info("Client {} subscribed to SSE stream", clientId);
        })
        .doOnCancel(() -> {
            logger.info("Client {} cancelled SSE stream", clientId);
            removeClient(clientId);
        })
        .doOnTerminate(() -> {
            logger.info("Client {} SSE stream terminated", clientId);
            removeClient(clientId);
        });
    }

    public boolean pushMessageToClient(String clientId, String message) {
        logger.info("=== PUSH MESSAGE TO CLIENT ===");
        logger.info("Target clientId: {}", clientId);
        logger.info("Message: {}", message);
        logger.info("Local clients: {}", localClientSinks.keySet());

        // Check if client is connected to this pod
        Sinks.Many<ServerSentEvent<String>> localSink = localClientSinks.get(clientId);
        if (localSink != null) {
            logger.info("Client {} found locally, sending directly", clientId);
            return sendToLocalClient(localSink, clientId, message);
        }

        // Client not on this pod, check Redis registry
        logger.info("Client {} not found locally, checking Redis registry", clientId);
        String targetPod = (String) redisTemplate.opsForHash().get(CLIENT_REGISTRY_KEY, clientId);
        logger.info("Redis registry lookup for client {}: targetPod = {}", clientId, targetPod);

        if (targetPod != null) {
            String channelName = SSE_CHANNEL_PREFIX + clientId;
            logger.info("Publishing message to Redis channel: {}", channelName);
            logger.info("Message content: {}", message);

            try {
                Long subscribers = redisTemplate.convertAndSend(channelName, message);
                logger.info("Message published to Redis. Channel: {}, Subscribers notified: {}", channelName, subscribers);

                if (subscribers == 0) {
                    logger.warn("No subscribers found for channel: {}. This might indicate a problem with Redis pub/sub setup.", channelName);
                }

                return true;
            } catch (Exception e) {
                logger.error("Failed to publish message to Redis channel: {}", channelName, e);
                return false;
            }
        }

        logger.warn("Client {} not found in any pod. Registry contents: {}", clientId,
                   redisTemplate.opsForHash().entries(CLIENT_REGISTRY_KEY));
        return false;
    }

    public void broadcastMessage(String message) {
        // Publish to Redis broadcast channel
        redisTemplate.convertAndSend(BROADCAST_CHANNEL, message);
        logger.info("Published broadcast message to Redis");
    }

    public void removeClient(String clientId) {
        localClientSinks.remove(clientId);
        redisTemplate.opsForHash().delete(CLIENT_REGISTRY_KEY, clientId);
        logger.info("Removed client {} from pod registry", clientId);
    }

    private void handleRedisMessage(String clientId, String message) {
        logger.info("=== HANDLE REDIS MESSAGE ===");
        logger.info("Processing Redis message for clientId: {}", clientId);
        logger.info("Redis message content: {}", message);
        logger.info("Available local clients: {}", localClientSinks.keySet());

        // Handle messages from Redis pub/sub for specific clients
        Sinks.Many<ServerSentEvent<String>> localSink = localClientSinks.get(clientId);

        if (localSink != null) {
            logger.info("Found local sink for client: {}", clientId);

            // Forward the message to the local client sink
            ServerSentEvent<String> sseEvent = ServerSentEvent.<String>builder()
                    .data(message)
                    .event("redis-message")
                    .id(String.valueOf(System.currentTimeMillis()))
                    .build();

            logger.info("Created SSE event: type=redis-message, id={}, data={}",
                       sseEvent.id(), sseEvent.data());

            Sinks.EmitResult result = localSink.tryEmitNext(sseEvent);
            logger.info("Emit result: {}", result);

            if (result.isSuccess()) {
                logger.info("✅ Successfully forwarded Redis message to local client {}", clientId);
            } else {
                logger.error("❌ Failed to forward Redis message to local client {}: {}", clientId, result);

                // Log additional debugging info
                logger.error("Local sink details: hasSubscribers={}, currentSubscriberCount={}",
                           localSink.currentSubscriberCount() > 0, localSink.currentSubscriberCount());
            }
        } else {
            logger.debug("No local sink found for client {} on this pod. Available clients: {}",
                        clientId, localClientSinks.keySet());
        }
    }

    private void handleBroadcastMessage(String message) {
        // Broadcast to all local clients
        ServerSentEvent<String> sseEvent = ServerSentEvent.<String>builder()
                .data(message)
                .event("broadcast-message")
                .id(String.valueOf(System.currentTimeMillis()))
                .build();

        localClientSinks.values().forEach(sink -> {
            sink.tryEmitNext(sseEvent);
        });

        logger.info("Broadcasted message to {} local clients", localClientSinks.size());
    }

    private boolean sendToLocalClient(Sinks.Many<ServerSentEvent<String>> sink, String clientId, String message) {
        ServerSentEvent<String> sseEvent = ServerSentEvent.<String>builder()
                .data(message)
                .event("custom-message")
                .id(String.valueOf(System.currentTimeMillis()))
                .build();

        Sinks.EmitResult result = sink.tryEmitNext(sseEvent);
        if (result.isSuccess()) {
            logger.info("Message emitted to local client {}", clientId);
            return true;
        } else {
            logger.error("Failed to emit message to local client {}: {}", clientId, result);
            return false;
        }
    }

    @PreDestroy
    public void cleanup() {
        // Clean up all local clients from Redis registry
        String podName = System.getenv("HOSTNAME");
        localClientSinks.keySet().forEach(clientId -> {
            String registeredPod = (String) redisTemplate.opsForHash().get(CLIENT_REGISTRY_KEY, clientId);
            if (podName.equals(registeredPod)) {
                redisTemplate.opsForHash().delete(CLIENT_REGISTRY_KEY, clientId);
            }
        });
        logger.info("Cleaned up client registry for pod {}", podName);
    }

    private String extractClientIdFromChannel(String channel) {
        logger.debug("Extracting clientId from channel: {}", channel);
        // Extract clientId from channel name: "sse:channel:clientId" -> "clientId"
        if (channel.startsWith(SSE_CHANNEL_PREFIX)) {
            String clientId = channel.substring(SSE_CHANNEL_PREFIX.length());
            logger.debug("Extracted clientId: {} from channel: {}", clientId, channel);
            return clientId;
        }
        logger.warn("Channel {} does not start with expected prefix {}", channel, SSE_CHANNEL_PREFIX);
        return channel;
    }
}
