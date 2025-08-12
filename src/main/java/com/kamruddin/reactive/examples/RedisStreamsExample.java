package com.kamruddin.reactive.examples;

import com.kamruddin.reactive.services.MongoStreamEvents;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.util.Map;

/**
 * Example demonstrating Redis Streams vs Pub/Sub for guaranteed message delivery
 *
 * Key Benefits of Redis Streams:
 * 1. Messages are persisted - no data loss if consumers are down
 * 2. Consumer groups provide load balancing across multiple pods
 * 3. Acknowledgments ensure messages are processed exactly once
 * 4. Automatic retry of failed messages
 * 5. Built-in monitoring and observability
 */
@Component
public class RedisStreamsExample {
    private static final Logger logger = LoggerFactory.getLogger(RedisStreamsExample.class);

    @Autowired
    private MongoStreamEvents mongoStreamEvents;

    @PostConstruct
    public void demonstrateRedisStreams() {
        // Example 1: Using traditional pub/sub (fire-and-forget)
//        startTraditionalPubSubMonitoring();

        // Example 2: Using Redis streams for guaranteed delivery
//        startRedisStreamsMonitoring();

        // Example 3: Monitor stream health
//        monitorStreamHealth();
    }

    /**
     * Traditional pub/sub approach - messages can be lost
     */
    private void startTraditionalPubSubMonitoring() {
        logger.info("=== Starting Traditional Pub/Sub Monitoring ===");

        mongoStreamEvents.startDistributedMonitoring("users", document -> {
            try {
                // Simulate processing
                processUserDocument(document);
                logger.info("✅ Processed user via pub/sub: {}", document.get("_id"));
            } catch (Exception e) {
                logger.error("❌ Failed to process user via pub/sub: {}", e.getMessage());
                // Message is lost - no retry mechanism
            }
        });
    }

    /**
     * Redis streams approach - guaranteed delivery with retries
     */
    private void startRedisStreamsMonitoring() {
        logger.info("=== Starting Redis Streams Monitoring ===");

        mongoStreamEvents.startDistributedMonitoringWithStreams("users", document -> {
            try {
                // Simulate processing with potential failures
                processUserDocumentWithRetry(document);
                logger.info("✅ Processed user via stream: {}", document.get("_id"));
                // Message will be acknowledged automatically on success
            } catch (Exception e) {
                logger.error("❌ Failed to process user via stream: {}", e.getMessage());
                // Message will NOT be acknowledged and will be retried
                throw e; // Re-throw to prevent acknowledgment
            }
        });
    }

    /**
     * Monitor stream health and performance
     */
    private void monitorStreamHealth() {
        logger.info("=== Monitoring Stream Health ===");

        // Schedule periodic health checks
        new Thread(() -> {
            while (true) {
                try {
                    Map<String, Object> streamInfo = mongoStreamEvents.getStreamMonitoringInfo("users");
                    logger.info("Stream Health Report: {}", streamInfo);

                    Thread.sleep(30000); // Check every 30 seconds
                } catch (Exception e) {
                    logger.error("Error monitoring stream health: {}", e.getMessage());
                }
            }
        }).start();
    }

    /**
     * Simulate document processing that might fail
     */
    private void processUserDocument(Document document) {
        // Simulate some business logic
        String userId = document.get("_id").toString();
        String username = document.getString("username");

        logger.debug("Processing user: {} with username: {}", userId, username);

        // Simulate external API call or database operation
        try {
            Thread.sleep(100); // Simulate processing time
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Simulate document processing with potential failures and retries
     */
    private void processUserDocumentWithRetry(Document document) {
        String userId = document.get("_id").toString();

        // Simulate random failures (20% failure rate)
        if (Math.random() < 0.2) {
            throw new RuntimeException("Simulated processing failure for user: " + userId);
        }

        processUserDocument(document);
    }
}

/**
 * Benefits Comparison:
 *
 * REDIS PUB/SUB (Traditional):
 * ❌ Fire-and-forget delivery
 * ❌ Messages lost if consumer is down
 * ❌ No acknowledgments
 * ❌ No retry mechanism
 * ❌ No message persistence
 * ❌ Limited observability
 * ✅ Simple to implement
 * ✅ Low latency
 *
 * REDIS STREAMS (Enhanced):
 * ✅ Guaranteed delivery
 * ✅ Messages persisted until acknowledged
 * ✅ Consumer groups for load balancing
 * ✅ Automatic retry of failed messages
 * ✅ Built-in acknowledgment system
 * ✅ Comprehensive monitoring
 * ✅ At-least-once delivery guarantee
 * ✅ Dead letter queue capability
 * ❌ Slightly higher complexity
 * ❌ Small latency overhead
 *
 * USAGE RECOMMENDATIONS:
 *
 * Use Redis Streams when:
 * - Message delivery is critical
 * - You need guaranteed processing
 * - System reliability is important
 * - You want built-in monitoring
 * - Processing failures should be retried
 *
 * Use Redis Pub/Sub when:
 * - Fire-and-forget is acceptable
 * - Ultra-low latency is required
 * - Message loss is tolerable
 * - Simple broadcasting is needed
 */
