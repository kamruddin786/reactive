//package com.kamruddin.reactive.services;
//
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.data.mongodb.core.MongoTemplate;
//import org.springframework.data.redis.core.RedisTemplate;
//import org.springframework.stereotype.Service;
//import com.mongodb.client.model.changestream.ChangeStreamDocument;
//import com.mongodb.client.model.changestream.OperationType;
//import org.bson.Document;
//import reactor.core.publisher.Flux;
//import reactor.core.scheduler.Schedulers;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.scheduling.annotation.Scheduled;
//import org.springframework.context.annotation.DependsOn;
//import org.springframework.data.redis.connection.stream.MapRecord;
//
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.function.Consumer;
//import java.util.concurrent.TimeUnit;
//import java.time.Duration;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.Map;
//import java.util.Set;
//
//@Service
//@DependsOn("redisTemplate")
//public class MongoStreamEvents {
//    private static final Logger logger = LoggerFactory.getLogger(MongoStreamEvents.class);
//
//    @Autowired
//    private MongoTemplate mongoTemplate;
//
//    @Autowired
//    private RedisTemplate<String, Object> redisTemplate;
//
//    @Autowired
//    private RedisStreamService redisStreamService;
//
//    private static final String CHANGE_STREAM_LOCK_PREFIX = "changestream:lock:";
//    private static final String HEARTBEAT_PREFIX = "changestream:heartbeat:";
//    private static final String PROCESSED_DOCS_KEY_PREFIX = "processed:";
//    private static final String LEADER_REGISTRY_KEY = "changestream:leaders";
//
//    private static final int LOCK_TIMEOUT_SECONDS = 30;
//    private static final int HEARTBEAT_INTERVAL_SECONDS = 10;
//    private static final int HEARTBEAT_TIMEOUT_SECONDS = 25;
//    private static final int PROCESSED_DOC_TTL_SECONDS = 300; // 5 minutes
//    private static final String REDIS_STREAM_PREFIX = "mongodb:events:";
//
//    // Track active change streams in this pod
//    private final Map<String, Boolean> activeStreams = new ConcurrentHashMap<>();
//    private final Map<String, Consumer<Document>> registeredProcessors = new ConcurrentHashMap<>();
//
//    /**
//     * Scheduled heartbeat to maintain leadership and detect failures
//     * Runs every 10 seconds to update heartbeat and check for failed leaders
//     */
//    @Scheduled(fixedRate = HEARTBEAT_INTERVAL_SECONDS * 1000)
//    public void maintainLeadershipAndCheckFailures() {
//        String podName = getPodName();
//
//        // Update heartbeat for streams this pod is leading
//        for (String collectionName : activeStreams.keySet()) {
//            if (activeStreams.get(collectionName)) {
//                updateHeartbeat(collectionName, podName);
//            }
//        }
//
//        // Check for failed leaders and attempt takeover
//        checkForFailedLeadersAndTakeover();
//    }
//
//    /**
//     * Check for failed leaders across all registered collections and attempt takeover
//     */
//    private void checkForFailedLeadersAndTakeover() {
//        try {
//            Set<Object> registeredCollections = redisTemplate.opsForSet().members(LEADER_REGISTRY_KEY);
//            if (registeredCollections != null) {
//                for (Object collectionObj : registeredCollections) {
//                    String collectionName = collectionObj.toString();
//
//                    // Skip if we're already leading this collection
//                    if (Boolean.TRUE.equals(activeStreams.get(collectionName))) {
//                        continue;
//                    }
//
//                    if (isLeadershipAvailable(collectionName)) {
//                        logger.warn("Detected failed leader for collection '{}'. Attempting takeover...", collectionName);
//                        attemptLeadershipTakeover(collectionName);
//                    }
//                }
//            }
//        } catch (Exception e) {
//            logger.error("Error checking for failed leaders: {}", e.getMessage(), e);
//        }
//    }
//
//    /**
//     * Check if leadership is available (leader failed or no leader exists)
//     */
//    private boolean isLeadershipAvailable(String collectionName) {
//        String lockKey = CHANGE_STREAM_LOCK_PREFIX + collectionName;
//        String heartbeatKey = HEARTBEAT_PREFIX + collectionName;
//
//        // Check if lock exists
//        String currentLock = (String) redisTemplate.opsForValue().get(lockKey);
//        if (currentLock == null) {
//            return true; // No lock exists
//        }
//
//        // Check if heartbeat is stale
//        String lastHeartbeat = (String) redisTemplate.opsForValue().get(heartbeatKey);
//        if (lastHeartbeat == null) {
//            return true; // No heartbeat exists
//        }
//
//        try {
//            long heartbeatTime = Long.parseLong(lastHeartbeat);
//            long currentTime = System.currentTimeMillis();
//            boolean isStale = (currentTime - heartbeatTime) > (HEARTBEAT_TIMEOUT_SECONDS * 1000);
//
//            if (isStale) {
//                logger.warn("Stale heartbeat detected for collection '{}'. Last heartbeat: {}ms ago",
//                    collectionName, currentTime - heartbeatTime);
//            }
//
//            return isStale;
//        } catch (NumberFormatException e) {
//            return true; // Invalid heartbeat format
//        }
//    }
//
//    /**
//     * Attempt to take over leadership for a failed collection
//     */
//    private void attemptLeadershipTakeover(String collectionName) {
//        String lockKey = CHANGE_STREAM_LOCK_PREFIX + collectionName;
//        String podName = getPodName();
//
//        // Force delete the stale lock
//        redisTemplate.delete(lockKey);
//        redisTemplate.delete(HEARTBEAT_PREFIX + collectionName);
//
//        // Wait a bit to avoid race conditions
//        try {
//            Thread.sleep(1000);
//        } catch (InterruptedException e) {
//            Thread.currentThread().interrupt();
//            return;
//        }
//
//        // Attempt to acquire new leadership
//        Consumer<Document> processor = registeredProcessors.get(collectionName);
//        if (processor != null) {
//            logger.info("Pod {} attempting to take over leadership for collection '{}'", podName, collectionName);
//            startDistributedMonitoring(collectionName, processor);
////            startDistributedMonitoringWithStreams(collectionName, processor);
//        }
//    }
//
//    /**
//     * Update heartbeat for a collection this pod is leading
//     */
//    private void updateHeartbeat(String collectionName, String podName) {
//        try {
//            String heartbeatKey = HEARTBEAT_PREFIX + collectionName;
//            String heartbeatValue = String.valueOf(System.currentTimeMillis());
//            redisTemplate.opsForValue().set(heartbeatKey, heartbeatValue, HEARTBEAT_TIMEOUT_SECONDS + 5, TimeUnit.SECONDS);
//
//            logger.debug("Updated heartbeat for collection '{}' from pod '{}'", collectionName, podName);
//        } catch (Exception e) {
//            logger.error("Error updating heartbeat for collection '{}': {}", collectionName, e.getMessage(), e);
//        }
//    }
//
//    /**
//     * Creates a distributed change stream for the specified collection to monitor new entries
//     * Only one pod in the cluster will actively watch the change stream
//     * @param collectionName the name of the collection to watch
//     * @return Flux of ChangeStreamDocument for processing new entries
//     */
//    public Flux<ChangeStreamDocument<Document>> watchForNewEntries(String collectionName) {
//        return watchForNewEntriesDistributed(collectionName);
//    }
//
//    /**
//     * Enhanced distributed change stream with failover support
//     */
//    private Flux<ChangeStreamDocument<Document>> watchForNewEntriesDistributed(String collectionName) {
//        String lockKey = CHANGE_STREAM_LOCK_PREFIX + collectionName;
//        String podName = getPodName();
//
//        return Flux.<ChangeStreamDocument<Document>>create(sink -> {
//            // Try to acquire distributed lock for this collection
//            String lockValue = podName + ":" + System.currentTimeMillis();
//            Boolean lockAcquired = redisTemplate.opsForValue()
//                .setIfAbsent(lockKey, lockValue, LOCK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
//
//            if (Boolean.TRUE.equals(lockAcquired)) {
//                logger.info("Pod {} acquired change stream lock for collection: {}", podName, collectionName);
//
//                // Mark as active and register in Redis
//                activeStreams.put(collectionName, true);
//                redisTemplate.opsForSet().add(LEADER_REGISTRY_KEY, collectionName);
//                updateHeartbeat(collectionName, podName);
//
//                try {
//                    mongoTemplate.getCollection(collectionName)
//                        .watch(Collections.singletonList(
//                            Document.parse("{ $match: { operationType: 'insert' } }")
//                        ))
//                        .forEach(changeDoc -> {
//                            if (changeDoc.getOperationType() == OperationType.INSERT) {
//                                // Update heartbeat periodically during processing
//                                updateHeartbeat(collectionName, podName);
//
//                                // Check if document was already processed (idempotency)
//                                String docId = getDocumentId(changeDoc);
//                                String processedKey = PROCESSED_DOCS_KEY_PREFIX + collectionName + ":" + docId;
//
//                                Boolean wasProcessed = redisTemplate.opsForValue()
//                                    .setIfAbsent(processedKey, "processed", PROCESSED_DOC_TTL_SECONDS, TimeUnit.SECONDS);
//
//                                if (Boolean.TRUE.equals(wasProcessed)) {
//                                    // Publish to Redis for other pods to consume
//                                    publishDocumentEvent(collectionName, changeDoc);
//                                    sink.next(changeDoc);
//                                    logger.info("New entry detected and published for collection '{}': {}",
//                                        collectionName, docId);
//                                } else {
//                                    logger.debug("Document {} already processed for collection '{}'", docId, collectionName);
//                                }
//
//                                // Renew lock if we're still processing
//                                renewLockIfOwned(lockKey, lockValue);
//                            }
//                        });
//                } catch (Exception e) {
//                    logger.error("Error in change stream for collection '{}': {}",
//                        collectionName, e.getMessage(), e);
//                    sink.error(e);
//                } finally {
//                    // Clean up leadership state
//                    cleanupLeadership(collectionName, lockKey, lockValue);
//                }
//            } else {
//                logger.info("Pod {} could not acquire change stream lock for collection: {} - another pod is watching",
//                    podName, collectionName);
//                sink.complete();
//            }
//        })
//        .subscribeOn(Schedulers.boundedElastic())
//        .doOnError(error -> {
//            logger.error("Error in distributed change stream for collection '{}': {}",
//                collectionName, error.getMessage(), error);
//            // Clean up on error
//            cleanupLeadership(collectionName, lockKey, podName + ":" + System.currentTimeMillis());
//        })
//        .onErrorResume(error -> {
//            logger.warn("Resuming change stream for collection '{}' after error", collectionName);
//            // Clean up and retry after delay
//            cleanupLeadership(collectionName, lockKey, podName + ":" + System.currentTimeMillis());
//            return Flux.empty().delayElements(Duration.ofSeconds(5))
//                .flatMap(ignored -> watchForNewEntriesDistributed(collectionName));
//        });
//    }
//
//    /**
//     * Enhanced distributed monitoring using Redis streams for guaranteed delivery
//     * This replaces the pub/sub approach with persistent streams
//     */
//    public void startDistributedMonitoringWithStreams(String collectionName, Consumer<Document> processor) {
//        logger.info("Starting distributed monitoring with Redis streams for collection: {}", collectionName);
//
//        // Register processor for potential failover
//        registeredProcessors.put(collectionName, processor);
//
//        // Start change stream watcher (only one pod will succeed initially)
//        watchForNewEntriesWithStreams(collectionName)
//            .subscribe(
//                changeDoc -> logger.debug("Change stream active for collection '{}'", collectionName),
//                error -> {
//                    logger.error("Change stream error for collection '{}': {}", collectionName, error.getMessage());
//                    activeStreams.put(collectionName, false);
//                },
//                () -> {
//                    logger.info("Change stream completed for collection '{}'", collectionName);
//                    activeStreams.put(collectionName, false);
//                }
//            );
//
//        // All pods subscribe to Redis stream events with guaranteed delivery
//        subscribeToDocumentStream(collectionName)
//            .subscribe(
//                streamRecord -> {
//                    try {
//                        // Extract document from stream record
//                        Map<Object, Object> recordData = streamRecord.getValue();
//                        String documentJson = (String) recordData.get("document");
//
//                        if (documentJson != null) {
//                            Document document = Document.parse(documentJson);
//                            processor.accept(document);
//                            logger.debug("Processed guaranteed document from stream '{}'", collectionName);
//                        }
//                    } catch (Exception e) {
//                        logger.error("Error processing stream document from collection '{}': {}",
//                            collectionName, e.getMessage(), e);
//                        // Return false to indicate processing failure - message won't be acknowledged
//                        throw new RuntimeException("Processing failed", e);
//                    }
//                },
//                error -> logger.error("Error in stream monitoring for collection '{}': {}",
//                    collectionName, error.getMessage()),
//                () -> logger.info("Stream monitoring completed for collection '{}'", collectionName)
//            );
//    }
//
//    /**
//     * Enhanced change stream that publishes to Redis streams instead of pub/sub
//     */
//    private Flux<ChangeStreamDocument<Document>> watchForNewEntriesWithStreams(String collectionName) {
//        String lockKey = CHANGE_STREAM_LOCK_PREFIX + collectionName;
//        String podName = getPodName();
//
//        return Flux.<ChangeStreamDocument<Document>>create(sink -> {
//            String lockValue = podName + ":" + System.currentTimeMillis();
//            Boolean lockAcquired = redisTemplate.opsForValue()
//                .setIfAbsent(lockKey, lockValue, LOCK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
//
//            if (Boolean.TRUE.equals(lockAcquired)) {
//                logger.info("Pod {} acquired change stream lock for collection: {}", podName, collectionName);
//
//                activeStreams.put(collectionName, true);
//                redisTemplate.opsForSet().add(LEADER_REGISTRY_KEY, collectionName);
//                updateHeartbeat(collectionName, podName);
//
//                try {
//                    mongoTemplate.getCollection(collectionName)
//                        .watch(Collections.singletonList(
//                            Document.parse("{ $match: { operationType: 'insert' } }")
//                        ))
//                        .forEach(changeDoc -> {
//                            if (changeDoc.getOperationType() == OperationType.INSERT) {
//                                updateHeartbeat(collectionName, podName);
//
//                                String docId = getDocumentId(changeDoc);
//                                String processedKey = PROCESSED_DOCS_KEY_PREFIX + collectionName + ":" + docId;
//
//                                Boolean wasProcessed = redisTemplate.opsForValue()
//                                    .setIfAbsent(processedKey, "processed", PROCESSED_DOC_TTL_SECONDS, TimeUnit.SECONDS);
//
//                                if (Boolean.TRUE.equals(wasProcessed)) {
//                                    // Publish to Redis stream for guaranteed delivery
//                                    publishDocumentToStream(collectionName, changeDoc);
//                                    sink.next(changeDoc);
//                                    logger.info("New entry published to stream for collection '{}': {}",
//                                        collectionName, docId);
//                                } else {
//                                    logger.debug("Document {} already processed for collection '{}'", docId, collectionName);
//                                }
//
//                                renewLockIfOwned(lockKey, lockValue);
//                            }
//                        });
//                } catch (Exception e) {
//                    logger.error("Error in change stream for collection '{}': {}", collectionName, e.getMessage(), e);
//                    sink.error(e);
//                } finally {
//                    cleanupLeadership(collectionName, lockKey, lockValue);
//                }
//            } else {
//                logger.info("Pod {} could not acquire change stream lock for collection: {}", podName, collectionName);
//                sink.complete();
//            }
//        })
//        .subscribeOn(Schedulers.boundedElastic())
//        .doOnError(error -> {
//            logger.error("Error in distributed change stream for collection '{}': {}", collectionName, error.getMessage());
//            cleanupLeadership(collectionName, lockKey, podName + ":" + System.currentTimeMillis());
//        })
//        .onErrorResume(error -> {
//            logger.warn("Resuming change stream for collection '{}' after error", collectionName);
//            cleanupLeadership(collectionName, lockKey, podName + ":" + System.currentTimeMillis());
//            return Flux.empty().delayElements(Duration.ofSeconds(5))
//                .flatMap(ignored -> watchForNewEntriesWithStreams(collectionName));
//        });
//    }
//
//    /**
//     * Subscribe to Redis stream for guaranteed message delivery
//     */
//    public Flux<MapRecord<String, Object, Object>> subscribeToDocumentStream(String collectionName) {
//        String streamKey = REDIS_STREAM_PREFIX + collectionName;
//
//        return redisStreamService.subscribeToStream(streamKey, message -> {
//            try {
//                // Process the message and return true for successful processing
//                logger.debug("Processing stream message: {}", message.getId());
//                return true; // Message will be acknowledged
//            } catch (Exception e) {
//                logger.error("Error processing stream message: {}", e.getMessage(), e);
//                return false; // Message won't be acknowledged and will be retried
//            }
//        });
//    }
//
//    /**
//     * Publish document to Redis stream instead of pub/sub
//     */
//    private void publishDocumentToStream(String collectionName, ChangeStreamDocument<Document> changeDoc) {
//        try {
//            String streamKey = REDIS_STREAM_PREFIX + collectionName;
//            Document fullDoc = changeDoc.getFullDocument();
//
//            if (fullDoc != null) {
//                Map<String, Object> streamData = new HashMap<>();
//                streamData.put("document", fullDoc.toJson());
//                streamData.put("collectionName", collectionName);
//                streamData.put("operationType", changeDoc.getOperationType().toString());
//                streamData.put("documentId", getDocumentId(changeDoc));
//                streamData.put("producer", getPodName());
//
//                String messageId = redisStreamService.publishToStream(streamKey, streamData);
//                logger.debug("Published document to stream '{}' with message ID: {}", streamKey, messageId);
//            }
//        } catch (Exception e) {
//            logger.error("Error publishing document to stream: {}", e.getMessage(), e);
//        }
//    }
//
//    /**
//     * Get monitoring information for Redis streams
//     */
//    public Map<String, Object> getStreamMonitoringInfo(String collectionName) {
//        String streamKey = REDIS_STREAM_PREFIX + collectionName;
//        Map<String, Object> info = redisStreamService.getStreamInfo(streamKey);
//
//        // Add consumer group information
//        redisStreamService.logConsumerGroupInfo(streamKey);
//
//        return info;
//    }
//
//    // ...existing startMonitoring method for backward compatibility...
//    public void startMonitoring(String collectionName, Consumer<Document> processor) {
//        logger.info("Starting monitoring for new entries in collection: {}", collectionName);
//
//        watchForNewEntries(collectionName)
//            .map(ChangeStreamDocument::getFullDocument)
//            .filter(java.util.Objects::nonNull)
//            .subscribe(
//                document -> {
//                    try {
//                        processor.accept(document);
//                        logger.debug("Processed new document from collection '{}'", collectionName);
//                    } catch (Exception e) {
//                        logger.error("Error processing document from collection '{}': {}",
//                            collectionName, e.getMessage(), e);
//                    }
//                },
//                error -> logger.error("Error in monitoring stream for collection '{}': {}",
//                    collectionName, error.getClass().getSimpleName(), error),
//                () -> logger.info("Monitoring stream completed for collection '{}'", collectionName)
//            );
//    }
//
//    /**
//     * Clean up leadership state when stream ends or fails
//     */
//    private void cleanupLeadership(String collectionName, String lockKey, String lockValue) {
//        try {
//            // Mark as inactive
//            activeStreams.put(collectionName, false);
//
//            // Remove heartbeat
//            redisTemplate.delete(HEARTBEAT_PREFIX + collectionName);
//
//            // Release lock only if we still own it
//            releaseLockIfOwned(lockKey, lockValue);
//
//            logger.info("Cleaned up leadership for collection '{}'", collectionName);
//        } catch (Exception e) {
//            logger.error("Error cleaning up leadership for collection '{}': {}", collectionName, e.getMessage(), e);
//        }
//    }
//
//    /**
//     * Subscribe to Redis pub/sub for distributed document processing
//     * All pods can subscribe to this without creating multiple change streams
//     */
//    public Flux<Document> subscribeToDocumentEvents(String collectionName) {
//        String channel = "mongodb:events:" + collectionName;
//
//        return Flux.<Document>create(sink -> {
//            redisTemplate.getConnectionFactory().getConnection()
//                .subscribe((message, pattern) -> {
//                    try {
//                        // Parse the published document
//                        String jsonDoc = new String(message.getBody());
//                        logger.debug("Received raw message for collection '{}': {}", collectionName, jsonDoc);
//
//                        // Validate that we have a non-empty JSON string
//                        if (jsonDoc == null || jsonDoc.trim().isEmpty()) {
//                            logger.warn("Received empty or null message for collection '{}'", collectionName);
//                            return;
//                        }
//
//                        // Try to parse the document - handle MongoDB extended JSON format
//                        Document document;
//                        try {
//                            // First try direct parsing
//                            document = Document.parse(jsonDoc);
//                        } catch (Exception e) {
//                            logger.warn("Direct JSON parsing failed for collection '{}', attempting to handle MongoDB extended JSON: {}",
//                                collectionName, e.getMessage());
//
//                            try {
//                                // Handle MongoDB extended JSON format (e.g., {"$date": "..."})
//                                String cleanedJson = preprocessMongoExtendedJson(jsonDoc);
//                                document = Document.parse(cleanedJson);
//                                logger.debug("Successfully parsed MongoDB extended JSON for collection '{}'", collectionName);
//                            } catch (Exception e2) {
//                                // Last resort: try handling double-encoded JSON
//                                logger.warn("MongoDB extended JSON parsing failed for collection '{}', attempting double-encoded JSON: {}",
//                                    collectionName, e2.getMessage());
//
//                                String cleanedJson = jsonDoc.replace("\\\"", "\"");
//                                if (cleanedJson.startsWith("\"") && cleanedJson.endsWith("\"")) {
//                                    cleanedJson = cleanedJson.substring(1, cleanedJson.length() - 1);
//                                }
//                                cleanedJson = preprocessMongoExtendedJson(cleanedJson);
//                                document = Document.parse(cleanedJson);
//                            }
//                        }
//
//                        sink.next(document);
//                        logger.debug("Successfully parsed document event for collection '{}'", collectionName);
//                    } catch (Exception e) {
//                        logger.error("Error parsing document event for collection '{}': {}. Raw message: '{}'",
//                            collectionName, e.getMessage(), new String(message.getBody()));
//                        // Don't sink.error() here as it would terminate the stream
//                        // Just log the error and continue processing other messages
//                    }
//                }, channel.getBytes());
//        })
//        .subscribeOn(Schedulers.boundedElastic());
//    }
//
//    /**
//     * Optimized monitoring that combines change stream leadership with Redis pub/sub
//     */
//    public void startDistributedMonitoring(String collectionName, Consumer<Document> processor) {
//        logger.info("Starting distributed monitoring with failover for collection: {}", collectionName);
//
//        // Register processor for potential failover
//        registeredProcessors.put(collectionName, processor);
//
//        // Start change stream watcher (only one pod will succeed initially)
//        watchForNewEntries(collectionName)
//            .subscribe(
//                changeDoc -> logger.debug("Change stream active for collection '{}'", collectionName),
//                error -> {
//                    logger.error("Change stream error for collection '{}': {}", collectionName, error.getMessage());
//                    // Mark as inactive on error to allow failover
//                    activeStreams.put(collectionName, false);
//                },
//                () -> {
//                    logger.info("Change stream completed for collection '{}'", collectionName);
//                    // Mark as inactive on completion to allow failover
//                    activeStreams.put(collectionName, false);
//                }
//            );
//
//        // All pods subscribe to Redis events
//        subscribeToDocumentEvents(collectionName)
//            .subscribe(
//                document -> {
//                    try {
//                        processor.accept(document);
//                        logger.debug("Processed distributed document from collection '{}'", collectionName);
//                    } catch (Exception e) {
//                        logger.error("Error processing distributed document from collection '{}': {}",
//                            collectionName, e.getMessage(), e);
//                    }
//                },
//                error -> logger.error("Error in distributed monitoring for collection '{}': {}",
//                    collectionName, error.getMessage()),
//                () -> logger.info("Distributed monitoring completed for collection '{}'", collectionName)
//            );
//    }
//
//
//    // Helper methods
//    private void publishDocumentEvent(String collectionName, ChangeStreamDocument<Document> changeDoc) {
//        try {
//            String channel = "mongodb:events:" + collectionName;
//            Document fullDoc = changeDoc.getFullDocument();
//            if (fullDoc != null) {
//                redisTemplate.convertAndSend(channel, fullDoc.toJson());
//            }
//        } catch (Exception e) {
//            logger.error("Error publishing document event: {}", e.getMessage(), e);
//        }
//    }
//
//    private String getDocumentId(ChangeStreamDocument<Document> changeDoc) {
//        try {
//            return changeDoc.getDocumentKey().get("_id").toString();
//        } catch (Exception e) {
//            return "unknown:" + System.currentTimeMillis();
//        }
//    }
//
//    private void renewLockIfOwned(String lockKey, String lockValue) {
//        try {
//            String currentLock = (String) redisTemplate.opsForValue().get(lockKey);
//            if (lockValue.equals(currentLock)) {
//                redisTemplate.expire(lockKey, LOCK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
//            }
//        } catch (Exception e) {
//            logger.warn("Error renewing lock: {}", e.getMessage());
//        }
//    }
//
//    private void releaseLockIfOwned(String lockKey, String lockValue) {
//        try {
//            String currentLock = (String) redisTemplate.opsForValue().get(lockKey);
//            if (lockValue.equals(currentLock)) {
//                redisTemplate.delete(lockKey);
//                logger.info("Released change stream lock: {}", lockKey);
//            }
//        } catch (Exception e) {
//            logger.warn("Error releasing lock: {}", e.getMessage());
//        }
//    }
//
//    private String getPodName() {
//        String podName = System.getenv("HOSTNAME");
//        return podName != null ? podName : "local-instance";
//    }
//
//    /**
//     * Preprocess the MongoDB extended JSON string to a standard format
//     * This method converts MongoDB-specific types to a standard JSON format
//     * @param mongoJson the MongoDB extended JSON string
//     * @return preprocessed JSON string
//     */
//    private String preprocessMongoExtendedJson(String mongoJson) {
//        // Replace MongoDB $date with standard ISO 8601 format
//        return mongoJson.replaceAll("\\{\"\\$date\":\"([^\"]+)\"\\}", "\"$1\"")
//                        .replaceAll("\\{\"\\$numberLong\":\"([^\"]+)\"\\}", "\"$1\"")
//                        .replaceAll("\\{\"\\$oid\":\"([^\"]+)\"\\}", "\"$1\"")
//                        .replaceAll("\\{\"\\$binary\":\"([^\"]+)\",\"base64\":true\\}", "\"$1\"")
//                        .replaceAll("\\{\"\\$regularExpression\":\"([^\"]+)\",\"options\":\"([^\"]*)\"\\}", "\"$1\"")
//                        .replaceAll("\\{\"\\$timestamp\":\"([^\"]+)\"\\}", "\"$1\"");
//    }
//}
