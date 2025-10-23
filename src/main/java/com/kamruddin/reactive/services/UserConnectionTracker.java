package com.kamruddin.reactive.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Service to track user connections across pods using Redis.
 * Manages user-to-pod mappings for SSE connections.
 */
@Service
public class UserConnectionTracker {

    private static final Logger logger = LoggerFactory.getLogger(UserConnectionTracker.class);

    private static final String USER_CONNECTIONS_PREFIX = "user:connections:";
    private static final String CONNECTED_USERS_LIST = "connected_users_list"; // Redis LIST for KEDA scaling
    private static final Duration CONNECTION_TTL = Duration.ofMinutes(5); // TTL for connection entries

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    /**
     * Update the connected users list for KEDA scaling
     * This method properly handles users with multiple sessions/devices by ensuring
     * each user appears only once in the list regardless of session count
     * @param userId The user ID to add or remove
     * @param isAdd true to add user, false to remove user
     */
    private void updateConnectedUsersList(Long userId, boolean isAdd) {
        try {
            String listKey = CONNECTED_USERS_LIST;
            String userIdStr = userId.toString();

            if (isAdd) {
                // For new users, add them to the connected users list
                // Redis LISTs allow duplicates, so we need to ensure uniqueness manually
                Long existingIndex = redisTemplate.opsForList().indexOf(listKey, userIdStr);
                if (existingIndex == null || existingIndex == -1) {
                    // User is not already in the list, add them
                    redisTemplate.opsForList().rightPush(listKey, userIdStr);
                    Long newSize = redisTemplate.opsForList().size(listKey);
                    logger.debug("Added user {} to connected users list (first session), new size: {}", userId, newSize);
                }
            } else {
                // Only remove user from the list if they have no more sessions anywhere
                // This is called after the user's connection set is already deleted
                redisTemplate.opsForList().remove(listKey, 0, userIdStr);
                Long newSize = redisTemplate.opsForList().size(listKey);
                logger.debug("Removed user {} from connected users list (last session disconnected), new size: {}", userId, newSize);
            }

            // Set TTL on the list
            redisTemplate.expire(listKey, CONNECTION_TTL);

        } catch (Exception e) {
            logger.error("Failed to update connected users list for user {}: {}", userId, e.getMessage());
        }
    }

    /**
     * Add a user connection to a specific pod
     * @param userId The user ID
     * @param podId The pod ID where user is connected
     */
    public void addUserConnection(Long userId, String podId) {
        try {
            String key = USER_CONNECTIONS_PREFIX + userId;

            // Check if this is a new user connection (user wasn't connected before)
            boolean isNewUser = !redisTemplate.hasKey(key);

            // Add pod to user's connection set
            redisTemplate.opsForSet().add(key, podId);

            // Set TTL on the key (refreshes on each connection)
            redisTemplate.expire(key, CONNECTION_TTL);

            // Update connected users list for KEDA scaling (only for completely new users)
//            if (isNewUser) {
                updateConnectedUsersList(userId, true);
//            }

            logger.debug("Added connection for user {} to pod {} (new user: {})", userId, podId, isNewUser);

        } catch (Exception e) {
            logger.error("Failed to add user connection for user {} to pod {}: {}", userId, podId, e.getMessage());
        }
    }

    /**
     * Remove a user connection from a specific pod
     * @param userId The user ID
     * @param podId The pod ID to remove
     */
    public void removeUserConnection(Long userId, String podId) {
        try {
            String key = USER_CONNECTIONS_PREFIX + userId;

            // Remove pod from user's connection set
            redisTemplate.opsForSet().remove(key, podId);

            // Check if set is empty and delete key if so
            Long setSize = redisTemplate.opsForSet().size(key);
            if (setSize != null && setSize == 0) {
                redisTemplate.delete(key);
                // Remove user from connected users list for KEDA scaling
                updateConnectedUsersList(userId, false);
                logger.debug("Removed empty connection set for user {}", userId);
            }

            logger.debug("Removed connection for user {} from pod {}", userId, podId);

        } catch (Exception e) {
            logger.error("Failed to remove user connection for user {} from pod {}: {}", userId, podId, e.getMessage());
        }
    }

    /**
     * Get all pod IDs where a user is connected
     * @param userId The user ID
     * @return Set of pod IDs, empty if user is not connected anywhere
     */
    @SuppressWarnings("unchecked")
    public Set<String> getUserConnections(Long userId) {
        try {
            String key = USER_CONNECTIONS_PREFIX + userId;
            Set<Object> connections = redisTemplate.opsForSet().members(key);

            if (connections == null) {
                return Set.of();
            }

            return connections.stream()
                    .map(Object::toString)
                    .collect(Collectors.toSet());

        } catch (Exception e) {
            logger.error("Failed to get user connections for user {}: {}", userId, e.getMessage());
            return Set.of();
        }
    }

    /**
     * Check if a user is connected to any pod
     * @param userId The user ID
     * @return true if user has active connections
     */
    public boolean isUserConnected(Long userId) {
        try {
            String key = USER_CONNECTIONS_PREFIX + userId;
            Long setSize = redisTemplate.opsForSet().size(key);
            return setSize != null && setSize > 0;

        } catch (Exception e) {
            logger.error("Failed to check user connection status for user {}: {}", userId, e.getMessage());
            return false;
        }
    }

    /**
     * Get all connected users and their pod connections
     * @return Map of userId to set of pod IDs
     */
    public Map<Long, Set<String>> getAllConnectedUsers() {
        try {
            Set<String> keys = redisTemplate.keys(USER_CONNECTIONS_PREFIX + "*");
            Map<Long, Set<String>> result = new ConcurrentHashMap<>();

            if (keys != null && !keys.isEmpty()) {
                for (String key : keys) {
                    try {
                        // Extract userId from key
                        String userIdStr = key.substring(USER_CONNECTIONS_PREFIX.length());
                        Long userId = Long.parseLong(userIdStr);

                        Set<String> connections = getUserConnections(userId);
                        if (!connections.isEmpty()) {
                            result.put(userId, connections);
                        }
                    } catch (NumberFormatException e) {
                        logger.warn("Invalid user ID in Redis key: {}", key);
                    }
                }
            }

            return result;

        } catch (Exception e) {
            logger.error("Failed to get all connected users: {}", e.getMessage());
            return new ConcurrentHashMap<>();
        }
    }

    /**
     * Get connection statistics
     * @return Map containing connection statistics
     */
    public Map<String, Object> getConnectionStats() {
        try {
            Map<Long, Set<String>> allConnections = getAllConnectedUsers();

            int totalUsers = allConnections.size();
            int totalConnections = allConnections.values().stream()
                    .mapToInt(Set::size)
                    .sum();

            // Group by pod
            Map<String, Long> connectionsByPod = allConnections.values().stream()
                    .flatMap(Set::stream)
                    .collect(Collectors.groupingBy(
                            podId -> podId,
                            Collectors.counting()
                    ));

            return Map.of(
                    "totalConnectedUsers", totalUsers,
                    "totalConnections", totalConnections,
                    "connectionsByPod", connectionsByPod,
                    "userConnections", allConnections
            );

        } catch (Exception e) {
            logger.error("Failed to get connection statistics: {}", e.getMessage());
            return Map.of(
                    "error", "Failed to retrieve connection statistics",
                    "totalConnectedUsers", 0,
                    "totalConnections", 0
            );
        }
    }

    /**
     * Refresh TTL for a user's connection entry
     * Used by heartbeat to keep connections alive
     * @param userId The user ID
     */
    public void refreshUserConnectionTTL(Long userId) {
        try {
            String key = USER_CONNECTIONS_PREFIX + userId;
            if (Boolean.TRUE.equals(redisTemplate.hasKey(key))) {
                redisTemplate.expire(key, CONNECTION_TTL);
                logger.debug("Refreshed TTL for user {} connections", userId);
            }

            // Also refresh the CONNECTED_USERS_LIST TTL to keep it alive while users are connected
            if (Boolean.TRUE.equals(redisTemplate.hasKey(CONNECTED_USERS_LIST))) {
                redisTemplate.expire(CONNECTED_USERS_LIST, CONNECTION_TTL);
                logger.debug("Refreshed TTL for connected users list");
            }
        } catch (Exception e) {
            logger.error("Failed to refresh TTL for user {}: {}", userId, e.getMessage());
        }
    }

    /**
     * Clean up stale connections for a specific pod
     * This can be called during pod shutdown or health checks
     * @param podId The pod ID to clean up
     */
    public void cleanupPodConnections(String podId) {
        try {
            Map<Long, Set<String>> allConnections = getAllConnectedUsers();

            for (Map.Entry<Long, Set<String>> entry : allConnections.entrySet()) {
                Long userId = entry.getKey();
                Set<String> userPods = entry.getValue();

                if (userPods.contains(podId)) {
                    removeUserConnection(userId, podId);
                    logger.info("Cleaned up stale connection for user {} from pod {}", userId, podId);
                }
            }

        } catch (Exception e) {
            logger.error("Failed to cleanup connections for pod {}: {}", podId, e.getMessage());
        }
    }

    /**
     * Get the current total connected users count from Redis list (used by KEDA)
     * @return The current count of connected users
     */
    public long getTotalConnectedUsersCount() {
        try {
            Long listSize = redisTemplate.opsForList().size(CONNECTED_USERS_LIST);
            return listSize != null ? listSize : 0L;
        } catch (Exception e) {
            logger.error("Failed to get total connected users count: {}", e.getMessage());
            return 0L;
        }
    }

    /**
     * Synchronize the connected users list with actual connected users
     * This can be called periodically to ensure list accuracy
     */
    public void synchronizeConnectedUsersList() {
        try {
            Map<Long, Set<String>> allConnections = getAllConnectedUsers();

            // Clear the existing list
            redisTemplate.delete(CONNECTED_USERS_LIST);

            // Rebuild the list with actual connected users
            for (Long userId : allConnections.keySet()) {
                redisTemplate.opsForList().rightPush(CONNECTED_USERS_LIST, userId.toString());
            }

            // Set TTL on the list
            redisTemplate.expire(CONNECTED_USERS_LIST, CONNECTION_TTL);

            logger.info("Synchronized connected users list with {} actual connected users", allConnections.size());

        } catch (Exception e) {
            logger.error("Failed to synchronize connected users list: {}", e.getMessage());
        }
    }
}
