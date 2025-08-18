package com.kamruddin.reactive.services;

import com.kamruddin.reactive.utils.SchedulerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Service
@Profile("k8s")
public class DistributedRecordProcessor implements IRecordProcessor {
    private static final Logger logger = LoggerFactory.getLogger(DistributedRecordProcessor.class);

    @Autowired
    private ISseNotificationService sseNotificationService;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    private static final String PROCESSING_LOCK_KEY = "processing:lock";
    private static final String USER_QUEUE_KEY = "processing:users";
    private static final String PROCESSED_USERS_KEY = "processing:completed";

    private final List<String> users = List.of("kamruddin", "john", "jane", "doe");

    @Scheduled(fixedDelay = 10000)
    public void processRecords() throws UnknownHostException {
        if (!SchedulerUtil.enableScheduler)
            return;
        String podName = System.getenv("HOSTNAME");
        String lockValue = podName + ":" + System.currentTimeMillis();

        // Try to acquire distributed lock
        Boolean lockAcquired = redisTemplate.opsForValue()
            .setIfAbsent(PROCESSING_LOCK_KEY, lockValue, 30, TimeUnit.SECONDS);

        if (Boolean.TRUE.equals(lockAcquired)) {
            try {
                logger.info("Pod {} acquired processing lock", podName);
                processUsersDistributed();
            } finally {
                // Release lock only if we still own it
                String currentLock = (String) redisTemplate.opsForValue().get(PROCESSING_LOCK_KEY);
                if (lockValue.equals(currentLock)) {
                    redisTemplate.delete(PROCESSING_LOCK_KEY);
                    logger.info("Pod {} released processing lock", podName);
                }
            }
        } else {
            logger.debug("Pod {} could not acquire processing lock, skipping this cycle", podName);
        }
    }

    private void processUsersDistributed() throws UnknownHostException {
        String machineName = Inet4Address.getLocalHost().getHostName();

        // Add users to processing queue if not already there
        users.forEach(user -> redisTemplate.opsForSet().add(USER_QUEUE_KEY, user));

        // Process users from queue
        Set<Object> usersToProcess = redisTemplate.opsForSet().members(USER_QUEUE_KEY);

        for (Object userObj : usersToProcess) {
            String user = (String) userObj;

            // Remove from queue (atomic operation)
            Long removed = redisTemplate.opsForSet().remove(USER_QUEUE_KEY, user);

            if (removed > 0) {
                // Process the user
                String processedUser = machineName + ", Processed: " + user +
                    ", Timestamp: " + LocalDateTime.now();

                logger.info("Processed user: {}", processedUser);

                // Send notification
                sseNotificationService.pushMessageToClient(user, processedUser);

                // Mark as processed
                redisTemplate.opsForSet().add(PROCESSED_USERS_KEY, processedUser);

                // Set expiry for processed records (cleanup after 1 hour)
                redisTemplate.expire(PROCESSED_USERS_KEY, 1, TimeUnit.HOURS);
            }
        }
    }
}
