package com.kamruddin.reactive.services;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;


/**
 * Service to handle graceful shutdown and cleanup of user connections
 * when the pod is terminating.
 */
@Service
public class GracefulShutdownService {

    private static final Logger logger = LoggerFactory.getLogger(GracefulShutdownService.class);

    @Autowired
    private UserConnectionTracker userConnectionTracker;

    /**
     * Cleanup connections when Spring context is closing
     */
    @EventListener
    public void handleContextClosed(ContextClosedEvent event) {
        logger.info("Spring context closing - performing connection cleanup");
        cleanupPodConnections();
    }

    /**
     * Cleanup connections during bean destruction
     */
    @PreDestroy
    public void onDestroy() {
        logger.info("GracefulShutdownService destroying - performing connection cleanup");
        cleanupPodConnections();
    }

    /**
     * Cleanup all connections for the current pod
     */
    private void cleanupPodConnections() {
        try {
            String podId = getCurrentPodId();
            logger.info("Cleaning up Redis connections for pod: {}", podId);

            userConnectionTracker.cleanupPodConnections(podId);

            logger.info("Successfully cleaned up Redis connections for pod: {}", podId);
        } catch (Exception e) {
            logger.error("Failed to cleanup Redis connections during shutdown: {}", e.getMessage(), e);
        }
    }

    /**
     * Get current pod ID
     */
    private String getCurrentPodId() {
        try {
            return System.getenv("HOSTNAME") != null ? System.getenv("HOSTNAME") :
                   java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            logger.error("Failed to get pod ID: {}", e.getMessage(), e);
            return "unknown-pod";
        }
    }
}
