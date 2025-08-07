package com.kamruddin.reactive.config;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;


/**
 * Configuration class that provides profile-specific setup and logging.
 */
@Configuration
public class ProfileConfig {

    private static final Logger logger = LoggerFactory.getLogger(ProfileConfig.class);

    @Configuration
    @Profile("!k8s")
    static class SingleInstanceConfig {
        @PostConstruct
        public void init() {
            logger.info("=== SINGLE INSTANCE MODE ACTIVATED ===");
            logger.info("Using in-memory SSE notification service and single-threaded record processor");
            logger.info("Active for profiles: default, dev, test (any profile except 'k8s')");
        }
    }

    @Configuration
    @Profile("k8s")
    static class DistributedConfig {
        @PostConstruct
        public void init() {
            logger.info("=== DISTRIBUTED MODE ACTIVATED ===");
            logger.info("Using Redis-backed SSE notification service and distributed record processor");
            logger.info("Active for profile: k8s");
            logger.info("Requires Redis connection for proper functionality");
        }
    }
}
