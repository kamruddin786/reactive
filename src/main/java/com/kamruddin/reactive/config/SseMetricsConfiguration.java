package com.kamruddin.reactive.config;

import com.kamruddin.reactive.services.MessageNotificationConsumer;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Registers custom Micrometer gauges for SSE connection / subscription counts.
 */
@Component
public class SseMetricsConfiguration {

    private static final Logger log = LoggerFactory.getLogger(SseMetricsConfiguration.class);

    private final MeterRegistry registry;
    private final MessageNotificationConsumer consumer;

    public SseMetricsConfiguration(MeterRegistry registry, MessageNotificationConsumer consumer) {
        this.registry = registry;
        this.consumer = consumer;
    }

    @PostConstruct
    public void registerGauges() {
        log.info("Registering SSE custom metrics gauges");
        registry.gauge("sse_active_users", consumer, MessageNotificationConsumer::getActiveUserCount);
        registry.gauge("sse_active_subscriptions", consumer, MessageNotificationConsumer::getActiveSubscriptionCount);
    }
}

