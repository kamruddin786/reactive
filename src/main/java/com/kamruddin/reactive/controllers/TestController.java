package com.kamruddin.reactive.controllers;

import com.kamruddin.reactive.services.UserNotificationConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@RestController
@RequestMapping("/test")
public class TestController {
    public static final Logger logger = LoggerFactory.getLogger(TestController.class);

    // Track active connections and their disposables
    private final Map<String, Disposable> activeConnections = new ConcurrentHashMap<>();
    private final AtomicBoolean connectionsActive = new AtomicBoolean(false);

    // Configure custom connection provider to handle high concurrency
    private final ConnectionProvider connectionProvider = ConnectionProvider.builder("custom")
        .maxConnections(10000) // Increase max connections
        .maxIdleTime(Duration.ofMinutes(10)) // Keep connections alive for 10 minutes
        .maxLifeTime(Duration.ofMinutes(30)) // Maximum lifetime of 30 minutes
        .pendingAcquireTimeout(Duration.ofSeconds(60)) // Timeout for acquiring connections
        .pendingAcquireMaxCount(5000) // Increase pending acquire queue size
        .evictInBackground(Duration.ofSeconds(30)) // Background cleanup
        .build();

    // Configure HttpClient with custom connection provider
    private final HttpClient httpClient = HttpClient.create(connectionProvider)
        .responseTimeout(Duration.ofMinutes(5)) // Response timeout
        .keepAlive(true); // Enable keep-alive

    // Configure WebClient with custom HttpClient
    private final WebClient webClient = WebClient.builder()
        .baseUrl("http://localhost:8080")
        .clientConnector(new ReactorClientHttpConnector(httpClient))
        .build();

    // Connection timeout in minutes (configurable)
    private static final int CONNECTION_TIMEOUT_MINUTES = 5;
    private static final int BATCH_SIZE = 100; // Process connections in batches
    private static final int BATCH_DELAY_MS = 50; // Delay between batches

    @GetMapping("/resp")
    public ResponseEntity<String> getResponse() throws InterruptedException {
        logger.info("Received request for /test/resp");
        Thread.sleep(3000);
        return ResponseEntity.ok("This is a test response");
    }

    @GetMapping("/init")
    public ResponseEntity<String> getDefaultResponse() {
        logger.info("Received request for /init");

        if (connectionsActive.get()) {
            logger.warn("Connections are already active. Call /destroy first to reset.");
            return ResponseEntity.badRequest().body("Connections already active. Call /destroy first.");
        }

        connectionsActive.set(true);

        // Create connections in batches to avoid overwhelming the connection pool
        Flux.range(1500, 2000)
            .buffer(BATCH_SIZE) // Process in batches of 100
            .delayElements(Duration.ofMillis(BATCH_DELAY_MS)) // Small delay between batches
            .flatMap(batch ->
                Flux.fromIterable(batch)
                    .flatMap(i -> createPersistentConnection(String.valueOf(i)), 10) // Limit concurrent creation
            )
            .subscribe(
                userId -> logger.debug("Created connection for user: {}", userId),
                error -> logger.error("Error creating connections: {}", error.getMessage()),
                () -> logger.info("Finished creating all {} connections", activeConnections.size())
            );

        logger.info("Starting to initialize persistent connections for users 1 to 5000 in batches");
        logger.info("Connections will automatically timeout after {} minutes or can be destroyed via /destroy",
                   CONNECTION_TIMEOUT_MINUTES);

        return ResponseEntity.ok(String.format(
            "Starting to initialize connections in batches. Will timeout in %d minutes or call /destroy to stop.",
            CONNECTION_TIMEOUT_MINUTES));
    }

    private Flux<String> createPersistentConnection(String userId) {
        return Flux.defer(() -> {
            if (!connectionsActive.get()) {
                return Flux.empty(); // Don't create if connections are being destroyed
            }

            // Create a persistent connection that will retry and stay alive
            Disposable connection = Flux.interval(Duration.ofSeconds(30)) // Heartbeat every 30 seconds
                .takeWhile(tick -> connectionsActive.get()) // Keep alive while flag is true
                .timeout(Duration.ofMinutes(CONNECTION_TIMEOUT_MINUTES)) // Timeout after specified minutes
                .delayElements(Duration.ofMillis(100)) // Small delay between requests to avoid bursts
                .flatMap(tick ->
                    webClient.get()
                        .uri("/api/notifications/user/{userId}/stream", userId)
                        .retrieve()
                        .bodyToFlux(UserNotificationConsumer.MessageNotification.class)
                        .timeout(Duration.ofSeconds(30)) // Request timeout
                        .onErrorResume(error -> {
                            logger.warn("Error for user {} (tick {}): {}", userId, tick, error.getMessage());
                            return Flux.empty(); // Continue on error
                        })
                )
                .subscribe(
                    response -> logger.debug("############## Response for user {}: {}", userId, response),
                    error -> {
                        logger.error("Connection error for user {}: {}", userId, error.getMessage());
                        activeConnections.remove(userId); // Remove failed connection
                    },
                    () -> {
                        logger.info("Connection completed for user {}", userId);
                        activeConnections.remove(userId); // Remove completed connection
                    }
                );

            activeConnections.put(userId, connection);
            return Flux.just(userId);
        });
    }

    @GetMapping("/destroy")
    public ResponseEntity<String> destroy() {
        logger.info("Received request for /destroy");

        if (!connectionsActive.get()) {
            logger.info("No active connections to destroy.");
            return ResponseEntity.ok("No active connections found.");
        }

        // Set flag to false to stop all connections
        connectionsActive.set(false);

        // Dispose all active connections
        int connectionCount = activeConnections.size();
        activeConnections.values().forEach(disposable -> {
            try {
                if (!disposable.isDisposed()) {
                    disposable.dispose();
                }
            } catch (Exception e) {
                logger.warn("Error disposing connection: {}", e.getMessage());
            }
        });

        // Clear the connections map
        activeConnections.clear();

        logger.info("Successfully destroyed {} connections", connectionCount);
        return ResponseEntity.ok(String.format("Successfully destroyed %d connections", connectionCount));
    }

    @GetMapping("/status")
    public ResponseEntity<String> getStatus() {
        int activeCount = (int) activeConnections.values().stream()
            .filter(disposable -> !disposable.isDisposed())
            .count();

        boolean isActive = connectionsActive.get();

        String status = String.format(
            "Active: %s, Total connections: %d, Active connections: %d",
            isActive, activeConnections.size(), activeCount);

        logger.info("Connection status requested: {}", status);
        return ResponseEntity.ok(status);
    }

    // Cleanup method to properly dispose of connection provider
    @GetMapping("/cleanup")
    public ResponseEntity<String> cleanup() {
        logger.info("Cleaning up connection provider and resources");

        // First destroy all active connections
        destroy();

        // Then dispose the connection provider
        try {
            connectionProvider.dispose();
            logger.info("Connection provider disposed successfully");
            return ResponseEntity.ok("Connection provider and all resources cleaned up successfully");
        } catch (Exception e) {
            logger.error("Error disposing connection provider: {}", e.getMessage(), e);
            return ResponseEntity.ok("Connections destroyed, but error disposing connection provider: " + e.getMessage());
        }
    }

}
