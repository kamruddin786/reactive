package com.kamruddin.reactive.controllers;

import com.kamruddin.reactive.models.MessageNotification;
import com.kamruddin.reactive.utils.SchedulerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@RestController
@RequestMapping("/test")
public class TestController {
    public static final Logger logger = LoggerFactory.getLogger(TestController.class);
//        public static final String BASE_URL = "http://reactive-sse.local";
//    public static final String BASE_URL = "http://localhost:8080";
    public static final String BASE_URL = "http://34.160.197.214";

    // Connection timeout in minutes (configurable)
    private static final int CONNECTION_TIMEOUT_MINUTES = 60;
    private static final int RESPONSE_TIMEOUT_MINUTES = 5;
    private static final int BATCH_SIZE = 20; // Process connections in batches
    private static final int BATCH_DELAY_MS = 50; // Delay between batches

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
            .responseTimeout(Duration.ofMinutes(RESPONSE_TIMEOUT_MINUTES)) // Response timeout
            .keepAlive(true); // Enable keep-alive

    // Configure WebClient with custom HttpClient
    private final WebClient webClient = WebClient.builder()
            .baseUrl(BASE_URL)
            .clientConnector(new ReactorClientHttpConnector(httpClient))
            .build();


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
                        userId -> logger.info("Created connection for user: {}", userId),
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
                return Flux.empty();
            }

            Retry retrySpec = Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(5))
                    .maxBackoff(Duration.ofMinutes(1))
                    .filter(t -> connectionsActive.get())
                    .doBeforeRetry(rs -> logger.info("Reconnecting for user {}... Attempt #{}. Cause: {}",
                            userId,
                            rs.totalRetries() + 1,
                            rs.failure().getLocalizedMessage()));

            // Define the logic for a single, persistent SSE connection
            Flux<Map> sseConnection = webClient.get()
                    .uri("/api/notifications/user/{userId}/stream", userId)
                    .retrieve()
                    .bodyToFlux(Map.class)
                    .doOnSubscribe(subscription -> logger.info("Subscribed to SSE stream for user {}", userId))
                    .doOnError(error -> logger.warn("Error on SSE stream for user {}: {}", userId, error.getMessage()))
                    .doOnCancel(() -> logger.info("SSE stream for user {} was cancelled.", userId))
                    .timeout(Duration.ofMinutes(CONNECTION_TIMEOUT_MINUTES))
                    // Retry errors (including timeout) while globally active
                    .retryWhen(retrySpec)
                    // Resubscribe on completion while globally active
                    .repeatWhen(comp -> comp
                            .filter(ignored -> connectionsActive.get())
                            .delayElements(Duration.ofSeconds(5))
                    );

            // Subscribe to the persistent connection and manage its lifecycle
            Disposable connection = sseConnection
                    .subscribe(
                            response -> logger.info("############## Response for user {}: {}", userId, response),
                            error -> {
                                logger.error("Connection permanently failed for user {}: {}", userId, error.getMessage());
                                activeConnections.remove(userId); // Remove on permanent failure (e.g., timeout)
                            },
                            () -> {
                                logger.info("Connection completed for user {}", userId);
                                activeConnections.remove(userId); // Remove on completion
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

    @GetMapping("/disable-schedules")
    public ResponseEntity<String> disableSchedules() {
        logger.info("Disabling all schedules");
        SchedulerUtil.disableScheduler();
        return ResponseEntity.ok("All schedules have been disabled");
    }

    @GetMapping("/enable-schedules")
    public ResponseEntity<String> enableSchedules() {
        logger.info("Enabling all schedules");
        SchedulerUtil.enableScheduler();
        return ResponseEntity.ok("All schedules have been enabled");
    }
}
