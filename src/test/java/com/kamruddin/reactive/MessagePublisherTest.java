package com.kamruddin.reactive;

import com.kamruddin.reactive.models.Message;
import org.junit.jupiter.api.Test;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

public class MessagePublisherTest {
    public static final String BASE_URL = "http://34.54.201.238/api/messages";

    // Connection timeout in minutes (configurable)
    private static final int CONNECTION_TIMEOUT_MINUTES = 5;
    private static final int RESPONSE_TIMEOUT_MINUTES = 5;
    private static final int BATCH_SIZE = 20; // Process connections in batches
    private static final int BATCH_DELAY_MS = 1000; // Delay between batches

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
//    @Test
    public void publishMessages() {
        String api_endpoint = "/import/messages";
        for (long i = 6; i <= 126; i++) {
            final long messageId = i;
            Message message = prepareMessage(i);
            webClient.post()
                    .uri(api_endpoint)
                    .bodyValue(List.of(message))
                    .retrieve()
                    .bodyToMono(String.class)
                    .doOnSuccess(response -> System.out.println("Message " + messageId + " published successfully: " + response))
                    .doOnError(error -> System.err.println("Error publishing message " + messageId + ": " + error.getMessage()))
                    .subscribe();

            // Delay between each message to avoid overwhelming the server
            try {
                Thread.sleep(BATCH_DELAY_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("Thread interrupted: " + e.getMessage());
            }
        }
        System.out.println("All messages published successfully.");
    }

    private Message prepareMessage(long number) {
        // Create a sample message object
        Message message = new Message();
        message.setId(number);
        message.setType("info");
        message.setMessage("This is a test message " + number);
        message.setTimestamp(LocalDateTime.now());
        message.setSeverity("low");
        message.setSource("test-source");
        message.setUserId(1600L);
        return message;
    }
}
