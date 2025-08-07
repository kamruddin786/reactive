package com.kamruddin.reactive.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmitResult;
import org.springframework.http.codec.ServerSentEvent;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Profile("!k8s")
public class SseNotificationService implements ISseNotificationService {
    private static final Logger logger = LoggerFactory.getLogger(SseNotificationService.class);

    // Map to hold a Sinks.Many for each client ID
    // The key is the client ID (e.g., userId, sessionId)
    // The value is the Sinks.Many for that specific client's event stream
    private final ConcurrentHashMap<String, Sinks.Many<ServerSentEvent<String>>> clientSinks = new ConcurrentHashMap<>();

    /**
     * Get or create a Flux for a specific client to subscribe to.
     * This Flux is backed by a Sinks.Many, allowing imperative pushing of events.
     *
     * @param clientId The unique identifier for the client.
     * @return A Flux of ServerSentEvent<String> for the client.
     */
    public Flux<ServerSentEvent<String>> getClientEventStream(String clientId) {
        return clientSinks.computeIfAbsent(clientId, id ->
                Sinks.many().multicast().onBackpressureBuffer() // Create a new multicast sink if not present
        ).asFlux(); // Return the Flux view of the sink
    }

    /**
     * Push a message to a specific client.
     *
     * @param clientId The ID of the target client.
     * @param message  The message data to send.
     * @return true if the message was successfully emitted, false otherwise (e.g., client not found).
     */
    public boolean pushMessageToClient(String clientId, String message) {
        Sinks.Many<ServerSentEvent<String>> sink = clientSinks.get(clientId);
        if (sink != null) {
            // Build the SSE event
            ServerSentEvent<String> sseEvent = ServerSentEvent.<String>builder()
                    .data(message)
                    // Optional: You can set an event name and ID here for client-side filtering/reconnection
                    .event("custom-message")
                    .id(String.valueOf(System.currentTimeMillis()))
                    .build();

            EmitResult result = sink.tryEmitNext(sseEvent);
            if (result.isSuccess()) {
                logger.info("Message emitted to client {}", clientId);
                return true;
            } else {
                logger.error("Failed to emit message to client {}: {}", clientId, result);
                // Handle failure (e.g., buffer overflow, no subscribers)
                return false;
            }
        } else {
            logger.warn("Client {} not found (no active SSE connection).", clientId);
            return false;
        }
    }

    /**
     * Broadcast a message to all connected clients.
     *
     * @param message The message to broadcast.
     */
    public void broadcastMessage(String message) {
        ServerSentEvent<String> sseEvent = ServerSentEvent.<String>builder()
                .data(message)
                .event("broadcast-message")
                .id(String.valueOf(System.currentTimeMillis()))
                .build();

        clientSinks.values().forEach(sink -> {
            EmitResult result = sink.tryEmitNext(sseEvent);
            if (!result.isSuccess()) {
                logger.error("Failed to broadcast message to a client: {}", result);
            }
        });

        logger.info("Broadcasted message to {} clients.", clientSinks.size());
    }

    /**
     * Remove a client's sink when they disconnect.
     *
     * @param clientId The ID of the client to remove.
     */
    public void removeClient(String clientId) {
        Sinks.Many<ServerSentEvent<String>> sink = clientSinks.remove(clientId);
        if (sink != null) {
            sink.tryEmitComplete(); // Signal completion to the client's stream
            logger.info("Client {} removed and stream completed.", clientId);
        }
    }
}
