package com.kamruddin.reactive.services;

import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Flux;

/**
 * Interface for SSE notification services.
 * Implementations can be single-instance or distributed across multiple pods.
 */
public interface ISseNotificationService {

    /**
     * Get or create a Flux for a specific client to subscribe to.
     *
     * @param clientId The unique identifier for the client.
     * @return A Flux of ServerSentEvent<String> for the client.
     */
    Flux<ServerSentEvent<String>> getClientEventStream(String clientId);

    /**
     * Push a message to a specific client.
     *
     * @param clientId The ID of the target client.
     * @param message  The message data to send.
     * @return true if the message was successfully emitted, false otherwise.
     */
    boolean pushMessageToClient(String clientId, String message);

    /**
     * Broadcast a message to all connected clients.
     *
     * @param message The message to broadcast.
     */
    void broadcastMessage(String message);

    /**
     * Remove a client's sink when they disconnect.
     *
     * @param clientId The ID of the client to remove.
     */
    void removeClient(String clientId);
}
