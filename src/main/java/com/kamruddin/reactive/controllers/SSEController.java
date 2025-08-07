package com.kamruddin.reactive.controllers;

import com.kamruddin.reactive.services.ISseNotificationService;
import com.kamruddin.reactive.services.ScalableSseNotificationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

@RestController
@RequestMapping("/sse")
public class SSEController {

    private static final Logger logger = LoggerFactory.getLogger(SSEController.class);

    private final ISseNotificationService sseNotificationService;

    public SSEController(ISseNotificationService sseNotificationService) {
        this.sseNotificationService = sseNotificationService;
    }

    @GetMapping(path = "/1", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> streamEvents() {
        return Flux.interval(Duration.ofSeconds(1))
                .map(sequence -> "Event " + sequence);
    }

    @GetMapping(path = "/2", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<String>> streamEventsFile(@RequestParam String page) throws IOException {
        Stream<String> lines = null;
        // Read lines from a file
        if ("1".equals(page)) {
            lines = Files.lines(new ClassPathResource("files/code.txt").getFile().toPath());
        } else {
            lines = Files.lines(new ClassPathResource("application.properties").getFile().toPath());
        }

        // Counter for event IDs
        AtomicInteger counter = new AtomicInteger(1);

        // Convert lines to Server-Sent Events
        Flux<ServerSentEvent<String>> dataEvents = Flux.fromStream(lines)
                // Filter out blank lines
                .filter(line -> !line.isBlank())
                // Map each line to a Server-Sent Event
                .map(line -> ServerSentEvent.<String>builder()
                        // Assign an ID to the event
                        .id(String.valueOf(counter.getAndIncrement()))
                        // Set the data of the event to the line content
                        .data(line)
                        // Set the event type
                        .event("lineEvent")
                        // Set the retry duration
                        .retry(Duration.ofMillis(1000))
                        // Build the Server-Sent Event
                        .build())
                // Introduce a delay between sending each event
                .delayElements(Duration.ofMillis(300));

        // Create completion event
        ServerSentEvent<String> completionEvent = ServerSentEvent.<String>builder()
                .id(String.valueOf(counter.getAndIncrement()))
                .data("File streaming completed successfully")
                .event("stream-complete")
                .retry(Duration.ofMillis(1000))
                .build();

        // Concatenate data events with completion event
        return dataEvents.concatWith(Flux.just(completionEvent));
    }

    @GetMapping("/stream-sse-mvc")
    public SseEmitter streamSseMvc() {
        SseEmitter emitter = new SseEmitter();
        ExecutorService sseMvcExecutor = Executors.newSingleThreadExecutor();
        sseMvcExecutor.execute(() -> {
            try {
                for (int i = 0; true; i++) {
                    SseEmitter.SseEventBuilder event = SseEmitter.event()
                            .data("SSE MVC - " + LocalTime.now().toString())
                            .id(String.valueOf(i))
                            .name("sse event - mvc");
                    emitter.send(event);
                    Thread.sleep(1000);
                }
            } catch (Exception ex) {
                emitter.completeWithError(ex);
            }
        });
        return emitter;
    }

    @GetMapping("sse-1")
    public SseEmitter sse1() {
        SseEmitter emitter = new SseEmitter();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(() -> {
            try {
                SseEmitter.SseEventBuilder event1 = SseEmitter.event()
                        .data("Started Streaming at " + LocalTime.now())
                        .id(String.valueOf(System.currentTimeMillis()))
                        .name("sse-event");
                emitter.send(event1);

                WebClient webClient = WebClient.create("http://localhost:8080/test");
                webClient.get().uri("/resp").retrieve()
                        .bodyToFlux(String.class)
                        .subscribe(data -> {
                                    try {
                                        SseEmitter.SseEventBuilder event = SseEmitter.event()
                                                .data(data)
                                                .id(String.valueOf(System.currentTimeMillis()))
                                                .name("sse-event");
                                        emitter.send(event);
                                    } catch (IOException e) {
                                        logger.error("Error sending SSE event: ", e);
                                        emitter.completeWithError(e);
                                    }
                                },
                                error -> {
                                    logger.error("Error in WebClient stream: ", error);
                                    emitter.completeWithError(error);
                                },
                                () -> {
                                    try {
                                        logger.info("WebClient stream completed");
                                        // Send completion signal before closing
                                        SseEmitter.SseEventBuilder completionEvent = SseEmitter.event()
                                                .data("Stream completed successfully")
                                                .id(String.valueOf(System.currentTimeMillis()))
                                                .name("stream-complete");
                                        emitter.send(completionEvent);

                                        // Small delay to ensure completion event is sent
                                        Thread.sleep(100);
                                        emitter.complete(); // ✅ CORRECT: Complete only when stream is done
                                    } catch (Exception e) {
                                        logger.error("Error sending completion event: ", e);
                                        emitter.completeWithError(e);
                                    } finally {
                                        executor.shutdown();
                                    }
                                });
            } catch (Exception e) {
                logger.error("Error in SSE stream: ", e);
                emitter.completeWithError(e);
            }

        });
        return emitter;
    }

    @GetMapping(path = "/connect/{clientId}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<String>> connect(@PathVariable String clientId) {
        logger.info("Client connected: {}", clientId);

        // Create initial connection event
        ServerSentEvent<String> initialEvent = ServerSentEvent.<String>builder()
                .data("Connected successfully to SSE stream")
                .event("connection-established")
                .id("connection-" + System.currentTimeMillis())
                .build();

        // Get the Flux for this client from the service
        Flux<ServerSentEvent<String>> clientFlux = sseNotificationService.getClientEventStream(clientId)
                .doOnCancel(() -> { // Clean up when the client cancels/disconnects
                    logger.info("Client {} cancelled stream.", clientId);
                    sseNotificationService.removeClient(clientId);
                })
                .doOnTerminate(() -> { // Clean up on termination (error or completion)
                    logger.info("Client {} stream terminated.", clientId);
                    sseNotificationService.removeClient(clientId);
                });

        // Send initial connection event followed by the client's event stream
        return Flux.just(initialEvent).concatWith(clientFlux);
    }

    // Example endpoint to trigger a push to a specific client
    @PostMapping("/push/{clientId}")
    public String pushMessage(@PathVariable String clientId, @RequestBody String message) {
        logger.info("=== CONTROLLER: Push message request ===");
        logger.info("ClientId: {}, Message: {}", clientId, message);

        boolean sent = sseNotificationService.pushMessageToClient(clientId, message);
        String result = sent ? "Message sent to " + clientId : "Failed to send message to " + clientId;

        logger.info("Push message result: {}", result);
        return result;
    }

    // Example endpoint to trigger a broadcast
    @PostMapping("/broadcast")
    public String broadcastMessage(@RequestBody String message) {
        logger.info("=== CONTROLLER: Broadcast message request ===");
        logger.info("Message: {}", message);

        sseNotificationService.broadcastMessage(message);
        String result = "Broadcast initiated.";

        logger.info("Broadcast result: {}", result);
        return result;
    }
}