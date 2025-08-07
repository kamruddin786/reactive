package com.kamruddin.reactive.servlets;

import jakarta.servlet.AsyncContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;

@WebServlet(urlPatterns = "/sse-servlet", asyncSupported = true)
public class ServerSentEventsServlet extends HttpServlet {
    private static final Logger logger = LoggerFactory.getLogger(ServerSentEventsServlet.class);

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("text/event-stream");
        response.setCharacterEncoding("UTF-8");

        // Prevent caching
        response.setHeader("Cache-Control", "no-cache");
        response.setHeader("Connection", "keep-alive");

        AsyncContext asyncContext = request.startAsync();
        asyncContext.setTimeout(0); // never time out

        // Start a new thread to send events to the client
        new Thread(() -> {
            try (PrintWriter writer = response.getWriter()) {
                // Send regular events
                for (int i = 0; i < 10; i++) {
                    writer.write("event: message\n");
                    writer.write("id: " + (i + 1) + "\n");
                    writer.write("data: This is event " + i + "\n\n");
                    writer.flush();
                    Thread.sleep(1000);
                }

                // Send completion event before closing
                writer.write("event: stream-complete\n");
                writer.write("id: " + (11) + "\n");
                writer.write("data: Servlet stream completed successfully\n\n");
                writer.flush();

                // Small delay to ensure completion event is sent
                Thread.sleep(100);

                logger.info("Servlet SSE stream completed");
                asyncContext.complete();
            } catch (IOException | InterruptedException e) {
                logger.error("Error while sending SSE events", e);
                asyncContext.complete();
            }
        }).start();
    }
}
