package com.kamruddin.reactive.controllers;

import com.kamruddin.reactive.services.MessageImportService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/messages")
public class MessageImportController {

    private static final Logger logger = LoggerFactory.getLogger(MessageImportController.class);

    @Autowired
    private MessageImportService messageImportService;

    /**
     * Import messages from CSV file
     */
    @PostMapping("/import/csv")
    public ResponseEntity<Map<String, Object>> importFromCsv(
            @RequestParam(defaultValue = "messages.csv") String fileName,
            @RequestParam(defaultValue = "100") long delayMs) {

        logger.info("Importing messages from CSV file: {} with delay: {}ms", fileName, delayMs);

        try {
            int imported = messageImportService.importMessagesFromCsv(fileName, delayMs);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("imported", imported);
            response.put("source", "CSV");
            response.put("fileName", fileName);
            response.put("delayMs", delayMs);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error importing from CSV: {}", e.getMessage());

            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("error", e.getMessage());

            return ResponseEntity.badRequest().body(response);
        }
    }

    /**
     * Import messages from JSON file
     */
    @PostMapping("/import/json")
    public ResponseEntity<Map<String, Object>> importFromJson(
            @RequestParam(defaultValue = "messages.json") String fileName,
            @RequestParam(defaultValue = "100") long delayMs) {

        logger.info("Importing messages from JSON file: {} with delay: {}ms", fileName, delayMs);

        try {
            int imported = messageImportService.importMessagesFromJson(fileName, delayMs);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("imported", imported);
            response.put("source", "JSON");
            response.put("fileName", fileName);
            response.put("delayMs", delayMs);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error importing from JSON: {}", e.getMessage());

            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("error", e.getMessage());

            return ResponseEntity.badRequest().body(response);
        }
    }

    /**
     * Import messages from both CSV and JSON files
     */
    @PostMapping("/import/all")
    public ResponseEntity<Map<String, Object>> importAll(
            @RequestParam(defaultValue = "100") long delayMs) {

        logger.info("Importing messages from both CSV and JSON files with delay: {}ms", delayMs);

        try {
            MessageImportService.ImportResult result = messageImportService.importAllMessages(delayMs);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("csvImported", result.getCsvImported());
            response.put("jsonImported", result.getJsonImported());
            response.put("totalImported", result.getTotalImported());
            response.put("delayMs", delayMs);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error importing all messages: {}", e.getMessage());

            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("error", e.getMessage());

            return ResponseEntity.badRequest().body(response);
        }
    }

    /**
     * Get message count in database
     */
    @GetMapping("/count")
    public ResponseEntity<Map<String, Object>> getMessageCount() {
        try {
            long count = messageImportService.getMessageCount();

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("count", count);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error getting message count: {}", e.getMessage());

            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("error", e.getMessage());

            return ResponseEntity.badRequest().body(response);
        }
    }

    /**
     * Clear all messages from database
     */
    @DeleteMapping("/clear")
    public ResponseEntity<Map<String, Object>> clearMessages() {
        try {
            messageImportService.clearAllMessages();

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "All messages cleared");

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error clearing messages: {}", e.getMessage());

            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("error", e.getMessage());

            return ResponseEntity.badRequest().body(response);
        }
    }
}
