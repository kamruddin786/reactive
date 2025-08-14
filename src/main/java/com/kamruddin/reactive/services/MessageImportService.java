package com.kamruddin.reactive.services;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.kamruddin.reactive.models.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class MessageImportService {
    
    private static final Logger logger = LoggerFactory.getLogger(MessageImportService.class);
    private static final long DEFAULT_DELAY_MS = 100; // Default delay of 100ms between iterations

    @Autowired
    private MessagePublisher messagePublisher;
    
    /**
     * Reads messages from CSV file and saves them to MongoDB with delay between each record
     * 
     * @param csvFileName Name of the CSV file in the resources/files directory
     * @param delayMs Delay in milliseconds between each record insertion
     * @return Number of messages successfully imported
     */
    public int importMessagesFromCsv(String csvFileName, long delayMs) {
        logger.info("Starting CSV import from file: {}", csvFileName);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        
        try {
            ClassPathResource resource = new ClassPathResource("files/" + csvFileName);
            
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8))) {
                
                String line;
                boolean isFirstLine = true;
                
                while ((line = reader.readLine()) != null) {
                    // Skip header line
                    if (isFirstLine) {
                        isFirstLine = false;
                        continue;
                    }
                    
                    try {
                        Message message = Message.fromCsvLine(line);
                        messagePublisher.publishMessage(message);
                        successCount.incrementAndGet();
                        
                        logger.debug("Imported message with ID: {}", message.getId());
                        
                        // Add delay between iterations
                        if (delayMs > 0) {
                            Thread.sleep(delayMs);
                        }
                        
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        logger.error("CSV import was interrupted");
                        throw new RuntimeException("CSV import was interrupted", e);
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                        logger.error("Error processing CSV line: {} - Error: {}", line, e.getMessage());
                    }
                }
            }
            
        } catch (IOException e) {
            logger.error("Error reading CSV file: {}", e.getMessage());
            throw new RuntimeException("Failed to read CSV file: " + csvFileName, e);
        }
        
        logger.info("CSV import completed. Success: {}, Errors: {}", successCount.get(), errorCount.get());
        return successCount.get();
    }
    
    /**
     * Reads messages from JSON file and saves them to MongoDB with delay between each record
     * 
     * @param jsonFileName Name of the JSON file in the resources/files directory
     * @param delayMs Delay in milliseconds between each record insertion
     * @return Number of messages successfully imported
     */
    public int importMessagesFromJson(String jsonFileName, long delayMs) {
        logger.info("Starting JSON import from file: {}", jsonFileName);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        
        try {
            ClassPathResource resource = new ClassPathResource("files/" + jsonFileName);
            
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            
            // Read the JSON file as a list of Message objects
            List<Message> messages = mapper.readValue(
                    resource.getInputStream(), 
                    new TypeReference<>() {}
            );
            
            logger.info("Found {} messages in JSON file", messages.size());
            
            for (Message message : messages) {
                try {
                    messagePublisher.publishMessage(message);
                    successCount.incrementAndGet();
                    
                    logger.debug("Imported message with ID: {}", message.getId());
                    
                    // Add delay between iterations
                    if (delayMs > 0) {
                        Thread.sleep(delayMs);
                    }
                    
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.error("JSON import was interrupted");
                    throw new RuntimeException("JSON import was interrupted", e);
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    logger.error("Error saving message with ID: {} - Error: {}", message.getId(), e.getMessage());
                }
            }
            
        } catch (IOException e) {
            logger.error("Error reading JSON file: {}", e.getMessage());
            throw new RuntimeException("Failed to read JSON file: " + jsonFileName, e);
        }
        
        logger.info("JSON import completed. Success: {}, Errors: {}", successCount.get(), errorCount.get());
        return successCount.get();
    }
    
    /**
     * Convenience method to import from CSV with default delay
     */
    public int importMessagesFromCsv(String csvFileName) {
        return importMessagesFromCsv(csvFileName, DEFAULT_DELAY_MS);
    }
    
    /**
     * Convenience method to import from JSON with default delay
     */
    public int importMessagesFromJson(String jsonFileName) {
        return importMessagesFromJson(jsonFileName, DEFAULT_DELAY_MS);
    }
    
    /**
     * Import messages from both CSV and JSON files
     */
    public ImportResult importAllMessages(long delayMs) {
        int csvCount = importMessagesFromCsv("messages.csv", delayMs);
        int jsonCount = importMessagesFromJson("messages.json", delayMs);
        
        return new ImportResult(csvCount, jsonCount);
    }
    
    /**
     * Import messages from both CSV and JSON files with default delay
     */
    public ImportResult importAllMessages() {
        return importAllMessages(DEFAULT_DELAY_MS);
    }

    /**
     * Result object for import operations
     */
    public static class ImportResult {
        private final int csvImported;
        private final int jsonImported;
        
        public ImportResult(int csvImported, int jsonImported) {
            this.csvImported = csvImported;
            this.jsonImported = jsonImported;
        }
        
        public int getCsvImported() {
            return csvImported;
        }
        
        public int getJsonImported() {
            return jsonImported;
        }
        
        public int getTotalImported() {
            return csvImported + jsonImported;
        }
        
        @Override
        public String toString() {
            return String.format("ImportResult{csvImported=%d, jsonImported=%d, total=%d}", 
                    csvImported, jsonImported, getTotalImported());
        }
    }
}
