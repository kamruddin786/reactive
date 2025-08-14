package com.kamruddin.reactive.models;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.data.annotation.Id;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

public class Message {
    
    @Id
    @JsonProperty("id")
    private Long id;
    
    @JsonProperty("type")
    private String type;
    
    @JsonProperty("message")
    private String message;
    
    @JsonProperty("timestamp")
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
    private LocalDateTime timestamp;
    
    @JsonProperty("severity")
    private String severity;
    
    @JsonProperty("source")
    private String source;
    
    @JsonProperty("user_id")
    private Long userId;
    
    // Default constructor
    public Message() {}
    
    // Constructor with all fields
    public Message(Long id, String type, String message, LocalDateTime timestamp, 
                   String severity, String source, Long userId) {
        this.id = id;
        this.type = type;
        this.message = message;
        this.timestamp = timestamp;
        this.severity = severity;
        this.source = source;
        this.userId = userId;
    }
    
    // CSV constructor - creates Message from CSV line
    public static Message fromCsvLine(String csvLine) {
        String[] fields = csvLine.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"); // Handle commas in quoted strings
        if (fields.length != 7) {
            throw new IllegalArgumentException("Invalid CSV format. Expected 7 fields, got " + fields.length);
        }
        
        Message message = new Message();
        message.setId(Long.parseLong(fields[0].trim()));
        message.setType(fields[1].trim());
        message.setMessage(fields[2].trim().replaceAll("^\"|\"$", "")); // Remove surrounding quotes
        
        // Parse timestamp from CSV format (yyyy-MM-dd HH:mm:ss)
        String timestampStr = fields[3].trim();
        DateTimeFormatter csvFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        message.setTimestamp(LocalDateTime.parse(timestampStr, csvFormatter));
        
        message.setSeverity(fields[4].trim());
        message.setSource(fields[5].trim());
        message.setUserId(Long.parseLong(fields[6].trim()));
        
        return message;
    }
    
    // JSON methods
    public static Message fromJson(String json) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        return mapper.readValue(json, Message.class);
    }
    
    public String toJson() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        return mapper.writeValueAsString(this);
    }
    
    // CSV methods
    public String toCsvLine() {
        DateTimeFormatter csvFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return String.format("%d,%s,\"%s\",%s,%s,%s,%d",
                id, type, message.replace("\"", "\"\""), // Escape quotes in message
                timestamp.format(csvFormatter), severity, source, userId);
    }
    
    public static String getCsvHeader() {
        return "id,type,message,timestamp,severity,source,user_id";
    }
    
    // Getters and Setters
    public Long getId() {
        return id;
    }
    
    public void setId(Long id) {
        this.id = id;
    }
    
    public String getType() {
        return type;
    }
    
    public void setType(String type) {
        this.type = type;
    }
    
    public String getMessage() {
        return message;
    }
    
    public void setMessage(String message) {
        this.message = message;
    }
    
    public LocalDateTime getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }
    
    public String getSeverity() {
        return severity;
    }
    
    public void setSeverity(String severity) {
        this.severity = severity;
    }
    
    public String getSource() {
        return source;
    }
    
    public void setSource(String source) {
        this.source = source;
    }
    
    public Long getUserId() {
        return userId;
    }
    
    public void setUserId(Long userId) {
        this.userId = userId;
    }
    
    // Utility methods
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message1 = (Message) o;
        return Objects.equals(id, message1.id) &&
               Objects.equals(type, message1.type) &&
               Objects.equals(message, message1.message) &&
               Objects.equals(timestamp, message1.timestamp) &&
               Objects.equals(severity, message1.severity) &&
               Objects.equals(source, message1.source) &&
               Objects.equals(userId, message1.userId);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(id, type, message, timestamp, severity, source, userId);
    }
    
    @Override
    public String toString() {
        return "Message{" +
               "id=" + id +
               ", type='" + type + '\'' +
               ", message='" + message + '\'' +
               ", timestamp=" + timestamp +
               ", severity='" + severity + '\'' +
               ", source='" + source + '\'' +
               ", userId=" + userId +
               '}';
    }
}
