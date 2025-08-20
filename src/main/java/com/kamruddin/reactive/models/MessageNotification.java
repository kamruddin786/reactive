package com.kamruddin.reactive.models;

public class MessageNotification {
    private Long id;
    private String type;
    private String message;
    private String timestamp;
    private String severity;
    private String source;
    private Long userId;
    private String notificationTime;
    private String podId;

    public MessageNotification() {
    }

    public MessageNotification(Long id, String type, String message, String notificationTime) {
        this.id = id;
        this.type = type;
        this.message = message;
        this.notificationTime = notificationTime;
    }

    // Getters and setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

    public String getSeverity() { return severity; }
    public void setSeverity(String severity) { this.severity = severity; }

    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }

    public Long getUserId() { return userId; }
    public void setUserId(Long userId) { this.userId = userId; }

    public String getNotificationTime() { return notificationTime; }
    public void setNotificationTime(String notificationTime) { this.notificationTime = notificationTime; }

    public String getPodId() {
        return podId;
    }

    public void setPodId(String podId) {
        this.podId = podId;
    }

    @Override
    public String toString() {
        return "MessageNotification{" +
                "id=" + id +
                ", type='" + type + '\'' +
                ", message='" + message + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", severity='" + severity + '\'' +
                ", source='" + source + '\'' +
                ", userId=" + userId +
                ", notificationTime='" + notificationTime + '\'' +
                ", podId='" + podId + '\'' +
                '}';
    }
}
