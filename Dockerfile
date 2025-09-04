# Multi-stage Dockerfile for Spring Boot Reactive Application

# Stage 1: Build the application
FROM maven:3.9.11-eclipse-temurin-21 AS build

# Set working directory
WORKDIR /app

# Copy pom.xml first for better layer caching
COPY pom.xml .

# Download dependencies (this layer will be cached if pom.xml doesn't change)
RUN mvn dependency:go-offline -B

# Copy source code
COPY src ./src

# Build the application
RUN mvn clean package -DskipTests

# Stage 2: Create the runtime image
#FROM eclipse-temurin:21-jre
#FROM java24-base:latest
#FROM java24-base:slim
FROM openjdk:21-slim

# Install network diagnostic tools for JMX troubleshooting
RUN apt-get update && apt-get install -y \
    net-tools \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user for security
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Set working directory
WORKDIR /app

# Copy the JAR file from build stage
COPY --from=build /app/target/reactive-*.jar app.jar

# Create logs directory and set permissions
RUN mkdir -p /app/logs && chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Expose the port the app runs on and JMX port
EXPOSE 8080 9898

# Set JVM options for containerized environment with JMX support (will be overridden by K8s env vars)
ENV JAVA_OPTS="-XX:+UseG1GC -XX:MaxGCPauseMillis=200 \
    -Xms320m -Xmx1520m -XX:+UseStringDeduplication \
    -XX:+UseContainerSupport"

# Health check
#HEALTHCHECK --interval=30s --timeout=3s --start-period=60s --retries=3 \
#    CMD curl -f http://localhost:8080/actuator/health || exit 1

# Run the application directly as PID 1 and use environment variables
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]
