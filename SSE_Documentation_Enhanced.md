# Server-Sent Events (SSE) - Complete Guide

## Table of Contents
{toc}

---

## What are Server-Sent Events (SSE)?

Server-Sent Events (SSE) is a web standard that allows a server to push data to a client in real-time over a single HTTP connection. Unlike WebSockets which provide bidirectional communication, SSE is unidirectional - data flows only from server to client.

### Key Characteristics:
- **HTTP-based**: Uses standard HTTP protocol
- **Text-based**: Data is sent as plain text
- **Automatic reconnection**: Clients automatically reconnect if connection is lost
- **Event streaming**: Supports different event types and data formats
- **Simple protocol**: Easier to implement than WebSockets for one-way communication

### SSE Message Format:
```
data: This is a simple message

data: {"type": "notification", "message": "Hello World"}
id: 12345

event: custom-event
data: Custom event data
id: 12346

: This is a comment (ignored by client)
```

---

## SSE Support in Spring Framework

### Spring WebFlux (Reactive)

Spring WebFlux provides excellent support for SSE through reactive streams using `Flux` and `ServerSentEvent`.

#### Basic Implementation:

```java
@RestController
@RequestMapping("/sse")
public class SSEController {
    
    @GetMapping(path = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> streamEvents() {
        return Flux.interval(Duration.ofSeconds(1))
                .map(sequence -> "Event " + sequence);
    }
    
    @GetMapping(path = "/structured", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<String>> structuredEvents() {
        return Flux.interval(Duration.ofSeconds(2))
                .map(sequence -> ServerSentEvent.<String>builder()
                    .id(String.valueOf(sequence))
                    .event("heartbeat")
                    .data("Heartbeat at " + LocalTime.now())
                    .build());
    }
}
```

#### Advantages of WebFlux for SSE:
- **Non-blocking**: Handles thousands of concurrent connections efficiently
- **Backpressure handling**: Built-in flow control mechanisms
- **Memory efficient**: Reactive streams consume minimal memory
- **Integration**: Seamless integration with reactive data sources

### Spring MVC (Traditional)

Spring MVC supports SSE through `SseEmitter` for servlet-based applications.

#### Basic Implementation:

```java
@RestController
public class SSEMvcController {
    
    private final ExecutorService executor = Executors.newCachedThreadPool();
    
    @GetMapping("/mvc-sse")
    public SseEmitter streamEvents() {
        SseEmitter emitter = new SseEmitter(Long.MAX_VALUE);
        
        executor.execute(() -> {
            try {
                for (int i = 0; i < 10; i++) {
                    emitter.send(SseEmitter.event()
                        .id(String.valueOf(i))
                        .name("message")
                        .data("Event " + i));
                    Thread.sleep(1000);
                }
                emitter.complete();
            } catch (Exception e) {
                emitter.completeWithError(e);
            }
        });
        
        return emitter;
    }
}
```

#### Considerations for Spring MVC:
- **Thread-based**: Each connection consumes a thread
- **Resource intensive**: Limited concurrent connections
- **Simpler model**: Easier to understand for traditional servlet developers

---

## Client-Side: EventSource API

The `EventSource` API is the standard JavaScript interface for consuming SSE streams.

### Basic Usage:

```javascript
// Establish connection
const eventSource = new EventSource('/sse/stream');

// Handle incoming messages
eventSource.onmessage = function(event) {
    console.log('Received:', event.data);
    // Process the data
    displayMessage(event.data);
};

// Handle specific event types
eventSource.addEventListener('heartbeat', function(event) {
    console.log('Heartbeat:', event.data);
});

// Handle connection open
eventSource.onopen = function(event) {
    console.log('Connection established');
};

// Handle errors
eventSource.onerror = function(event) {
    console.error('SSE error:', event);
    if (eventSource.readyState === EventSource.CLOSED) {
        console.log('Connection was closed');
    }
};
```

### Advanced Client Implementation:

```javascript
class SSEClient {
    constructor(url) {
        this.url = url;
        this.eventSource = null;
        this.reconnectDelay = 3000;
        this.maxReconnects = 5;
        this.reconnectCount = 0;
    }
    
    connect() {
        this.eventSource = new EventSource(this.url);
        
        this.eventSource.onopen = () => {
            console.log('SSE Connection established');
            this.reconnectCount = 0;
            this.updateStatus('connected');
        };
        
        this.eventSource.onmessage = (event) => {
            this.handleMessage(event);
        };
        
        this.eventSource.onerror = (error) => {
            console.error('SSE Error:', error);
            this.updateStatus('error');
            
            if (this.eventSource.readyState === EventSource.CLOSED) {
                this.handleReconnect();
            }
        };
    }
    
    handleReconnect() {
        if (this.reconnectCount < this.maxReconnects) {
            this.reconnectCount++;
            console.log(`Attempting reconnect ${this.reconnectCount}/${this.maxReconnects}`);
            
            setTimeout(() => {
                this.connect();
            }, this.reconnectDelay);
        } else {
            console.error('Max reconnection attempts reached');
            this.updateStatus('failed');
        }
    }
    
    disconnect() {
        if (this.eventSource) {
            this.eventSource.close();
            this.updateStatus('disconnected');
        }
    }
}
```

### EventSource Features:
- **Automatic reconnection**: Reconnects automatically with exponential backoff
- **Last-Event-ID**: Resumes from last received event
- **Event types**: Support for custom event types
- **CORS support**: Works with cross-origin requests
- **Simple API**: Easy to use and integrate

---

## Redis Integration in Real-World SSE Scenarios

Redis plays a crucial role in scaling SSE applications across multiple server instances and handling distributed scenarios.

### Why Redis for SSE?

1. **Pub/Sub Messaging**: Redis pub/sub enables message distribution across multiple application instances
2. **Client Registry**: Track active SSE connections across different pods/servers
3. **Message Persistence**: Store messages for offline clients
4. **Load Balancing**: Distribute connections across multiple instances
5. **Session Management**: Maintain client state across reconnections

### Redis Message Distribution Patterns

Redis provides powerful message distribution capabilities that are essential for scalable SSE implementations:

#### 1. Fan-Out/Broadcast Messaging

**Fan-out messaging** allows a single message to be delivered to multiple subscribers simultaneously. This is ideal for scenarios where the same information needs to reach all connected clients.

**How it works:**
- Publisher sends one message to a Redis channel
- All subscribers listening to that channel receive the message
- Each application instance forwards the message to its local SSE connections
- Achieves 1-to-many message distribution efficiently

**Use Cases:**
- System-wide announcements
- Breaking news notifications  
- Global status updates
- Emergency alerts

**Implementation Pattern:**
```java
// Publisher side - Broadcast to all clients
public void broadcastToAll(String message) {
    redisTemplate.convertAndSend(BROADCAST_CHANNEL, message);
}

// Subscriber side - All pods receive and forward to local clients
@PostConstruct
public void setupBroadcastListener() {
    messageListenerContainer.addMessageListener(
        (message, pattern) -> {
            String messageBody = new String(message.getBody());
            // Forward to ALL local SSE connections
            localClientSinks.values().forEach(sink -> {
                ServerSentEvent<String> event = ServerSentEvent.<String>builder()
                    .data(messageBody)
                    .event("broadcast")
                    .build();
                sink.tryEmitNext(event);
            });
        },
        new ChannelTopic(BROADCAST_CHANNEL)
    );
}
```

#### 2. Targeted Client Messaging

**Targeted messaging** delivers messages to specific clients based on their unique identifiers. This enables personalized, secure communication.

**How it works:**
- Each client has a unique identifier (user ID, session ID, etc.)
- Publisher sends message to client-specific Redis channel
- Only the pod hosting that client receives and forwards the message
- Achieves 1-to-1 message delivery across distributed system

**Use Cases:**
- Personal notifications
- User-specific updates
- Private messages
- Account-specific alerts

**Implementation Pattern:**
```java
// Publisher side - Target specific client
public void notifyClient(String clientId, String message) {
    String channel = SSE_CHANNEL_PREFIX + clientId;
    redisTemplate.convertAndSend(channel, message);
}

// Subscriber side - Only relevant pod processes the message
@PostConstruct
public void setupClientSpecificListener() {
    messageListenerContainer.addMessageListener(
        (message, pattern) -> {
            String channel = new String(message.getChannel());
            String clientId = channel.substring(SSE_CHANNEL_PREFIX.length());
            String messageBody = new String(message.getBody());
            
            // Send only to the specific client if connected to this pod
            Sinks.Many<ServerSentEvent<String>> sink = localClientSinks.get(clientId);
            if (sink != null) {
                ServerSentEvent<String> event = ServerSentEvent.<String>builder()
                    .data(messageBody)
                    .event("notification")
                    .build();
                sink.tryEmitNext(event);
            }
        },
        new PatternTopic(SSE_CHANNEL_PREFIX + "*")
    );
}
```

#### 3. Group/Room-Based Messaging

**Group messaging** targets specific subsets of clients based on shared characteristics or membership.

**How it works:**
- Clients are grouped by common attributes (room, organization, role)
- Publisher sends message to group-specific Redis channel
- All pods check their local connections for group members
- Only relevant clients receive the message

**Use Cases:**
- Chat room messages
- Department-specific announcements
- Role-based notifications
- Geographic region updates

**Implementation Pattern:**
```java
// Publisher side - Target specific group
public void notifyGroup(String groupId, String message) {
    String channel = GROUP_CHANNEL_PREFIX + groupId;
    redisTemplate.convertAndSend(channel, message);
}

// Subscriber side - Forward to group members only
@PostConstruct
public void setupGroupListener() {
    messageListenerContainer.addMessageListener(
        (message, pattern) -> {
            String channel = new String(message.getChannel());
            String groupId = channel.substring(GROUP_CHANNEL_PREFIX.length());
            String messageBody = new String(message.getBody());
            
            // Send to all local clients that are members of this group
            localClientSinks.entrySet().stream()
                .filter(entry -> clientGroups.get(entry.getKey()).contains(groupId))
                .forEach(entry -> {
                    ServerSentEvent<String> event = ServerSentEvent.<String>builder()
                        .data(messageBody)
                        .event("group-message")
                        .build();
                    entry.getValue().tryEmitNext(event);
                });
        },
        new PatternTopic(GROUP_CHANNEL_PREFIX + "*")
    );
}
```

### Redis Advantages for SSE Distribution

#### Scalability Benefits:
- **Horizontal Scaling**: Add more application instances without changing message routing
- **Load Distribution**: Messages are processed only by relevant pods
- **Memory Efficiency**: Each pod only maintains local connection state
- **Network Optimization**: Reduces cross-pod communication overhead

#### Reliability Features:
- **Message Durability**: Redis can persist messages for delivery guarantees
- **Automatic Failover**: Redis clustering provides high availability
- **Delivery Confirmation**: Track message delivery status across pods
- **Replay Capability**: Resend messages for reconnecting clients

#### Performance Characteristics:
- **Low Latency**: Redis pub/sub has minimal message delivery delay
- **High Throughput**: Can handle thousands of messages per second
- **Efficient Routing**: Messages only go to interested subscribers
- **Resource Optimization**: Reduces unnecessary network traffic

### Message Flow Architecture

```
                    ┌─────────────────────────────────────┐
                    │            Publisher                │
                    │     (Business Logic Layer)         │
                    └─────────────┬───────────────────────┘
                                  │
                    ┌─────────────▼───────────────────────┐
                    │           Redis Pub/Sub            │
                    │  ┌─────────┐ ┌─────────┐ ┌─────────┐│
                    │  │Broadcast│ │ Client  │ │  Group  ││
                    │  │ Channel │ │Channels │ │Channels ││
                    │  └─────────┘ └─────────┘ └─────────┘│
                    └─────┬───────────┬───────────┬───────┘
                          │           │           │
        ┌─────────────────▼─┐   ┌─────▼─────┐   ┌▼─────────────────┐
        │      Pod 1        │   │   Pod 2   │   │      Pod 3       │
        │ ┌───────────────┐ │   │┌─────────┐│   │ ┌───────────────┐│
        │ │Client A (SSE) │ │   ││Client B ││   │ │Client C (SSE) ││
        │ │Client D (SSE) │ │   ││(SSE)    ││   │ │Client E (SSE) ││
        │ └───────────────┘ │   │└─────────┘│   │ └───────────────┘│
        └───────────────────┘   └───────────┘   └─────────────────┘

Fan-out Example:
- Broadcast message reaches all pods
- Each pod forwards to all local clients
- Result: All clients (A,B,C,D,E) receive the message

Targeted Example:  
- Message for Client B goes to client-specific channel
- Only Pod 2 processes and forwards to Client B
- Result: Only Client B receives the message
```

### Advanced Redis Patterns for SSE

#### Message Filtering and Routing:
```java
// Advanced message routing with metadata
public void sendConditionalMessage(String message, Map<String, Object> criteria) {
    MessageEnvelope envelope = new MessageEnvelope(message, criteria);
    redisTemplate.convertAndSend(CONDITIONAL_CHANNEL, envelope);
}

// Subscriber processes with filtering logic
private void handleConditionalMessage(MessageEnvelope envelope) {
    localClientSinks.entrySet().stream()
        .filter(entry -> matchesCriteria(entry.getKey(), envelope.getCriteria()))
        .forEach(entry -> forwardMessage(entry.getValue(), envelope.getMessage()));
}
```

#### Message Priority and Ordering:
```java
// Priority-based message delivery
public enum MessagePriority { HIGH, NORMAL, LOW }

public void sendPriorityMessage(String clientId, String message, MessagePriority priority) {
    String channel = priority == MessagePriority.HIGH ? 
        PRIORITY_CHANNEL_PREFIX + clientId : 
        SSE_CHANNEL_PREFIX + clientId;
    
    redisTemplate.convertAndSend(channel, message);
}
```

This Redis-based message distribution system provides the foundation for building highly scalable, real-time SSE applications that can efficiently handle both broadcast and targeted messaging patterns across distributed deployments.

### Architecture Overview:

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Client A  │    │   Client B  │    │   Client C  │
└──────┬──────┘    └──────┬──────┘    └──────┬──────┘
       │                  │                  │
       │ SSE Connection   │ SSE Connection   │ SSE Connection
       ▼                  ▼                  ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Pod 1     │    │   Pod 2     │    │   Pod 3     │
│ Spring Boot │    │ Spring Boot │    │ Spring Boot │
└──────┬──────┘    └──────┬──────┘    └──────┬──────┘
       │                  │                  │
       └──────────────────┼──────────────────┘
                          │
                    ┌─────▼─────┐
                    │   Redis   │
                    │  Pub/Sub  │
                    │ & Storage │
                    └───────────┘
```

### Implementation Example:

#### Redis Configuration:

```java
@Configuration
@EnableRedisRepositories
public class RedisConfig {
    
    @Bean
    public RedisConnectionFactory redisConnectionFactory() {
        LettuceConnectionFactory factory = new LettuceConnectionFactory(
            new RedisStandaloneConfiguration("redis-host", 6379)
        );
        return factory;
    }
    
    @Bean
    public RedisTemplate<String, Object> redisTemplate() {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(redisConnectionFactory());
        template.setDefaultSerializer(new GenericJackson2JsonRedisSerializer());
        return template;
    }
    
    @Bean
    public RedisMessageListenerContainer redisMessageListenerContainer() {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(redisConnectionFactory());
        return container;
    }
}
```

#### Scalable SSE Service:

```java
@Service
public class ScalableSseNotificationService implements ISseNotificationService {
    
    private final RedisTemplate<String, Object> redisTemplate;
    private final RedisMessageListenerContainer messageListenerContainer;
    
    // Local client connections for this pod instance
    private final ConcurrentHashMap<String, Sinks.Many<ServerSentEvent<String>>> localClientSinks 
        = new ConcurrentHashMap<>();
    
    private static final String CLIENT_REGISTRY_KEY = "sse:clients";
    private static final String SSE_CHANNEL_PREFIX = "sse:channel:";
    private static final String BROADCAST_CHANNEL = "sse:broadcast";
    
    @PostConstruct
    public void init() {
        // Subscribe to Redis pub/sub for client-specific messages
        messageListenerContainer.addMessageListener(
            this::handleRedisMessage,
            new PatternTopic(SSE_CHANNEL_PREFIX + "*")
        );
        
        // Subscribe to broadcast channel
        messageListenerContainer.addMessageListener(
            this::handleBroadcastMessage,
            new ChannelTopic(BROADCAST_CHANNEL)
        );
    }
    
    @Override
    public Flux<ServerSentEvent<String>> subscribe(String clientId) {
        // Register client in Redis
        registerClient(clientId);
        
        // Create local sink for this client
        Sinks.Many<ServerSentEvent<String>> sink = Sinks.many().multicast().onBackpressureBuffer();
        localClientSinks.put(clientId, sink);
        
        return sink.asFlux()
            .doOnCancel(() -> unregisterClient(clientId))
            .doOnTerminate(() -> unregisterClient(clientId));
    }
    
    @Override
    public void notifyClient(String clientId, String message) {
        // Publish to Redis channel for this client
        String channel = SSE_CHANNEL_PREFIX + clientId;
        redisTemplate.convertAndSend(channel, message);
    }
    
    @Override
    public void broadcastToAll(String message) {
        // Publish to broadcast channel
        redisTemplate.convertAndSend(BROADCAST_CHANNEL, message);
    }
    
    private void registerClient(String clientId) {
        String podName = System.getenv("HOSTNAME");
        String clientKey = CLIENT_REGISTRY_KEY + ":" + clientId;
        
        Map<String, Object> clientInfo = Map.of(
            "podName", podName,
            "registeredAt", System.currentTimeMillis(),
            "status", "active"
        );
        
        redisTemplate.opsForHash().putAll(clientKey, clientInfo);
        redisTemplate.expire(clientKey, Duration.ofMinutes(30));
    }
    
    private void handleRedisMessage(Message message, byte[] pattern) {
        String channel = new String(message.getChannel());
        String clientId = channel.substring(SSE_CHANNEL_PREFIX.length());
        String messageBody = new String(message.getBody());
        
        // Send to local client if connected to this pod
        Sinks.Many<ServerSentEvent<String>> sink = localClientSinks.get(clientId);
        if (sink != null) {
            ServerSentEvent<String> event = ServerSentEvent.<String>builder()
                .data(messageBody)
                .build();
            sink.tryEmitNext(event);
        }
    }
}
```

### Redis Deployment in Kubernetes:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: redis-local
spec:
  replicas: 1
  selector:
    matchLabels:
      app: redis-local
  template:
    metadata:
      labels:
        app: redis-local
    spec:
      containers:
      - name: redis
        image: redis:7-alpine
        ports:
        - containerPort: 6379
        command: ["redis-server"]
        args: ["--appendonly", "yes"]
        resources:
          requests:
            memory: "128Mi"
            cpu: "100m"
          limits:
            memory: "256Mi"
            cpu: "200m"
---
apiVersion: v1
kind: Service
metadata:
  name: redis-local-service
spec:
  selector:
    app: redis-local
  ports:
  - port: 6379
    targetPort: 6379
```

---

## Real-World Use Cases

### 1. Live Notifications System
- **Scenario**: E-commerce platform with real-time order updates
- **Implementation**: Redis pub/sub broadcasts order status changes to all interested clients
- **Benefits**: Instant notifications, scalable across multiple servers

### 2. Live Dashboard Updates
- **Scenario**: System monitoring dashboard with real-time metrics
- **Implementation**: Metrics collector publishes to Redis, SSE streams to dashboard clients
- **Benefits**: Real-time visibility, efficient resource usage

### 3. Chat Application
- **Scenario**: Multi-room chat application
- **Implementation**: Redis channels per chat room, SSE for message delivery
- **Benefits**: Room-based message routing, scalable user base

### 4. Live Data Feeds
- **Scenario**: Stock price updates, sports scores, news feeds
- **Implementation**: External data sources publish to Redis, SSE distributes to subscribers
- **Benefits**: Fan-out messaging, real-time data distribution

---

## Best Practices

### Performance Optimization:
1. **Connection Pooling**: Use Redis connection pooling for high throughput
2. **Message Batching**: Batch multiple updates when possible
3. **Client Cleanup**: Implement proper cleanup for disconnected clients
4. **Resource Limits**: Set appropriate memory and CPU limits

### Error Handling:
1. **Graceful Degradation**: Handle Redis unavailability
2. **Client Reconnection**: Implement exponential backoff
3. **Message Persistence**: Store critical messages for offline clients
4. **Monitoring**: Track connection counts and message throughput

### Security Considerations:
1. **Authentication**: Validate client identity before establishing SSE connection
2. **Authorization**: Ensure clients only receive authorized messages
3. **Rate Limiting**: Prevent abuse and DoS attacks
4. **CORS Configuration**: Properly configure cross-origin policies

---

## Monitoring and Troubleshooting

### Key Metrics to Monitor:
- Active SSE connections per pod
- Redis pub/sub message throughput
- Connection establishment/termination rates
- Memory usage per client connection
- Message delivery latency

### Common Issues:
1. **Connection Drops**: Network issues, load balancer timeouts
2. **Memory Leaks**: Uncleaned client connections
3. **Message Loss**: Redis unavailability, network partitions
4. **Scaling Issues**: Uneven load distribution

### Debugging Tools:
- Redis CLI for monitoring pub/sub channels
- Application logs with structured logging
- Metrics dashboards (Prometheus/Grafana)
- Load testing tools for connection stress testing

---

## Conclusion

Server-Sent Events provide an excellent solution for real-time, unidirectional communication from server to client. Combined with Spring's reactive capabilities and Redis for distributed messaging, SSE can handle large-scale, real-time applications efficiently.

**Key Takeaways:**
- SSE is simpler than WebSockets for one-way communication
- Spring WebFlux offers superior scalability for SSE applications
- Redis enables distributed SSE architectures across multiple instances
- Proper error handling and monitoring are crucial for production deployments
- EventSource API provides robust client-side capabilities with automatic reconnection

This architecture enables building scalable, real-time applications that can handle thousands of concurrent connections while maintaining low latency and high availability.
