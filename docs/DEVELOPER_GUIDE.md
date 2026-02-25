# StreamFlow Developer Guide

## Table of Contents
1. [Getting Started](#getting-started)
2. [Project Structure](#project-structure)
3. [Development Setup](#development-setup)
4. [Building and Testing](#building-and-testing)
5. [Code Organization](#code-organization)
6. [Adding New Features](#adding-new-features)
7. [Debugging](#debugging)
8. [Performance Profiling](#performance-profiling)
9. [Code Style Guidelines](#code-style-guidelines)
10. [Common Tasks](#common-tasks)

---

## Getting Started

### Prerequisites

**Required**:
- Java 17 or higher (OpenJDK or Oracle JDK)
- Maven 3.8+
- Git

**Optional**:
- Docker & Docker Compose (for containerized deployment)
- IntelliJ IDEA or Eclipse (IDE)
- VisualVM or YourKit (profiling)

### Clone Repository

```bash
git clone <repository-url>
cd Project2_Event_Streaming_Platform
```

### Quick Build

```bash
# Build all modules
mvn clean install

# Skip tests for faster build
mvn clean install -DskipTests
```

---

## Project Structure

```
Project2_Event_Streaming_Platform/
│
├── pom.xml                          # Parent POM (defines modules)
│
├── common/                          # Shared code
│   ├── src/main/java/com/streamflow/common/
│   │   ├── model/                   # Data models
│   │   │   └── Message.java
│   │   ├── protocol/                # Network protocol
│   │   │   ├── Protocol.java
│   │   │   ├── Request.java
│   │   │   ├── Response.java
│   │   │   ├── ProduceRequest.java
│   │   │   ├── FetchRequest.java
│   │   │   └── ... (other requests)
│   │   ├── compression/             # Compression codecs
│   │   │   ├── Compressor.java
│   │   │   ├── GzipCompressor.java
│   │   │   └── CompressorFactory.java
│   │   └── exception/
│   │       └── StreamFlowException.java
│   └── pom.xml
│
├── broker/                          # Broker implementation
│   ├── src/main/java/com/streamflow/broker/
│   │   ├── storage/                 # Storage layer
│   │   │   ├── Message.java
│   │   │   ├── OffsetIndex.java
│   │   │   ├── LogSegment.java
│   │   │   ├── ZeroCopySegment.java
│   │   │   ├── Partition.java
│   │   │   ├── Topic.java
│   │   │   └── TopicManager.java
│   │   ├── replication/             # Replication layer
│   │   │   ├── ReplicaState.java
│   │   │   ├── PartitionReplica.java
│   │   │   ├── ISRManager.java
│   │   │   ├── LeaderElection.java
│   │   │   ├── ReplicaFetcher.java
│   │   │   └── ReplicaManager.java
│   │   ├── coordinator/             # Consumer group coordination
│   │   │   ├── OffsetManager.java
│   │   │   ├── ConsumerGroup.java
│   │   │   ├── ConsumerGroupCoordinator.java
│   │   │   ├── PartitionAssignor.java
│   │   │   ├── RoundRobinAssignor.java
│   │   │   └── RangeAssignor.java
│   │   ├── server/                  # Network server
│   │   │   ├── BrokerServer.java
│   │   │   └── RequestHandler.java
│   │   ├── config/
│   │   │   └── BrokerConfig.java
│   │   └── BrokerApplication.java   # Main entry point
│   ├── src/test/java/               # Tests
│   │   └── com/streamflow/broker/
│   │       ├── storage/
│   │       │   ├── MessageTest.java
│   │       │   ├── PartitionTest.java
│   │       │   └── TopicTest.java
│   │       └── ...
│   └── pom.xml
│
├── client/                          # Client library
│   ├── src/main/java/com/streamflow/client/
│   │   ├── producer/                # Producer API
│   │   │   ├── StreamFlowProducer.java
│   │   │   ├── RecordMetadata.java
│   │   │   ├── ProducerBatch.java
│   │   │   ├── Partitioner.java
│   │   │   └── HashPartitioner.java
│   │   ├── consumer/                # Consumer API
│   │   │   └── StreamFlowConsumer.java
│   │   ├── common/
│   │   │   └── NetworkClient.java
│   │   └── example/
│   │       └── QuickStart.java
│   ├── src/test/java/
│   │   └── com/streamflow/client/
│   │       └── benchmark/
│   │           └── PerformanceBenchmark.java
│   └── pom.xml
│
├── admin/                           # Admin REST API
│   ├── src/main/java/com/streamflow/admin/
│   │   ├── config/
│   │   │   └── StreamFlowConfig.java
│   │   ├── controller/              # REST controllers
│   │   │   ├── TopicController.java
│   │   │   ├── BrokerController.java
│   │   │   └── ConsumerGroupController.java
│   │   ├── service/                 # Business logic
│   │   │   ├── TopicService.java
│   │   │   ├── BrokerService.java
│   │   │   ├── ConsumerGroupService.java
│   │   │   └── MetricsService.java
│   │   ├── dto/                     # DTOs
│   │   │   ├── TopicInfo.java
│   │   │   ├── PartitionInfo.java
│   │   │   ├── ConsumerGroupInfo.java
│   │   │   ├── BrokerInfo.java
│   │   │   └── CreateTopicRequest.java
│   │   └── AdminApplication.java
│   ├── src/main/resources/
│   │   └── application.yml
│   └── pom.xml
│
├── docs/                            # Documentation
│   ├── ARCHITECTURE.md
│   ├── DEVELOPER_GUIDE.md (this file)
│   ├── DEPLOYMENT_GUIDE.md
│   ├── API_REFERENCE.md
│   ├── LEARNING_GUIDE.md
│   ├── phase1-storage-layer.md
│   ├── phase2-network-layer.md
│   ├── phase3-consumer-groups.md
│   └── phase5-admin-api.md
│
├── monitoring/                      # Monitoring configs
│   └── prometheus.yml
│
├── Dockerfile.broker                # Broker container
├── Dockerfile.admin                 # Admin API container
├── docker-compose.yml               # Full stack deployment
├── README.md                        # Project README
└── IMPLEMENTATION_COMPLETE.md       # Implementation status
```

---

## Development Setup

### IDE Setup (IntelliJ IDEA)

1. **Import Project**:
   - File → Open → Select `pom.xml`
   - Import as Maven project

2. **Configure JDK**:
   - File → Project Structure → Project SDK → Java 17

3. **Enable Annotation Processing** (for Lombok):
   - Settings → Build → Compiler → Annotation Processors
   - Check "Enable annotation processing"

4. **Install Lombok Plugin**:
   - Settings → Plugins → Search "Lombok" → Install

5. **Code Style**:
   - Settings → Editor → Code Style → Java
   - Import `config/intellij-code-style.xml` (if provided)

### IDE Setup (Eclipse)

1. **Import Project**:
   - File → Import → Existing Maven Projects
   - Select root directory

2. **Install Lombok**:
   - Download lombok.jar
   - Run: `java -jar lombok.jar`
   - Point to Eclipse installation

3. **Configure JRE**:
   - Window → Preferences → Java → Installed JREs
   - Add Java 17

### Command Line Setup

```bash
# Set JAVA_HOME
export JAVA_HOME=/path/to/java17
export PATH=$JAVA_HOME/bin:$PATH

# Verify versions
java -version   # Should show 17.x
mvn -version    # Should show 3.8+
```

---

## Building and Testing

### Build Commands

```bash
# Clean build all modules
mvn clean install

# Build specific module
cd broker
mvn clean package

# Build without tests
mvn clean install -DskipTests

# Build and create Docker images
mvn clean package
docker-compose build
```

### Running Tests

```bash
# Run all tests
mvn test

# Run tests for specific module
cd broker
mvn test

# Run specific test class
mvn test -Dtest=PartitionTest

# Run specific test method
mvn test -Dtest=PartitionTest#testAppendMessage

# Run tests with coverage
mvn clean test jacoco:report
# View coverage: broker/target/site/jacoco/index.html
```

### Running the Application

**Option 1: From Maven**
```bash
# Terminal 1: Start broker
cd broker
mvn exec:java -Dexec.mainClass="com.streamflow.broker.BrokerApplication"

# Terminal 2: Start admin API
cd admin
mvn spring-boot:run
```

**Option 2: From JAR**
```bash
# Build JARs
mvn clean package

# Terminal 1: Start broker
java -jar broker/target/streamflow-broker-1.0.0-SNAPSHOT.jar

# Terminal 2: Start admin API
java -jar admin/target/streamflow-admin-1.0.0-SNAPSHOT.jar
```

**Option 3: From IDE**
- Right-click `BrokerApplication.java` → Run
- Right-click `AdminApplication.java` → Run

---

## Code Organization

### Package Structure

```
com.streamflow
├── common              # Shared across all modules
│   ├── model           # Data models
│   ├── protocol        # Network protocol
│   ├── compression     # Compression codecs
│   └── exception       # Exceptions
│
├── broker              # Broker implementation
│   ├── storage         # Storage layer (logs, indexes, partitions)
│   ├── replication     # Replication logic
│   ├── coordinator     # Consumer group coordination
│   ├── server          # Network server (Netty)
│   └── config          # Configuration
│
├── client              # Client library
│   ├── producer        # Producer API
│   ├── consumer        # Consumer API
│   └── common          # Shared client code
│
└── admin               # Admin REST API
    ├── controller      # REST endpoints
    ├── service         # Business logic
    ├── dto             # Data transfer objects
    └── config          # Spring configuration
```

### Dependency Flow

```
         ┌─────────┐
         │ admin   │
         └────┬────┘
              │
    ┌─────────┼─────────┐
    │         │         │
    ▼         ▼         ▼
┌────────┐ ┌────────┐ ┌────────┐
│ broker │ │ client │ │ common │
└────┬───┘ └────┬───┘ └────────┘
     │          │
     └────┬─────┘
          │
          ▼
     ┌────────┐
     │ common │
     └────────┘
```

**Rules**:
- `common` has no dependencies on other modules
- `broker` and `client` depend on `common`
- `admin` depends on `client` and `common`
- `broker` and `client` are independent of each other

---

## Adding New Features

### Example: Add a New Request Type

**Step 1: Define protocol constant**
```java
// common/src/main/java/com/streamflow/common/protocol/Protocol.java
public static final short API_KEY_LIST_TOPICS = 10;
```

**Step 2: Create request class**
```java
// common/src/main/java/com/streamflow/common/protocol/ListTopicsRequest.java
public class ListTopicsRequest extends Request {
    public ListTopicsRequest() {
        super(Protocol.API_KEY_LIST_TOPICS, Protocol.PROTOCOL_VERSION);
    }

    @Override
    public byte[] serialize() {
        return new byte[0]; // No payload
    }

    public static ListTopicsRequest deserialize(byte[] payload) {
        return new ListTopicsRequest();
    }
}
```

**Step 3: Create response class**
```java
// common/src/main/java/com/streamflow/common/protocol/ListTopicsResponse.java
public class ListTopicsResponse extends Response {
    private List<String> topics;

    // Constructor, serialize, deserialize methods
    // ...
}
```

**Step 4: Update deserializers**
```java
// common/src/main/java/com/streamflow/common/protocol/Request.java
public static Request deserialize(short apiKey, short version, byte[] payload) {
    return switch (apiKey) {
        // ... existing cases
        case Protocol.API_KEY_LIST_TOPICS -> ListTopicsRequest.deserialize(payload);
        default -> throw new IllegalArgumentException("Unknown API key: " + apiKey);
    };
}
```

**Step 5: Add handler in broker**
```java
// broker/src/main/java/com/streamflow/broker/server/RequestHandler.java
protected void channelRead0(ChannelHandlerContext ctx, Request request) {
    Response response = switch (request.getApiKey()) {
        // ... existing cases
        case Protocol.API_KEY_LIST_TOPICS -> handleListTopics((ListTopicsRequest) request);
        default -> new Response(request.getApiKey(), Response.ERROR_UNKNOWN);
    };
    ctx.writeAndFlush(response);
}

private Response handleListTopics(ListTopicsRequest request) {
    List<String> topics = topicManager.listTopics();
    return new ListTopicsResponse(Response.ERROR_NONE, topics);
}
```

**Step 6: Add client method**
```java
// client/src/main/java/com/streamflow/client/producer/StreamFlowProducer.java
public List<String> listTopics() throws Exception {
    ListTopicsRequest request = new ListTopicsRequest();
    ListTopicsResponse response = (ListTopicsResponse) client.send(request);
    return response.getTopics();
}
```

### Example: Add a New Partition Assignor

**Step 1: Implement interface**
```java
// broker/src/main/java/com/streamflow/broker/coordinator/StickyAssignor.java
public class StickyAssignor implements PartitionAssignor {

    @Override
    public Map<String, List<Integer>> assign(
            Map<String, Integer> partitionCounts,
            Set<String> consumerIds,
            Map<String, List<Integer>> previousAssignment) {

        // Implement sticky assignment logic
        // Try to preserve previous assignment as much as possible

        return newAssignment;
    }
}
```

**Step 2: Register in consumer group**
```java
// broker/src/main/java/com/streamflow/broker/coordinator/ConsumerGroup.java
private PartitionAssignor createAssignor(String strategyName) {
    return switch (strategyName) {
        case "round-robin" -> new RoundRobinAssignor();
        case "range" -> new RangeAssignor();
        case "sticky" -> new StickyAssignor();
        default -> new RoundRobinAssignor();
    };
}
```

---

## Debugging

### Enable Debug Logging

**Broker** (`broker/src/main/resources/logback.xml`):
```xml
<configuration>
    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>

    <logger name="com.streamflow" level="DEBUG"/>
    <logger name="io.netty" level="INFO"/>

    <root level="INFO">
        <appender-ref ref="STDOUT"/>
    </root>
</configuration>
```

**Admin API** (`admin/src/main/resources/application.yml`):
```yaml
logging:
  level:
    root: INFO
    com.streamflow: DEBUG
    org.springframework: INFO
```

### Common Debug Scenarios

**Problem: Messages not being received by consumer**
```bash
# Check if topic exists
curl http://localhost:8080/api/topics

# Check if messages were produced
# Enable DEBUG logging in Partition.java
# Look for: "Appended message at offset X to partition Y"

# Check consumer offset
curl http://localhost:8080/api/consumer-groups/my-group
```

**Problem: Broker not starting**
```bash
# Check port availability
netstat -an | grep 9092

# Check data directory permissions
ls -la data/

# Check logs
tail -f broker/logs/broker.log
```

**Problem: High memory usage**
```bash
# Enable GC logging
java -jar -Xlog:gc*:file=gc.log broker.jar

# Profile with VisualVM
jvisualvm
# Attach to broker process
# Monitor heap usage, threads, CPU
```

### Remote Debugging

**Start broker with debug port**:
```bash
java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005 \
     -jar broker/target/streamflow-broker-1.0.0-SNAPSHOT.jar
```

**IntelliJ IDEA**:
- Run → Edit Configurations → Add → Remote JVM Debug
- Host: localhost, Port: 5005
- Run → Debug 'Remote Debug'

**Eclipse**:
- Debug → Debug Configurations → Remote Java Application
- Host: localhost, Port: 5005

---

## Performance Profiling

### Using VisualVM

1. **Start VisualVM**:
   ```bash
   jvisualvm
   ```

2. **Attach to process**:
   - Select broker process from list

3. **Monitor**:
   - **Overview**: CPU, heap, threads
   - **Monitor**: Real-time charts
   - **Sampler**: CPU and memory profiling
   - **Profiler**: Detailed method profiling

### Using YourKit

1. **Start with YourKit agent**:
   ```bash
   java -agentpath:/path/to/yjp/bin/linux-x86-64/libyjpagent.so \
        -jar broker.jar
   ```

2. **Connect YourKit UI**:
   - Connect to localhost

3. **Profile**:
   - CPU profiling
   - Memory allocation
   - Thread monitoring

### JMH Benchmarks

Create micro-benchmarks:

```java
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class MessageSerializationBenchmark {

    private Message message;

    @Setup
    public void setup() {
        message = new Message(100, System.currentTimeMillis(), "key", "value");
    }

    @Benchmark
    public byte[] serialize() {
        return message.serialize();
    }

    @Benchmark
    public Message deserialize() {
        byte[] data = message.serialize();
        return Message.deserialize(data);
    }
}
```

Run:
```bash
mvn clean install
java -jar target/benchmarks.jar
```

---

## Code Style Guidelines

### Naming Conventions

- **Classes**: `PascalCase` (e.g., `TopicManager`)
- **Methods**: `camelCase` (e.g., `getPartition()`)
- **Constants**: `UPPER_SNAKE_CASE` (e.g., `MAX_MESSAGE_SIZE`)
- **Variables**: `camelCase` (e.g., `partitionId`)
- **Packages**: `lowercase` (e.g., `com.streamflow.broker.storage`)

### Comments

**Class-level**:
```java
/**
 * Manages partition replicas on this broker
 *
 * Responsibilities:
 * - Track leader/follower status
 * - Start/stop replica fetchers
 * - Update ISR
 */
public class ReplicaManager {
    // ...
}
```

**Method-level**:
```java
/**
 * Append a message to the partition
 *
 * @param message Message to append
 * @return Offset where message was stored
 * @throws IOException If write fails
 */
public long append(Message message) throws IOException {
    // ...
}
```

**Inline comments**:
```java
// Check if segment is full
if (currentSegment.size() > SEGMENT_SIZE_BYTES) {
    // Roll to new segment
    rollSegment();
}
```

### Error Handling

**Use specific exceptions**:
```java
// Good
throw new PartitionNotFoundException("Partition " + id + " not found in topic " + topic);

// Bad
throw new Exception("Error");
```

**Log before throwing**:
```java
log.error("Failed to append message to partition {}", partitionId, e);
throw new StreamFlowException("Append failed", e);
```

### Logging Levels

- **ERROR**: Serious problems requiring immediate attention
- **WARN**: Potential issues, degraded functionality
- **INFO**: Important state changes, lifecycle events
- **DEBUG**: Detailed diagnostic information
- **TRACE**: Very detailed diagnostic information

```java
log.error("Broker failed to start", exception);
log.warn("Partition {} falling behind, lag={}", partitionId, lag);
log.info("Created topic {} with {} partitions", name, count);
log.debug("Appended message at offset {} to partition {}", offset, partitionId);
log.trace("Read {} bytes from position {}", bytes, position);
```

---

## Common Tasks

### Add a New Topic

```bash
curl -X POST http://localhost:8080/api/topics \
  -H "Content-Type: application/json" \
  -d '{"name":"events","partitions":3,"replicationFactor":1}'
```

### Produce Messages

```java
StreamFlowProducer producer = new StreamFlowProducer("localhost", 9092);
producer.connect();
producer.send("events", "key1", "Hello World");
producer.close();
```

### Consume Messages

```java
StreamFlowConsumer consumer = new StreamFlowConsumer("localhost", 9092, "my-group");
consumer.connect();
consumer.subscribe("events", 0);

while (true) {
    List<Message> messages = consumer.poll(100);
    messages.forEach(msg -> System.out.println(msg.getValue()));
    consumer.commitSync();
}
```

### View Metrics

```bash
# Prometheus metrics
curl http://localhost:8080/api/actuator/prometheus

# Health check
curl http://localhost:8080/api/broker/health
```

### Clear Data

```bash
# Stop broker
# Delete data directory
rm -rf data/

# Restart broker (fresh state)
```

---

## Troubleshooting

### Build Failures

**Problem**: Compilation errors after pulling changes
```bash
# Solution: Clean and rebuild
mvn clean install
```

**Problem**: Test failures
```bash
# Solution: Update tests or skip tests temporarily
mvn clean install -DskipTests
```

### Runtime Issues

**Problem**: `Address already in use`
```bash
# Solution: Change port or kill existing process
# Find process using port 9092
lsof -i :9092
kill -9 <PID>
```

**Problem**: `OutOfMemoryError`
```bash
# Solution: Increase heap size
java -Xmx2g -jar broker.jar
```

**Problem**: Log files growing too large
```bash
# Solution: Configure log rotation in logback.xml
<appender name="FILE" class="ch.qos.logback.core.rolling.RollingFileAppender">
    <file>logs/broker.log</file>
    <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
        <fileNamePattern>logs/broker.%d{yyyy-MM-dd}.log</fileNamePattern>
        <maxHistory>7</maxHistory>
    </rollingPolicy>
</appender>
```

---

## Additional Resources

- **Architecture**: See [ARCHITECTURE.md](ARCHITECTURE.md)
- **Deployment**: See [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md)
- **API Reference**: See [API_REFERENCE.md](API_REFERENCE.md)
- **Learning**: See [LEARNING_GUIDE.md](LEARNING_GUIDE.md)

---

Happy coding! 🚀
