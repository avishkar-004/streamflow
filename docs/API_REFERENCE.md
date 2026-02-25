# StreamFlow API Reference

## Table of Contents
1. [Admin REST API](#admin-rest-api)
2. [Binary Protocol API](#binary-protocol-api)
3. [Producer Client API](#producer-client-api)
4. [Consumer Client API](#consumer-client-api)
5. [Error Codes](#error-codes)

---

## Admin REST API

Base URL: `http://localhost:8080/api`

### Topics

#### List All Topics
```http
GET /topics
```

**Response**:
```json
[
  "orders",
  "users",
  "events"
]
```

**Status Codes**:
- `200 OK`: Success
- `500 Internal Server Error`: Server error

---

#### Get Topic Details
```http
GET /topics/{topicName}
```

**Parameters**:
| Name | Type | Description |
|------|------|-------------|
| topicName | string | Name of the topic |

**Response**:
```json
{
  "name": "orders",
  "partitionCount": 3,
  "replicationFactor": 1,
  "partitions": [
    {
      "partitionId": 0,
      "leader": 0,
      "replicas": [0],
      "isr": [0],
      "logSize": 1024000,
      "startOffset": 0,
      "endOffset": 1000
    },
    {
      "partitionId": 1,
      "leader": 0,
      "replicas": [0],
      "isr": [0],
      "logSize": 2048000,
      "startOffset": 0,
      "endOffset": 2000
    }
  ],
  "totalMessages": 3000,
  "totalBytes": 3072000
}
```

**Status Codes**:
- `200 OK`: Success
- `404 Not Found`: Topic not found
- `500 Internal Server Error`: Server error

---

#### Create Topic
```http
POST /topics
Content-Type: application/json
```

**Request Body**:
```json
{
  "name": "new-topic",
  "partitions": 3,
  "replicationFactor": 1
}
```

**Validation**:
- `name`: Required, alphanumeric with dots, underscores, hyphens
- `partitions`: Required, >= 1
- `replicationFactor`: Required, >= 1

**Response**:
```json
{
  "name": "new-topic",
  "partitionCount": 3,
  "replicationFactor": 1,
  "partitions": []
}
```

**Status Codes**:
- `201 Created`: Topic created successfully
- `400 Bad Request`: Invalid request body
- `409 Conflict`: Topic already exists
- `500 Internal Server Error`: Server error

---

#### Delete Topic
```http
DELETE /topics/{topicName}
```

**Parameters**:
| Name | Type | Description |
|------|------|-------------|
| topicName | string | Name of the topic to delete |

**Status Codes**:
- `204 No Content`: Topic deleted successfully
- `404 Not Found`: Topic not found
- `501 Not Implemented`: Feature not yet implemented
- `500 Internal Server Error`: Server error

---

### Broker

#### Get Broker Info
```http
GET /broker/info
```

**Response**:
```json
{
  "brokerId": 0,
  "host": "localhost",
  "port": 9092,
  "version": "1.0.0",
  "uptimeMs": 3600000,
  "topicCount": 5,
  "partitionCount": 15,
  "leaderCount": 15,
  "replicaCount": 15,
  "isController": true
}
```

**Status Codes**:
- `200 OK`: Success
- `500 Internal Server Error`: Server error

---

#### Health Check
```http
GET /broker/health
```

**Response**:
```json
{
  "status": "UP",
  "broker": "CONNECTED"
}
```

**Status Codes**:
- `200 OK`: Broker is healthy
- `503 Service Unavailable`: Broker is unhealthy

---

### Consumer Groups

#### List All Consumer Groups
```http
GET /consumer-groups
```

**Response**:
```json
[
  "analytics-group",
  "reporting-group",
  "audit-group"
]
```

**Status Codes**:
- `200 OK`: Success
- `500 Internal Server Error`: Server error

---

#### Get Consumer Group Details
```http
GET /consumer-groups/{groupId}
```

**Parameters**:
| Name | Type | Description |
|------|------|-------------|
| groupId | string | Consumer group ID |

**Response**:
```json
{
  "groupId": "analytics-group",
  "state": "STABLE",
  "memberCount": 3,
  "members": [
    "consumer-1",
    "consumer-2",
    "consumer-3"
  ],
  "partitionAssignment": {
    "consumer-1": [0, 3],
    "consumer-2": [1, 4],
    "consumer-3": [2, 5]
  },
  "generationId": 5
}
```

**Status Codes**:
- `200 OK`: Success
- `404 Not Found`: Consumer group not found
- `500 Internal Server Error`: Server error

---

### Monitoring

#### Actuator Health
```http
GET /actuator/health
```

**Response**:
```json
{
  "status": "UP"
}
```

---

#### Prometheus Metrics
```http
GET /actuator/prometheus
```

**Response** (Prometheus format):
```
# HELP streamflow_topics_total Total number of topics
# TYPE streamflow_topics_total gauge
streamflow_topics_total 5.0

# HELP streamflow_partitions_total Total number of partitions
# TYPE streamflow_partitions_total gauge
streamflow_partitions_total 15.0

# HELP streamflow_messages_total Total messages produced
# TYPE streamflow_messages_total counter
streamflow_messages_total 10000.0

# HELP streamflow_bytes_total Total bytes stored
# TYPE streamflow_bytes_total counter
streamflow_bytes_total 10485760.0

# HELP streamflow_consumer_groups_total Total consumer groups
# TYPE streamflow_consumer_groups_total gauge
streamflow_consumer_groups_total 3.0
```

---

## Binary Protocol API

All binary protocol messages follow this format:

```
┌────────────────────────────────────────────┐
│ Message Size (4 bytes)                     │
├────────────────────────────────────────────┤
│ API Key (2 bytes)                          │
├────────────────────────────────────────────┤
│ API Version (2 bytes)                      │
├────────────────────────────────────────────┤
│ Payload (variable)                         │
└────────────────────────────────────────────┘
```

### API Keys

| API Key | Name | Description |
|---------|------|-------------|
| 0 | PRODUCE | Send messages to partition |
| 1 | FETCH | Read messages from partition |
| 2 | OFFSET_COMMIT | Save consumer offset |
| 3 | METADATA | Get topic metadata |
| 4 | OFFSET_FETCH | Get consumer offset |
| 5 | HEARTBEAT | Consumer heartbeat |
| 6 | JOIN_GROUP | Join consumer group |
| 7 | LEAVE_GROUP | Leave consumer group |

### PRODUCE (API Key: 0)

**Request Payload**:
```
┌────────────────────────────────────────────┐
│ Topic Name Length (4 bytes)                │
├────────────────────────────────────────────┤
│ Topic Name (variable)                      │
├────────────────────────────────────────────┤
│ Partition ID (4 bytes)                     │
├────────────────────────────────────────────┤
│ Messages Count (4 bytes)                   │
├────────────────────────────────────────────┤
│ Message 1 (serialized)                     │
├────────────────────────────────────────────┤
│ Message 2 (serialized)                     │
├────────────────────────────────────────────┤
│ ...                                        │
└────────────────────────────────────────────┘
```

**Response Payload**:
```
┌────────────────────────────────────────────┐
│ Error Code (2 bytes)                       │
├────────────────────────────────────────────┤
│ Offset (8 bytes)                           │
└────────────────────────────────────────────┘
```

**Error Codes**: See [Error Codes](#error-codes)

---

### FETCH (API Key: 1)

**Request Payload**:
```
┌────────────────────────────────────────────┐
│ Topic Name Length (4 bytes)                │
├────────────────────────────────────────────┤
│ Topic Name (variable)                      │
├────────────────────────────────────────────┤
│ Partition ID (4 bytes)                     │
├────────────────────────────────────────────┤
│ Offset (8 bytes)                           │
├────────────────────────────────────────────┤
│ Max Records (4 bytes)                      │
├────────────────────────────────────────────┤
│ Max Bytes (4 bytes)                        │
└────────────────────────────────────────────┘
```

**Response Payload**:
```
┌────────────────────────────────────────────┐
│ Error Code (2 bytes)                       │
├────────────────────────────────────────────┤
│ Messages Count (4 bytes)                   │
├────────────────────────────────────────────┤
│ Message 1 (serialized)                     │
├────────────────────────────────────────────┤
│ Message 2 (serialized)                     │
├────────────────────────────────────────────┤
│ ...                                        │
└────────────────────────────────────────────┘
```

---

### METADATA (API Key: 3)

**Request Payload**:
```
┌────────────────────────────────────────────┐
│ Topic Name Length (4 bytes)                │
├────────────────────────────────────────────┤
│ Topic Name (variable) - null for all       │
└────────────────────────────────────────────┘
```

**Response Payload**:
```
┌────────────────────────────────────────────┐
│ Error Code (2 bytes)                       │
├────────────────────────────────────────────┤
│ Topic Name Length (4 bytes)                │
├────────────────────────────────────────────┤
│ Topic Name (variable)                      │
├────────────────────────────────────────────┤
│ Partition Count (4 bytes)                  │
└────────────────────────────────────────────┘
```

---

### OFFSET_COMMIT (API Key: 2)

**Request Payload**:
```
┌────────────────────────────────────────────┐
│ Group ID Length (4 bytes)                  │
├────────────────────────────────────────────┤
│ Group ID (variable)                        │
├────────────────────────────────────────────┤
│ Topic Name Length (4 bytes)                │
├────────────────────────────────────────────┤
│ Topic Name (variable)                      │
├────────────────────────────────────────────┤
│ Partition ID (4 bytes)                     │
├────────────────────────────────────────────┤
│ Offset (8 bytes)                           │
└────────────────────────────────────────────┘
```

**Response Payload**:
```
┌────────────────────────────────────────────┐
│ Error Code (2 bytes)                       │
└────────────────────────────────────────────┘
```

---

### JOIN_GROUP (API Key: 6)

**Request Payload**:
```
┌────────────────────────────────────────────┐
│ Group ID Length (4 bytes)                  │
├────────────────────────────────────────────┤
│ Group ID (variable)                        │
├────────────────────────────────────────────┤
│ Consumer ID Length (4 bytes)               │
├────────────────────────────────────────────┤
│ Consumer ID (variable)                     │
├────────────────────────────────────────────┤
│ Topics Count (4 bytes)                     │
├────────────────────────────────────────────┤
│ Topic 1 Length + Name                      │
├────────────────────────────────────────────┤
│ Topic 2 Length + Name                      │
├────────────────────────────────────────────┤
│ ...                                        │
└────────────────────────────────────────────┘
```

**Response Payload**:
```
┌────────────────────────────────────────────┐
│ Error Code (2 bytes)                       │
├────────────────────────────────────────────┤
│ Generation ID (4 bytes)                    │
├────────────────────────────────────────────┤
│ Assigned Partitions Count (4 bytes)       │
├────────────────────────────────────────────┤
│ Partition 1 (4 bytes)                      │
├────────────────────────────────────────────┤
│ Partition 2 (4 bytes)                      │
├────────────────────────────────────────────┤
│ ...                                        │
└────────────────────────────────────────────┘
```

---

## Producer Client API

### Class: StreamFlowProducer

#### Constructor
```java
public StreamFlowProducer(String host, int port)
```

**Parameters**:
- `host`: Broker hostname
- `port`: Broker port (default: 9092)

**Example**:
```java
StreamFlowProducer producer = new StreamFlowProducer("localhost", 9092);
```

---

#### connect()
```java
public void connect() throws Exception
```

Establishes connection to broker.

**Throws**:
- `Exception`: If connection fails

**Example**:
```java
producer.connect();
```

---

#### send()
```java
public RecordMetadata send(String topic, String key, String value) throws Exception
```

Sends a message synchronously.

**Parameters**:
- `topic`: Topic name
- `key`: Message key (for partitioning)
- `value`: Message payload

**Returns**:
- `RecordMetadata`: Metadata about sent message

**Throws**:
- `Exception`: If send fails

**Example**:
```java
RecordMetadata metadata = producer.send("orders", "user-123", "Order created");
System.out.println("Sent to offset: " + metadata.offset());
```

---

#### sendAsync()
```java
public CompletableFuture<RecordMetadata> sendAsync(String topic, String key, String value)
```

Sends a message asynchronously.

**Returns**:
- `CompletableFuture<RecordMetadata>`: Future that completes when send finishes

**Example**:
```java
CompletableFuture<RecordMetadata> future = producer.sendAsync("orders", "key", "value");
future.thenAccept(metadata -> {
    System.out.println("Sent to offset: " + metadata.offset());
});
```

---

#### close()
```java
public void close() throws Exception
```

Closes producer and releases resources.

**Example**:
```java
producer.close();
```

---

### Class: RecordMetadata

```java
public record RecordMetadata(String topic, int partition, long offset, int sizeInBytes)
```

**Fields**:
- `topic`: Topic name
- `partition`: Partition ID
- `offset`: Offset where message was stored
- `sizeInBytes`: Size of serialized message

---

## Consumer Client API

### Class: StreamFlowConsumer

#### Constructor
```java
public StreamFlowConsumer(String host, int port, String consumerGroup)
```

**Parameters**:
- `host`: Broker hostname
- `port`: Broker port
- `consumerGroup`: Consumer group ID (optional, can be null)

**Example**:
```java
StreamFlowConsumer consumer = new StreamFlowConsumer("localhost", 9092, "analytics-group");
```

---

#### connect()
```java
public void connect() throws Exception
```

Establishes connection to broker.

---

#### subscribe()
```java
public void subscribe(String topic, int partition) throws Exception
```

Subscribes to a topic partition.

**Parameters**:
- `topic`: Topic name
- `partition`: Partition ID

**Example**:
```java
consumer.subscribe("orders", 0);
consumer.subscribe("orders", 1);
```

---

#### poll()
```java
public List<Message> poll(int maxRecords) throws Exception
```

Polls for messages from subscribed partitions.

**Parameters**:
- `maxRecords`: Maximum number of records to fetch

**Returns**:
- `List<Message>`: List of messages

**Example**:
```java
List<Message> messages = consumer.poll(100);
for (Message msg : messages) {
    System.out.println("Offset: " + msg.getOffset());
    System.out.println("Key: " + msg.getKey());
    System.out.println("Value: " + msg.getValue());
}
```

---

#### commitSync()
```java
public void commitSync() throws Exception
```

Commits current offsets synchronously.

**Example**:
```java
consumer.commitSync();
```

---

#### commitAsync()
```java
public void commitAsync()
```

Commits current offsets asynchronously.

---

#### close()
```java
public void close() throws Exception
```

Closes consumer and releases resources.

---

## Error Codes

### Protocol Error Codes

| Code | Name | Description |
|------|------|-------------|
| 0 | ERROR_NONE | Success |
| 1 | ERROR_UNKNOWN | Unknown error |
| 2 | ERROR_OFFSET_OUT_OF_RANGE | Requested offset is out of range |
| 3 | ERROR_INVALID_MESSAGE | Message format is invalid |
| 4 | ERROR_UNKNOWN_TOPIC_OR_PARTITION | Topic or partition does not exist |
| 5 | ERROR_INVALID_MESSAGE_SIZE | Message size exceeds limit |
| 6 | ERROR_LEADER_NOT_AVAILABLE | Partition leader is not available |
| 7 | ERROR_NOT_LEADER_FOR_PARTITION | This broker is not the leader |
| 8 | ERROR_REQUEST_TIMED_OUT | Request timed out |

### HTTP Status Codes

| Code | Description |
|------|-------------|
| 200 | OK - Request successful |
| 201 | Created - Resource created |
| 204 | No Content - Request successful, no body |
| 400 | Bad Request - Invalid request |
| 404 | Not Found - Resource not found |
| 409 | Conflict - Resource already exists |
| 500 | Internal Server Error - Server error |
| 501 | Not Implemented - Feature not implemented |
| 503 | Service Unavailable - Service is down |

---

## Examples

### Complete Producer Example

```java
import com.streamflow.client.producer.StreamFlowProducer;
import com.streamflow.client.producer.RecordMetadata;

public class ProducerExample {
    public static void main(String[] args) {
        try (StreamFlowProducer producer = new StreamFlowProducer("localhost", 9092)) {
            producer.connect();

            // Send messages
            for (int i = 0; i < 100; i++) {
                String key = "user-" + (i % 10);
                String value = "Order " + i;

                RecordMetadata metadata = producer.send("orders", key, value);
                System.out.printf("Sent message %d to partition %d, offset %d%n",
                        i, metadata.partition(), metadata.offset());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

### Complete Consumer Example

```java
import com.streamflow.client.consumer.StreamFlowConsumer;
import com.streamflow.common.model.Message;

import java.util.List;

public class ConsumerExample {
    public static void main(String[] args) {
        try (StreamFlowConsumer consumer = new StreamFlowConsumer(
                "localhost", 9092, "analytics-group")) {

            consumer.connect();
            consumer.subscribe("orders", 0);

            // Consume messages
            while (true) {
                List<Message> messages = consumer.poll(100);

                for (Message msg : messages) {
                    System.out.printf("Consumed: offset=%d, key=%s, value=%s%n",
                            msg.getOffset(), msg.getKey(), msg.getValue());

                    // Process message...
                }

                // Commit offsets
                if (!messages.isEmpty()) {
                    consumer.commitSync();
                }

                Thread.sleep(1000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

### REST API Example

```bash
# Create topic
curl -X POST http://localhost:8080/api/topics \
  -H "Content-Type: application/json" \
  -d '{
    "name": "events",
    "partitions": 3,
    "replicationFactor": 1
  }'

# Get topic info
curl http://localhost:8080/api/topics/events | jq

# Get broker health
curl http://localhost:8080/api/broker/health | jq

# Get Prometheus metrics
curl http://localhost:8080/api/actuator/prometheus
```

---

For more information, see:
- [Architecture](ARCHITECTURE.md)
- [Developer Guide](DEVELOPER_GUIDE.md)
- [Deployment Guide](DEPLOYMENT_GUIDE.md)
