# StreamFlow Learning Guide

## 🎓 Understanding the Architecture

This guide explains the StreamFlow codebase in a learning-friendly way. Read this to understand how all components fit together.

---

## 📚 Table of Contents

1. [System Overview](#system-overview)
2. [Storage Layer Deep Dive](#storage-layer-deep-dive)
3. [Network Layer Deep Dive](#network-layer-deep-dive)
4. [Data Flow Examples](#data-flow-examples)
5. [Key Concepts](#key-concepts)

---

## System Overview

### What is StreamFlow?

StreamFlow is like Apache Kafka - a distributed event streaming platform. Think of it as:
- **A message queue**: Producers send messages, consumers read them
- **A distributed log**: Append-only storage that preserves order
- **A pub-sub system**: Topics with multiple subscribers

### Architecture Layers

```
┌─────────────────────────────────────────────────────────────┐
│                    APPLICATION LAYER                         │
│              (Your Producer/Consumer Code)                   │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│                    CLIENT LIBRARY LAYER                      │
│          StreamFlowProducer / StreamFlowConsumer             │
│          - Serialization                                     │
│          - Partitioning logic                                │
│          - Offset management                                 │
└───────────────────────────┬─────────────────────────────────┘
                            │ TCP (Binary Protocol)
┌───────────────────────────▼─────────────────────────────────┐
│                    NETWORK LAYER (Netty)                     │
│          - ProtocolDecoder/Encoder                           │
│          - Request/Response handling                         │
│          - Connection management                             │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│                    BROKER LAYER                              │
│          - RequestHandler (routes requests)                  │
│          - TopicManager (manages topics)                     │
│          - OffsetManager (tracks consumer positions)         │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│                    STORAGE LAYER                             │
│          Topic → Partition → LogSegment → Disk              │
│          - Append-only logs                                  │
│          - Offset indexing                                   │
│          - Segment rolling                                   │
└─────────────────────────────────────────────────────────────┘
```

---

## Storage Layer Deep Dive

### Concept: The Commit Log

The fundamental data structure is an **append-only commit log**:

```
Offset:  0      1      2      3      4      5
        ┌──────┬──────┬──────┬──────┬──────┬──────┐
        │ Msg1 │ Msg2 │ Msg3 │ Msg4 │ Msg5 │ Msg6 │
        └──────┴──────┴──────┴──────┴──────┴──────┘
         ↑                                         ↑
    Log Start                                 Log End
    (oldest)                                  (newest)
```

**Key Properties:**
- **Append-only**: New messages added to the end
- **Immutable**: Old messages never modified
- **Ordered**: Offset increases monotonically
- **Persistent**: Survives crashes

### Hierarchy: Topic → Partition → Segment

```
Topic: "orders"
├── Partition 0
│   ├── Segment 00000000000000000000.log (offsets 0-999)
│   ├── Segment 00000000000001000000.log (offsets 1000-1999)
│   └── Segment 00000000000002000000.log (offsets 2000+)
├── Partition 1
│   └── ...
└── Partition 2
    └── ...
```

**Why Partitions?**
- **Parallelism**: Multiple consumers can read different partitions simultaneously
- **Scalability**: Spread data across multiple brokers
- **Ordering**: Messages in same partition maintain order

**Why Segments?**
- **Deletion**: Can delete old segments without rewriting entire log
- **Performance**: Smaller files = faster I/O
- **Management**: Easier to backup, compress, or move

### File Structure on Disk

```
data/
└── orders/
    ├── partition-0/
    │   ├── 00000000000000000000.log    ← Messages
    │   ├── 00000000000000000000.index  ← Offset → Position mapping
    │   ├── 00000000000001000000.log
    │   └── 00000000000001000000.index
    ├── partition-1/
    │   └── ...
    └── partition-2/
        └── ...
```

### How Offset Index Works

**Problem**: Finding a message at offset 12,345 in a 1GB log file

**Naive Approach**: Read from start, count messages (SLOW!)

**Our Solution**: Sparse Index

```
Index File (Memory-Mapped):
┌─────────┬──────────┐
│ Offset  │ Position │ ← Every Nth message indexed
├─────────┼──────────┤
│   0     │     0    │ ← Message 0 starts at byte 0
│ 1,000   │  45,678  │ ← Message 1000 starts at byte 45,678
│ 2,000   │  91,234  │ ← Message 2000 starts at byte 91,234
│ 3,000   │ 137,890  │
└─────────┴──────────┘

To find offset 2,500:
1. Binary search index → Find entry ≤ 2,500 → (2,000, 91,234)
2. Seek to position 91,234 in log file
3. Read sequentially from offset 2,000 to 2,500
```

**Trade-off**: Index size vs search time
- **Sparse index**: Small, but read a few extra messages
- **Dense index**: Fast, but huge index file

---

## Network Layer Deep Dive

### Binary Protocol Design

**Why not HTTP/JSON?**
- **Efficiency**: Binary is 5-10x smaller than JSON
- **Speed**: No parsing overhead
- **Kafka compatibility**: Learn real-world protocol design

**Message Format:**
```
┌─────────────┬─────────────┬─────────────┬──────────────┐
│  Size (4B)  │ API Key (2B)│ Version (2B)│   Payload    │
└─────────────┴─────────────┴─────────────┴──────────────┘
      │             │             │              │
      │             │             │              └─ Request-specific data
      │             │             └─ Protocol version (backward compatibility)
      │             └─ Request type (PRODUCE=0, FETCH=1, etc.)
      └─ Total bytes after this field
```

### Request/Response Flow

**Example: Producer Sending a Message**

```
STEP 1: Client creates request
────────────────────────────────
ProduceRequest {
  topic: "orders"
  partition: 0
  messages: [
    Message { key: "order-123", value: "Order created" }
  ]
}

STEP 2: Serialize to bytes
────────────────────────────────
[Size][0][1][Payload]
  │    │  │     │
  │    │  │     └─ Serialized: topic, partition, messages
  │    │  └─ Version: 1
  │    └─ API Key: 0 (PRODUCE)
  └─ Size: 150 bytes

STEP 3: Send over TCP
────────────────────────────────
Client → [bytes] → Broker

STEP 4: Broker decodes
────────────────────────────────
ProtocolDecoder reads bytes
→ Creates ProduceRequest object
→ Passes to RequestHandler

STEP 5: Broker processes
────────────────────────────────
RequestHandler:
1. Route to handleProduce()
2. Get/create topic
3. Append messages to partition
4. Return offset

STEP 6: Broker responds
────────────────────────────────
ProduceResponse {
  offset: 12345
  errorCode: 0 (no error)
}

STEP 7: Client receives
────────────────────────────────
Client gets offset, knows message was saved
```

### Netty Pipeline

Netty uses a **pipeline** of handlers:

```
INBOUND (Receiving):
Bytes → ProtocolDecoder → RequestHandler
 │           │                  │
 │           │                  └─ Business logic
 │           └─ Bytes → Request object
 └─ Raw bytes from socket

OUTBOUND (Sending):
Response → ProtocolEncoder → Bytes
    │            │              │
    │            │              └─ Send to socket
    │            └─ Response → Bytes
    └─ Java object
```

**Why Pipeline?**
- **Separation of concerns**: Decoding ≠ Business logic
- **Reusability**: Same decoder for all request types
- **Testability**: Can test each handler independently

---

## Data Flow Examples

### Example 1: Producer Sends Message

```
┌──────────┐      ┌────────┐      ┌─────────┐      ┌──────┐
│ Producer │─────→│ Netty  │─────→│ Broker  │─────→│ Disk │
└──────────┘      └────────┘      └─────────┘      └──────┘

1. producer.send("orders", "key1", "Order created")
   ↓
2. Partition selection: hash("key1") % 3 = Partition 1
   ↓
3. Create ProduceRequest(topic="orders", partition=1, messages=[...])
   ↓
4. Serialize to bytes
   ↓
5. Send over TCP to broker
   ↓
6. Netty receives bytes → ProtocolDecoder
   ↓
7. Decoder creates ProduceRequest object
   ↓
8. RequestHandler.handleProduce()
   ↓
9. TopicManager.getTopic("orders")
   ↓
10. Topic.append(partition=1, message)
    ↓
11. Partition.append(message)
    ↓
12. LogSegment.append(message)
    ↓
13. Write to file, update index
    ↓
14. Return offset (e.g., 12345)
    ↓
15. Create ProduceResponse(offset=12345)
    ↓
16. Encoder serializes response
    ↓
17. Send bytes back to producer
    ↓
18. Producer receives: "Message saved at offset 12345"
```

### Example 2: Consumer Fetches Messages

```
┌──────────┐      ┌────────┐      ┌─────────┐      ┌──────┐
│ Consumer │◀─────│ Netty  │◀─────│ Broker  │◀─────│ Disk │
└──────────┘      └────────┘      └─────────┘      └──────┘

1. consumer.poll(maxRecords=100)
   ↓
2. Create FetchRequest(topic="orders", partition=1, offset=12000, maxRecords=100)
   ↓
3. Send to broker
   ↓
4. Broker receives, decodes
   ↓
5. RequestHandler.handleFetch()
   ↓
6. Topic.read(partition=1, offset=12000, maxRecords=100)
   ↓
7. Partition.read() → finds correct segment
   ↓
8. OffsetIndex.lookup(12000) → returns file position
   ↓
9. LogSegment.read(position, maxRecords)
   ↓
10. Read messages from disk
    ↓
11. Create FetchResponse(messages=[...])
    ↓
12. Serialize and send back
    ↓
13. Consumer receives messages
    ↓
14. Process each message
    ↓
15. consumer.commitSync() → Save offset 12100
```

---

## Key Concepts

### 1. **Offset**
- Unique ID for each message in a partition
- Monotonically increasing (0, 1, 2, 3, ...)
- Never reused (even if message is deleted)
- Consumer's "bookmark" in the log

### 2. **Partition**
- Ordered, immutable sequence of messages
- Unit of parallelism (one consumer per partition)
- Provides ordering guarantee within partition
- No ordering guarantee across partitions

### 3. **Consumer Group**
- Multiple consumers working together
- Each partition consumed by exactly one consumer in the group
- Load balancing across consumers
- If consumer dies, partitions are reassigned (rebalancing)

### 4. **Offset Commit**
- Saving consumer's current position
- Enables resuming after crash
- Trade-off: Commit frequently = slower, less data loss
           Commit rarely = faster, more duplicate processing

### 5. **Replication** (Phase 4)
- Multiple copies of each partition
- One leader, N-1 followers
- Writes go to leader, replicated to followers
- If leader dies, follower becomes leader

### 6. **Segment Rolling**
- When segment reaches size limit (e.g., 100MB)
- Close current segment
- Create new segment with next offset as base
- Allows deleting old data (delete old segment files)

---

## Performance Optimizations

### 1. **Sequential I/O**
- Append-only log = sequential writes
- Disk sequential write: ~600 MB/s
- Disk random write: ~100 KB/s
- **6000x faster!**

### 2. **Memory-Mapped Files**
- OS maps file to memory
- Read/write as if it's RAM
- OS handles page cache automatically
- Fast without explicit caching

### 3. **Zero-Copy** (Phase 6)
- FileChannel.transferTo()
- Data goes directly from disk to network
- No copying to user space
- Reduces CPU and memory usage

### 4. **Batching**
- Send multiple messages in one request
- Amortize network overhead
- Higher throughput, slightly higher latency

---

## Common Patterns

### Pattern 1: At-Least-Once Delivery
```java
while (true) {
    List<Message> messages = consumer.poll(100);
    processMessages(messages);
    consumer.commitSync(); // Commit after processing
}
```
If crash after processing but before commit → reprocess (duplicate)

### Pattern 2: At-Most-Once Delivery
```java
while (true) {
    List<Message> messages = consumer.poll(100);
    consumer.commitSync(); // Commit before processing
    processMessages(messages);
}
```
If crash after commit but before processing → message lost

### Pattern 3: Exactly-Once (Complex)
Requires transactional support + idempotent processing

---

## Debugging Tips

### Enable Debug Logging
Edit `logback.xml`:
```xml
<logger name="com.streamflow" level="DEBUG" />
```

### Common Issues

**Issue**: Consumer not receiving messages
- Check: Is producer writing to same topic?
- Check: Is consumer subscribed to correct partition?
- Check: Check offset - might be at end of log

**Issue**: "Topic not found" error
- Topics are created automatically on first produce
- Or create explicitly via TopicManager

**Issue**: Slow performance
- Check: Are you committing after every message? (too frequent)
- Check: Are messages very large? (increase maxBytes)
- Check: Is disk full?

---

## Next Steps

1. **Read the code**: Start with `Message.java` → `LogSegment.java` → `Partition.java`
2. **Run the example**: `QuickStart.java` to see it in action
3. **Experiment**: Try different partition counts, message sizes
4. **Study Kafka**: Compare our implementation to Apache Kafka
5. **Contribute**: Implement Phase 3 (Consumer Groups)!

---

**Questions?** Read the comments in each file - they explain the "why" behind every design decision!
