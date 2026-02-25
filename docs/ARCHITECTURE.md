# StreamFlow Architecture

## Table of Contents
1. [System Overview](#system-overview)
2. [Architecture Layers](#architecture-layers)
3. [Component Diagram](#component-diagram)
4. [Data Flow](#data-flow)
5. [Storage Architecture](#storage-architecture)
6. [Network Protocol](#network-protocol)
7. [Replication Architecture](#replication-architecture)
8. [Consumer Group Coordination](#consumer-group-coordination)
9. [Performance Optimizations](#performance-optimizations)

---

## System Overview

StreamFlow is a distributed event streaming platform designed with a layered architecture similar to Apache Kafka. It provides:
- **Persistent, ordered message storage** using append-only logs
- **High-throughput message publishing** through batching and zero-copy transfers
- **Scalable message consumption** via partitioning and consumer groups
- **Fault tolerance** through replication and leader election
- **Management APIs** for operations and monitoring

### Key Design Principles
1. **Sequential I/O over Random I/O**: All writes are appends; all reads are sequential scans
2. **Zero-Copy Transfers**: Use OS kernel features to avoid data copying
3. **Batching**: Amortize network and disk overhead across multiple messages
4. **Immutable Log**: Messages are never modified after writing
5. **Pull-Based Consumption**: Consumers control their read position

---

## Architecture Layers

```
┌─────────────────────────────────────────────────────────────┐
│                    Management Layer                          │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Admin API (Spring Boot REST)                        │   │
│  │  - Topic Management    - Health Checks               │   │
│  │  - Consumer Groups     - Metrics (Prometheus)        │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                      Client Layer                            │
│  ┌──────────────────┐              ┌──────────────────┐     │
│  │   Producer       │              │   Consumer       │     │
│  │  - Batching      │              │  - Poll API      │     │
│  │  - Partitioning  │              │  - Offset Mgmt   │     │
│  │  - Compression   │              │  - Auto-commit   │     │
│  └──────────────────┘              └──────────────────┘     │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼ (Binary Protocol over TCP)
┌─────────────────────────────────────────────────────────────┐
│                      Broker Layer                            │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Network Server (Netty)                              │   │
│  │  - Request Handling  - Connection Management         │   │
│  └──────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Coordination Layer                                  │   │
│  │  - Consumer Groups    - Offset Management            │   │
│  │  - Rebalancing        - Heartbeat Monitoring         │   │
│  └──────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Replication Layer                                   │   │
│  │  - Leader Election    - ISR Management               │   │
│  │  - Replica Fetchers   - High Watermark              │   │
│  └──────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Storage Layer                                       │   │
│  │  - Topic Management   - Partition Management         │   │
│  │  - Log Segments       - Offset Indexes               │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Persistent Storage                         │
│  /data/topics/                                               │
│    ├── orders/                                               │
│    │   ├── partition-0/                                      │
│    │   │   ├── 00000000000000000000.log                     │
│    │   │   ├── 00000000000000000000.index                   │
│    │   │   └── 00000000000000100000.log                     │
│    │   └── partition-1/                                      │
│    └── users/                                                │
└─────────────────────────────────────────────────────────────┘
```

---

## Component Diagram

### Broker Components

```
BrokerApplication
    │
    ├── TopicManager
    │   └── Topic (Map)
    │       └── Partition (List)
    │           └── LogSegment (List)
    │               ├── FileChannel (log file)
    │               └── OffsetIndex (index file)
    │
    ├── ReplicaManager
    │   ├── LeaderElection
    │   ├── ISRManager
    │   └── ReplicaFetcher (per follower partition)
    │
    ├── ConsumerGroupCoordinator
    │   ├── ConsumerGroup (Map)
    │   │   ├── Members (heartbeats)
    │   │   ├── Assignments (partitions)
    │   │   └── PartitionAssignor
    │   └── ScheduledExecutor (heartbeat monitoring)
    │
    ├── OffsetManager
    │   └── Consumer Offsets (topic-partition-group)
    │
    └── BrokerServer (Netty)
        ├── ProtocolDecoder
        ├── RequestHandler
        └── ProtocolEncoder
```

### Client Components

```
StreamFlowProducer
    ├── NetworkClient (TCP connection)
    ├── Partitioner (hash-based routing)
    ├── ProducerBatch (optional batching)
    └── Compressor (GZIP/NONE)

StreamFlowConsumer
    ├── NetworkClient (TCP connection)
    ├── Subscriptions (topic-partition set)
    ├── Offsets (current position)
    └── ConsumerGroup (optional)
```

---

## Data Flow

### Producer Flow

```
1. Producer.send("orders", "key123", "data")
         │
         ▼
2. Partitioner.partition("orders", "key123")
         │ (returns partition = hash(key) % numPartitions)
         ▼
3. [Optional] ProducerBatch.append(message)
         │ (wait for batch to fill or linger time)
         ▼
4. [Optional] Compressor.compress(batch)
         │
         ▼
5. NetworkClient.send(ProduceRequest)
         │ (binary protocol over TCP)
         ▼
6. BrokerServer receives request
         │
         ▼
7. RequestHandler.handleProduce()
         │
         ▼
8. Topic.getPartition(partitionId)
         │
         ▼
9. Partition.append(message)
         │
         ▼
10. LogSegment.write(message)
         │  ├── Write to FileChannel (sequential)
         │  └── Update OffsetIndex (every 4KB)
         │
         ▼
11. [If replicated] ReplicaFetcher copies to followers
         │
         ▼
12. ISRManager updates high watermark
         │
         ▼
13. ProduceResponse(offset) returned to producer
```

### Consumer Flow

```
1. Consumer.poll(maxRecords=100)
         │
         ▼
2. NetworkClient.send(FetchRequest)
         │ (with current offset)
         ▼
3. BrokerServer receives request
         │
         ▼
4. RequestHandler.handleFetch()
         │
         ▼
5. Partition.read(offset, maxRecords, maxBytes)
         │
         ▼
6. LogSegment.read()
         │  ├── OffsetIndex.lookup(offset) → position
         │  └── FileChannel.read(position, maxBytes)
         │
         ▼
7. FetchResponse(messages) returned
         │
         ▼
8. [Optional] Compressor.decompress(messages)
         │
         ▼
9. Consumer processes messages
         │
         ▼
10. Consumer.commitSync()
         │
         ▼
11. OffsetCommitRequest sent to broker
         │
         ▼
12. OffsetManager.commit(group, topic, partition, offset)
```

---

## Storage Architecture

### Log Structure

Each partition is stored as a sequence of log segments:

```
/data/topics/orders/partition-0/
├── 00000000000000000000.log      # Segment 1 (offsets 0-99999)
├── 00000000000000000000.index
├── 00000000000000100000.log      # Segment 2 (offsets 100000-199999)
├── 00000000000000100000.index
└── 00000000000000200000.log      # Active segment
    └── 00000000000000200000.index
```

### Log File Format

```
┌─────────────────────────────────────────────────────┐
│  Message 1                                          │
│  ┌──────────────────────────────────────────────┐   │
│  │ Size (4 bytes) │ Serialized Message          │   │
│  └──────────────────────────────────────────────┘   │
│  Message 2                                          │
│  ┌──────────────────────────────────────────────┐   │
│  │ Size (4 bytes) │ Serialized Message          │   │
│  └──────────────────────────────────────────────┘   │
│  ...                                                │
└─────────────────────────────────────────────────────┘
```

### Index File Format

Sparse index (one entry per 4KB of log):

```
┌─────────────────────────────────────────┐
│ Entry 1: Offset (8 bytes) | Position (4 bytes) │
├─────────────────────────────────────────┤
│ Entry 2: Offset (8 bytes) | Position (4 bytes) │
├─────────────────────────────────────────┤
│ Entry 3: Offset (8 bytes) | Position (4 bytes) │
└─────────────────────────────────────────┘
```

**Lookup Algorithm**:
1. Binary search index for largest offset ≤ target
2. Get file position from index entry
3. Sequential scan from that position to target offset

### Message Format

```java
Message {
    long offset;          // Unique offset in partition
    long timestamp;       // Message timestamp (ms)
    String key;           // Optional routing key
    String value;         // Message payload
}
```

Serialized format:
```
┌──────────────────────────────────────────────────────┐
│ Offset (8 bytes)                                     │
├──────────────────────────────────────────────────────┤
│ Timestamp (8 bytes)                                  │
├──────────────────────────────────────────────────────┤
│ Key Length (4 bytes) | Key (variable)                │
├──────────────────────────────────────────────────────┤
│ Value Length (4 bytes) | Value (variable)            │
└──────────────────────────────────────────────────────┘
```

---

## Network Protocol

### Binary Protocol Format

All requests/responses follow this format:

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

| API Key | Request Type | Description |
|---------|--------------|-------------|
| 0 | PRODUCE | Send messages to partition |
| 1 | FETCH | Read messages from partition |
| 2 | OFFSET_COMMIT | Save consumer offset |
| 3 | METADATA | Get topic metadata |
| 4 | OFFSET_FETCH | Get consumer offset |
| 5 | HEARTBEAT | Consumer heartbeat |
| 6 | JOIN_GROUP | Join consumer group |
| 7 | LEAVE_GROUP | Leave consumer group |

### Request/Response Flow

```
Client                          Broker
  │                               │
  │  1. Open TCP Connection       │
  │ ────────────────────────────► │
  │                               │
  │  2. Send ProduceRequest       │
  │     (binary encoded)          │
  │ ────────────────────────────► │
  │                               │
  │                     3. Process request
  │                        (write to log)
  │                               │
  │  4. Send ProduceResponse      │
  │     (with offset)             │
  │ ◄──────────────────────────── │
  │                               │
  │  5. Keep connection open      │
  │     (for subsequent requests) │
  │ ◄────────────────────────────►│
```

---

## Replication Architecture

### Leader-Follower Model

```
Topic: orders, Partition: 0, Replication Factor: 3

┌──────────────┐         ┌──────────────┐         ┌──────────────┐
│   Broker 0   │         │   Broker 1   │         │   Broker 2   │
│   (LEADER)   │         │  (FOLLOWER)  │         │  (FOLLOWER)  │
├──────────────┤         ├──────────────┤         ├──────────────┤
│ LEO: 1000    │         │ LEO: 999     │         │ LEO: 995     │
│ HWM: 995     │◄────────│ Fetching...  │         │ Fetching...  │
└──────────────┘         └──────────────┘         └──────────────┘
       │                        ▲                        ▲
       │                        │                        │
       └────────── Replication ─┴────────────────────────┘
              (ReplicaFetcher pulls from leader)
```

**Terms**:
- **LEO (Log End Offset)**: Highest offset in the log
- **HWM (High Watermark)**: Minimum offset in ISR (consumer-visible)
- **ISR (In-Sync Replicas)**: Replicas that are caught up

### Replication Flow

1. **Producer writes to leader**:
   - Message appended to leader's log
   - LEO incremented

2. **Followers fetch from leader**:
   - ReplicaFetcher threads continuously poll leader
   - New messages appended to follower's log
   - Follower LEO updated

3. **ISR updated**:
   - ISRManager checks follower lag
   - If lag < 100 messages: Add to ISR
   - If lag > 100 messages: Remove from ISR

4. **High watermark updated**:
   - HWM = min(LEO of all ISR members)
   - Only messages ≤ HWM are visible to consumers

5. **Leader failure**:
   - LeaderElection selects new leader from ISR
   - New leader broadcasts to all followers
   - Followers update their configuration

---

## Consumer Group Coordination

### Consumer Group State Machine

```
        addMember()
    ┌──────────────┐
    │    EMPTY     │
    └──────┬───────┘
           │
           ▼
    ┌──────────────────┐
    │  PREPARING_      │  rebalance()
    │  REBALANCE       │◄─────────┐
    └──────┬───────────┘          │
           │                      │
           │ assignPartitions()   │
           ▼                      │
    ┌──────────────┐              │
    │   STABLE     │──────────────┘
    └──────┬───────┘   member joins/leaves
           │
           │ allLeave()
           ▼
    ┌──────────────┐
    │    DEAD      │
    └──────────────┘
```

### Partition Assignment

**Round-Robin Strategy**:
```
Topic: orders (6 partitions)
Consumer Group: analytics (3 consumers)

Consumer A → Partitions [0, 3]
Consumer B → Partitions [1, 4]
Consumer C → Partitions [2, 5]
```

**Range Strategy**:
```
Topic: orders (6 partitions)
Consumer Group: analytics (3 consumers)

Consumer A → Partitions [0, 1]
Consumer B → Partitions [2, 3]
Consumer C → Partitions [4, 5]
```

### Rebalancing Process

1. **Member joins**:
   - Send JoinGroupRequest
   - Group state → PREPARING_REBALANCE

2. **Wait for all members**:
   - Heartbeat timeout: 30 seconds
   - Collect all active members

3. **Assign partitions**:
   - Use PartitionAssignor (RoundRobin/Range)
   - Generate new assignment

4. **Broadcast assignment**:
   - Send JoinGroupResponse to all members
   - Include partition assignment

5. **Members start consuming**:
   - Group state → STABLE
   - Resume message processing

---

## Performance Optimizations

### 1. Zero-Copy Transfers

**Traditional Read**:
```
Disk → Kernel Buffer → User Space → Socket Buffer → Network
        (Copy 1)         (Copy 2)      (Copy 3)
```

**Zero-Copy (transferTo)**:
```
Disk → Kernel Buffer → Network
           (DMA transfer, no CPU copy)
```

Implementation:
```java
// ZeroCopySegment.java
FileChannel.transferTo(position, bytesToTransfer, socketChannel);
```

### 2. Batching

**Individual Sends** (10,000 messages):
```
10,000 network round trips
10,000 disk writes
High overhead
```

**Batched Sends** (10,000 messages in 100 batches):
```
100 network round trips
100 disk writes
10x less overhead
```

### 3. Compression

**Message sizes**:
```
Uncompressed: 1 KB
GZIP: ~200-300 bytes (3-5x compression)
```

**Trade-offs**:
- ✅ Less network bandwidth
- ✅ Less disk usage
- ❌ More CPU usage
- ❌ Higher latency

### 4. Memory-Mapped I/O

**Index File Access**:
```java
MappedByteBuffer mmap = channel.map(READ_WRITE, 0, size);
// OS handles caching and page faults
// No explicit read() calls needed
```

**Benefits**:
- OS page cache manages memory
- No system calls for reads
- Automatic prefetching

### 5. Sequential I/O

**Design principle**: All writes are appends, all reads are scans

**Performance**:
- Sequential disk: ~100-200 MB/s
- Random disk: ~1-5 MB/s
- **40x performance improvement!**

---

## Scalability Considerations

### Horizontal Scaling

**Scale producers**: Add more producer instances (stateless)
**Scale consumers**: Add to consumer group (auto-rebalancing)
**Scale brokers**: Add more brokers to cluster (requires coordinator)

### Partitioning Strategy

**Too few partitions**:
- Limited parallelism
- Hot spots

**Too many partitions**:
- More memory overhead
- More files open
- Slower rebalancing

**Rule of thumb**: 10-30 partitions per broker

---

## Monitoring & Observability

### Key Metrics

**Broker Metrics**:
- `streamflow_messages_total` - Total messages stored
- `streamflow_bytes_total` - Total bytes stored
- `streamflow_topics_total` - Number of topics
- `streamflow_partitions_total` - Number of partitions

**Producer Metrics**:
- Messages/sec
- Bytes/sec
- Request latency (p50, p95, p99)

**Consumer Metrics**:
- Messages/sec
- Consumer lag (offset behind HWM)
- Rebalance frequency

### Health Checks

1. **Broker health**: TCP connection + metadata request
2. **Storage health**: Disk space available
3. **Replication health**: ISR size == replication factor
4. **Consumer health**: Lag < threshold

---

## Security Considerations (Future)

Currently not implemented, but would include:

1. **Authentication**: TLS/SSL certificates, SASL
2. **Authorization**: ACLs for topics/consumer groups
3. **Encryption**: TLS for network, encryption at rest
4. **Audit Logging**: Track all administrative actions

---

## Failure Scenarios

### Broker Failure
1. Leader election triggered
2. New leader elected from ISR
3. Followers update to new leader
4. Producers/consumers reconnect

### Network Partition
1. Minority partition cannot form quorum
2. Majority partition continues operating
3. Minority partition rejects writes

### Disk Failure
1. Broker marks partition as offline
2. Replica on another broker promoted
3. Affected partition's data lost (if no replicas)

---

This architecture enables StreamFlow to provide high-throughput, fault-tolerant, scalable event streaming while maintaining simplicity and educational value.
