package com.streamflow.broker.storage;

import com.streamflow.common.model.Message;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Represents a topic with multiple partitions
 *
 * A topic is a logical grouping of partitions. Messages are distributed
 * across partitions based on the message key or round-robin.
 */
@Slf4j
public class Topic {

    private final String name;
    private final int numPartitions;
    private final File baseDir;
    private final Map<Integer, Partition> partitions;

    public Topic(String name, int numPartitions, File baseDir) {
        if (numPartitions <= 0) {
            throw new IllegalArgumentException("Number of partitions must be positive");
        }

        this.name = name;
        this.numPartitions = numPartitions;
        this.baseDir = baseDir;
        this.partitions = new ConcurrentHashMap<>();

        // Create all partitions
        createPartitions();

        log.info("Created topic: name={}, partitions={}", name, numPartitions);
    }

    /**
     * Append a message to a specific partition
     */
    public long append(int partitionId, Message message) {
        validatePartitionId(partitionId);
        Partition partition = partitions.get(partitionId);
        return partition.append(message);
    }

    /**
     * Append a message to a partition determined by the key hash
     */
    public long append(Message message) {
        int partitionId = selectPartition(message.getKey());
        return append(partitionId, message);
    }

    /**
     * Append a batch of messages to a specific partition
     */
    public long appendBatch(int partitionId, List<Message> messages) {
        validatePartitionId(partitionId);
        Partition partition = partitions.get(partitionId);
        return partition.appendBatch(messages);
    }

    /**
     * Read messages from a specific partition
     */
    public List<Message> read(int partitionId, long startOffset, int maxRecords) {
        validatePartitionId(partitionId);
        Partition partition = partitions.get(partitionId);
        return partition.read(startOffset, maxRecords);
    }

    /**
     * Read messages from a specific partition with byte limit
     */
    public List<Message> read(int partitionId, long startOffset, int maxRecords, int maxBytes) {
        validatePartitionId(partitionId);
        Partition partition = partitions.get(partitionId);
        return partition.read(startOffset, maxRecords, maxBytes);
    }

    /**
     * Get a specific partition
     */
    public Partition getPartition(int partitionId) {
        validatePartitionId(partitionId);
        return partitions.get(partitionId);
    }

    /**
     * Get all partitions
     */
    public List<Partition> getAllPartitions() {
        return new ArrayList<>(partitions.values());
    }

    /**
     * Get the number of partitions
     */
    public int getNumPartitions() {
        return numPartitions;
    }

    /**
     * Get the topic name
     */
    public String getName() {
        return name;
    }

    /**
     * Get the log end offset for a specific partition
     */
    public long getLogEndOffset(int partitionId) {
        validatePartitionId(partitionId);
        return partitions.get(partitionId).getLogEndOffset();
    }

    /**
     * Get the log start offset for a specific partition
     */
    public long getLogStartOffset(int partitionId) {
        validatePartitionId(partitionId);
        return partitions.get(partitionId).getLogStartOffset();
    }

    /**
     * Flush all partitions to disk
     */
    public void flush() {
        for (Partition partition : partitions.values()) {
            partition.flush();
        }
        log.debug("Flushed all partitions for topic: {}", name);
    }

    /**
     * Close the topic and all its partitions
     */
    public void close() {
        for (Partition partition : partitions.values()) {
            partition.close();
        }
        log.info("Closed topic: {}", name);
    }

    /**
     * Delete old segments from all partitions up to the given retention time
     */
    public void deleteOldSegments(long retentionMs) {
        long deleteBeforeOffset = System.currentTimeMillis() - retentionMs;

        for (Partition partition : partitions.values()) {
            // This is a simplified approach - in reality, you'd need to track
            // message timestamps and delete based on time, not just offset
            // For now, we'll just expose the method for future implementation
            log.debug("Retention check for partition {}", partition.getPartitionId());
        }
    }

    /**
     * Get topic statistics
     */
    public TopicStats getStats() {
        long totalMessages = 0;
        long totalBytes = 0;

        for (Partition partition : partitions.values()) {
            long endOffset = partition.getLogEndOffset();
            long startOffset = partition.getLogStartOffset();
            totalMessages += (endOffset - startOffset);
        }

        return new TopicStats(name, numPartitions, totalMessages, totalBytes);
    }

    /**
     * Create all partitions for this topic
     */
    private void createPartitions() {
        for (int i = 0; i < numPartitions; i++) {
            Partition partition = new Partition(name, i, baseDir);
            partitions.put(i, partition);
        }
    }

    /**
     * Select a partition based on the message key
     * Uses hash of key for consistent partitioning
     */
    private int selectPartition(String key) {
        if (key == null || key.isEmpty()) {
            // Round-robin if no key provided
            return (int) (System.nanoTime() % numPartitions);
        }

        // Hash-based partitioning for consistent routing
        int hash = Math.abs(key.hashCode());
        return hash % numPartitions;
    }

    /**
     * Validate that partition ID is within valid range
     */
    private void validatePartitionId(int partitionId) {
        if (partitionId < 0 || partitionId >= numPartitions) {
            throw new IllegalArgumentException(
                    String.format("Invalid partition ID: %d. Topic %s has %d partitions",
                            partitionId, name, numPartitions));
        }
    }

    /**
     * Statistics for a topic
     */
    public static class TopicStats {
        public final String name;
        public final int partitions;
        public final long totalMessages;
        public final long totalBytes;

        public TopicStats(String name, int partitions, long totalMessages, long totalBytes) {
            this.name = name;
            this.partitions = partitions;
            this.totalMessages = totalMessages;
            this.totalBytes = totalBytes;
        }

        @Override
        public String toString() {
            return String.format("TopicStats{name='%s', partitions=%d, messages=%d, bytes=%d}",
                    name, partitions, totalMessages, totalBytes);
        }
    }
}
