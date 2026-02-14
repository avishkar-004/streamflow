package com.streamflow.client.producer;

import com.streamflow.common.model.Message;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Batch of messages for a single topic-partition
 *
 * Batching benefits:
 * - Reduces number of network requests
 * - Amortizes request overhead
 * - Improves compression efficiency
 * - Increases throughput
 *
 * Trade-off:
 * - Adds latency (wait for batch to fill)
 * - Uses more memory (buffering)
 */
@Data
public class ProducerBatch {

    private final String topic;
    private final int partition;
    private final List<Message> messages;
    private final List<CompletableFuture<RecordMetadata>> futures;
    private final long createdAt;

    // Batch limits
    private final int maxMessages;
    private final int maxBytes;

    private int currentBytes;

    public ProducerBatch(String topic, int partition, int maxMessages, int maxBytes) {
        this.topic = topic;
        this.partition = partition;
        this.messages = new ArrayList<>();
        this.futures = new ArrayList<>();
        this.createdAt = System.currentTimeMillis();
        this.maxMessages = maxMessages;
        this.maxBytes = maxBytes;
        this.currentBytes = 0;
    }

    /**
     * Try to add message to batch
     * Returns true if added, false if batch is full
     */
    public synchronized boolean tryAppend(Message message, CompletableFuture<RecordMetadata> future) {
        int messageSize = message.sizeInBytes();

        // Check if batch is full
        if (messages.size() >= maxMessages || currentBytes + messageSize > maxBytes) {
            return false;
        }

        messages.add(message);
        futures.add(future);
        currentBytes += messageSize;

        return true;
    }

    /**
     * Check if batch is ready to send
     */
    public synchronized boolean isReady(int lingerMs) {
        // Ready if batch is full
        if (messages.size() >= maxMessages || currentBytes >= maxBytes) {
            return true;
        }

        // Ready if linger time has passed
        long age = System.currentTimeMillis() - createdAt;
        return age >= lingerMs;
    }

    /**
     * Check if batch is full
     */
    public synchronized boolean isFull() {
        return messages.size() >= maxMessages || currentBytes >= maxBytes;
    }

    /**
     * Get batch size in bytes
     */
    public synchronized int sizeInBytes() {
        return currentBytes;
    }

    /**
     * Get number of messages in batch
     */
    public synchronized int messageCount() {
        return messages.size();
    }

    /**
     * Get batch age in milliseconds
     */
    public long ageMs() {
        return System.currentTimeMillis() - createdAt;
    }

    /**
     * Complete all futures with success
     */
    public synchronized void completeAll(long baseOffset) {
        for (int i = 0; i < futures.size(); i++) {
            CompletableFuture<RecordMetadata> future = futures.get(i);
            RecordMetadata metadata = new RecordMetadata(
                    topic,
                    partition,
                    baseOffset + i,
                    messages.get(i).sizeInBytes()
            );
            future.complete(metadata);
        }
    }

    /**
     * Complete all futures with error
     */
    public synchronized void completeAllExceptionally(Exception e) {
        for (CompletableFuture<RecordMetadata> future : futures) {
            future.completeExceptionally(e);
        }
    }
}
