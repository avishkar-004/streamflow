package com.streamflow.client.consumer;

import com.streamflow.common.protocol.*;
import com.streamflow.common.model.Message;
import com.streamflow.client.common.NetworkClient;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Consumer client for reading messages from topics
 *
 * Usage:
 * <pre>
 * StreamFlowConsumer consumer = new StreamFlowConsumer("localhost", 9092, "my-group");
 * consumer.connect();
 * consumer.subscribe("my-topic", 0); // Subscribe to partition 0
 *
 * while (true) {
 *     List&lt;Message&gt; messages = consumer.poll(1000);
 *     for (Message message : messages) {
 *         System.out.println("Received: " + message.getValue());
 *     }
 *     consumer.commitSync();
 * }
 * </pre>
 */
@Slf4j
public class StreamFlowConsumer {

    private final NetworkClient client;
    private final String consumerGroup;
    private final String consumerId;

    // Track subscriptions: topic -> list of partitions
    private final Map<String, List<Integer>> subscriptions;

    // Track current offsets: (topic, partition) -> offset
    private final Map<TopicPartition, Long> currentOffsets;

    // Track committed offsets: (topic, partition) -> offset
    private final Map<TopicPartition, Long> committedOffsets;

    public StreamFlowConsumer(String host, int port, String consumerGroup) {
        this(host, port, consumerGroup, generateConsumerId());
    }

    public StreamFlowConsumer(String host, int port, String consumerGroup, String consumerId) {
        this.client = new NetworkClient(host, port);
        this.consumerGroup = consumerGroup;
        this.consumerId = consumerId;
        this.subscriptions = new HashMap<>();
        this.currentOffsets = new HashMap<>();
        this.committedOffsets = new HashMap<>();
    }

    /**
     * Connect to the broker
     */
    public void connect() throws InterruptedException {
        client.connect();
        log.info("Consumer connected: group={}, id={}", consumerGroup, consumerId);
    }

    /**
     * Subscribe to a specific partition of a topic
     */
    public void subscribe(String topic, int partition) {
        subscriptions.computeIfAbsent(topic, k -> new ArrayList<>()).add(partition);

        // Initialize offset for this partition
        TopicPartition tp = new TopicPartition(topic, partition);
        if (!currentOffsets.containsKey(tp)) {
            currentOffsets.put(tp, 0L);
        }

        log.info("Subscribed to topic={}, partition={}", topic, partition);
    }

    /**
     * Poll for messages from subscribed partitions
     *
     * @param maxRecords Maximum number of records to fetch
     * @return List of messages
     */
    public List<Message> poll(int maxRecords) throws Exception {
        List<Message> allMessages = new ArrayList<>();

        for (Map.Entry<String, List<Integer>> entry : subscriptions.entrySet()) {
            String topic = entry.getKey();

            for (int partition : entry.getValue()) {
                TopicPartition tp = new TopicPartition(topic, partition);
                long offset = currentOffsets.getOrDefault(tp, 0L);

                // Fetch messages
                FetchRequest request = FetchRequest.builder()
                        .topic(topic)
                        .partition(partition)
                        .offset(offset)
                        .maxRecords(maxRecords)
                        .maxBytes(1024 * 1024) // 1MB
                        .build();

                Response response = client.send(request);

                if (response instanceof FetchResponse fetchResponse) {
                    if (fetchResponse.hasError()) {
                        log.error("Fetch failed: {}",
                                Response.getErrorMessage(fetchResponse.getErrorCode()));
                        continue;
                    }

                    List<Message> messages = fetchResponse.getMessages();

                    if (!messages.isEmpty()) {
                        allMessages.addAll(messages);

                        // Update current offset to the last message + 1
                        Message lastMessage = messages.get(messages.size() - 1);
                        currentOffsets.put(tp, lastMessage.getOffset() + 1);

                        log.debug("Fetched {} messages from topic={}, partition={}, offset={}",
                                messages.size(), topic, partition, offset);
                    }
                }
            }
        }

        return allMessages;
    }

    /**
     * Commit current offsets synchronously
     */
    public void commitSync() throws Exception {
        for (Map.Entry<TopicPartition, Long> entry : currentOffsets.entrySet()) {
            TopicPartition tp = entry.getKey();
            long offset = entry.getValue();

            // Only commit if offset has changed
            Long committedOffset = committedOffsets.get(tp);
            if (committedOffset == null || offset != committedOffset) {
                OffsetCommitRequest request = OffsetCommitRequest.builder()
                        .consumerGroup(consumerGroup)
                        .topic(tp.topic)
                        .partition(tp.partition)
                        .offset(offset)
                        .build();

                Response response = client.send(request);

                if (response instanceof OffsetCommitResponse commitResponse) {
                    if (commitResponse.hasError()) {
                        log.error("Offset commit failed: {}",
                                Response.getErrorMessage(commitResponse.getErrorCode()));
                    } else {
                        committedOffsets.put(tp, offset);
                        log.debug("Committed offset: topic={}, partition={}, offset={}",
                                tp.topic, tp.partition, offset);
                    }
                }
            }
        }
    }

    /**
     * Seek to a specific offset for a partition
     */
    public void seek(String topic, int partition, long offset) {
        TopicPartition tp = new TopicPartition(topic, partition);
        currentOffsets.put(tp, offset);
        log.info("Seek to offset: topic={}, partition={}, offset={}", topic, partition, offset);
    }

    /**
     * Get the current offset for a partition
     */
    public long position(String topic, int partition) {
        TopicPartition tp = new TopicPartition(topic, partition);
        return currentOffsets.getOrDefault(tp, 0L);
    }

    /**
     * Get the committed offset for a partition
     */
    public long committed(String topic, int partition) {
        TopicPartition tp = new TopicPartition(topic, partition);
        return committedOffsets.getOrDefault(tp, -1L);
    }

    /**
     * Close the consumer
     */
    public void close() {
        try {
            commitSync();
        } catch (Exception e) {
            log.error("Failed to commit offsets on close", e);
        }

        client.close();
        log.info("Consumer closed: group={}, id={}", consumerGroup, consumerId);
    }

    /**
     * Check if connected
     */
    public boolean isConnected() {
        return client.isConnected();
    }

    /**
     * Generate a unique consumer ID
     */
    private static String generateConsumerId() {
        return "consumer-" + System.currentTimeMillis() + "-" +
                (int) (Math.random() * 10000);
    }

    /**
     * Topic-Partition pair
     */
    private record TopicPartition(String topic, int partition) {
    }
}
