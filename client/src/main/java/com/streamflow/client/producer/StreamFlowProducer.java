package com.streamflow.client.producer;

import com.streamflow.common.protocol.ProduceRequest;
import com.streamflow.common.protocol.ProduceResponse;
import com.streamflow.common.protocol.Response;
import com.streamflow.common.model.Message;
import com.streamflow.client.common.NetworkClient;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Producer client for sending messages to topics
 *
 * Usage:
 * <pre>
 * StreamFlowProducer producer = new StreamFlowProducer("localhost", 9092);
 * producer.connect();
 *
 * RecordMetadata metadata = producer.send("my-topic", "key", "value");
 * System.out.println("Sent to offset: " + metadata.offset());
 *
 * producer.close();
 * </pre>
 */
@Slf4j
public class StreamFlowProducer {

    private final NetworkClient client;
    private final Partitioner partitioner;
    private final int defaultPartitions;

    public StreamFlowProducer(String host, int port) {
        this(host, port, 3);
    }

    public StreamFlowProducer(String host, int port, int defaultPartitions) {
        this.client = new NetworkClient(host, port);
        this.partitioner = new HashPartitioner();
        this.defaultPartitions = defaultPartitions;
    }

    /**
     * Connect to the broker
     */
    public void connect() throws InterruptedException {
        client.connect();
    }

    /**
     * Send a single message to a topic
     * Partition is determined by key hash
     */
    public RecordMetadata send(String topic, String key, String value) throws Exception {
        int partition = partitioner.partition(topic, key, defaultPartitions);
        return send(topic, partition, key, value);
    }

    /**
     * Send a single message to a specific partition
     */
    public RecordMetadata send(String topic, int partition, String key, String value) throws Exception {
        Message message = Message.builder()
                .key(key)
                .value(value)
                .timestamp(System.currentTimeMillis())
                .build();

        List<Message> messages = new ArrayList<>();
        messages.add(message);

        ProduceRequest request = ProduceRequest.builder()
                .topic(topic)
                .partition(partition)
                .messages(messages)
                .build();

        Response response = client.send(request);

        if (response instanceof ProduceResponse produceResponse) {
            if (produceResponse.hasError()) {
                throw new Exception("Produce failed: " +
                        Response.getErrorMessage(produceResponse.getErrorCode()));
            }

            log.debug("Sent message to topic={}, partition={}, offset={}",
                    topic, partition, produceResponse.getOffset());

            return new RecordMetadata(topic, partition, produceResponse.getOffset());
        }

        throw new Exception("Unexpected response type: " + response.getClass().getName());
    }

    /**
     * Send a batch of messages to a specific partition
     */
    public RecordMetadata sendBatch(String topic, int partition, List<Message> messages) throws Exception {
        ProduceRequest request = ProduceRequest.builder()
                .topic(topic)
                .partition(partition)
                .messages(messages)
                .build();

        Response response = client.send(request);

        if (response instanceof ProduceResponse produceResponse) {
            if (produceResponse.hasError()) {
                throw new Exception("Produce failed: " +
                        Response.getErrorMessage(produceResponse.getErrorCode()));
            }

            log.info("Sent {} messages to topic={}, partition={}, offset={}",
                    messages.size(), topic, partition, produceResponse.getOffset());

            return new RecordMetadata(topic, partition, produceResponse.getOffset());
        }

        throw new Exception("Unexpected response type: " + response.getClass().getName());
    }

    /**
     * Send a message asynchronously
     */
    public CompletableFuture<RecordMetadata> sendAsync(String topic, String key, String value) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return send(topic, key, value);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Flush any buffered messages (for future batching implementation)
     */
    public void flush() {
        // Currently a no-op, but would flush batched messages in a full implementation
        log.debug("Flush called");
    }

    /**
     * Close the producer
     */
    public void close() {
        flush();
        client.close();
    }

    /**
     * Check if connected
     */
    public boolean isConnected() {
        return client.isConnected();
    }

}
