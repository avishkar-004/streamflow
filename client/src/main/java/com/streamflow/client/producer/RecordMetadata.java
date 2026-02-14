package com.streamflow.client.producer;

/**
 * Metadata for a successfully sent record
 *
 * Contains information about where the message was stored:
 * - topic: Topic name
 * - partition: Partition ID
 * - offset: Offset within the partition
 * - sizeInBytes: Size of the serialized message
 */
public record RecordMetadata(String topic, int partition, long offset, int sizeInBytes) {

    /**
     * Constructor without size (for backwards compatibility)
     */
    public RecordMetadata(String topic, int partition, long offset) {
        this(topic, partition, offset, 0);
    }

    @Override
    public String toString() {
        return String.format("RecordMetadata{topic=%s, partition=%d, offset=%d, sizeInBytes=%d}",
                topic, partition, offset, sizeInBytes);
    }
}
