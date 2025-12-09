package com.streamflow.common.protocol;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.nio.ByteBuffer;

/**
 * Request to commit consumer offset
 *
 * Payload Format:
 * - Consumer Group (string)
 * - Topic Name (string)
 * - Partition ID (4 bytes)
 * - Offset (8 bytes)
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class OffsetCommitRequest extends Request {

    private String consumerGroup;
    private String topic;
    private int partition;
    private long offset;

    @Builder
    public OffsetCommitRequest(String consumerGroup, String topic, int partition, long offset) {
        super(Protocol.API_KEY_OFFSET_COMMIT, Protocol.PROTOCOL_VERSION);
        this.consumerGroup = consumerGroup;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    @Override
    public byte[] serialize() {
        int size = stringSize(consumerGroup) + stringSize(topic) + 4 + 8;
        ByteBuffer buffer = ByteBuffer.allocate(size);

        writeString(buffer, consumerGroup);
        writeString(buffer, topic);
        buffer.putInt(partition);
        buffer.putLong(offset);

        return buffer.array();
    }

    public static OffsetCommitRequest deserialize(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);

        String consumerGroup = readString(buffer);
        String topic = readString(buffer);
        int partition = buffer.getInt();
        long offset = buffer.getLong();

        return OffsetCommitRequest.builder()
                .consumerGroup(consumerGroup)
                .topic(topic)
                .partition(partition)
                .offset(offset)
                .build();
    }
}
