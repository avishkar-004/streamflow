package com.streamflow.common.protocol;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.nio.ByteBuffer;

/**
 * Request to fetch messages from a topic partition
 *
 * Payload Format:
 * - Topic Name (string)
 * - Partition ID (4 bytes)
 * - Offset (8 bytes)
 * - Max Records (4 bytes)
 * - Max Bytes (4 bytes)
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class FetchRequest extends Request {

    private String topic;
    private int partition;
    private long offset;
    private int maxRecords;
    private int maxBytes;

    @Builder
    public FetchRequest(String topic, int partition, long offset, int maxRecords, int maxBytes) {
        super(Protocol.API_KEY_FETCH, Protocol.PROTOCOL_VERSION);
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.maxRecords = maxRecords;
        this.maxBytes = maxBytes;
    }

    @Override
    public byte[] serialize() {
        int size = stringSize(topic) + 4 + 8 + 4 + 4;
        ByteBuffer buffer = ByteBuffer.allocate(size);

        writeString(buffer, topic);
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(maxRecords);
        buffer.putInt(maxBytes);

        return buffer.array();
    }

    public static FetchRequest deserialize(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);

        String topic = readString(buffer);
        int partition = buffer.getInt();
        long offset = buffer.getLong();
        int maxRecords = buffer.getInt();
        int maxBytes = buffer.getInt();

        return FetchRequest.builder()
                .topic(topic)
                .partition(partition)
                .offset(offset)
                .maxRecords(maxRecords)
                .maxBytes(maxBytes)
                .build();
    }
}
