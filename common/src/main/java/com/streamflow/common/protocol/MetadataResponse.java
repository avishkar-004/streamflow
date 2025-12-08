package com.streamflow.common.protocol;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.nio.ByteBuffer;

/**
 * Response containing topic metadata
 *
 * Payload Format:
 * - Topic Name (string)
 * - Partition Count (4 bytes)
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class MetadataResponse extends Response {

    private String topic;
    private int partitions;

    @Builder
    public MetadataResponse(short errorCode, String topic, int partitions) {
        super(Protocol.API_KEY_METADATA, errorCode);
        this.topic = topic;
        this.partitions = partitions;
    }

    public MetadataResponse(String topic, int partitions) {
        this(ERROR_NONE, topic, partitions);
    }

    @Override
    public byte[] serialize() {
        int size = stringSize(topic) + 4;
        ByteBuffer buffer = ByteBuffer.allocate(size);

        writeString(buffer, topic);
        buffer.putInt(partitions);

        return buffer.array();
    }

    public static MetadataResponse deserialize(short errorCode, byte[] payload) {
        if (payload.length < 8) {
            return MetadataResponse.builder()
                    .errorCode(errorCode)
                    .topic(null)
                    .partitions(0)
                    .build();
        }

        ByteBuffer buffer = ByteBuffer.wrap(payload);
        String topic = readString(buffer);
        int partitions = buffer.getInt();

        return MetadataResponse.builder()
                .errorCode(errorCode)
                .topic(topic)
                .partitions(partitions)
                .build();
    }
}
