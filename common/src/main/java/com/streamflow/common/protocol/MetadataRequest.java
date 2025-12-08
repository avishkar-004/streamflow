package com.streamflow.common.protocol;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.nio.ByteBuffer;

/**
 * Request to fetch topic metadata
 *
 * Payload Format:
 * - Topic Name (string) - null for all topics
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class MetadataRequest extends Request {

    private String topic;

    @Builder
    public MetadataRequest(String topic) {
        super(Protocol.API_KEY_METADATA, Protocol.PROTOCOL_VERSION);
        this.topic = topic;
    }

    @Override
    public byte[] serialize() {
        int size = stringSize(topic);
        ByteBuffer buffer = ByteBuffer.allocate(size);
        writeString(buffer, topic);
        return buffer.array();
    }

    public static MetadataRequest deserialize(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        String topic = readString(buffer);
        return MetadataRequest.builder().topic(topic).build();
    }
}
