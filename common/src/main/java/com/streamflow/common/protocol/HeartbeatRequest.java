package com.streamflow.common.protocol;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.nio.ByteBuffer;

/**
 * Request to send heartbeat from consumer
 *
 * Payload Format:
 * - Consumer Group (string)
 * - Consumer ID (string)
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class HeartbeatRequest extends Request {

    private String consumerGroup;
    private String consumerId;

    @Builder
    public HeartbeatRequest(String consumerGroup, String consumerId) {
        super(Protocol.API_KEY_HEARTBEAT, Protocol.PROTOCOL_VERSION);
        this.consumerGroup = consumerGroup;
        this.consumerId = consumerId;
    }

    @Override
    public byte[] serialize() {
        int size = stringSize(consumerGroup) + stringSize(consumerId);
        ByteBuffer buffer = ByteBuffer.allocate(size);

        writeString(buffer, consumerGroup);
        writeString(buffer, consumerId);

        return buffer.array();
    }

    public static HeartbeatRequest deserialize(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);

        String consumerGroup = readString(buffer);
        String consumerId = readString(buffer);

        return HeartbeatRequest.builder()
                .consumerGroup(consumerGroup)
                .consumerId(consumerId)
                .build();
    }
}
