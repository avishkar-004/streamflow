package com.streamflow.common.protocol;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.nio.ByteBuffer;

/**
 * Request to leave a consumer group
 *
 * Sent when a consumer is shutting down gracefully or wants to stop
 * consuming from a topic.
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class LeaveGroupRequest extends Request {

    private String groupId;
    private String consumerId;

    @Builder
    public LeaveGroupRequest(String groupId, String consumerId) {
        super(Protocol.API_KEY_LEAVE_GROUP, Protocol.PROTOCOL_VERSION);
        this.groupId = groupId;
        this.consumerId = consumerId;
    }

    @Override
    public byte[] serialize() {
        int size = stringSize(groupId) + stringSize(consumerId);
        ByteBuffer buffer = ByteBuffer.allocate(size);

        writeString(buffer, groupId);
        writeString(buffer, consumerId);

        return buffer.array();
    }

    public static LeaveGroupRequest deserialize(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);

        String groupId = readString(buffer);
        String consumerId = readString(buffer);

        return LeaveGroupRequest.builder()
                .groupId(groupId)
                .consumerId(consumerId)
                .build();
    }
}
