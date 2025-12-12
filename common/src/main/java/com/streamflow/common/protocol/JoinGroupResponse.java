package com.streamflow.common.protocol;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Response to JoinGroupRequest
 *
 * Returns the partition assignment for the consumer that joined the group.
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class JoinGroupResponse extends Response {

    private List<Integer> assignedPartitions;  // Partitions assigned to this consumer

    @Builder
    public JoinGroupResponse(short errorCode, List<Integer> assignedPartitions) {
        super(Protocol.API_KEY_JOIN_GROUP, errorCode);
        this.assignedPartitions = assignedPartitions != null ? assignedPartitions : new ArrayList<>();
    }

    public JoinGroupResponse(List<Integer> assignedPartitions) {
        this(ERROR_NONE, assignedPartitions);
    }

    @Override
    public byte[] serialize() {
        int size = 4; // partition count
        size += assignedPartitions.size() * 4; // partition IDs

        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.putInt(assignedPartitions.size());
        for (int partition : assignedPartitions) {
            buffer.putInt(partition);
        }

        return buffer.array();
    }

    public static JoinGroupResponse deserialize(short errorCode, byte[] payload) {
        if (payload.length < 4) {
            return JoinGroupResponse.builder()
                    .errorCode(errorCode)
                    .assignedPartitions(new ArrayList<>())
                    .build();
        }

        ByteBuffer buffer = ByteBuffer.wrap(payload);
        int count = buffer.getInt();

        List<Integer> partitions = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            partitions.add(buffer.getInt());
        }

        return JoinGroupResponse.builder()
                .errorCode(errorCode)
                .assignedPartitions(partitions)
                .build();
    }
}
