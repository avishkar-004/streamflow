package com.streamflow.common.protocol;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

/**
 * Request to join a consumer group
 *
 * Sent when a consumer wants to join a group and participate in
 * partition assignment and load balancing.
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class JoinGroupRequest extends Request {

    private String groupId;      // Consumer group to join
    private String consumerId;   // Unique consumer identifier
    private Set<String> topics;  // Topics to subscribe to

    @Builder
    public JoinGroupRequest(String groupId, String consumerId, Set<String> topics) {
        super(Protocol.API_KEY_JOIN_GROUP, Protocol.PROTOCOL_VERSION);
        this.groupId = groupId;
        this.consumerId = consumerId;
        this.topics = topics != null ? topics : new HashSet<>();
    }

    @Override
    public byte[] serialize() {
        int size = stringSize(groupId) + stringSize(consumerId) + 4; // +4 for topic count

        for (String topic : topics) {
            size += stringSize(topic);
        }

        ByteBuffer buffer = ByteBuffer.allocate(size);

        writeString(buffer, groupId);
        writeString(buffer, consumerId);
        buffer.putInt(topics.size());
        for (String topic : topics) {
            writeString(buffer, topic);
        }

        return buffer.array();
    }

    public static JoinGroupRequest deserialize(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);

        String groupId = readString(buffer);
        String consumerId = readString(buffer);

        int topicCount = buffer.getInt();
        Set<String> topics = new HashSet<>();
        for (int i = 0; i < topicCount; i++) {
            topics.add(readString(buffer));
        }

        return JoinGroupRequest.builder()
                .groupId(groupId)
                .consumerId(consumerId)
                .topics(topics)
                .build();
    }
}
