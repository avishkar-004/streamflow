package com.streamflow.common.protocol;

import com.streamflow.common.model.Message;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Request to produce (append) messages to a topic partition
 *
 * Payload Format:
 * - Topic Name (string)
 * - Partition ID (4 bytes)
 * - Message Count (4 bytes)
 * - Messages (repeated)
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class ProduceRequest extends Request {

    private String topic;
    private int partition;
    private List<Message> messages;

    @Builder
    public ProduceRequest(String topic, int partition, List<Message> messages) {
        super(Protocol.API_KEY_PRODUCE, Protocol.PROTOCOL_VERSION);
        this.topic = topic;
        this.partition = partition;
        this.messages = messages != null ? messages : new ArrayList<>();
    }

    @Override
    public byte[] serialize() {
        // Calculate size
        int size = stringSize(topic) + 4 + 4; // topic + partition + message count

        for (Message message : messages) {
            size += 4; // message size prefix
            size += message.sizeInBytes();
        }

        ByteBuffer buffer = ByteBuffer.allocate(size);

        // Write fields
        writeString(buffer, topic);
        buffer.putInt(partition);
        buffer.putInt(messages.size());

        // Write messages
        for (Message message : messages) {
            byte[] messageData = message.serialize();
            buffer.putInt(messageData.length);
            buffer.put(messageData);
        }

        return buffer.array();
    }

    public static ProduceRequest deserialize(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);

        String topic = readString(buffer);
        int partition = buffer.getInt();
        int messageCount = buffer.getInt();

        List<Message> messages = new ArrayList<>(messageCount);
        for (int i = 0; i < messageCount; i++) {
            int messageSize = buffer.getInt();
            byte[] messageData = new byte[messageSize];
            buffer.get(messageData);
            messages.add(Message.deserialize(messageData));
        }

        return ProduceRequest.builder()
                .topic(topic)
                .partition(partition)
                .messages(messages)
                .build();
    }
}
