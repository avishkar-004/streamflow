package com.streamflow.common.protocol;

import com.streamflow.common.model.Message;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Response to a fetch request
 *
 * Payload Format:
 * - Message Count (4 bytes)
 * - Messages (repeated)
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class FetchResponse extends Response {

    private List<Message> messages;

    @Builder
    public FetchResponse(short errorCode, List<Message> messages) {
        super(Protocol.API_KEY_FETCH, errorCode);
        this.messages = messages != null ? messages : new ArrayList<>();
    }

    public FetchResponse(List<Message> messages) {
        this(ERROR_NONE, messages);
    }

    @Override
    public byte[] serialize() {
        // Calculate size
        int size = 4; // message count
        for (Message message : messages) {
            size += 4; // message size prefix
            size += message.sizeInBytes();
        }

        ByteBuffer buffer = ByteBuffer.allocate(size);

        // Write message count
        buffer.putInt(messages.size());

        // Write messages
        for (Message message : messages) {
            byte[] messageData = message.serialize();
            buffer.putInt(messageData.length);
            buffer.put(messageData);
        }

        return buffer.array();
    }

    public static FetchResponse deserialize(short errorCode, byte[] payload) {
        if (payload.length < 4) {
            return FetchResponse.builder()
                    .errorCode(errorCode)
                    .messages(new ArrayList<>())
                    .build();
        }

        ByteBuffer buffer = ByteBuffer.wrap(payload);
        int messageCount = buffer.getInt();

        List<Message> messages = new ArrayList<>(messageCount);
        for (int i = 0; i < messageCount; i++) {
            int messageSize = buffer.getInt();
            byte[] messageData = new byte[messageSize];
            buffer.get(messageData);
            messages.add(Message.deserialize(messageData));
        }

        return FetchResponse.builder()
                .errorCode(errorCode)
                .messages(messages)
                .build();
    }
}
