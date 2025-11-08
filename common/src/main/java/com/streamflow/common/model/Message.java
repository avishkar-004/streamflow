package com.streamflow.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Represents a single message in the event streaming system.
 *
 * Messages are the fundamental unit of data. Each message has:
 * - offset: unique sequential ID within a partition
 * - timestamp: when the message was created
 * - key: optional routing identifier
 * - value: the actual payload
 *
 * Serialization Format (Binary):
 * [Offset 8B][Timestamp 8B][KeySize 4B][Key][ValueSize 4B][Value]
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Message {

    private long offset;
    private long timestamp;
    private String key;
    private String value;

    public int sizeInBytes() {
        int keyBytes = (key != null) ? key.getBytes(StandardCharsets.UTF_8).length : 0;
        int valueBytes = (value != null) ? value.getBytes(StandardCharsets.UTF_8).length : 0;
        return 8 + 8 + 4 + keyBytes + 4 + valueBytes;
    }

    /**
     * Serialize message to byte array for storage
     */
    public byte[] serialize() {
        byte[] keyBytes = (key != null) ? key.getBytes(StandardCharsets.UTF_8) : new byte[0];
        byte[] valueBytes = (value != null) ? value.getBytes(StandardCharsets.UTF_8) : new byte[0];

        ByteBuffer buffer = ByteBuffer.allocate(sizeInBytes());
        buffer.putLong(offset);
        buffer.putLong(timestamp);
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.putInt(valueBytes.length);
        buffer.put(valueBytes);
        return buffer.array();
    }

    /**
     * Deserialize message from byte array
     */
    public static Message deserialize(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        long offset = buffer.getLong();
        long timestamp = buffer.getLong();

        int keySize = buffer.getInt();
        byte[] keyBytes = new byte[keySize];
        if (keySize > 0) buffer.get(keyBytes);
        String key = new String(keyBytes, StandardCharsets.UTF_8);

        int valueSize = buffer.getInt();
        byte[] valueBytes = new byte[valueSize];
        if (valueSize > 0) buffer.get(valueBytes);
        String value = new String(valueBytes, StandardCharsets.UTF_8);

        return Message.builder()
                .offset(offset).timestamp(timestamp)
                .key(key).value(value).build();
    }

    public static int parseMessageSize(ByteBuffer buffer, int position) {
        int keySize = buffer.getInt(position + 16);
        int keyDataLength = Math.max(keySize, 0);
        int valueSize = buffer.getInt(position + 20 + keyDataLength);
        int valueDataLength = Math.max(valueSize, 0);
        return 8 + 8 + 4 + keyDataLength + 4 + valueDataLength;
    }

    @Override
    public String toString() {
        return String.format("Message{offset=%d, timestamp=%d, key='%s', value='%s'}",
                offset, timestamp, key, value);
    }
}
