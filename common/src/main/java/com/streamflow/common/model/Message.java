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

    /**
     * Calculate the serialized size of this message in bytes
     */
    public int sizeInBytes() {
        int keyBytes = (key != null) ? key.getBytes(StandardCharsets.UTF_8).length : 0;
        int valueBytes = (value != null) ? value.getBytes(StandardCharsets.UTF_8).length : 0;
        return 8 + 8 + 4 + keyBytes + 4 + valueBytes;
    }

    @Override
    public String toString() {
        return String.format("Message{offset=%d, timestamp=%d, key='%s', value='%s'}",
                offset, timestamp, key, value);
    }
}
