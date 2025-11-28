package com.streamflow.broker.storage;

import com.streamflow.common.model.Message;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Message class
 */
class MessageTest {

    @Test
    void testMessageSerialization() {
        Message original = Message.builder()
                .offset(100)
                .timestamp(System.currentTimeMillis())
                .key("test-key")
                .value("test-value")
                .build();

        byte[] serialized = original.serialize();
        Message deserialized = Message.deserialize(serialized);

        assertEquals(original.getOffset(), deserialized.getOffset());
        assertEquals(original.getTimestamp(), deserialized.getTimestamp());
        assertEquals(original.getKey(), deserialized.getKey());
        assertEquals(original.getValue(), deserialized.getValue());
    }

    @Test
    void testMessageWithNullKey() {
        Message original = Message.builder()
                .offset(200)
                .timestamp(System.currentTimeMillis())
                .key(null)
                .value("value-only")
                .build();

        byte[] serialized = original.serialize();
        Message deserialized = Message.deserialize(serialized);

        assertEquals(original.getOffset(), deserialized.getOffset());
        assertNull(deserialized.getKey());
        assertEquals(original.getValue(), deserialized.getValue());
    }

    @Test
    void testMessageWithNullValue() {
        Message original = Message.builder()
                .offset(300)
                .timestamp(System.currentTimeMillis())
                .key("key-only")
                .value(null)
                .build();

        byte[] serialized = original.serialize();
        Message deserialized = Message.deserialize(serialized);

        assertEquals(original.getOffset(), deserialized.getOffset());
        assertEquals(original.getKey(), deserialized.getKey());
        assertNull(deserialized.getValue());
    }

    @Test
    void testMessageSizeCalculation() {
        Message message = Message.builder()
                .offset(0)
                .timestamp(0)
                .key("key")
                .value("value")
                .build();

        int expectedSize = 8 + 8 + 4 + 3 + 4 + 5; // offset + timestamp + keySize + key + valueSize + value
        assertEquals(expectedSize, message.sizeInBytes());
    }

    @Test
    void testEmptyMessage() {
        Message original = Message.builder()
                .offset(0)
                .timestamp(0)
                .key("")
                .value("")
                .build();

        byte[] serialized = original.serialize();
        Message deserialized = Message.deserialize(serialized);

        assertEquals(0, deserialized.getOffset());
        assertEquals("", deserialized.getKey());
        assertEquals("", deserialized.getValue());
    }

    @Test
    void testLargeMessage() {
        String largeValue = "x".repeat(10000);
        Message original = Message.builder()
                .offset(999)
                .timestamp(System.currentTimeMillis())
                .key("large")
                .value(largeValue)
                .build();

        byte[] serialized = original.serialize();
        Message deserialized = Message.deserialize(serialized);

        assertEquals(original.getValue(), deserialized.getValue());
        assertEquals(largeValue.length(), deserialized.getValue().length());
    }
}
