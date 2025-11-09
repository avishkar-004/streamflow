package com.streamflow.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * ═══════════════════════════════════════════════════════════════════════════
 * MESSAGE - THE FUNDAMENTAL UNIT OF DATA IN STREAMFLOW
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * PURPOSE:
 * Represents a single message in the event streaming system. Think of it like
 * a letter in the mail - it has a unique ID (offset), timestamp, optional key
 * for routing, and the actual content (value).
 *
 * KEY CONCEPTS:
 * 1. OFFSET: A unique, monotonically increasing ID for each message in a partition
 *            Similar to a page number in a book - starts at 0 and increases
 *
 * 2. TIMESTAMP: When the message was created (in milliseconds since epoch)
 *               Useful for time-based processing and debugging
 *
 * 3. KEY: Optional routing identifier (like a ZIP code for mail)
 *         - Messages with the same key go to the same partition
 *         - Ensures ordering for related messages
 *         - Can be null for round-robin distribution
 *
 * 4. VALUE: The actual message payload/content (the letter itself)
 *
 * SERIALIZATION FORMAT (Binary):
 * ┌──────────────┬──────────────┬──────────┬─────┬────────────┬───────┐
 * │ Offset (8B)  │ Timestamp(8B)│KeySize(4)│ Key │ValueSize(4)│ Value │
 * └──────────────┴──────────────┴──────────┴─────┴────────────┴───────┘
 *
 * WHY BINARY FORMAT?
 * - More efficient than JSON/XML (smaller size, faster parsing)
 * - Fixed-size fields allow for direct memory access
 * - Length-prefixed strings (size first, then data)
 *
 * EXAMPLE:
 * Message msg = Message.builder()
 *     .offset(100)
 *     .timestamp(System.currentTimeMillis())
 *     .key("user-123")      // Route all user-123 messages to same partition
 *     .value("Order placed") // The actual event
 *     .build();
 *
 * @author StreamFlow Team
 * @version 1.0
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Message {

    /**
     * OFFSET: Unique identifier for this message within its partition
     * - Starts at 0 for the first message in a partition
     * - Increases by 1 for each new message
     * - Never reused (even if message is deleted)
     * - Used by consumers to track their position
     */
    private long offset;

    /**
     * TIMESTAMP: When this message was created (milliseconds since Unix epoch)
     * - Set automatically when message is produced
     * - Used for time-based queries and retention
     * - Example: 1678901234567 = March 15, 2023 at 12:20:34 PM
     */
    private long timestamp;

    /**
     * KEY: Optional routing identifier for this message
     * - Determines which partition this message goes to (via hash)
     * - Messages with same key always go to same partition
     * - Null means random partition assignment
     * - Examples: "user-id", "order-id", "session-id"
     */
    private String key;

    /**
     * VALUE: The actual message content/payload
     * - The data you want to send (event, command, etc.)
     * - Stored as string for simplicity (could be JSON, Avro, Protobuf, etc.)
     * - Examples: JSON event, plain text, serialized object
     */
    private String value;

    /**
     * ═════════════════════════════════════════════════════════════════════
     * CALCULATE SIZE IN BYTES
     * ═════════════════════════════════════════════════════════════════════
     *
     * PURPOSE: Determine how much disk space this message will consume
     *
     * CALCULATION BREAKDOWN:
     * 1. Fixed-size fields: 8 + 8 + 4 + 4 = 24 bytes
     *    - offset (long)      = 8 bytes
     *    - timestamp (long)   = 8 bytes
     *    - keySize (int)      = 4 bytes  (uses -1 for null)
     *    - valueSize (int)    = 4 bytes  (uses -1 for null)
     *
     * 2. Variable-size fields:
     *    - key bytes (UTF-8 encoding)
     *    - value bytes (UTF-8 encoding)
     *
     * WHY UTF-8?
     * - Universal character encoding
     * - ASCII characters = 1 byte, others may be 2-4 bytes
     * - "hello" = 5 bytes, "你好" = 6 bytes (Chinese)
     *
     * @return Total size in bytes
     */
    public int sizeInBytes() {
        // Convert strings to bytes using UTF-8
        // null keys/values contribute 0 bytes of data (size field is -1)
        int keyBytes = (key != null) ? key.getBytes(StandardCharsets.UTF_8).length : 0;
        int valueBytes = (value != null) ? value.getBytes(StandardCharsets.UTF_8).length : 0;

        // Total = fixed fields + variable fields
        // offset(8) + timestamp(8) + keySize(4) + key + valueSize(4) + value
        return 8 + 8 + 4 + keyBytes + 4 + valueBytes;
    }

    /**
     * ═════════════════════════════════════════════════════════════════════
     * SERIALIZE TO BINARY FORMAT
     * ═════════════════════════════════════════════════════════════════════
     *
     * PURPOSE: Convert this message object into a byte array for storage/transmission
     *
     * STEP-BY-STEP PROCESS:
     * 1. Convert strings to UTF-8 bytes
     * 2. Allocate a ByteBuffer with exact size needed
     * 3. Write fields in order: offset, timestamp, key (with length prefix), value (with length prefix)
     * 4. Return the byte array
     *
     * WHY BYTEBUFFER?
     * - Handles endianness (byte order) automatically
     * - Efficient for writing different data types
     * - Java NIO standard for binary operations
     *
     * EXAMPLE SERIALIZATION:
     * Message: offset=100, timestamp=1678901234567, key="user1", value="hi"
     * Bytes: [0,0,0,0,0,0,0,100][1,135,125,68,215,7][0,0,0,5][u,s,e,r,1][0,0,0,2][h,i]
     *        \___ offset ___/\__ timestamp __/\keySize/\__key__/\valSize/\value/
     *
     * @return Byte array representation of this message
     */
    public byte[] serialize() {
        // STEP 1: Convert strings to bytes
        // null strings are stored with a length of -1 (no data bytes)
        // empty strings are stored with a length of 0 (no data bytes)
        byte[] keyBytes = (key != null) ? key.getBytes(StandardCharsets.UTF_8) : null;
        byte[] valueBytes = (value != null) ? value.getBytes(StandardCharsets.UTF_8) : null;

        // STEP 2: Allocate buffer with exact size
        ByteBuffer buffer = ByteBuffer.allocate(sizeInBytes());

        // STEP 3: Write fields in order
        buffer.putLong(offset);              // 8 bytes: message offset
        buffer.putLong(timestamp);           // 8 bytes: creation time

        // Write key: use -1 for null, actual length for non-null (including 0 for empty)
        if (keyBytes != null) {
            buffer.putInt(keyBytes.length);   // 4 bytes: length of key (>= 0)
            buffer.put(keyBytes);             // N bytes: key data
        } else {
            buffer.putInt(-1);               // 4 bytes: -1 means null key
        }

        // Write value: same convention
        if (valueBytes != null) {
            buffer.putInt(valueBytes.length); // 4 bytes: length of value (>= 0)
            buffer.put(valueBytes);           // M bytes: value data
        } else {
            buffer.putInt(-1);               // 4 bytes: -1 means null value
        }

        // STEP 4: Return the backing array
        return buffer.array();
    }

    /**
     * ═════════════════════════════════════════════════════════════════════
     * DESERIALIZE FROM BINARY FORMAT
     * ═════════════════════════════════════════════════════════════════════
     *
     * PURPOSE: Reconstruct a Message object from a byte array (reverse of serialize)
     *
     * STEP-BY-STEP PROCESS:
     * 1. Wrap byte array in a ByteBuffer for easy reading
     * 2. Read offset (8 bytes)
     * 3. Read timestamp (8 bytes)
     * 4. Read key size, then key data (4 + N bytes)
     * 5. Read value size, then value data (4 + M bytes)
     * 6. Build and return Message object
     *
     * ERROR HANDLING:
     * - Handles null keys/values gracefully
     * - Uses size field to know how many bytes to read
     *
     * @param data Byte array to deserialize
     * @return Reconstructed Message object
     */
    public static Message deserialize(byte[] data) {
        // STEP 1: Wrap in ByteBuffer for sequential reading
        ByteBuffer buffer = ByteBuffer.wrap(data);

        // STEP 2: Read fixed-size fields
        long offset = buffer.getLong();        // Read 8 bytes
        long timestamp = buffer.getLong();     // Read 8 bytes

        // STEP 3: Read key (length-prefixed)
        // -1 means null, 0 means empty string, >0 means string with data
        int keySize = buffer.getInt();         // Read 4 bytes: how long is key?
        String key;
        if (keySize < 0) {
            key = null;                        // -1 = null key
        } else {
            byte[] keyBytes = new byte[keySize];
            if (keySize > 0) {
                buffer.get(keyBytes);          // Read keySize bytes: the key data
            }
            key = new String(keyBytes, StandardCharsets.UTF_8);
        }

        // STEP 4: Read value (length-prefixed)
        int valueSize = buffer.getInt();       // Read 4 bytes: how long is value?
        String value;
        if (valueSize < 0) {
            value = null;                      // -1 = null value
        } else {
            byte[] valueBytes = new byte[valueSize];
            if (valueSize > 0) {
                buffer.get(valueBytes);        // Read valueSize bytes: the value data
            }
            value = new String(valueBytes, StandardCharsets.UTF_8);
        }

        // STEP 5: Build and return
        return Message.builder()
                .offset(offset)
                .timestamp(timestamp)
                .key(key)
                .value(value)
                .build();
    }

    /**
     * ═════════════════════════════════════════════════════════════════════
     * PARSE MESSAGE SIZE FROM BUFFER (WITHOUT FULL DESERIALIZATION)
     * ═════════════════════════════════════════════════════════════════════
     *
     * PURPOSE: Quickly determine message size for efficient reading
     *
     * WHY NEEDED?
     * When reading from a file, we need to know how many bytes to read
     * without deserializing the entire message. This is faster.
     *
     * HOW IT WORKS:
     * 1. Skip to key size field (at position + 16)
     *    - Offset = 8 bytes, Timestamp = 8 bytes, Total = 16 bytes
     * 2. Read key size
     * 3. Read value size (which is after the key data)
     * 4. Calculate total = 8 + 8 + 4 + keySize + 4 + valueSize
     *
     * ANALOGY:
     * Like reading the table of contents in a book to know chapter lengths
     * instead of reading every page
     *
     * @param buffer ByteBuffer containing the message
     * @param position Starting position of the message in the buffer
     * @return Total size of the message in bytes
     */
    public static int parseMessageSize(ByteBuffer buffer, int position) {
        // Read key size from buffer
        // Position breakdown: offset(8) + timestamp(8) = 16
        int keySize = buffer.getInt(position + 16);

        // Handle null key: keySize == -1 means no key data bytes
        int keyDataLength = Math.max(keySize, 0);

        // Read value size from buffer
        // Position: offset(8) + timestamp(8) + keySize(4) + key(keyDataLength) = 20 + keyDataLength
        int valueSize = buffer.getInt(position + 20 + keyDataLength);

        // Handle null value: valueSize == -1 means no value data bytes
        int valueDataLength = Math.max(valueSize, 0);

        // Calculate total message size
        // offset(8) + timestamp(8) + keySizeField(4) + keyData + valueSizeField(4) + valueData
        return 8 + 8 + 4 + keyDataLength + 4 + valueDataLength;
    }

    /**
     * ═════════════════════════════════════════════════════════════════════
     * TO STRING (FOR DEBUGGING)
     * ═════════════════════════════════════════════════════════════════════
     *
     * Provides a human-readable representation of the message
     * Useful for logging and debugging
     */
    @Override
    public String toString() {
        return String.format("Message{offset=%d, timestamp=%d, key='%s', value='%s'}",
                offset, timestamp, key, value);
    }
}
