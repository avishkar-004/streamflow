package com.streamflow.common.protocol;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.ByteBuffer;

/**
 * Base class for all response types
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public abstract class Response {

    private short apiKey;
    private short errorCode;

    // Error codes
    public static final short ERROR_NONE = 0;
    public static final short ERROR_UNKNOWN = -1;
    public static final short ERROR_TOPIC_NOT_FOUND = 1;
    public static final short ERROR_INVALID_PARTITION = 2;
    public static final short ERROR_OFFSET_OUT_OF_RANGE = 3;
    public static final short ERROR_INVALID_MESSAGE = 4;

    /**
     * Serialize response to byte array
     */
    public abstract byte[] serialize();

    /**
     * Deserialize payload into response object
     */
    public static Response deserialize(short apiKey, short errorCode, byte[] payload) {
        return switch (apiKey) {
            case Protocol.API_KEY_PRODUCE -> ProduceResponse.deserialize(errorCode, payload);
            case Protocol.API_KEY_FETCH -> FetchResponse.deserialize(errorCode, payload);
            case Protocol.API_KEY_METADATA -> MetadataResponse.deserialize(errorCode, payload);
            case Protocol.API_KEY_OFFSET_COMMIT -> OffsetCommitResponse.deserialize(errorCode, payload);
            case Protocol.API_KEY_HEARTBEAT -> HeartbeatResponse.deserialize(errorCode, payload);
            case Protocol.API_KEY_JOIN_GROUP -> JoinGroupResponse.deserialize(errorCode, payload);
            case Protocol.API_KEY_LEAVE_GROUP -> LeaveGroupResponse.deserialize(errorCode, payload);
            default -> throw new IllegalArgumentException("Unknown API key: " + apiKey);
        };
    }

    /**
     * Write string to buffer (length-prefixed)
     */
    protected static void writeString(ByteBuffer buffer, String str) {
        if (str == null) {
            buffer.putInt(-1);
        } else {
            byte[] bytes = str.getBytes();
            buffer.putInt(bytes.length);
            buffer.put(bytes);
        }
    }

    /**
     * Read string from buffer (length-prefixed)
     */
    protected static String readString(ByteBuffer buffer) {
        int length = buffer.getInt();
        if (length < 0) {
            return null;
        }
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes);
    }

    /**
     * Calculate size needed for a string
     */
    protected static int stringSize(String str) {
        return 4 + (str != null ? str.getBytes().length : 0);
    }

    /**
     * Check if the response has an error
     */
    public boolean hasError() {
        return errorCode != ERROR_NONE;
    }

    /**
     * Get error message
     */
    public static String getErrorMessage(short errorCode) {
        return switch (errorCode) {
            case ERROR_NONE -> "No error";
            case ERROR_TOPIC_NOT_FOUND -> "Topic not found";
            case ERROR_INVALID_PARTITION -> "Invalid partition";
            case ERROR_OFFSET_OUT_OF_RANGE -> "Offset out of range";
            case ERROR_INVALID_MESSAGE -> "Invalid message";
            default -> "Unknown error";
        };
    }
}
