package com.streamflow.common.protocol;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.ByteBuffer;

/**
 * Base class for all request types
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public abstract class Request {

    private short apiKey;
    private short version;

    /**
     * Serialize request to byte array
     */
    public abstract byte[] serialize();

    /**
     * Deserialize payload into request object
     */
    public static Request deserialize(short apiKey, short version, byte[] payload) {
        return switch (apiKey) {
            case Protocol.API_KEY_PRODUCE -> ProduceRequest.deserialize(payload);
            case Protocol.API_KEY_FETCH -> FetchRequest.deserialize(payload);
            case Protocol.API_KEY_METADATA -> MetadataRequest.deserialize(payload);
            case Protocol.API_KEY_OFFSET_COMMIT -> OffsetCommitRequest.deserialize(payload);
            case Protocol.API_KEY_HEARTBEAT -> HeartbeatRequest.deserialize(payload);
            case Protocol.API_KEY_JOIN_GROUP -> JoinGroupRequest.deserialize(payload);
            case Protocol.API_KEY_LEAVE_GROUP -> LeaveGroupRequest.deserialize(payload);
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
}
