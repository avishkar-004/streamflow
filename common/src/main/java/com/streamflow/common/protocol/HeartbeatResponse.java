package com.streamflow.common.protocol;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Response to heartbeat request
 *
 * Payload Format: Empty (only error code matters)
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class HeartbeatResponse extends Response {

    @Builder
    public HeartbeatResponse(short errorCode) {
        super(Protocol.API_KEY_HEARTBEAT, errorCode);
    }

    public HeartbeatResponse() {
        this(ERROR_NONE);
    }

    @Override
    public byte[] serialize() {
        return new byte[0]; // Empty payload
    }

    public static HeartbeatResponse deserialize(short errorCode, byte[] payload) {
        return HeartbeatResponse.builder()
                .errorCode(errorCode)
                .build();
    }
}
