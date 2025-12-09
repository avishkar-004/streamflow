package com.streamflow.common.protocol;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Response to offset commit request
 *
 * Payload Format: Empty (only error code matters)
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class OffsetCommitResponse extends Response {

    @Builder
    public OffsetCommitResponse(short errorCode) {
        super(Protocol.API_KEY_OFFSET_COMMIT, errorCode);
    }

    public OffsetCommitResponse() {
        this(ERROR_NONE);
    }

    @Override
    public byte[] serialize() {
        return new byte[0]; // Empty payload
    }

    public static OffsetCommitResponse deserialize(short errorCode, byte[] payload) {
        return OffsetCommitResponse.builder()
                .errorCode(errorCode)
                .build();
    }
}
