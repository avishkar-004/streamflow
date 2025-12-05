package com.streamflow.common.protocol;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.nio.ByteBuffer;

/**
 * Response to a produce request
 *
 * Payload Format:
 * - Offset (8 bytes): Offset of the first message appended
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class ProduceResponse extends Response {

    private long offset;

    @Builder
    public ProduceResponse(short errorCode, long offset) {
        super(Protocol.API_KEY_PRODUCE, errorCode);
        this.offset = offset;
    }

    public ProduceResponse(long offset) {
        this(ERROR_NONE, offset);
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(offset);
        return buffer.array();
    }

    public static ProduceResponse deserialize(short errorCode, byte[] payload) {
        if (payload.length < 8) {
            return ProduceResponse.builder()
                    .errorCode(errorCode)
                    .offset(-1)
                    .build();
        }

        ByteBuffer buffer = ByteBuffer.wrap(payload);
        long offset = buffer.getLong();

        return ProduceResponse.builder()
                .errorCode(errorCode)
                .offset(offset)
                .build();
    }
}
