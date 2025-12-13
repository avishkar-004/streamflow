package com.streamflow.common.protocol;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Response to LeaveGroupRequest
 *
 * Simple acknowledgment that the consumer has left the group.
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class LeaveGroupResponse extends Response {

    @Builder
    public LeaveGroupResponse(short errorCode) {
        super(Protocol.API_KEY_LEAVE_GROUP, errorCode);
    }

    public LeaveGroupResponse() {
        this(ERROR_NONE);
    }

    @Override
    public byte[] serialize() {
        return new byte[0]; // Empty payload
    }

    public static LeaveGroupResponse deserialize(short errorCode, byte[] payload) {
        return LeaveGroupResponse.builder()
                .errorCode(errorCode)
                .build();
    }
}
