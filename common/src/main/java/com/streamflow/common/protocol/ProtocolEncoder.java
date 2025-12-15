package com.streamflow.common.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import lombok.extern.slf4j.Slf4j;

/**
 * Encoder for the StreamFlow binary protocol
 *
 * Encodes Response objects into bytes
 *
 * Message Format:
 * ┌─────────────┬─────────────┬─────────────┬──────────────┐
 * │  Size (4B)  │ API Key (2B)│ Error (2B)  │   Payload    │
 * └─────────────┴─────────────┴─────────────┴──────────────┘
 */
@Slf4j
public class ProtocolEncoder extends MessageToByteEncoder<Response> {

    @Override
    protected void encode(ChannelHandlerContext ctx, Response response, ByteBuf out) {
        try {
            // Serialize payload
            byte[] payload = response.serialize();

            // Calculate total size (apiKey + errorCode + payload)
            int size = 2 + 2 + payload.length;

            log.debug("Encoding response: apiKey={} ({}), errorCode={}, payloadSize={}",
                    response.getApiKey(),
                    Protocol.getApiKeyName(response.getApiKey()),
                    response.getErrorCode(),
                    payload.length);

            // Write message
            out.writeInt(size);
            out.writeShort(response.getApiKey());
            out.writeShort(response.getErrorCode());
            out.writeBytes(payload);

            log.debug("Encoded response: {}, totalSize={}", response.getClass().getSimpleName(), size + 4);

        } catch (Exception e) {
            log.error("Failed to encode response: {}", response.getClass().getSimpleName(), e);
            ctx.close();
        }
    }
}
