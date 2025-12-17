package com.streamflow.common.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * Decoder for the StreamFlow binary protocol
 *
 * Decodes incoming bytes into Request objects
 *
 * Message Format:
 * ┌─────────────┬─────────────┬─────────────┬──────────────┐
 * │  Size (4B)  │ API Key (2B)│ Version (2B)│   Payload    │
 * └─────────────┴─────────────┴─────────────┴──────────────┘
 */
@Slf4j
public class ProtocolDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        // Need at least header size bytes
        if (in.readableBytes() < Protocol.HEADER_SIZE) {
            return;
        }

        // Mark the current reader index
        in.markReaderIndex();

        // Read message size
        int size = in.readInt();

        // Validate message size
        if (size < 0 || size > Protocol.MAX_MESSAGE_SIZE) {
            log.error("Invalid message size: {}", size);
            ctx.close();
            return;
        }

        // Check if we have the full message
        if (in.readableBytes() < size) {
            // Not enough data yet, reset reader index
            in.resetReaderIndex();
            return;
        }

        // Read header
        short apiKey = in.readShort();
        short version = in.readShort();

        log.debug("Decoding request: apiKey={} ({}), version={}, size={}",
                apiKey, Protocol.getApiKeyName(apiKey), version, size);

        // Read payload
        int payloadSize = size - 4; // subtract apiKey(2) + version(2)
        byte[] payload = new byte[payloadSize];
        in.readBytes(payload);

        try {
            // Deserialize request
            Request request = Request.deserialize(apiKey, version, payload);
            out.add(request);

            log.debug("Decoded request: {}", request.getClass().getSimpleName());

        } catch (Exception e) {
            log.error("Failed to decode request: apiKey={}, version={}", apiKey, version, e);
            ctx.close();
        }
    }
}
