package com.streamflow.client.common;

import com.streamflow.common.protocol.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Network client for communicating with the broker
 *
 * Handles connection management and request/response correlation
 */
@Slf4j
public class NetworkClient {

    private final String host;
    private final int port;
    private final EventLoopGroup eventLoopGroup;
    private final ConcurrentHashMap<Integer, CompletableFuture<Response>> pendingRequests;
    private final AtomicInteger requestIdGenerator;

    private Channel channel;
    private boolean connected;

    public NetworkClient(String host, int port) {
        this.host = host;
        this.port = port;
        this.eventLoopGroup = new NioEventLoopGroup(1);
        this.pendingRequests = new ConcurrentHashMap<>();
        this.requestIdGenerator = new AtomicInteger(0);
        this.connected = false;
    }

    /**
     * Connect to the broker
     */
    public void connect() throws InterruptedException {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();

                        // Add protocol codec
                        pipeline.addLast(new ProtocolDecoder());
                        pipeline.addLast(new ProtocolEncoder());

                        // Add response handler
                        pipeline.addLast(new ResponseHandler());
                    }
                });

        ChannelFuture future = bootstrap.connect(host, port).sync();
        channel = future.channel();
        connected = true;

        log.info("Connected to broker at {}:{}", host, port);
    }

    /**
     * Send a request and wait for response
     */
    public Response send(Request request, long timeoutMs) throws Exception {
        if (!connected || channel == null || !channel.isActive()) {
            throw new IllegalStateException("Not connected to broker");
        }

        CompletableFuture<Response> responseFuture = new CompletableFuture<>();
        int requestId = requestIdGenerator.getAndIncrement();
        pendingRequests.put(requestId, responseFuture);

        // Write request to channel
        channel.writeAndFlush(request).addListener((ChannelFutureListener) future -> {
            if (!future.isSuccess()) {
                pendingRequests.remove(requestId);
                responseFuture.completeExceptionally(future.cause());
            }
        });

        // Wait for response with timeout
        try {
            return responseFuture.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            pendingRequests.remove(requestId);
            throw e;
        }
    }

    /**
     * Send a request with default timeout (5 seconds)
     */
    public Response send(Request request) throws Exception {
        return send(request, 5000);
    }

    /**
     * Close the connection
     */
    public void close() {
        if (channel != null) {
            channel.close();
        }
        eventLoopGroup.shutdownGracefully();
        connected = false;
        log.info("Disconnected from broker");
    }

    /**
     * Check if connected
     */
    public boolean isConnected() {
        return connected && channel != null && channel.isActive();
    }

    /**
     * Response handler that completes pending requests
     */
    private class ResponseHandler extends SimpleChannelInboundHandler<Response> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Response response) {
            // For simplicity, we complete the oldest pending request
            // In a full implementation, you'd use correlation IDs
            if (!pendingRequests.isEmpty()) {
                Integer requestId = pendingRequests.keys().nextElement();
                CompletableFuture<Response> future = pendingRequests.remove(requestId);
                if (future != null) {
                    future.complete(response);
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("Exception in response handler", cause);

            // Complete all pending requests with error
            pendingRequests.values().forEach(future ->
                    future.completeExceptionally(cause));
            pendingRequests.clear();

            ctx.close();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            log.warn("Connection to broker lost");
            connected = false;

            // Complete all pending requests with error
            pendingRequests.values().forEach(future ->
                    future.completeExceptionally(new Exception("Connection lost")));
            pendingRequests.clear();
        }
    }
}
