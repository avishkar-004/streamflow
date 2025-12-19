package com.streamflow.broker.server;

import com.streamflow.broker.config.BrokerConfig;
import com.streamflow.broker.controller.TopicManager;
import com.streamflow.broker.coordinator.OffsetManager;
import com.streamflow.common.protocol.ProtocolDecoder;
import com.streamflow.common.protocol.ProtocolEncoder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;

/**
 * Netty-based TCP server for the broker
 *
 * Handles incoming connections from producers and consumers
 */
@Slf4j
public class BrokerServer {

    private final BrokerConfig config;
    private final TopicManager topicManager;
    private final OffsetManager offsetManager;
    private final com.streamflow.broker.coordinator.ConsumerGroupCoordinator groupCoordinator;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public BrokerServer(BrokerConfig config, TopicManager topicManager, OffsetManager offsetManager,
                       com.streamflow.broker.coordinator.ConsumerGroupCoordinator groupCoordinator) {
        this.config = config;
        this.topicManager = topicManager;
        this.offsetManager = offsetManager;
        this.groupCoordinator = groupCoordinator;
    }

    /**
     * Start the broker server
     */
    public void start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(config.getNumNetworkThreads());
        workerGroup = new NioEventLoopGroup(config.getNumIoThreads());

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();

                            // Add protocol codec
                            pipeline.addLast("decoder", new ProtocolDecoder());
                            pipeline.addLast("encoder", new ProtocolEncoder());

                            // Add request handler
                            pipeline.addLast("handler", new RequestHandler(topicManager, offsetManager, groupCoordinator));
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_SNDBUF, config.getSocketSendBufferBytes())
                    .childOption(ChannelOption.SO_RCVBUF, config.getSocketReceiveBufferBytes());

            // Bind and start to accept incoming connections
            ChannelFuture future = bootstrap.bind(config.getHost(), config.getPort()).sync();
            serverChannel = future.channel();

            int actualPort = ((java.net.InetSocketAddress) serverChannel.localAddress()).getPort();

            log.info("╔═══════════════════════════════════════════════════════════════╗");
            log.info("║           StreamFlow Broker Started Successfully              ║");
            log.info("╠═══════════════════════════════════════════════════════════════╣");
            log.info("║  Broker ID      : {}                                            ║", config.getBrokerId());
            log.info("║  Host           : {}                                      ║", config.getHost());
            log.info("║  Port           : {}                                           ║", actualPort);
            log.info("║  Data Directory : {}        ║", config.getDataDir().getAbsolutePath());
            log.info("╚═══════════════════════════════════════════════════════════════╝");

        } catch (Exception e) {
            log.error("Failed to start broker server", e);
            shutdown();
            throw e;
        }
    }

    /**
     * Shutdown the broker server
     */
    public void shutdown() {
        log.info("Shutting down broker server...");

        try {
            if (serverChannel != null) {
                serverChannel.close().sync();
            }
        } catch (InterruptedException e) {
            log.error("Error closing server channel", e);
        }

        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }

        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }

        // Flush and close topic manager
        topicManager.flushAll();
        topicManager.closeAll();

        log.info("Broker server shutdown complete");
    }

    /**
     * Wait for the server to terminate
     */
    public void awaitTermination() throws InterruptedException {
        if (serverChannel != null) {
            serverChannel.closeFuture().sync();
        }
    }

    /**
     * Get the actual port the server is listening on
     */
    public int getPort() {
        if (serverChannel != null) {
            return ((java.net.InetSocketAddress) serverChannel.localAddress()).getPort();
        }
        return config.getPort();
    }
}
