package com.streamflow.broker;

import com.streamflow.broker.config.BrokerConfig;
import com.streamflow.broker.controller.TopicManager;
import com.streamflow.broker.coordinator.OffsetManager;
import com.streamflow.broker.coordinator.ConsumerGroupCoordinator;
import com.streamflow.broker.replication.ReplicaManager;
import com.streamflow.broker.server.BrokerServer;
import lombok.extern.slf4j.Slf4j;

import java.io.File;

/**
 * Main application class for the StreamFlow Broker
 */
@Slf4j
public class BrokerApplication {

    private final BrokerConfig config;
    private final TopicManager topicManager;
    private final OffsetManager offsetManager;
    private final ConsumerGroupCoordinator groupCoordinator;
    private final ReplicaManager replicaManager;
    private final BrokerServer server;

    public BrokerApplication(BrokerConfig config) {
        this.config = config;
        this.config.validate();

        // Initialize components
        this.topicManager = new TopicManager(config.getDataDir(), config.getDefaultPartitions());
        this.offsetManager = new OffsetManager(config.getDataDir());
        this.groupCoordinator = new ConsumerGroupCoordinator(topicManager);

        // Initialize replication manager
        this.replicaManager = new ReplicaManager(
                config.getBrokerId(),
                topicManager,
                config.getClusterBrokerIds(),
                config.getReplicationFactor()
        );

        this.server = new BrokerServer(config, topicManager, offsetManager, groupCoordinator);

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    /**
     * Start the broker
     */
    public void start() throws InterruptedException {
        log.info("Starting StreamFlow Broker...");
        server.start();
    }

    /**
     * Shutdown the broker
     */
    public void shutdown() {
        log.info("Shutting down StreamFlow Broker...");
        server.shutdown();
        groupCoordinator.shutdown();
        replicaManager.shutdown();
    }

    /**
     * Wait for termination
     */
    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    /**
     * Main entry point
     */
    public static void main(String[] args) {
        try {
            // Parse command line arguments (simplified)
            BrokerConfig config = parseArgs(args);

            // Create and start broker
            BrokerApplication broker = new BrokerApplication(config);
            broker.start();
            broker.awaitTermination();

        } catch (Exception e) {
            log.error("Failed to start broker", e);
            System.exit(1);
        }
    }

    /**
     * Parse command line arguments
     */
    private static BrokerConfig parseArgs(String[] args) {
        BrokerConfig.BrokerConfigBuilder builder = BrokerConfig.builder();

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--broker-id":
                    builder.brokerId(Integer.parseInt(args[++i]));
                    break;
                case "--host":
                    builder.host(args[++i]);
                    break;
                case "--port":
                    builder.port(Integer.parseInt(args[++i]));
                    break;
                case "--data-dir":
                    builder.dataDir(new File(args[++i]));
                    break;
                case "--partitions":
                    builder.defaultPartitions(Integer.parseInt(args[++i]));
                    break;
                default:
                    if (args[i].startsWith("--")) {
                        log.warn("Unknown argument: {}", args[i]);
                    }
            }
        }

        return builder.build();
    }

    /**
     * Get the broker server (for testing)
     */
    public BrokerServer getServer() {
        return server;
    }

    /**
     * Get the topic manager (for testing)
     */
    public TopicManager getTopicManager() {
        return topicManager;
    }

    /**
     * Get the offset manager (for testing)
     */
    public OffsetManager getOffsetManager() {
        return offsetManager;
    }

    /**
     * Get the replica manager (for testing)
     */
    public ReplicaManager getReplicaManager() {
        return replicaManager;
    }
}
