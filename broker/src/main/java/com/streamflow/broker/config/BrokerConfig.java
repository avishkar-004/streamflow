package com.streamflow.broker.config;

import lombok.Builder;
import lombok.Data;

import java.io.File;

/**
 * Configuration for the broker
 */
@Data
@Builder
public class BrokerConfig {

    // Broker identity
    @Builder.Default
    private int brokerId = 0;

    @Builder.Default
    private String host = "localhost";

    @Builder.Default
    private int port = 9092;

    // Storage configuration
    @Builder.Default
    private File dataDir = new File("data/broker");

    @Builder.Default
    private int defaultPartitions = 3;

    @Builder.Default
    private long segmentSizeBytes = 100 * 1024 * 1024; // 100MB

    @Builder.Default
    private long retentionMs = 7 * 24 * 60 * 60 * 1000L; // 7 days

    // Network configuration
    @Builder.Default
    private int numNetworkThreads = 3;

    @Builder.Default
    private int numIoThreads = 8;

    @Builder.Default
    private int socketSendBufferBytes = 100 * 1024; // 100KB

    @Builder.Default
    private int socketReceiveBufferBytes = 100 * 1024; // 100KB

    @Builder.Default
    private int maxMessageSize = 1 * 1024 * 1024; // 1MB

    // Replication configuration
    @Builder.Default
    private int replicationFactor = 1;

    @Builder.Default
    private int minInSyncReplicas = 1;

    // Cluster configuration - list of all broker IDs in cluster
    // For single-broker mode, just contains this broker's ID
    @Builder.Default
    private java.util.List<Integer> clusterBrokerIds = java.util.Arrays.asList(0);

    // Flush configuration
    @Builder.Default
    private long flushIntervalMs = 1000; // 1 second

    @Builder.Default
    private int flushIntervalMessages = 10000;

    /**
     * Create a default configuration
     */
    public static BrokerConfig defaultConfig() {
        return BrokerConfig.builder().build();
    }

    /**
     * Create a configuration for testing
     */
    public static BrokerConfig testConfig() {
        return BrokerConfig.builder()
                .dataDir(new File("target/test-data/broker-" + System.currentTimeMillis()))
                .port(0) // Random port
                .build();
    }

    /**
     * Validate the configuration
     */
    public void validate() {
        if (brokerId < 0) {
            throw new IllegalArgumentException("Broker ID must be non-negative");
        }

        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 0 and 65535");
        }

        if (defaultPartitions <= 0) {
            throw new IllegalArgumentException("Default partitions must be positive");
        }

        if (segmentSizeBytes <= 0) {
            throw new IllegalArgumentException("Segment size must be positive");
        }

        if (retentionMs <= 0) {
            throw new IllegalArgumentException("Retention time must be positive");
        }

        // Create data directory if it doesn't exist
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }
    }
}
