package com.streamflow.broker.replication;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Simplified leader election for partitions
 *
 * Based on simplified Raft-like algorithm:
 * - Each partition has one leader
 * - Leader is elected from replicas
 * - If leader fails, new election happens
 *
 * Simplified approach (not full Raft):
 * - Static assignment based on partition ID
 * - Round-robin across brokers
 * - Deterministic (no voting needed for simple case)
 *
 * Example: 3 brokers, 6 partitions
 * - Partition 0 → Leader: Broker 0
 * - Partition 1 → Leader: Broker 1
 * - Partition 2 → Leader: Broker 2
 * - Partition 3 → Leader: Broker 0
 * - Partition 4 → Leader: Broker 1
 * - Partition 5 → Leader: Broker 2
 */
@Slf4j
@Data
public class LeaderElection {

    // Topic-Partition -> Leader broker ID
    private final Map<String, Integer> partitionLeaders;

    // Available broker IDs
    private final List<Integer> availableBrokers;

    // Random for tiebreaking
    private final Random random;

    public LeaderElection(List<Integer> brokerIds) {
        this.partitionLeaders = new HashMap<>();
        this.availableBrokers = brokerIds;
        this.random = new Random();
    }

    /**
     * Elect a leader for a partition
     * Uses round-robin distribution across brokers
     */
    public int electLeader(String topic, int partition) {
        String key = makeKey(topic, partition);

        // Check if leader already exists
        if (partitionLeaders.containsKey(key)) {
            return partitionLeaders.get(key);
        }

        // Assign leader using round-robin
        int leaderIndex = partition % availableBrokers.size();
        int leaderId = availableBrokers.get(leaderIndex);

        partitionLeaders.put(key, leaderId);

        log.info("Elected broker {} as leader for {}-{}", leaderId, topic, partition);
        return leaderId;
    }

    /**
     * Force re-election (when current leader fails)
     */
    public int reElectLeader(String topic, int partition, int failedLeaderId) {
        String key = makeKey(topic, partition);

        // Remove failed leader from available brokers temporarily
        availableBrokers.remove(Integer.valueOf(failedLeaderId));

        // Elect new leader from remaining brokers
        if (availableBrokers.isEmpty()) {
            log.error("No available brokers for re-election of {}-{}", topic, partition);
            return -1;
        }

        int newLeaderIndex = partition % availableBrokers.size();
        int newLeaderId = availableBrokers.get(newLeaderIndex);

        partitionLeaders.put(key, newLeaderId);

        log.warn("Re-elected broker {} as leader for {}-{} (old leader {} failed)",
                newLeaderId, topic, partition, failedLeaderId);

        return newLeaderId;
    }

    /**
     * Get current leader for a partition
     */
    public int getLeader(String topic, int partition) {
        String key = makeKey(topic, partition);
        Integer leader = partitionLeaders.get(key);

        if (leader == null) {
            // No leader yet, elect one
            return electLeader(topic, partition);
        }

        return leader;
    }

    /**
     * Check if a broker is the leader for a partition
     */
    public boolean isLeader(String topic, int partition, int brokerId) {
        return getLeader(topic, partition) == brokerId;
    }

    /**
     * Set leader explicitly (for testing or manual assignment)
     */
    public void setLeader(String topic, int partition, int brokerId) {
        String key = makeKey(topic, partition);
        partitionLeaders.put(key, brokerId);
        log.info("Manually set broker {} as leader for {}-{}", brokerId, topic, partition);
    }

    /**
     * Remove leader assignment (when partition is deleted)
     */
    public void removeLeader(String topic, int partition) {
        String key = makeKey(topic, partition);
        Integer oldLeader = partitionLeaders.remove(key);
        log.info("Removed leader {} for {}-{}", oldLeader, topic, partition);
    }

    /**
     * Get all partition assignments
     */
    public Map<String, Integer> getAllLeaders() {
        return new HashMap<>(partitionLeaders);
    }

    /**
     * Create key for partition leaders map
     */
    private String makeKey(String topic, int partition) {
        return topic + "-" + partition;
    }

    /**
     * Add a broker to available brokers
     */
    public void addBroker(int brokerId) {
        if (!availableBrokers.contains(brokerId)) {
            availableBrokers.add(brokerId);
            log.info("Added broker {} to available brokers", brokerId);
        }
    }

    /**
     * Remove a broker (when it goes offline)
     */
    public void removeBroker(int brokerId) {
        availableBrokers.remove(Integer.valueOf(brokerId));
        log.warn("Removed broker {} from available brokers", brokerId);

        // Re-elect leaders for partitions that had this broker as leader
        reElectAffectedPartitions(brokerId);
    }

    /**
     * Re-elect leaders for partitions affected by broker failure
     */
    private void reElectAffectedPartitions(int failedBrokerId) {
        for (Map.Entry<String, Integer> entry : partitionLeaders.entrySet()) {
            if (entry.getValue() == failedBrokerId) {
                String[] parts = entry.getKey().split("-");
                String topic = parts[0];
                int partition = Integer.parseInt(parts[1]);

                reElectLeader(topic, partition, failedBrokerId);
            }
        }
    }
}
