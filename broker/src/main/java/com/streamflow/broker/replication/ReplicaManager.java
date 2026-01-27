package com.streamflow.broker.replication;

import com.streamflow.broker.controller.TopicManager;
import com.streamflow.broker.storage.Partition;
import com.streamflow.broker.storage.Topic;
import com.streamflow.common.model.Message;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;

/**
 * Manages all replicas on this broker
 *
 * Responsibilities:
 * - Track which partitions this broker is leader/follower for
 * - Start/stop replica fetchers for follower partitions
 * - Update ISR (In-Sync Replicas)
 * - Coordinate leader election
 * - Manage high watermark
 *
 * Example setup:
 * - Broker 1: Leader for partitions [0,3], Follower for [1,2,4,5]
 * - Broker 2: Leader for partitions [1,4], Follower for [0,2,3,5]
 * - Broker 3: Leader for partitions [2,5], Follower for [0,1,3,4]
 */
@Slf4j
public class ReplicaManager {

    private final int brokerId;
    private final TopicManager topicManager;
    private final ISRManager isrManager;
    private final LeaderElection leaderElection;

    // Topic-Partition -> PartitionReplica
    private final Map<String, PartitionReplica> replicas;

    // Active replica fetchers for follower partitions
    private final Map<String, ReplicaFetcher> activeFetchers;
    private final ExecutorService fetcherExecutor;

    // Replication configuration
    private final int replicationFactor;

    public ReplicaManager(int brokerId, TopicManager topicManager,
                         List<Integer> allBrokerIds, int replicationFactor) {
        this.brokerId = brokerId;
        this.topicManager = topicManager;
        this.replicationFactor = replicationFactor;

        this.isrManager = new ISRManager();
        this.leaderElection = new LeaderElection(allBrokerIds);
        this.replicas = new ConcurrentHashMap<>();
        this.activeFetchers = new ConcurrentHashMap<>();
        this.fetcherExecutor = Executors.newCachedThreadPool();

        log.info("ReplicaManager initialized for broker {}, replicationFactor={}",
                brokerId, replicationFactor);
    }

    /**
     * Add a partition replica to this broker
     */
    public void addReplica(String topicName, int partitionId) {
        String key = makeKey(topicName, partitionId);

        if (replicas.containsKey(key)) {
            log.warn("Replica {}-{} already exists on broker {}", topicName, partitionId, brokerId);
            return;
        }

        // Get partition from topic manager
        Topic topic = topicManager.getTopic(topicName);
        Partition partition = topic.getPartition(partitionId);

        // Create replica
        PartitionReplica replica = new PartitionReplica(topicName, partitionId, brokerId, partition);

        // Determine if this broker should be leader
        int leaderId = leaderElection.electLeader(topicName, partitionId);

        if (leaderId == brokerId) {
            // This broker is the leader
            replica.becomeLeader();
            isrManager.initializeISR(topicName, partitionId, brokerId);
        } else {
            // This broker is a follower
            replica.becomeFollower();
            startReplicaFetcher(replica, leaderId);
        }

        replicas.put(key, replica);

        log.info("Added replica {}-{} on broker {} as {}",
                topicName, partitionId, brokerId, replica.getState());
    }

    /**
     * Remove a partition replica from this broker
     */
    public void removeReplica(String topicName, int partitionId) {
        String key = makeKey(topicName, partitionId);

        PartitionReplica replica = replicas.remove(key);
        if (replica == null) {
            return;
        }

        // Stop fetcher if this was a follower
        if (replica.isFollower()) {
            stopReplicaFetcher(key);
        }

        // Clear ISR if this was a leader
        if (replica.isLeader()) {
            isrManager.clearISR(topicName, partitionId);
        }

        log.info("Removed replica {}-{} from broker {}", topicName, partitionId, brokerId);
    }

    /**
     * Make this replica the leader (after election)
     */
    public void becomeLeader(String topicName, int partitionId) {
        String key = makeKey(topicName, partitionId);
        PartitionReplica replica = replicas.get(key);

        if (replica == null) {
            log.error("Cannot become leader: replica {}-{} not found on broker {}",
                    topicName, partitionId, brokerId);
            return;
        }

        // Stop fetcher if was follower
        stopReplicaFetcher(key);

        // Promote to leader
        replica.becomeLeader();
        isrManager.initializeISR(topicName, partitionId, brokerId);

        log.info("Broker {} became leader for {}-{}", brokerId, topicName, partitionId);
    }

    /**
     * Make this replica a follower (after leader election elsewhere)
     */
    public void becomeFollower(String topicName, int partitionId, int newLeaderId) {
        String key = makeKey(topicName, partitionId);
        PartitionReplica replica = replicas.get(key);

        if (replica == null) {
            log.error("Cannot become follower: replica {}-{} not found on broker {}",
                    topicName, partitionId, brokerId);
            return;
        }

        // Demote to follower
        replica.becomeFollower();

        // Start fetching from new leader
        startReplicaFetcher(replica, newLeaderId);

        log.info("Broker {} became follower for {}-{}, leader is broker {}",
                brokerId, topicName, partitionId, newLeaderId);
    }

    /**
     * Get a replica
     */
    public PartitionReplica getReplica(String topicName, int partitionId) {
        String key = makeKey(topicName, partitionId);
        return replicas.get(key);
    }

    /**
     * Check if this broker is the leader for a partition
     */
    public boolean isLeader(String topicName, int partitionId) {
        PartitionReplica replica = getReplica(topicName, partitionId);
        return replica != null && replica.isLeader();
    }

    /**
     * Fetch messages from a partition (used by replica fetchers)
     */
    public List<Message> fetchMessages(String topicName, int partitionId,
                                       long offset, int maxRecords, int maxBytes) {
        PartitionReplica replica = getReplica(topicName, partitionId);
        if (replica == null) {
            return Collections.emptyList();
        }

        return replica.getPartition().read(offset, maxRecords, maxBytes);
    }

    /**
     * Update ISR for a partition
     */
    public void updateISR(String topicName, int partitionId) {
        PartitionReplica replica = getReplica(topicName, partitionId);
        if (replica == null || !replica.isLeader()) {
            return;
        }

        // In a full implementation, would collect follower offsets from all replicas
        // For simplicity, we just maintain ISR with leader only
        Map<Integer, Long> followerOffsets = new HashMap<>();
        long leaderOffset = replica.getLogEndOffset();

        isrManager.updateISR(topicName, partitionId, brokerId, leaderOffset, followerOffsets);

        // Update high watermark
        Set<Integer> isr = isrManager.getISR(topicName, partitionId);
        long hwm = isrManager.calculateHighWatermark(leaderOffset, followerOffsets, isr);
        replica.updateHighWatermark(hwm);
    }

    /**
     * Get ISR for a partition
     */
    public Set<Integer> getISR(String topicName, int partitionId) {
        return isrManager.getISR(topicName, partitionId);
    }

    /**
     * Start a replica fetcher for a follower partition
     */
    private void startReplicaFetcher(PartitionReplica replica, int leaderBrokerId) {
        String key = makeKey(replica.getTopicName(), replica.getPartitionId());

        // Stop existing fetcher if any
        stopReplicaFetcher(key);

        // Create and start new fetcher
        ReplicaFetcher fetcher = new ReplicaFetcher(
                replica.getTopicName(),
                replica.getPartitionId(),
                replica.getPartition(),
                this,
                leaderBrokerId
        );

        activeFetchers.put(key, fetcher);
        fetcherExecutor.submit(fetcher);

        log.info("Started replica fetcher for {}-{} from broker {}",
                replica.getTopicName(), replica.getPartitionId(), leaderBrokerId);
    }

    /**
     * Stop a replica fetcher
     */
    private void stopReplicaFetcher(String key) {
        ReplicaFetcher fetcher = activeFetchers.remove(key);
        if (fetcher != null) {
            fetcher.stop();
            log.info("Stopped replica fetcher for {}", key);
        }
    }

    /**
     * Shutdown replica manager
     */
    public void shutdown() {
        log.info("Shutting down ReplicaManager");

        // Stop all fetchers
        for (ReplicaFetcher fetcher : activeFetchers.values()) {
            fetcher.stop();
        }

        // Shutdown executor
        fetcherExecutor.shutdown();
        try {
            if (!fetcherExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                fetcherExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            fetcherExecutor.shutdownNow();
        }

        log.info("ReplicaManager shutdown complete");
    }

    /**
     * Create key for replica map
     */
    private String makeKey(String topic, int partition) {
        return topic + "-" + partition;
    }

    /**
     * Get all replicas on this broker
     */
    public Collection<PartitionReplica> getAllReplicas() {
        return replicas.values();
    }

    /**
     * Get number of replicas
     */
    public int getReplicaCount() {
        return replicas.size();
    }
}
