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

    public void shutdown() {
        log.info("Shutting down ReplicaManager");
        isrManager.shutdown();
        for (ReplicaFetcher fetcher : fetchers.values()) {
            fetcher.shutdown();
        }
    }
}
