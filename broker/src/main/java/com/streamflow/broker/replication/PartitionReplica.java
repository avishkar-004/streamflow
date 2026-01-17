package com.streamflow.broker.replication;

import com.streamflow.broker.storage.Partition;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * Represents a replica of a partition
 *
 * Each partition can have multiple replicas across different brokers.
 * One replica is the LEADER (handles writes), others are FOLLOWERS (replicate data).
 *
 * Example: partition "orders-0" with 3 replicas
 * - Broker 1: LEADER (offset 10000)
 * - Broker 2: FOLLOWER (offset 9998) - slightly behind
 * - Broker 3: FOLLOWER (offset 10000) - caught up
 */
@Slf4j
@Data
public class PartitionReplica {

    private final String topicName;
    private final int partitionId;
    private final int brokerId;
    private final Partition partition;  // Actual storage

    private ReplicaState state;
    private long highWatermark;         // Offset visible to consumers
    private long logEndOffset;          // Last offset in log (may not be replicated yet)

    public PartitionReplica(String topicName, int partitionId, int brokerId, Partition partition) {
        this.topicName = topicName;
        this.partitionId = partitionId;
        this.brokerId = brokerId;
        this.partition = partition;
        this.state = ReplicaState.FOLLOWER; // Start as follower
        this.highWatermark = 0;
        this.logEndOffset = partition.getLogEndOffset();
    }

    /**
     * Promote this replica to leader
     */
    public void becomeLeader() {
        this.state = ReplicaState.LEADER;
        updateHighWatermark(logEndOffset);
        log.info("Replica became LEADER: topic={}, partition={}, broker={}",
                topicName, partitionId, brokerId);
    }

    /**
     * Demote this replica to follower
     */
    public void becomeFollower() {
        this.state = ReplicaState.FOLLOWER;
        log.info("Replica became FOLLOWER: topic={}, partition={}, broker={}",
                topicName, partitionId, brokerId);
    }

    /**
     * Update high watermark (offset visible to consumers)
     * HWM = minimum offset replicated to all ISR members
     */
    public void updateHighWatermark(long newHWM) {
        if (newHWM > this.highWatermark) {
            this.highWatermark = newHWM;
            log.debug("Updated HWM: topic={}, partition={}, hwm={}",
                    topicName, partitionId, highWatermark);
        }
    }

    /**
     * Update log end offset
     */
    public void updateLogEndOffset() {
        this.logEndOffset = partition.getLogEndOffset();
    }

    /**
     * Check if this replica is the leader
     */
    public boolean isLeader() {
        return state == ReplicaState.LEADER;
    }

    /**
     * Check if this replica is a follower
     */
    public boolean isFollower() {
        return state == ReplicaState.FOLLOWER;
    }

    /**
     * Get lag (how far behind leader)
     */
    public long getLag(long leaderOffset) {
        return leaderOffset - logEndOffset;
    }
}

    public long getLogEndOffset() { return logEndOffset; }
    public int getBrokerId() { return brokerId; }
    public ReplicaState getState() { return state; }
    public long getLastFetchTimeMs() { return lastFetchTimeMs; }
}
