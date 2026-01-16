package com.streamflow.broker.replication;

/**
 * States for a partition replica
 *
 * LEADER: This broker is the leader for this partition
 *         - Handles all writes
 *         - Followers replicate from this broker
 *
 * FOLLOWER: This broker is a follower for this partition
 *           - Fetches data from leader
 *           - Read-only (no writes accepted)
 *
 * OFFLINE: Replica is not available
 *          - Broker is down or partition is being deleted
 */
public enum ReplicaState {
    LEADER,
    FOLLOWER,
    OFFLINE
}
