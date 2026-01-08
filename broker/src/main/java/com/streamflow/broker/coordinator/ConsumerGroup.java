package com.streamflow.broker.coordinator;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Represents a consumer group - a set of consumers working together
 *
 * Key responsibilities:
 * - Track group members (consumers)
 * - Monitor heartbeats to detect failures
 * - Manage partition assignments
 * - Trigger rebalancing when membership changes
 *
 * Lifecycle:
 * 1. Empty: No consumers
 * 2. Rebalancing: Members changed, reassigning partitions
 * 3. Stable: All members active with assigned partitions
 * 4. Dead: No activity for too long, can be deleted
 */
@Slf4j
@Data
public class ConsumerGroup {

    // Group identity
    private final String groupId;

    // Members: consumerId -> last heartbeat time
    private final Map<String, Instant> members;

    // Current partition assignments: consumerId -> list of partition IDs
    private final Map<String, List<Integer>> currentAssignment;

    // Topics this group is subscribed to
    private final Set<String> subscribedTopics;

    // Group state
    private GroupState state;

    // Generation ID - incremented on each rebalance
    private int generationId;

    // Assignment strategy
    private final PartitionAssignor assignor;

    // Heartbeat timeout (30 seconds)
    private static final long HEARTBEAT_TIMEOUT_MS = 30_000;

    /**
     * Consumer group states
     */
    public enum GroupState {
        EMPTY,          // No members
        PREPARING_REBALANCE,  // Rebalance triggered, waiting for members
        STABLE,         // All members active with assignments
        DEAD            // Inactive, can be deleted
    }

    public ConsumerGroup(String groupId, PartitionAssignor assignor) {
        this.groupId = groupId;
        this.members = new ConcurrentHashMap<>();
        this.currentAssignment = new ConcurrentHashMap<>();
        this.subscribedTopics = ConcurrentHashMap.newKeySet();
        this.state = GroupState.EMPTY;
        this.generationId = 0;
        this.assignor = assignor;
    }

    /**
     * Add a member to the group
     * Triggers rebalancing
     */
    public synchronized void addMember(String consumerId, Set<String> topics) {
        log.info("Consumer {} joining group {}", consumerId, groupId);

        members.put(consumerId, Instant.now());
        subscribedTopics.addAll(topics);

        // Trigger rebalance when new member joins
        if (state == GroupState.STABLE) {
            state = GroupState.PREPARING_REBALANCE;
            log.info("Triggered rebalance for group {} due to new member", groupId);
        }
    }

    /**
     * Remove a member from the group
     * Triggers rebalancing
     */
    public synchronized void removeMember(String consumerId) {
        log.info("Consumer {} leaving group {}", consumerId, groupId);

        members.remove(consumerId);
        currentAssignment.remove(consumerId);

        // Update state
        if (members.isEmpty()) {
            state = GroupState.EMPTY;
        } else if (state == GroupState.STABLE) {
            state = GroupState.PREPARING_REBALANCE;
            log.info("Triggered rebalance for group {} due to member leaving", groupId);
        }
    }

    /**
     * Update heartbeat for a member
     * Returns false if member not in group
     */
    public boolean heartbeat(String consumerId) {
        if (!members.containsKey(consumerId)) {
            return false;
        }

        members.put(consumerId, Instant.now());
        log.debug("Heartbeat from {} in group {}", consumerId, groupId);
        return true;
    }

    /**
     * Check for dead members (missed heartbeats)
     * Returns list of dead member IDs
     */
    public List<String> findDeadMembers() {
        Instant now = Instant.now();
        List<String> deadMembers = new ArrayList<>();


    public boolean isEmpty() {
        return members.isEmpty();
    }

    public String getGroupId() {
        return groupId;
    }

    public Set<String> getSubscribedTopics() {
        Set<String> topics = new HashSet<>();
        members.values().forEach(topics::addAll);
        return topics;
    }

    public int getMemberCount() {
        return members.size();
    }
}
