package com.streamflow.broker.coordinator;

import com.streamflow.broker.controller.TopicManager;
import com.streamflow.broker.storage.Topic;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;

/**
 * Coordinates all consumer groups in the broker
 *
 * Responsibilities:
 * - Manage multiple consumer groups
 * - Handle consumer join/leave/heartbeat
 * - Trigger rebalancing when needed
 * - Monitor for dead consumers
 * - Clean up empty groups
 *
 * Background tasks:
 * - Heartbeat monitoring (every 10 seconds)
 * - Dead group cleanup (every 60 seconds)
 */
@Slf4j
public class ConsumerGroupCoordinator {

    // All consumer groups: groupId -> ConsumerGroup
    private final Map<String, ConsumerGroup> groups;

    // Topic manager for getting partition counts
    private final TopicManager topicManager;

    // Default partition assignor
    private final PartitionAssignor defaultAssignor;

    // Background task scheduler
    private final ScheduledExecutorService scheduler;

    // Heartbeat check interval (10 seconds)
    private static final long HEARTBEAT_CHECK_INTERVAL_MS = 10_000;

    // Group cleanup interval (60 seconds)
    private static final long GROUP_CLEANUP_INTERVAL_MS = 60_000;

    public ConsumerGroupCoordinator(TopicManager topicManager) {
        this.groups = new ConcurrentHashMap<>();
        this.topicManager = topicManager;
        this.defaultAssignor = new RoundRobinAssignor(); // Can be configurable
        this.scheduler = Executors.newScheduledThreadPool(1);

        // Start background tasks
        startHeartbeatMonitoring();
        startGroupCleanup();

        log.info("ConsumerGroupCoordinator initialized with {} assignor", defaultAssignor.name());
    }

    /**
     * Consumer joins a group
     *
     * @param groupId Group to join
     * @param consumerId Consumer identifier
     * @param topics Topics to subscribe to
     * @return Partition assignment for this consumer
     */
    public synchronized List<Integer> joinGroup(String groupId, String consumerId, Set<String> topics) {
        log.info("Consumer {} joining group {} for topics {}", consumerId, groupId, topics);

        // Get or create group
        ConsumerGroup group = groups.computeIfAbsent(groupId,
                id -> new ConsumerGroup(id, defaultAssignor));

        // Add member to group
        group.addMember(consumerId, topics);

        // Trigger rebalance
        Map<String, Integer> partitionCounts = getPartitionCounts(topics);
        group.rebalance(partitionCounts);

        // Return assignment for this consumer
        return group.getAssignment(consumerId);
    }

    /**
     * Consumer leaves a group
     */
    public synchronized void leaveGroup(String groupId, String consumerId) {
        log.info("Consumer {} leaving group {}", consumerId, groupId);

        ConsumerGroup group = groups.get(groupId);
        if (group == null) {
            log.warn("Group {} not found", groupId);
            return;
        }

        // Remove member
        group.removeMember(consumerId);

        // Rebalance if group still has members
        if (!group.isEmpty()) {
            Map<String, Integer> partitionCounts = getPartitionCounts(group.getSubscribedTopics());
            group.rebalance(partitionCounts);
        }

        // Clean up empty group
        if (group.isEmpty()) {
            groups.remove(groupId);
            log.info("Removed empty group {}", groupId);
        }
    }

    public List<Integer> getAssignment(String groupId, String consumerId) {
        ConsumerGroup group = groups.get(groupId);
        if (group == null) return Collections.emptyList();
        return group.getAssignment(consumerId);
    }

    public List<ConsumerGroup> getAllGroups() {
        return new ArrayList<>(groups.values());
    }

    public ConsumerGroup getGroup(String groupId) {
        return groups.get(groupId);
    }

    private Map<String, Integer> getPartitionCounts(Set<String> topics) {
        Map<String, Integer> counts = new HashMap<>();
        for (String topicName : topics) {
            try {
                Topic topic = topicManager.getTopic(topicName);
                counts.put(topicName, topic.getNumPartitions());
            } catch (Exception e) {
                log.warn("Topic {} not found, skipping", topicName);
            }
        }
        return counts;
    }

    public void shutdown() {
        log.info("Shutting down ConsumerGroupCoordinator");
    }
}
