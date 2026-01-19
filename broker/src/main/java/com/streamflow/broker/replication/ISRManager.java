package com.streamflow.broker.replication;

import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages In-Sync Replicas (ISR) for partitions
 *
 * ISR = replicas that are caught up with the leader
 * - Leader is always in ISR
 * - Followers are in ISR if lag < threshold
 * - Messages are "committed" only when all ISR members have them
 *
 * Example: partition with 3 replicas
 * - Leader: offset 10000 (always in ISR)
 * - Follower1: offset 9999 (lag=1, in ISR)
 * - Follower2: offset 9500 (lag=500, NOT in ISR)
 * - High watermark = 9999 (min offset in ISR)
 */
@Slf4j
public class ISRManager {

    // Topic-Partition -> Set of broker IDs in ISR
    private final ConcurrentHashMap<String, Set<Integer>> isrMap;

    // Maximum lag to stay in ISR (in number of messages)
    private static final long MAX_LAG_MESSAGES = 100;

    // Maximum time lag to stay in ISR (10 seconds)
    private static final long MAX_LAG_MS = 10_000;

    public ISRManager() {
        this.isrMap = new ConcurrentHashMap<>();
    }

    /**
     * Initialize ISR for a partition with leader
     */
    public void initializeISR(String topic, int partition, int leaderBrokerId) {
        String key = makeKey(topic, partition);
        Set<Integer> isr = ConcurrentHashMap.newKeySet();
        isr.add(leaderBrokerId);
        isrMap.put(key, isr);

        log.info("Initialized ISR for {}-{}: {}", topic, partition, isr);
    }

    /**
     * Add a replica to ISR (when it catches up)
     */
    public void addToISR(String topic, int partition, int brokerId) {
        String key = makeKey(topic, partition);
        Set<Integer> isr = isrMap.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet());

        if (isr.add(brokerId)) {
            log.info("Added broker {} to ISR for {}-{}", brokerId, topic, partition);
        }
    }

    /**
     * Remove a replica from ISR (when it falls behind)
     */
    public void removeFromISR(String topic, int partition, int brokerId) {
        String key = makeKey(topic, partition);
        Set<Integer> isr = isrMap.get(key);

        if (isr != null && isr.remove(brokerId)) {
            log.warn("Removed broker {} from ISR for {}-{}", brokerId, topic, partition);
        }
    }

    /**
     * Get current ISR for a partition
     */
    public Set<Integer> getISR(String topic, int partition) {
        String key = makeKey(topic, partition);
        return new HashSet<>(isrMap.getOrDefault(key, Set.of()));
    }

    /**
     * Check if a replica is in ISR
     */
    public boolean isInISR(String topic, int partition, int brokerId) {
        String key = makeKey(topic, partition);
        Set<Integer> isr = isrMap.get(key);
        return isr != null && isr.contains(brokerId);
    }

    /**
     * Update ISR based on replica lag
     * Remove replicas that have fallen too far behind
     */
    public void updateISR(String topic, int partition, int leaderBrokerId,
                         long leaderOffset, java.util.Map<Integer, Long> followerOffsets) {
        String key = makeKey(topic, partition);
        Set<Integer> currentISR = isrMap.get(key);


    public Set<Integer> getISR(String topic, int partition) {
        String key = topic + "-" + partition;
        return isrMap.getOrDefault(key, new ConcurrentSkipListSet<>());
    }

    public void shutdown() {
        scheduler.shutdown();
    }
}
