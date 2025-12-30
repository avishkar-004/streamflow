package com.streamflow.broker.coordinator;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages consumer group offset commits
 *
 * Stores and retrieves committed offsets for consumer groups
 */
@Slf4j
public class OffsetManager {

    private final Map<GroupTopicPartition, Long> committedOffsets;
    private final File offsetFile;

    public OffsetManager(File dataDir) {
        this.committedOffsets = new ConcurrentHashMap<>();
        this.offsetFile = new File(dataDir, "offsets.dat");

        // Load existing offsets
        loadOffsets();

        log.info("OffsetManager initialized with {} committed offsets", committedOffsets.size());
    }

    /**
     * Commit an offset for a consumer group
     */
    public void commitOffset(String group, String topic, int partition, long offset) {
        GroupTopicPartition key = new GroupTopicPartition(group, topic, partition);
        committedOffsets.put(key, offset);

        log.debug("Committed offset: group={}, topic={}, partition={}, offset={}",
                group, topic, partition, offset);

        // Persist to disk asynchronously
        persistOffsets();
    }

    /**
     * Get the committed offset for a consumer group
     * Returns -1 if no offset has been committed
     */
    public long getCommittedOffset(String group, String topic, int partition) {
        GroupTopicPartition key = new GroupTopicPartition(group, topic, partition);
        return committedOffsets.getOrDefault(key, -1L);
    }

    /**
     * Delete offsets for a consumer group
     */
    public void deleteGroup(String group) {
        committedOffsets.keySet().removeIf(key -> key.group.equals(group));
        persistOffsets();
        log.info("Deleted offsets for group: {}", group);
    }

    /**
     * Get all committed offsets for a consumer group
     */
    public Map<GroupTopicPartition, Long> getGroupOffsets(String group) {
        Map<GroupTopicPartition, Long> result = new ConcurrentHashMap<>();
        committedOffsets.forEach((key, value) -> {
            if (key.group.equals(group)) {
                result.put(key, value);
            }
        });
        return result;
    }

    /**
     * Persist offsets to disk
     */
    private void persistOffsets() {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(offsetFile))) {
            oos.writeObject(committedOffsets);
            log.debug("Persisted {} offsets to disk", committedOffsets.size());
        } catch (IOException e) {
            log.error("Failed to persist offsets", e);
        }
    }

    /**
     * Load offsets from disk
     */
    @SuppressWarnings("unchecked")
    private void loadOffsets() {
        if (!offsetFile.exists()) {
            return;
        }

        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(offsetFile))) {
            Map<GroupTopicPartition, Long> loaded =
                    (Map<GroupTopicPartition, Long>) ois.readObject();
            committedOffsets.putAll(loaded);
            log.info("Loaded {} offsets from disk", loaded.size());
        } catch (IOException | ClassNotFoundException e) {
            log.error("Failed to load offsets", e);
        }
    }

    /**
     * Key for storing committed offsets
     */
    @Data
    @AllArgsConstructor
    public static class GroupTopicPartition implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String group;
        private final String topic;
        private final int partition;
    }
}
