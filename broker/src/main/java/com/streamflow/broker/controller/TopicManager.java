package com.streamflow.broker.controller;

import com.streamflow.broker.storage.Topic;
import com.streamflow.common.exception.StreamFlowException;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages all topics in the broker
 *
 * Responsible for creating, deleting, and retrieving topics
 */
@Slf4j
public class TopicManager {

    private final File dataDir;
    private final Map<String, Topic> topics;
    private final int defaultPartitions;

    public TopicManager(File dataDir, int defaultPartitions) {
        this.dataDir = dataDir;
        this.topics = new ConcurrentHashMap<>();
        this.defaultPartitions = defaultPartitions;

        // Create data directory if it doesn't exist
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }

        // Load existing topics
        loadExistingTopics();

        log.info("TopicManager initialized with {} topics", topics.size());
    }

    /**
     * Create a new topic
     */
    public synchronized Topic createTopic(String name, int numPartitions) {
        if (topics.containsKey(name)) {
            throw new StreamFlowException("Topic already exists: " + name);
        }

        if (numPartitions <= 0) {
            numPartitions = defaultPartitions;
        }

        Topic topic = new Topic(name, numPartitions, dataDir);
        topics.put(name, topic);

        log.info("Created topic: name={}, partitions={}", name, numPartitions);
        return topic;
    }

    /**
     * Create a topic with default number of partitions
     */
    public Topic createTopic(String name) {
        return createTopic(name, defaultPartitions);
    }

    /**
     * Get a topic by name
     */
    public Topic getTopic(String name) {
        Topic topic = topics.get(name);
        if (topic == null) {
            throw new StreamFlowException("Topic not found: " + name);
        }
        return topic;
    }

    /**
     * Check if a topic exists
     */
    public boolean topicExists(String name) {
        return topics.containsKey(name);
    }

    /**
     * Get or create a topic (idempotent)
     */
    public Topic getOrCreateTopic(String name, int numPartitions) {
        if (topicExists(name)) {
            return getTopic(name);
        }
        return createTopic(name, numPartitions);
    }

    /**
     * Delete a topic
     */
    public synchronized void deleteTopic(String name) {
        Topic topic = topics.remove(name);
        if (topic == null) {
            throw new StreamFlowException("Topic not found: " + name);
        }

        topic.close();

        // Delete topic directory
        File topicDir = new File(dataDir, name);
        deleteDirectory(topicDir);

        log.info("Deleted topic: {}", name);
    }

    /**
     * List all topic names
     */
    public List<String> listTopics() {
        return new ArrayList<>(topics.keySet());
    }

    /**
     * Get all topics
     */
    public List<Topic> getAllTopics() {
        return new ArrayList<>(topics.values());
    }

    /**
     * Get the number of topics
     */
    public int getTopicCount() {
        return topics.size();
    }

    /**
     * Flush all topics
     */
    public void flushAll() {
        for (Topic topic : topics.values()) {
            topic.flush();
        }
        log.debug("Flushed all topics");
    }

    /**
     * Close all topics
     */
    public void closeAll() {
        for (Topic topic : topics.values()) {
            topic.close();
        }
        log.info("Closed all topics");
    }

    /**
     * Load existing topics from disk
     */
    private void loadExistingTopics() {
        File[] topicDirs = dataDir.listFiles(File::isDirectory);
        if (topicDirs == null) {
            return;
        }

        for (File topicDir : topicDirs) {
            String topicName = topicDir.getName();

            try {
                // Count partitions
                File[] partitionDirs = topicDir.listFiles(
                        (dir, name) -> name.startsWith("partition-"));

                if (partitionDirs != null && partitionDirs.length > 0) {
                    int numPartitions = partitionDirs.length;
                    Topic topic = new Topic(topicName, numPartitions, dataDir);
                    topics.put(topicName, topic);
                    log.info("Loaded existing topic: name={}, partitions={}", topicName, numPartitions);
                }
            } catch (Exception e) {
                log.error("Failed to load topic: {}", topicName, e);
            }
        }
    }

    /**
     * Recursively delete a directory
     */
    private void deleteDirectory(File directory) {
        if (!directory.exists()) {
            return;
        }

        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }

        directory.delete();
    }
}
