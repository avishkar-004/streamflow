package com.streamflow.client.producer;

/**
 * Interface for partition selection strategy
 */
public interface Partitioner {

    /**
     * Select a partition for the given topic and key
     *
     * @param topic         Topic name
     * @param key           Message key (can be null)
     * @param numPartitions Number of partitions in the topic
     * @return Partition number (0-based)
     */
    int partition(String topic, String key, int numPartitions);
}
