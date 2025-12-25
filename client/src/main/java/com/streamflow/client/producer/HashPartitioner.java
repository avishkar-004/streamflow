package com.streamflow.client.producer;

/**
 * Hash-based partitioner for consistent partition assignment
 *
 * Messages with the same key will always go to the same partition
 */
public class HashPartitioner implements Partitioner {

    @Override
    public int partition(String topic, String key, int numPartitions) {
        if (key == null || key.isEmpty()) {
            // Round-robin for null keys
            return (int) (System.nanoTime() % numPartitions);
        }

        // Hash-based partitioning for consistent routing
        int hash = Math.abs(key.hashCode());
        return hash % numPartitions;
    }
}
