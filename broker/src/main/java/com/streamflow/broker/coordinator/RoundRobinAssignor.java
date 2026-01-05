package com.streamflow.broker.coordinator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Round-robin partition assignment strategy
 *
 * Distributes partitions evenly across consumers in a round-robin manner.
 * This provides the most balanced distribution.
 *
 * Example with 7 partitions and 3 consumers:
 * - Consumer 1: partitions [0, 3, 6]
 * - Consumer 2: partitions [1, 4]
 * - Consumer 3: partitions [2, 5]
 *
 * Advantages:
 * - Even load distribution
 * - Simple to understand
 *
 * Disadvantages:
 * - Partitions may not be contiguous for a consumer
 */
public class RoundRobinAssignor implements PartitionAssignor {

    @Override
    public Map<String, List<Integer>> assign(List<Integer> partitions, List<String> consumers) {
        Map<String, List<Integer>> assignment = new HashMap<>();

        // Initialize empty lists for each consumer
        for (String consumer : consumers) {
            assignment.put(consumer, new ArrayList<>());
        }

        // Assign partitions in round-robin fashion
        // Partition i goes to consumer (i % numConsumers)
        for (int i = 0; i < partitions.size(); i++) {
            int partitionId = partitions.get(i);
            String consumer = consumers.get(i % consumers.size());
            assignment.get(consumer).add(partitionId);
        }

        return assignment;
    }

    @Override
    public String name() {
        return "RoundRobin";
    }
}
