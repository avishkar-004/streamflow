package com.streamflow.broker.coordinator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Range partition assignment strategy
 *
 * Assigns contiguous ranges of partitions to each consumer.
 * Useful when you want each consumer to process sequential data.
 *
 * Example with 7 partitions and 3 consumers:
 * - Consumer 1: partitions [0, 1, 2]   (3 partitions)
 * - Consumer 2: partitions [3, 4]      (2 partitions)
 * - Consumer 3: partitions [5, 6]      (2 partitions)
 *
 * Advantages:
 * - Contiguous partition ranges
 * - Predictable assignment
 *
 * Disadvantages:
 * - May be slightly imbalanced (first consumer gets more if not evenly divisible)
 */
public class RangeAssignor implements PartitionAssignor {

    @Override
    public Map<String, List<Integer>> assign(List<Integer> partitions, List<String> consumers) {
        Map<String, List<Integer>> assignment = new HashMap<>();

        int numPartitions = partitions.size();
        int numConsumers = consumers.size();

        // Calculate partitions per consumer
        int partitionsPerConsumer = numPartitions / numConsumers;
        int remainingPartitions = numPartitions % numConsumers;

        int currentPartitionIndex = 0;

        // Assign ranges to each consumer
        for (int i = 0; i < numConsumers; i++) {
            String consumer = consumers.get(i);
            List<Integer> assignedPartitions = new ArrayList<>();

            // First 'remainingPartitions' consumers get one extra partition
            int partitionsToAssign = partitionsPerConsumer + (i < remainingPartitions ? 1 : 0);

            // Assign contiguous range
            for (int j = 0; j < partitionsToAssign; j++) {
                assignedPartitions.add(partitions.get(currentPartitionIndex++));
            }

            assignment.put(consumer, assignedPartitions);
        }

        return assignment;
    }

    @Override
    public String name() {
        return "Range";
    }
}
