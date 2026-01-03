package com.streamflow.broker.coordinator;

import java.util.List;
import java.util.Map;

/**
 * Strategy interface for assigning partitions to consumers in a group
 *
 * Different strategies distribute partitions differently:
 * - RoundRobin: Distribute evenly in round-robin fashion
 * - Range: Assign contiguous ranges of partitions
 *
 * Example: 6 partitions, 3 consumers
 * - RoundRobin: C1=[0,3], C2=[1,4], C3=[2,5]
 * - Range:      C1=[0,1], C2=[2,3], C3=[4,5]
 */
public interface PartitionAssignor {

    /**
     * Assign partitions to consumers
     *
     * @param partitions List of partition IDs to assign (e.g., [0,1,2,3,4,5])
     * @param consumers List of consumer IDs in the group (e.g., ["c1","c2","c3"])
     * @return Map of consumer ID to assigned partition IDs
     */
    Map<String, List<Integer>> assign(List<Integer> partitions, List<String> consumers);

    /**
     * Get the name of this assignment strategy
     */
    String name();
}
