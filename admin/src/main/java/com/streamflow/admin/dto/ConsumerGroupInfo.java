package com.streamflow.admin.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * Consumer group information DTO
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConsumerGroupInfo {
    private String groupId;
    private String state;
    private int memberCount;
    private List<String> members;
    private Map<String, List<Integer>> partitionAssignment;
    private int generationId;
}
