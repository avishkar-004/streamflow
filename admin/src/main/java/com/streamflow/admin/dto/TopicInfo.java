package com.streamflow.admin.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Topic information DTO
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TopicInfo {
    private String name;
    private int partitionCount;
    private int replicationFactor;
    private List<PartitionInfo> partitions;
    private long totalMessages;
    private long totalBytes;
}
