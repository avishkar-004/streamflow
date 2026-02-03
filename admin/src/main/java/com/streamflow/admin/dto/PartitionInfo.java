package com.streamflow.admin.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Partition information DTO
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PartitionInfo {
    private int partitionId;
    private int leader;
    private List<Integer> replicas;
    private List<Integer> isr;
    private long logSize;
    private long startOffset;
    private long endOffset;
}
