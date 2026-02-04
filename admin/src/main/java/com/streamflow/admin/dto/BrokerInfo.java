package com.streamflow.admin.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Broker information DTO
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BrokerInfo {
    private int brokerId;
    private String host;
    private int port;
    private String version;
    private long uptimeMs;
    private int topicCount;
    private int partitionCount;
    private int leaderCount;
    private int replicaCount;
    private boolean isController;
}
