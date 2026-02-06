package com.streamflow.admin.service;

import com.streamflow.admin.dto.BrokerInfo;
import com.streamflow.client.common.NetworkClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Service for broker health and cluster information
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class BrokerService {

    private final NetworkClient networkClient;
    private final long startTime = System.currentTimeMillis();

    /**
     * Get broker information
     */
    public BrokerInfo getBrokerInfo() {
        log.debug("Getting broker info");

        return BrokerInfo.builder()
                .brokerId(0)
                .host("localhost")
                .port(9092)
                .version("1.0.0")
                .uptimeMs(System.currentTimeMillis() - startTime)
                .topicCount(0)  // Would query from broker
                .partitionCount(0)
                .leaderCount(0)
                .replicaCount(0)
                .isController(true)
                .build();
    }

    /**
     * Check broker health
     */
    public boolean isHealthy() {
        try {
            // Try to connect to broker
            if (!networkClient.isConnected()) {
                networkClient.connect();
            }
            return networkClient.isConnected();
        } catch (Exception e) {
            log.error("Health check failed", e);
            return false;
        }
    }
}
