package com.streamflow.admin.controller;

import com.streamflow.admin.dto.BrokerInfo;
import com.streamflow.admin.service.BrokerService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * REST API for broker and cluster information
 */
@RestController
@RequestMapping("/broker")
@Tag(name = "Broker", description = "Broker health and cluster status APIs")
@Slf4j
@RequiredArgsConstructor
public class BrokerController {

    private final BrokerService brokerService;

    /**
     * Get broker information
     */
    @GetMapping("/info")
    @Operation(summary = "Get broker info", description = "Get information about the broker")
    public ResponseEntity<BrokerInfo> getBrokerInfo() {
        try {
            BrokerInfo info = brokerService.getBrokerInfo();
            return ResponseEntity.ok(info);
        } catch (Exception e) {
            log.error("Failed to get broker info", e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Check broker health
     */
    @GetMapping("/health")
    @Operation(summary = "Health check", description = "Check if the broker is healthy and responsive")
    public ResponseEntity<Map<String, Object>> getHealth() {
        boolean healthy = brokerService.isHealthy();

        Map<String, Object> response = Map.of(
                "status", healthy ? "UP" : "DOWN",
                "broker", healthy ? "CONNECTED" : "DISCONNECTED"
        );

        return healthy
                ? ResponseEntity.ok(response)
                : ResponseEntity.status(503).body(response);
    }
}
