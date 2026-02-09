package com.streamflow.admin.controller;

import com.streamflow.admin.dto.ConsumerGroupInfo;
import com.streamflow.admin.service.ConsumerGroupService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * REST API for consumer group management
 */
@RestController
@RequestMapping("/consumer-groups")
@Tag(name = "Consumer Groups", description = "Consumer group monitoring APIs")
@Slf4j
@RequiredArgsConstructor
public class ConsumerGroupController {

    private final ConsumerGroupService consumerGroupService;

    /**
     * List all consumer groups
     */
    @GetMapping
    @Operation(summary = "List consumer groups", description = "Get a list of all consumer groups")
    public ResponseEntity<List<String>> listGroups() {
        try {
            List<String> groups = consumerGroupService.listGroups();
            return ResponseEntity.ok(groups);
        } catch (Exception e) {
            log.error("Failed to list consumer groups", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Get consumer group information
     */
    @GetMapping("/{groupId}")
    @Operation(summary = "Get group info", description = "Get detailed information about a specific consumer group")
    public ResponseEntity<ConsumerGroupInfo> getGroup(@PathVariable String groupId) {
        try {
            ConsumerGroupInfo info = consumerGroupService.getGroupInfo(groupId);
            return ResponseEntity.ok(info);
        } catch (Exception e) {
            log.error("Failed to get consumer group info for {}", groupId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}
