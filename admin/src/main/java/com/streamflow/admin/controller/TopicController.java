package com.streamflow.admin.controller;

import com.streamflow.admin.dto.CreateTopicRequest;
import com.streamflow.admin.dto.TopicInfo;
import com.streamflow.admin.service.TopicService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * REST API for topic management
 */
@RestController
@RequestMapping("/topics")
@Tag(name = "Topics", description = "Topic management APIs")
@Slf4j
@RequiredArgsConstructor
public class TopicController {

    private final TopicService topicService;

    /**
     * List all topics
     */
    @GetMapping
    @Operation(summary = "List all topics", description = "Get a list of all topics in the cluster")
    public ResponseEntity<List<String>> listTopics() {
        try {
            List<String> topics = topicService.listTopics();
            return ResponseEntity.ok(topics);
        } catch (Exception e) {
            log.error("Failed to list topics", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Get topic information
     */
    @GetMapping("/{topicName}")
    @Operation(summary = "Get topic info", description = "Get detailed information about a specific topic")
    public ResponseEntity<TopicInfo> getTopic(@PathVariable String topicName) {
        try {
            TopicInfo info = topicService.getTopicInfo(topicName);
            return ResponseEntity.ok(info);
        } catch (Exception e) {
            log.error("Failed to get topic info for {}", topicName, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Create a new topic
     */
    @PostMapping
    @Operation(summary = "Create topic", description = "Create a new topic with specified partitions and replication factor")
    public ResponseEntity<TopicInfo> createTopic(@Valid @RequestBody CreateTopicRequest request) {
        try {
            TopicInfo info = topicService.createTopic(request);
            return ResponseEntity.status(HttpStatus.CREATED).body(info);
        } catch (Exception e) {
            log.error("Failed to create topic {}", request.getName(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Delete a topic
     */
    @DeleteMapping("/{topicName}")
    @Operation(summary = "Delete topic", description = "Delete a topic and all its data")
    public ResponseEntity<Void> deleteTopic(@PathVariable String topicName) {
        try {
            topicService.deleteTopic(topicName);
            return ResponseEntity.noContent().build();
        } catch (UnsupportedOperationException e) {
            log.warn("Delete topic not implemented");
            return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).build();
        } catch (Exception e) {
            log.error("Failed to delete topic {}", topicName, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}
