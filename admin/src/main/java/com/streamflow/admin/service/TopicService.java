package com.streamflow.admin.service;

import com.streamflow.admin.dto.CreateTopicRequest;
import com.streamflow.admin.dto.PartitionInfo;
import com.streamflow.admin.dto.TopicInfo;
import com.streamflow.client.common.NetworkClient;
import com.streamflow.common.protocol.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * Service for topic management operations
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class TopicService {

    private final NetworkClient networkClient;

    /**
     * List all topics
     */
    public List<String> listTopics() throws Exception {
        log.debug("Listing all topics");

        // In current implementation, we don't have a LIST_TOPICS request
        // Would need to add that to the protocol
        // For now, return empty list
        log.warn("List all topics not yet fully implemented in protocol");
        return new ArrayList<>();
    }

    /**
     * Get detailed information about a topic
     */
    public TopicInfo getTopicInfo(String topicName) throws Exception {
        log.debug("Getting info for topic: {}", topicName);

        MetadataRequest request = new MetadataRequest(topicName);
        MetadataResponse response = (MetadataResponse) networkClient.send(request);

        if (response.getErrorCode() != 0) {
            throw new RuntimeException("Failed to get topic info: error code " + response.getErrorCode());
        }

        int partitionCount = response.getPartitions();

        // Build partition info list
        List<PartitionInfo> partitions = new ArrayList<>();
        for (int i = 0; i < partitionCount; i++) {
            partitions.add(PartitionInfo.builder()
                    .partitionId(i)
                    .leader(0)  // Simplified: single broker
                    .replicas(List.of(0))
                    .isr(List.of(0))
                    .logSize(0)
                    .startOffset(0)
                    .endOffset(0)
                    .build());
        }

        return TopicInfo.builder()
                .name(topicName)
                .partitionCount(partitionCount)
                .replicationFactor(1)  // Simplified
                .partitions(partitions)
                .totalMessages(0)
                .totalBytes(0)
                .build();
    }

    /**
     * Create a new topic
     */
    public TopicInfo createTopic(CreateTopicRequest request) throws Exception {
        log.info("Creating topic: {} with {} partitions", request.getName(), request.getPartitions());

        // For now, we don't have a CREATE_TOPIC protocol request
        // In a real implementation, we would send a CreateTopicRequest to the broker
        // For this simplified version, topics are created automatically on first produce

        log.warn("Topic creation via admin API not yet fully implemented. " +
                "Topics are created automatically on first produce.");

        return TopicInfo.builder()
                .name(request.getName())
                .partitionCount(request.getPartitions())
                .replicationFactor(request.getReplicationFactor())
                .partitions(new ArrayList<>())
                .build();
    }

    /**
     * Delete a topic
     */
    public void deleteTopic(String topicName) throws Exception {
        log.info("Deleting topic: {}", topicName);

        // For now, we don't have a DELETE_TOPIC protocol request
        // In a real implementation, we would send a DeleteTopicRequest to the broker

        log.warn("Topic deletion via admin API not yet fully implemented");
        throw new UnsupportedOperationException("Topic deletion not yet implemented");
    }
}
