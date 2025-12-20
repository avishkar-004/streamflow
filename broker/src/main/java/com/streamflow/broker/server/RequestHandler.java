package com.streamflow.broker.server;

import com.streamflow.broker.controller.TopicManager;
import com.streamflow.broker.coordinator.OffsetManager;
import com.streamflow.common.protocol.*;
import com.streamflow.common.model.Message;
import com.streamflow.broker.storage.Topic;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * Handles incoming requests and generates responses
 */
@Slf4j
public class RequestHandler extends SimpleChannelInboundHandler<Request> {

    private final TopicManager topicManager;
    private final OffsetManager offsetManager;
    private final com.streamflow.broker.coordinator.ConsumerGroupCoordinator groupCoordinator;

    public RequestHandler(TopicManager topicManager, OffsetManager offsetManager,
                         com.streamflow.broker.coordinator.ConsumerGroupCoordinator groupCoordinator) {
        this.topicManager = topicManager;
        this.offsetManager = offsetManager;
        this.groupCoordinator = groupCoordinator;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Request request) {
        log.debug("Handling request: {}", request.getClass().getSimpleName());

        Response response = switch (request.getApiKey()) {
            case Protocol.API_KEY_PRODUCE -> handleProduce((ProduceRequest) request);
            case Protocol.API_KEY_FETCH -> handleFetch((FetchRequest) request);
            case Protocol.API_KEY_METADATA -> handleMetadata((MetadataRequest) request);
            case Protocol.API_KEY_OFFSET_COMMIT -> handleOffsetCommit((OffsetCommitRequest) request);
            case Protocol.API_KEY_HEARTBEAT -> handleHeartbeat((HeartbeatRequest) request);
            case Protocol.API_KEY_JOIN_GROUP -> handleJoinGroup((JoinGroupRequest) request);
            case Protocol.API_KEY_LEAVE_GROUP -> handleLeaveGroup((LeaveGroupRequest) request);
            default -> createErrorResponse(request.getApiKey(), Response.ERROR_UNKNOWN);
        };

        ctx.writeAndFlush(response);
    }

    /**
     * Handle PRODUCE request
     */
    private Response handleProduce(ProduceRequest request) {
        try {
            // Get or create topic
            Topic topic = topicManager.getOrCreateTopic(request.getTopic(), 3);

            // Validate partition
            if (request.getPartition() < 0 || request.getPartition() >= topic.getNumPartitions()) {
                return ProduceResponse.builder()
                        .errorCode(Response.ERROR_INVALID_PARTITION)
                        .offset(-1)
                        .build();
            }

            // Append messages
            long offset = topic.appendBatch(request.getPartition(), request.getMessages());

            log.info("Produced {} messages to topic={}, partition={}, offset={}",
                    request.getMessages().size(), request.getTopic(), request.getPartition(), offset);

            return new ProduceResponse(offset);

        } catch (Exception e) {
            log.error("Failed to handle produce request", e);
            return ProduceResponse.builder()
                    .errorCode(Response.ERROR_UNKNOWN)
                    .offset(-1)
                    .build();
        }
    }

    /**
     * Handle FETCH request
     */
    private Response handleFetch(FetchRequest request) {
        try {
            // Check if topic exists
            if (!topicManager.topicExists(request.getTopic())) {
                return FetchResponse.builder()
                        .errorCode(Response.ERROR_TOPIC_NOT_FOUND)
                        .messages(List.of())
                        .build();
            }

            Topic topic = topicManager.getTopic(request.getTopic());

            // Validate partition
            if (request.getPartition() < 0 || request.getPartition() >= topic.getNumPartitions()) {
                return FetchResponse.builder()
                        .errorCode(Response.ERROR_INVALID_PARTITION)
                        .messages(List.of())
                        .build();
            }

            // Read messages
            List<Message> messages = topic.read(
                    request.getPartition(),
                    request.getOffset(),
                    request.getMaxRecords(),
                    request.getMaxBytes()
            );

            log.info("Fetched {} messages from topic={}, partition={}, offset={}",
                    messages.size(), request.getTopic(), request.getPartition(), request.getOffset());

            return new FetchResponse(messages);

        } catch (Exception e) {
            log.error("Failed to handle fetch request", e);
            return FetchResponse.builder()
                    .errorCode(Response.ERROR_UNKNOWN)
                    .messages(List.of())
                    .build();
        }
    }

    /**
     * Handle METADATA request
     */
    private Response handleMetadata(MetadataRequest request) {
        try {
            String topicName = request.getTopic();

            // If no specific topic requested, return error for now
            // (could return all topics in a full implementation)
            if (topicName == null) {
                return MetadataResponse.builder()
                        .errorCode(Response.ERROR_INVALID_MESSAGE)
                        .topic(null)
                        .partitions(0)
                        .build();
            }

            // Check if topic exists
            if (!topicManager.topicExists(topicName)) {
                return MetadataResponse.builder()
                        .errorCode(Response.ERROR_TOPIC_NOT_FOUND)
                        .topic(topicName)
                        .partitions(0)
                        .build();
            }

            Topic topic = topicManager.getTopic(topicName);

            log.info("Returning metadata for topic={}, partitions={}", topicName, topic.getNumPartitions());

            return new MetadataResponse(topicName, topic.getNumPartitions());

        } catch (Exception e) {
            log.error("Failed to handle metadata request", e);
            return MetadataResponse.builder()
                    .errorCode(Response.ERROR_UNKNOWN)
                    .topic(null)
                    .partitions(0)
                    .build();
        }
    }

    /**
     * Handle OFFSET_COMMIT request
     */
    private Response handleOffsetCommit(OffsetCommitRequest request) {
        try {
            offsetManager.commitOffset(
                    request.getConsumerGroup(),
                    request.getTopic(),
                    request.getPartition(),
                    request.getOffset()
            );

            log.info("Committed offset: group={}, topic={}, partition={}, offset={}",
                    request.getConsumerGroup(), request.getTopic(),
                    request.getPartition(), request.getOffset());

            return new OffsetCommitResponse();

        } catch (Exception e) {
            log.error("Failed to handle offset commit request", e);
            return OffsetCommitResponse.builder()
                    .errorCode(Response.ERROR_UNKNOWN)
                    .build();
        }
    }

    /**
     * Handle HEARTBEAT request
     */
    private Response handleHeartbeat(HeartbeatRequest request) {
        try {
            log.debug("Heartbeat from group={}, consumer={}",
                    request.getConsumerGroup(), request.getConsumerId());

            return new HeartbeatResponse();

        } catch (Exception e) {
            log.error("Failed to handle heartbeat request", e);
            return HeartbeatResponse.builder()
                    .errorCode(Response.ERROR_UNKNOWN)
                    .build();
        }
    }

    /**
     * Handle JOIN_GROUP request
     */
    private Response handleJoinGroup(JoinGroupRequest request) {
        try {
            List<Integer> assignment = groupCoordinator.joinGroup(
                    request.getGroupId(),
                    request.getConsumerId(),
                    request.getTopics()
            );

            log.info("Consumer {} joined group {} with assignment: {}",
                    request.getConsumerId(), request.getGroupId(), assignment);

            return new JoinGroupResponse(assignment);

        } catch (Exception e) {
            log.error("Failed to handle join group request", e);
            return JoinGroupResponse.builder()
                    .errorCode(Response.ERROR_UNKNOWN)
                    .assignedPartitions(List.of())
                    .build();
        }
    }

    /**
     * Handle LEAVE_GROUP request
     */
    private Response handleLeaveGroup(LeaveGroupRequest request) {
        try {
            groupCoordinator.leaveGroup(request.getGroupId(), request.getConsumerId());

            log.info("Consumer {} left group {}", request.getConsumerId(), request.getGroupId());

            return new LeaveGroupResponse();

        } catch (Exception e) {
            log.error("Failed to handle leave group request", e);
            return LeaveGroupResponse.builder()
                    .errorCode(Response.ERROR_UNKNOWN)
                    .build();
        }
    }

    /**
     * Create an error response for unknown requests
     */
    private Response createErrorResponse(short apiKey, short errorCode) {
        return new Response(apiKey, errorCode) {
            @Override
            public byte[] serialize() {
                return new byte[0];
            }
        };
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Exception in request handler", cause);
        ctx.close();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        log.info("Client connected: {}", ctx.channel().remoteAddress());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.info("Client disconnected: {}", ctx.channel().remoteAddress());
    }
}
