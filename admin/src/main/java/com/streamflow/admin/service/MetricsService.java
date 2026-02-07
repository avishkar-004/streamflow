package com.streamflow.admin.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Service for collecting and exposing metrics to Prometheus
 *
 * Metrics exposed:
 * - streamflow_topics_total - Total number of topics
 * - streamflow_partitions_total - Total number of partitions
 * - streamflow_messages_total - Total messages produced
 * - streamflow_bytes_total - Total bytes stored
 * - streamflow_consumer_groups_total - Total consumer groups
 */
@Service
@Slf4j
public class MetricsService {

    private final MeterRegistry meterRegistry;
    private final BrokerService brokerService;

    // Metric gauges
    private final AtomicLong topicsCount = new AtomicLong(0);
    private final AtomicLong partitionsCount = new AtomicLong(0);
    private final AtomicLong consumerGroupsCount = new AtomicLong(0);
    private final Counter messagesCounter;
    private final Counter bytesCounter;

    public MetricsService(MeterRegistry meterRegistry, BrokerService brokerService) {
        this.meterRegistry = meterRegistry;
        this.brokerService = brokerService;

        // Register gauges
        Gauge.builder("streamflow_topics_total", topicsCount, AtomicLong::get)
                .description("Total number of topics")
                .register(meterRegistry);

        Gauge.builder("streamflow_partitions_total", partitionsCount, AtomicLong::get)
                .description("Total number of partitions")
                .register(meterRegistry);

        Gauge.builder("streamflow_consumer_groups_total", consumerGroupsCount, AtomicLong::get)
                .description("Total number of consumer groups")
                .register(meterRegistry);

        // Register counters
        this.messagesCounter = Counter.builder("streamflow_messages_total")
                .description("Total messages produced")
                .register(meterRegistry);

        this.bytesCounter = Counter.builder("streamflow_bytes_total")
                .description("Total bytes stored")
                .register(meterRegistry);

        log.info("MetricsService initialized with Prometheus registry");
    }

    /**
     * Update metrics periodically (every 30 seconds)
     */
    @Scheduled(fixedRate = 30000)
    public void updateMetrics() {
        try {
            // In a real implementation, would query broker for actual metrics
            // For now, these are placeholder values
            log.debug("Updating metrics");
        } catch (Exception e) {
            log.error("Failed to update metrics", e);
        }
    }

    /**
     * Record a message produced
     */
    public void recordMessage(long bytes) {
        messagesCounter.increment();
        bytesCounter.increment(bytes);
    }

    /**
     * Update topic count
     */
    public void setTopicCount(long count) {
        topicsCount.set(count);
    }

    /**
     * Update partition count
     */
    public void setPartitionCount(long count) {
        partitionsCount.set(count);
    }

    /**
     * Update consumer group count
     */
    public void setConsumerGroupCount(long count) {
        consumerGroupsCount.set(count);
    }
}
