package com.streamflow.client.benchmark;

import com.streamflow.broker.BrokerApplication;
import com.streamflow.broker.config.BrokerConfig;
import com.streamflow.client.producer.StreamFlowProducer;
import com.streamflow.client.consumer.StreamFlowConsumer;
import com.streamflow.common.model.Message;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Performance benchmark for StreamFlow
 *
 * Tests:
 * 1. Producer throughput (messages/sec)
 * 2. Consumer throughput (messages/sec)
 * 3. End-to-end latency (ms)
 * 4. Batch vs individual sends
 * 5. Compression impact
 */
@Slf4j
public class PerformanceBenchmark {

    private static final String TOPIC = "benchmark-topic";
    private static final int WARMUP_MESSAGES = 10_000;
    private static final int BENCHMARK_MESSAGES = 100_000;
    private static final int MESSAGE_SIZE = 1024; // 1KB

    public static void main(String[] args) throws Exception {
        log.info("Starting StreamFlow Performance Benchmark");

        // Start broker
        BrokerConfig config = BrokerConfig.testConfig();
        BrokerApplication broker = new BrokerApplication(config);
        broker.start();

        // Wait for broker to be ready
        Thread.sleep(2000);

        try {
            // Run benchmarks
            runProducerThroughputBenchmark();
            runConsumerThroughputBenchmark();
            runLatencyBenchmark();
            runBatchingBenchmark();

        } finally {
            broker.shutdown();
        }

        log.info("Benchmark complete!");
    }

    /**
     * Benchmark 1: Producer throughput
     */
    private static void runProducerThroughputBenchmark() throws Exception {
        log.info("=== Producer Throughput Benchmark ===");

        StreamFlowProducer producer = new StreamFlowProducer("localhost", 9092);
        producer.connect();

        // Warmup
        log.info("Warming up...");
        for (int i = 0; i < WARMUP_MESSAGES; i++) {
            String value = generateMessage(MESSAGE_SIZE);
            producer.send(TOPIC, "key-" + i, value);
        }

        // Benchmark
        log.info("Running benchmark with {} messages...", BENCHMARK_MESSAGES);
        long startTime = System.currentTimeMillis();
        long startBytes = 0;

        for (int i = 0; i < BENCHMARK_MESSAGES; i++) {
            String value = generateMessage(MESSAGE_SIZE);
            producer.send(TOPIC, "key-" + i, value);
            startBytes += value.getBytes().length;
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        // Results
        double messagesPerSec = (BENCHMARK_MESSAGES * 1000.0) / duration;
        double mbPerSec = (startBytes / (1024.0 * 1024.0)) / (duration / 1000.0);

        log.info("Producer Throughput Results:");
        log.info("  Messages sent: {}", BENCHMARK_MESSAGES);
        log.info("  Duration: {} ms", duration);
        log.info("  Throughput: {:.2f} messages/sec", messagesPerSec);
        log.info("  Throughput: {:.2f} MB/sec", mbPerSec);

        producer.close();
    }

    /**
     * Benchmark 2: Consumer throughput
     */
    private static void runConsumerThroughputBenchmark() throws Exception {
        log.info("\n=== Consumer Throughput Benchmark ===");

        StreamFlowConsumer consumer = new StreamFlowConsumer("localhost", 9092, "benchmark-group");
        consumer.connect();
        consumer.subscribe(TOPIC, 0);

        long messagesConsumed = 0;
        long bytesConsumed = 0;
        long startTime = System.currentTimeMillis();

        while (messagesConsumed < BENCHMARK_MESSAGES) {
            List<Message> messages = consumer.poll(1000);
            messagesConsumed += messages.size();

            for (Message msg : messages) {
                bytesConsumed += msg.sizeInBytes();
            }
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        // Results
        double messagesPerSec = (messagesConsumed * 1000.0) / duration;
        double mbPerSec = (bytesConsumed / (1024.0 * 1024.0)) / (duration / 1000.0);

        log.info("Consumer Throughput Results:");
        log.info("  Messages consumed: {}", messagesConsumed);
        log.info("  Duration: {} ms", duration);
        log.info("  Throughput: {:.2f} messages/sec", messagesPerSec);
        log.info("  Throughput: {:.2f} MB/sec", mbPerSec);

        consumer.close();
    }

    /**
     * Benchmark 3: End-to-end latency
     */
    private static void runLatencyBenchmark() throws Exception {
        log.info("\n=== End-to-End Latency Benchmark ===");

        StreamFlowProducer producer = new StreamFlowProducer("localhost", 9092);
        producer.connect();

        StreamFlowConsumer consumer = new StreamFlowConsumer("localhost", 9092, "latency-group");
        consumer.connect();
        consumer.subscribe(TOPIC, 0);

        // Measure latencies
        long[] latencies = new long[1000];

        for (int i = 0; i < 1000; i++) {
            long sendTime = System.nanoTime();
            producer.send(TOPIC, "latency-key", "latency-test-" + i);

            // Poll until we get the message
            while (true) {
                List<Message> messages = consumer.poll(1);
                if (!messages.isEmpty()) {
                    long receiveTime = System.nanoTime();
                    latencies[i] = (receiveTime - sendTime) / 1_000_000; // Convert to ms
                    break;
                }
            }
        }

        // Calculate statistics
        long sum = 0;
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;

        for (long latency : latencies) {
            sum += latency;
            min = Math.min(min, latency);
            max = Math.max(max, latency);
        }

        double avg = sum / 1000.0;

        // Calculate p50, p95, p99
        java.util.Arrays.sort(latencies);
        long p50 = latencies[500];
        long p95 = latencies[950];
        long p99 = latencies[990];

        log.info("Latency Results:");
        log.info("  Average: {:.2f} ms", avg);
        log.info("  Min: {} ms", min);
        log.info("  Max: {} ms", max);
        log.info("  P50: {} ms", p50);
        log.info("  P95: {} ms", p95);
        log.info("  P99: {} ms", p99);

        producer.close();
        consumer.close();
    }

    /**
     * Benchmark 4: Batching vs individual sends
     */
    private static void runBatchingBenchmark() throws Exception {
        log.info("\n=== Batching Impact Benchmark ===");

        StreamFlowProducer producer = new StreamFlowProducer("localhost", 9092);
        producer.connect();

        int testMessages = 10_000;

        // Test individual sends
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < testMessages; i++) {
            producer.send(TOPIC, "key-" + i, generateMessage(MESSAGE_SIZE));
        }
        long individualDuration = System.currentTimeMillis() - startTime;

        // TODO: Test batched sends (when batching is implemented)
        // For now, just report individual send performance

        log.info("Batching Results:");
        log.info("  Individual sends: {} messages in {} ms ({:.2f} msg/sec)",
                testMessages, individualDuration, (testMessages * 1000.0) / individualDuration);
        log.info("  Batched sends: Not yet implemented");

        producer.close();
    }

    /**
     * Generate a message of the specified size
     */
    private static String generateMessage(int size) {
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            sb.append((char) ('a' + (i % 26)));
        }
        return sb.toString();
    }
}
