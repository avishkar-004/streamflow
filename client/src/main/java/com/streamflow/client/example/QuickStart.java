package com.streamflow.client.example;

import com.streamflow.common.model.Message;
import com.streamflow.client.consumer.StreamFlowConsumer;
import com.streamflow.client.producer.StreamFlowProducer;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * Quick start example demonstrating producer and consumer usage
 *
 * Run this after starting the broker with:
 * java -jar broker/target/streamflow-broker-1.0.0-SNAPSHOT.jar
 */
@Slf4j
public class QuickStart {

    public static void main(String[] args) throws Exception {
        String brokerHost = "localhost";
        int brokerPort = 9092;
        String topic = "quickstart-topic";

        // Example 1: Producer
        System.out.println("\n=== Producer Example ===");
        producerExample(brokerHost, brokerPort, topic);

        // Wait a bit
        Thread.sleep(1000);

        // Example 2: Consumer
        System.out.println("\n=== Consumer Example ===");
        consumerExample(brokerHost, brokerPort, topic);
    }

    /**
     * Producer example: Send messages to a topic
     */
    private static void producerExample(String host, int port, String topic) throws Exception {
        StreamFlowProducer producer = new StreamFlowProducer(host, port);

        try {
            // Connect to broker
            producer.connect();
            System.out.println("Producer connected to broker");

            // Send individual messages
            for (int i = 0; i < 10; i++) {
                String key = "user-" + (i % 3); // Keys 0, 1, 2 for partitioning
                String value = "Message " + i + " at " + System.currentTimeMillis();

                StreamFlowProducer.RecordMetadata metadata = producer.send(topic, key, value);

                System.out.printf("Sent: key=%s, value=%s -> %s%n",
                        key, value, metadata);
            }

            // Flush any buffered messages
            producer.flush();
            System.out.println("All messages sent successfully");

        } finally {
            producer.close();
        }
    }

    /**
     * Consumer example: Read messages from a topic
     */
    private static void consumerExample(String host, int port, String topic) throws Exception {
        StreamFlowConsumer consumer = new StreamFlowConsumer(host, port, "quickstart-group");

        try {
            // Connect to broker
            consumer.connect();
            System.out.println("Consumer connected to broker");

            // Subscribe to all partitions of the topic
            for (int partition = 0; partition < 3; partition++) {
                consumer.subscribe(topic, partition);
            }

            System.out.println("Subscribed to topic: " + topic);

            // Poll for messages
            int totalMessages = 0;
            int emptyPolls = 0;
            int maxEmptyPolls = 3;

            while (emptyPolls < maxEmptyPolls) {
                List<Message> messages = consumer.poll(100);

                if (messages.isEmpty()) {
                    emptyPolls++;
                    Thread.sleep(500);
                } else {
                    emptyPolls = 0;
                    for (Message message : messages) {
                        System.out.printf("Received: offset=%d, key=%s, value=%s%n",
                                message.getOffset(),
                                message.getKey(),
                                message.getValue());
                        totalMessages++;
                    }

                    // Commit offsets
                    consumer.commitSync();
                }
            }

            System.out.println("Total messages consumed: " + totalMessages);

        } finally {
            consumer.close();
        }
    }
}
