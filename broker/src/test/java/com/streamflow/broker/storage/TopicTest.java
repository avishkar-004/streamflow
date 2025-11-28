package com.streamflow.broker.storage;

import com.streamflow.common.model.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Topic class
 */
class TopicTest {

    private File testDir;
    private Topic topic;

    @BeforeEach
    void setUp() {
        testDir = new File("target/test-data/topic-test-" + System.currentTimeMillis());
        testDir.mkdirs();
        topic = new Topic("test-topic", 3, testDir);
    }

    @AfterEach
    void tearDown() {
        if (topic != null) {
            topic.close();
        }
        deleteDirectory(testDir);
    }

    @Test
    void testTopicCreation() {
        assertEquals("test-topic", topic.getName());
        assertEquals(3, topic.getNumPartitions());
        assertEquals(3, topic.getAllPartitions().size());
    }

    @Test
    void testAppendToSpecificPartition() {
        Message message = Message.builder()
                .key("key1")
                .value("value1")
                .build();

        long offset = topic.append(0, message);
        assertEquals(0, offset);

        List<Message> messages = topic.read(0, 0, 10);
        assertEquals(1, messages.size());
        assertEquals("value1", messages.get(0).getValue());
    }

    @Test
    void testAppendWithKeyHashing() {
        // Append messages with same key - should go to same partition
        Message msg1 = Message.builder().key("same-key").value("value1").build();
        Message msg2 = Message.builder().key("same-key").value("value2").build();

        topic.append(msg1);
        topic.append(msg2);

        // Verify they're in the same partition
        boolean found = false;
        for (int i = 0; i < 3; i++) {
            List<Message> messages = topic.read(i, 0, 10);
            if (messages.size() == 2) {
                found = true;
                assertEquals("value1", messages.get(0).getValue());
                assertEquals("value2", messages.get(1).getValue());
            }
        }
        assertTrue(found, "Messages with same key should be in the same partition");
    }

    @Test
    void testAppendToAllPartitions() {
        // Append messages to each partition
        for (int i = 0; i < 3; i++) {
            Message message = Message.builder()
                    .key("key" + i)
                    .value("partition-" + i)
                    .build();
            topic.append(i, message);
        }

        // Verify each partition has one message
        for (int i = 0; i < 3; i++) {
            List<Message> messages = topic.read(i, 0, 10);
            assertEquals(1, messages.size());
            assertEquals("partition-" + i, messages.get(0).getValue());
        }
    }

    @Test
    void testGetPartition() {
        Partition partition0 = topic.getPartition(0);
        assertNotNull(partition0);
        assertEquals(0, partition0.getPartitionId());
        assertEquals("test-topic", partition0.getTopicName());
    }

    @Test
    void testInvalidPartitionId() {
        assertThrows(IllegalArgumentException.class, () -> {
            topic.append(5, Message.builder().key("k").value("v").build());
        });

        assertThrows(IllegalArgumentException.class, () -> {
            topic.append(-1, Message.builder().key("k").value("v").build());
        });
    }

    @Test
    void testLogEndOffset() {
        assertEquals(0, topic.getLogEndOffset(0));

        topic.append(0, Message.builder().key("k1").value("v1").build());
        assertEquals(1, topic.getLogEndOffset(0));

        topic.append(0, Message.builder().key("k2").value("v2").build());
        assertEquals(2, topic.getLogEndOffset(0));

        // Other partitions should still be at 0
        assertEquals(0, topic.getLogEndOffset(1));
        assertEquals(0, topic.getLogEndOffset(2));
    }

    @Test
    void testTopicStats() {
        // Append messages to different partitions
        for (int p = 0; p < 3; p++) {
            for (int i = 0; i < 5; i++) {
                topic.append(p, Message.builder()
                        .key("key-" + p + "-" + i)
                        .value("value-" + p + "-" + i)
                        .build());
            }
        }

        Topic.TopicStats stats = topic.getStats();
        assertEquals("test-topic", stats.name);
        assertEquals(3, stats.partitions);
        assertEquals(15, stats.totalMessages); // 3 partitions * 5 messages
    }

    @Test
    void testFlush() {
        for (int i = 0; i < 5; i++) {
            topic.append(0, Message.builder()
                    .key("key" + i)
                    .value("value" + i)
                    .build());
        }

        assertDoesNotThrow(() -> topic.flush());
    }

    private void deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
            directory.delete();
        }
    }
}
