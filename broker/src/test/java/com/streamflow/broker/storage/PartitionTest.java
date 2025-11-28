package com.streamflow.broker.storage;

import com.streamflow.common.model.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Partition class
 */
class PartitionTest {

    private File testDir;
    private Partition partition;

    @BeforeEach
    void setUp() {
        testDir = new File("target/test-data/partition-test-" + System.currentTimeMillis());
        testDir.mkdirs();
        partition = new Partition("test-topic", 0, testDir);
    }

    @AfterEach
    void tearDown() {
        if (partition != null) {
            partition.close();
        }
        deleteDirectory(testDir);
    }

    @Test
    void testAppendAndRead() {
        Message message = Message.builder()
                .key("key1")
                .value("value1")
                .build();

        long offset = partition.append(message);
        assertEquals(0, offset);

        List<Message> messages = partition.read(0, 10);
        assertEquals(1, messages.size());
        assertEquals("key1", messages.get(0).getKey());
        assertEquals("value1", messages.get(0).getValue());
    }

    @Test
    void testAppendMultipleMessages() {
        for (int i = 0; i < 10; i++) {
            Message message = Message.builder()
                    .key("key" + i)
                    .value("value" + i)
                    .build();
            long offset = partition.append(message);
            assertEquals(i, offset);
        }

        List<Message> messages = partition.read(0, 10);
        assertEquals(10, messages.size());

        for (int i = 0; i < 10; i++) {
            assertEquals("key" + i, messages.get(i).getKey());
            assertEquals("value" + i, messages.get(i).getValue());
        }
    }

    @Test
    void testReadFromMiddle() {
        // Append 10 messages
        for (int i = 0; i < 10; i++) {
            Message message = Message.builder()
                    .key("key" + i)
                    .value("value" + i)
                    .build();
            partition.append(message);
        }

        // Read from offset 5
        List<Message> messages = partition.read(5, 10);
        assertEquals(5, messages.size());
        assertEquals("key5", messages.get(0).getKey());
        assertEquals("key9", messages.get(4).getKey());
    }

    @Test
    void testReadWithMaxRecords() {
        // Append 10 messages
        for (int i = 0; i < 10; i++) {
            Message message = Message.builder()
                    .key("key" + i)
                    .value("value" + i)
                    .build();
            partition.append(message);
        }

        // Read only 3 messages
        List<Message> messages = partition.read(0, 3);
        assertEquals(3, messages.size());
    }

    @Test
    void testAppendBatch() {
        List<Message> batch = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            batch.add(Message.builder()
                    .key("batch-key" + i)
                    .value("batch-value" + i)
                    .build());
        }

        long firstOffset = partition.appendBatch(batch);
        assertEquals(0, firstOffset);

        List<Message> messages = partition.read(0, 10);
        assertEquals(5, messages.size());
    }

    @Test
    void testLogEndOffset() {
        assertEquals(0, partition.getLogEndOffset());

        partition.append(Message.builder().key("k1").value("v1").build());
        assertEquals(1, partition.getLogEndOffset());

        partition.append(Message.builder().key("k2").value("v2").build());
        assertEquals(2, partition.getLogEndOffset());
    }

    @Test
    void testLogStartOffset() {
        assertEquals(0, partition.getLogStartOffset());

        for (int i = 0; i < 5; i++) {
            partition.append(Message.builder()
                    .key("key" + i)
                    .value("value" + i)
                    .build());
        }

        assertEquals(0, partition.getLogStartOffset());
    }

    @Test
    void testFlush() {
        for (int i = 0; i < 5; i++) {
            partition.append(Message.builder()
                    .key("key" + i)
                    .value("value" + i)
                    .build());
        }

        // Flush should not throw exception
        assertDoesNotThrow(() -> partition.flush());
    }

    @Test
    void testPersistenceAcrossRestarts() {
        // Write some messages
        for (int i = 0; i < 5; i++) {
            partition.append(Message.builder()
                    .key("key" + i)
                    .value("value" + i)
                    .build());
        }

        partition.flush();
        partition.close();

        // Reopen the partition
        partition = new Partition("test-topic", 0, testDir);

        // Verify messages are still there
        List<Message> messages = partition.read(0, 10);
        assertEquals(5, messages.size());
        assertEquals("key0", messages.get(0).getKey());
        assertEquals("key4", messages.get(4).getKey());
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
