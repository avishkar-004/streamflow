package com.streamflow.broker.storage;

import com.streamflow.common.model.Message;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Represents a single partition of a topic
 *
 * A partition consists of multiple log segments. New segments are created
 * when the current segment reaches a certain size (rolling).
 *
 * Thread-safe: Uses ReadWriteLock to allow concurrent reads but exclusive writes
 */
@Slf4j
public class Partition {

    private static final long SEGMENT_SIZE_BYTES = 100 * 1024 * 1024; // 100MB per segment

    private final String topicName;
    private final int partitionId;
    private final File partitionDir;
    private final List<LogSegment> segments;
    private final ReadWriteLock lock;

    private LogSegment activeSegment;
    private long nextOffset;

    public Partition(String topicName, int partitionId, File baseDir) {
        this.topicName = topicName;
        this.partitionId = partitionId;
        this.partitionDir = new File(baseDir, topicName + "/partition-" + partitionId);
        this.segments = new ArrayList<>();
        this.lock = new ReentrantReadWriteLock();

        // Create partition directory
        if (!partitionDir.exists()) {
            partitionDir.mkdirs();
        }

        // Load existing segments or create a new one
        loadSegments();

        log.info("Created partition: topic={}, partition={}, segments={}, nextOffset={}",
                topicName, partitionId, segments.size(), nextOffset);
    }

    /**
     * Append a message to the partition
     * Returns the offset assigned to the message
     */
    public long append(Message message) {
        lock.writeLock().lock();
        try {
            // Check if we need to roll to a new segment
            if (activeSegment.size() >= SEGMENT_SIZE_BYTES) {
                roll();
            }

            // Append to active segment
            long offset = activeSegment.append(message);
            nextOffset = offset + 1;

            return offset;

        } finally {
            lock.writeLock().unlock();
        }
    }

    public long getLogEndOffset() {
        lock.readLock().lock();
        try { return nextOffset; } finally { lock.readLock().unlock(); }
    }

    public long getLogStartOffset() {
        lock.readLock().lock();
        try {
            return segments.isEmpty() ? 0 : segments.get(0).getBaseOffset();
        } finally { lock.readLock().unlock(); }
    }

    public int getPartitionId() { return partitionId; }
    public String getTopicName() { return topicName; }

    public int getSegmentCount() {
        lock.readLock().lock();
        try { return segments.size(); } finally { lock.readLock().unlock(); }
    }

    private void roll() {
        log.info("Rolling to new segment: topic={}, partition={}, currentOffset={}",
                topicName, partitionId, nextOffset);
        LogSegment newSegment = new LogSegment(partitionDir, nextOffset);
        segments.add(newSegment);
        activeSegment = newSegment;
    }

    private void loadSegments() {
        File[] files = partitionDir.listFiles((dir, name) -> name.endsWith(".log"));
        if (files == null || files.length == 0) {
            LogSegment segment = new LogSegment(partitionDir, 0);
            segments.add(segment);
            activeSegment = segment;
            nextOffset = 0;
        } else {
            for (File logFile : files) {
                String fileName = logFile.getName();
                long baseOffset = Long.parseLong(fileName.substring(0, fileName.length() - 4));
                LogSegment segment = new LogSegment(partitionDir, baseOffset);
                segments.add(segment);
            }
            segments.sort((s1, s2) -> Long.compare(s1.getBaseOffset(), s2.getBaseOffset()));
            activeSegment = segments.get(segments.size() - 1);
            nextOffset = activeSegment.getNextOffset();
        }
    }
}
