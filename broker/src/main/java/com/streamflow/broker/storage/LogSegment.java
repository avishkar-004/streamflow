package com.streamflow.broker.storage;

import com.streamflow.common.model.Message;

import com.streamflow.common.exception.StorageException;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a single segment of the log file
 *
 * A log is split into segments for easier management:
 * - Easier to delete old messages (just delete old segment files)
 * - Easier to flush to disk (flush current segment)
 * - Limits size of index files
 *
 * Each segment consists of:
 * - A log file (.log): Contains the actual messages
 * - An index file (.index): Maps offsets to file positions
 */
@Slf4j
public class LogSegment {

    private static final int INDEX_INTERVAL_BYTES = 4096; // Index every 4KB

    private final File logFile;
    private final File indexFile;
    private final long baseOffset;
    private final RandomAccessFile raf;
    private final FileChannel fileChannel;
    private final OffsetIndex offsetIndex;

    private long nextOffset;
    private int bytesSinceLastIndex;

    public LogSegment(File dir, long baseOffset) {
        this.baseOffset = baseOffset;
        this.logFile = new File(dir, formatFileName(baseOffset, ".log"));
        this.indexFile = new File(dir, formatFileName(baseOffset, ".index"));

        try {
            // Create directory if it doesn't exist
            if (!dir.exists()) {
                dir.mkdirs();
            }

            // Open or create the log file
            boolean isNewFile = !logFile.exists();
            this.raf = new RandomAccessFile(logFile, "rw");
            this.fileChannel = raf.getChannel();

            // Open or create the index
            this.offsetIndex = new OffsetIndex(indexFile, baseOffset);

            // Set next offset
            if (isNewFile) {
                this.nextOffset = baseOffset;
                this.bytesSinceLastIndex = 0;
            } else {
                // Recover the next offset from existing file
                this.nextOffset = recoverNextOffset();
                this.bytesSinceLastIndex = (int) fileChannel.size() % INDEX_INTERVAL_BYTES;
            }

            log.info("Opened log segment: {}, baseOffset: {}, nextOffset: {}, size: {} bytes",
                    logFile.getName(), baseOffset, nextOffset, fileChannel.size());

        } catch (IOException e) {
            throw new StorageException("Failed to open log segment: " + logFile, e);
        }
    }

    /**
     * Append a message to the log segment
     * Returns the offset assigned to the message
     */
    public synchronized long append(Message message) {
        try {
            // Assign offset and timestamp
            message.setOffset(nextOffset);
            if (message.getTimestamp() == 0) {
                message.setTimestamp(System.currentTimeMillis());
            }

            // Serialize the message
            byte[] data = message.serialize();

            // Get current position before writing
            int position = (int) fileChannel.size();

            // Write to the log file
            ByteBuffer buffer = ByteBuffer.wrap(data);
            while (buffer.hasRemaining()) {
                fileChannel.write(buffer);
            }

            // Update index if needed (sparse indexing)
            bytesSinceLastIndex += data.length;
            if (bytesSinceLastIndex >= INDEX_INTERVAL_BYTES) {
                offsetIndex.append(nextOffset, position);
                bytesSinceLastIndex = 0;
            }

            log.debug("Appended message: offset={}, size={} bytes, position={}",
                    nextOffset, data.length, position);

            return nextOffset++;

        } catch (IOException e) {
            throw new StorageException("Failed to append message to log segment", e);
        }
    }

    public synchronized long size() {
        try {
            return fileChannel.size();
        } catch (IOException e) {
            throw new StorageException("Failed to get segment size", e);
        }
    }

    public long getNextOffset() { return nextOffset; }
    public long getBaseOffset() { return baseOffset; }
    public boolean isEmpty() { return nextOffset == baseOffset; }

    public synchronized void flush() {
        try {
            fileChannel.force(true);
            offsetIndex.flush();
        } catch (IOException e) {
            throw new StorageException("Failed to flush log segment", e);
        }
    }

    public synchronized void close() {
        try {
            flush();
            fileChannel.close();
            raf.close();
            offsetIndex.close();
        } catch (IOException e) {
            throw new StorageException("Failed to close log segment", e);
        }
    }

    private static String formatFileName(long offset, String extension) {
        return String.format("%020d%s", offset, extension);
    }
}
