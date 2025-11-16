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

    /**
     * Read messages starting from the given offset
     * Returns up to maxRecords messages or maxBytes worth of data
     */
    public synchronized List<Message> read(long startOffset, int maxRecords, int maxBytes) {
        List<Message> messages = new ArrayList<>();

        try {
            // Validate offset range
            if (startOffset < baseOffset || startOffset >= nextOffset) {
                return messages;
            }

            // Look up the file position for the start offset
            OffsetIndex.OffsetPosition indexEntry = offsetIndex.lookup(startOffset);
            int position = indexEntry.position;

            // Read from the log file
            ByteBuffer buffer = ByteBuffer.allocate(Math.min(maxBytes, (int) (fileChannel.size() - position)));
            fileChannel.read(buffer, position);
            buffer.flip();

            // Parse messages
            int bytesRead = 0;
            while (buffer.hasRemaining() && messages.size() < maxRecords && bytesRead < maxBytes) {
                int messageStart = buffer.position();

                // Check if we have enough bytes to read the message header
                if (buffer.remaining() < 24) { // min header size
                    break;
                }

                // Parse message
                int messageSize = Message.parseMessageSize(buffer, messageStart);
                if (buffer.remaining() < messageSize) {
                    break; // Incomplete message
                }

                byte[] messageData = new byte[messageSize];
                buffer.get(messageData);

                Message message = Message.deserialize(messageData);

                // Only include messages >= startOffset
                if (message.getOffset() >= startOffset) {
                    messages.add(message);
                    bytesRead += messageSize;
                }
            }

            log.debug("Read {} messages from offset {}, bytes read: {}", messages.size(), startOffset, bytesRead);

        } catch (IOException e) {
            throw new StorageException("Failed to read from log segment", e);
        }

        return messages;
    }

    /**
     * Get the size of this segment in bytes
     */
    public synchronized long size() {
        try {
            return fileChannel.size();
        } catch (IOException e) {
            throw new StorageException("Failed to get segment size", e);
        }
    }

    /**
     * Get the next offset that will be assigned
     */
    public long getNextOffset() {
        return nextOffset;
    }

    /**
     * Get the base offset of this segment
     */
    public long getBaseOffset() {
        return baseOffset;
    }

    /**
     * Check if this segment is empty
     */
    public boolean isEmpty() {
        return nextOffset == baseOffset;
    }

    /**
     * Flush the segment to disk
     */
    public synchronized void flush() {
        try {
            fileChannel.force(true);
            offsetIndex.flush();
            log.debug("Flushed log segment: {}", logFile.getName());
        } catch (IOException e) {
            throw new StorageException("Failed to flush log segment", e);
        }
    }

    /**
     * Close the segment
     */
    public synchronized void close() {
        try {
            flush();
            fileChannel.close();
            raf.close();
            offsetIndex.close();
            log.info("Closed log segment: {}", logFile.getName());
        } catch (IOException e) {
            throw new StorageException("Failed to close log segment", e);
        }
    }

    /**
     * Delete the segment files
     */
    public synchronized void delete() {
        try {
            close();
            if (logFile.exists() && !logFile.delete()) {
                log.warn("Failed to delete log file: {}", logFile);
            }
            if (indexFile.exists() && !indexFile.delete()) {
                log.warn("Failed to delete index file: {}", indexFile);
            }
            log.info("Deleted log segment: {}", logFile.getName());
        } catch (Exception e) {
            throw new StorageException("Failed to delete log segment", e);
        }
    }

    /**
     * Recover the next offset by scanning the log file
     */
    private long recoverNextOffset() throws IOException {
        long offset = baseOffset;
        long position = 0;
        long fileSize = fileChannel.size();

        ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024); // 1MB buffer

        while (position < fileSize) {
            buffer.clear();
            int bytesRead = fileChannel.read(buffer, position);
            if (bytesRead <= 0) {
                break;
            }

            buffer.flip();

            while (buffer.hasRemaining()) {
                if (buffer.remaining() < 24) {
                    break; // Not enough for header
                }

                int messageStart = buffer.position();
                int messageSize = Message.parseMessageSize(buffer, messageStart);

                if (buffer.remaining() < messageSize) {
                    break; // Incomplete message
                }

                // Skip to next message
                buffer.position(messageStart + messageSize);
                offset++;
            }

            position += bytesRead;
        }

        return offset;
    }

    /**
     * Format the segment file name based on base offset
     */
    private static String formatFileName(long offset, String extension) {
        return String.format("%020d%s", offset, extension);
    }
}
