package com.streamflow.broker.storage;

import com.streamflow.common.exception.StorageException;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Sparse index that maps message offsets to physical file positions
 *
 * Index Format:
 * Each entry is 12 bytes:
 * - Offset (8 bytes): Logical offset in partition
 * - Position (4 bytes): Physical position in log file
 *
 * The index is sparse - we don't index every message, only every Nth message
 * to save space. Binary search is used to find the closest indexed offset.
 */
@Slf4j
public class OffsetIndex {

    private static final int INDEX_ENTRY_SIZE = 12; // 8 bytes offset + 4 bytes position
    private static final int MAX_INDEX_SIZE = 10 * 1024 * 1024; // 10MB max index file

    private final File file;
    private final long baseOffset;
    private final RandomAccessFile raf;
    private final FileChannel channel;
    private final MappedByteBuffer mmap;
    private int entries;

    public OffsetIndex(File file, long baseOffset) {
        this.file = file;
        this.baseOffset = baseOffset;

        try {
            boolean newFile = !file.exists();
            this.raf = new RandomAccessFile(file, "rw");
            this.channel = raf.getChannel();

            if (newFile) {
                // Pre-allocate the index file
                raf.setLength(MAX_INDEX_SIZE);
            }

            // Memory-map the index file for fast access
            this.mmap = channel.map(FileChannel.MapMode.READ_WRITE, 0, MAX_INDEX_SIZE);

            // Count existing entries
            this.entries = countEntries();

            log.info("Opened index file: {}, baseOffset: {}, entries: {}",
                    file.getName(), baseOffset, entries);

        } catch (IOException e) {
            throw new StorageException("Failed to open index file: " + file, e);
        }
    }

    /**
     * Append a new offset-position mapping to the index
     */
    public synchronized void append(long offset, int position) {
        if (entries * INDEX_ENTRY_SIZE >= MAX_INDEX_SIZE - INDEX_ENTRY_SIZE) {
            throw new StorageException("Index file is full: " + file);
        }

        int entryOffset = entries * INDEX_ENTRY_SIZE;
        mmap.putLong(entryOffset, offset);
        mmap.putInt(entryOffset + 8, position);
        entries++;

        log.debug("Appended to index: offset={}, position={}, entries={}", offset, position, entries);
    }

    /**
     * Lookup the file position for a given offset using binary search
     * Returns the position of the largest offset <= target offset
     */
    public synchronized OffsetPosition lookup(long targetOffset) {
        if (entries == 0) {
            return new OffsetPosition(baseOffset, 0);
        }

        // Binary search to find the largest offset <= targetOffset
        int low = 0;
        int high = entries - 1;
        int resultIndex = -1;

        while (low <= high) {
            int mid = (low + high) / 2;
            long midOffset = readOffset(mid);

            if (midOffset == targetOffset) {
                resultIndex = mid;
                break;
            } else if (midOffset < targetOffset) {
                resultIndex = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        if (resultIndex == -1) {
            return new OffsetPosition(baseOffset, 0);
        }

        long offset = readOffset(resultIndex);
        int position = readPosition(resultIndex);

        return new OffsetPosition(offset, position);
    }

    /**
     * Read offset at given index position
     */
    private long readOffset(int index) {
        return mmap.getLong(index * INDEX_ENTRY_SIZE);
    }

    /**
     * Read file position at given index position
     */
    private int readPosition(int index) {
        return mmap.getInt(index * INDEX_ENTRY_SIZE + 8);
    }

    /**
     * Count number of valid entries in the index
     */
    private int countEntries() {
        int count = 0;
        for (int i = 0; i < MAX_INDEX_SIZE / INDEX_ENTRY_SIZE; i++) {
            long offset = mmap.getLong(i * INDEX_ENTRY_SIZE);
            if (offset == 0) {
                break;
            }
            count++;
        }
        return count;
    }

    /**
     * Get the number of entries in the index
     */
    public int getEntries() {
        return entries;
    }

    /**
     * Get the base offset for this index
     */
    public long getBaseOffset() {
        return baseOffset;
    }

    /**
     * Flush the memory-mapped buffer to disk
     */
    public synchronized void flush() {
        mmap.force();
    }

    /**
     * Close the index file
     */
    public synchronized void close() {
        try {
            flush();
            channel.close();
            raf.close();
            log.info("Closed index file: {}", file.getName());
        } catch (IOException e) {
            throw new StorageException("Failed to close index file: " + file, e);
        }
    }

    /**
     * Result of offset lookup containing both offset and position
     */
    public static class OffsetPosition {
        public final long offset;
        public final int position;

        public OffsetPosition(long offset, int position) {
            this.offset = offset;
            this.position = position;
        }
    }
}
