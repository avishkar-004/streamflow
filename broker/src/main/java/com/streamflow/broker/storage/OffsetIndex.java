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

    /**
     * Append an index entry
     */
    public synchronized void append(long offset, int position) {
        if (entries >= maxEntries) {
            log.warn("Index full, cannot add more entries: {}", file);
            return;
        }

        mmap.putLong(offset);
        mmap.putInt(position);
        entries++;
    }

    /**
     * Flush index to disk
     */
    public void flush() {
        if (mmap != null) {
            mmap.force();
        }
    }

    /**
     * Close the index
     */
    public void close() {
        try {
            flush();
            if (fileChannel != null) fileChannel.close();
            if (raf != null) raf.close();
        } catch (IOException e) {
            throw new StorageException("Failed to close index: " + file, e);
        }
    }
}
