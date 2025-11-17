package com.streamflow.broker.storage;

import com.streamflow.common.model.Message;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Enhanced LogSegment with zero-copy transfer support
 *
 * Zero-copy transfer uses FileChannel.transferTo() to send data
 * directly from disk to network socket without copying to user space.
 *
 * Performance benefits:
 * - Reduces CPU usage (no copy to/from kernel space)
 * - Reduces memory usage (no intermediate buffers)
 * - Increases throughput (direct DMA transfer)
 *
 * Traditional read:
 *   Disk -> Kernel buffer -> User buffer -> Socket buffer -> Network
 *
 * Zero-copy read:
 *   Disk -> Kernel buffer -> Network (via DMA)
 */
@Slf4j
public class ZeroCopySegment {

    private final File logFile;
    private final FileChannel fileChannel;
    private final OffsetIndex offsetIndex;
    private final long baseOffset;

    public ZeroCopySegment(File logFile, long baseOffset) throws Exception {
        this.logFile = logFile;
        this.baseOffset = baseOffset;

        // Open file with read-write access (RAF will be managed by FileChannel)
        @SuppressWarnings("resource")
        RandomAccessFile raf = new RandomAccessFile(logFile, "rw");
        this.fileChannel = raf.getChannel();

        // Create index file
        File indexFile = new File(logFile.getParent(),
                logFile.getName().replace(".log", ".index"));
        this.offsetIndex = new OffsetIndex(indexFile, baseOffset);

        log.info("Created ZeroCopySegment for base offset {} at {}", baseOffset, logFile);
    }

    /**
     * Zero-copy transfer to socket channel
     * Transfers data directly from file to network socket
     *
     * @param socketChannel Target socket
     * @param startOffset Starting offset to read from
     * @param maxBytes Maximum bytes to transfer
     * @return Number of bytes transferred
     */
    public long transferTo(SocketChannel socketChannel, long startOffset, long maxBytes) throws Exception {
        // Find file position for this offset
        OffsetIndex.OffsetPosition pos = offsetIndex.lookup(startOffset);
        if (pos == null) {
            log.warn("Offset {} not found in segment {}", startOffset, baseOffset);
            return 0;
        }

        long position = pos.position;
        long bytesToTransfer = Math.min(maxBytes, fileChannel.size() - position);

        // Zero-copy transfer using transferTo
        long bytesTransferred = fileChannel.transferTo(position, bytesToTransfer, socketChannel);

        log.debug("Zero-copy transferred {} bytes from offset {} to socket",
                bytesTransferred, startOffset);

        return bytesTransferred;
    }

    /**
     * Read messages using traditional I/O (for comparison)
     */
    public List<Message> read(long startOffset, int maxRecords, int maxBytes) throws Exception {
        List<Message> messages = new ArrayList<>();
        OffsetIndex.OffsetPosition pos = offsetIndex.lookup(startOffset);

        if (pos == null) {
            return messages;
        }

        long position = pos.position;
        int bytesRead = 0;

        while (messages.size() < maxRecords && bytesRead < maxBytes) {
            // Read message size
            ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
            fileChannel.read(sizeBuffer, position);
            sizeBuffer.flip();
            int messageSize = sizeBuffer.getInt();

            if (messageSize <= 0 || messageSize > maxBytes) {
                break;
            }

            // Read message data
            ByteBuffer messageBuffer = ByteBuffer.allocate(messageSize);
            fileChannel.read(messageBuffer, position + 4);
            messageBuffer.flip();

            Message message = Message.deserialize(messageBuffer.array());
            messages.add(message);

            position += 4 + messageSize;
            bytesRead += 4 + messageSize;
        }

        return messages;
    }

    /**
     * Append message to segment
     */
    public long append(Message message) throws Exception {
        byte[] data = message.serialize();
        int size = data.length;

        long position = fileChannel.size();

        // Write size + data
        ByteBuffer buffer = ByteBuffer.allocate(4 + size);
        buffer.putInt(size);
        buffer.put(data);
        buffer.flip();

        fileChannel.write(buffer);

        // Update index
        offsetIndex.append(message.getOffset(), (int) position);

        return message.getOffset();
    }

    /**
     * Get segment size in bytes
     */
    public long size() throws Exception {
        return fileChannel.size();
    }

    /**
     * Close segment and flush to disk
     */
    public void close() throws Exception {
        fileChannel.force(true);
        fileChannel.close();
        offsetIndex.close();
        log.info("Closed ZeroCopySegment for base offset {}", baseOffset);
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    public File getLogFile() {
        return logFile;
    }
}
