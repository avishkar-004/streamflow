package com.streamflow.common.compression;

/**
 * Interface for message compression
 *
 * Compression benefits:
 * - Reduces network bandwidth usage
 * - Reduces disk storage usage
 * - Increases throughput (less data to transfer)
 *
 * Compression trade-offs:
 * - Adds CPU overhead
 * - Adds latency (compression/decompression time)
 *
 * Supported types:
 * - NONE: No compression (fastest, largest)
 * - GZIP: Good compression ratio, slower (best for text)
 * - SNAPPY: Fast compression, good ratio (balanced)
 * - LZ4: Very fast, moderate compression (best for throughput)
 */
public interface Compressor {

    /**
     * Compress data
     */
    byte[] compress(byte[] data) throws Exception;

    /**
     * Decompress data
     */
    byte[] decompress(byte[] compressed) throws Exception;

    /**
     * Get compression type
     */
    CompressionType getType();

    /**
     * Compression types
     */
    enum CompressionType {
        NONE((byte) 0),
        GZIP((byte) 1),
        SNAPPY((byte) 2),
        LZ4((byte) 3);

        private final byte id;

        CompressionType(byte id) {
            this.id = id;
        }

        public byte getId() {
            return id;
        }

        public static CompressionType fromId(byte id) {
            for (CompressionType type : values()) {
                if (type.id == id) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unknown compression type: " + id);
        }
    }
}
