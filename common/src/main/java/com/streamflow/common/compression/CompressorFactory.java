package com.streamflow.common.compression;

/**
 * Factory for creating compressor instances
 */
public class CompressorFactory {

    /**
     * Get compressor for the specified type
     */
    public static Compressor getCompressor(Compressor.CompressionType type) {
        return switch (type) {
            case NONE -> new NoOpCompressor();
            case GZIP -> new GzipCompressor();
            case SNAPPY, LZ4 -> throw new UnsupportedOperationException(
                    type + " compression not implemented. Use NONE or GZIP.");
        };
    }
}
