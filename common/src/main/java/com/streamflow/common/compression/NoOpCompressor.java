package com.streamflow.common.compression;

/**
 * No-op compressor that returns data unchanged
 */
public class NoOpCompressor implements Compressor {

    @Override
    public byte[] compress(byte[] data) {
        return data;
    }

    @Override
    public byte[] decompress(byte[] compressed) {
        return compressed;
    }

    @Override
    public CompressionType getType() {
        return CompressionType.NONE;
    }
}
