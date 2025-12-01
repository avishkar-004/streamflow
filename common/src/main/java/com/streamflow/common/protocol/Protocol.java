package com.streamflow.common.protocol;

/**
 * Protocol constants and API keys for the StreamFlow binary protocol
 *
 * Message Format:
 * ┌─────────────┬─────────────┬─────────────┬──────────────┐
 * │  Size (4B)  │ API Key (2B)│ Version (2B)│   Payload    │
 * └─────────────┴─────────────┴─────────────┴──────────────┘
 */
public class Protocol {

    // Protocol version
    public static final short PROTOCOL_VERSION = 1;

    // API Keys for different request types
    public static final short API_KEY_PRODUCE = 0;
    public static final short API_KEY_FETCH = 1;
    public static final short API_KEY_METADATA = 3;
    public static final short API_KEY_OFFSET_COMMIT = 8;
    public static final short API_KEY_OFFSET_FETCH = 9;
    public static final short API_KEY_JOIN_GROUP = 11;
    public static final short API_KEY_HEARTBEAT = 12;
    public static final short API_KEY_LEAVE_GROUP = 13;
    public static final short API_KEY_SYNC_GROUP = 14;

    // Header size in bytes (size + apiKey + version)
    public static final int HEADER_SIZE = 4 + 2 + 2;

    // Maximum message size (10MB)
    public static final int MAX_MESSAGE_SIZE = 10 * 1024 * 1024;

    /**
     * Get API key name for logging
     */
    public static String getApiKeyName(short apiKey) {
        return switch (apiKey) {
            case API_KEY_PRODUCE -> "PRODUCE";
            case API_KEY_FETCH -> "FETCH";
            case API_KEY_METADATA -> "METADATA";
            case API_KEY_OFFSET_COMMIT -> "OFFSET_COMMIT";
            case API_KEY_OFFSET_FETCH -> "OFFSET_FETCH";
            case API_KEY_JOIN_GROUP -> "JOIN_GROUP";
            case API_KEY_HEARTBEAT -> "HEARTBEAT";
            case API_KEY_LEAVE_GROUP -> "LEAVE_GROUP";
            case API_KEY_SYNC_GROUP -> "SYNC_GROUP";
            default -> "UNKNOWN";
        };
    }
}
