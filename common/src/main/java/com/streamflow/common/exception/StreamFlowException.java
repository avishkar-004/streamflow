package com.streamflow.common.exception;

/**
 * Base exception class for all StreamFlow exceptions
 */
public class StreamFlowException extends RuntimeException {

    public StreamFlowException(String message) {
        super(message);
    }

    public StreamFlowException(String message, Throwable cause) {
        super(message, cause);
    }
}
