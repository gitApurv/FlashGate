package com.flashgate.common.exception;

public class FlashGateException extends RuntimeException {
    public FlashGateException(String message) {
        super(message);
    }

    public FlashGateException(String message, Throwable cause) {
        super(message, cause);
    }
}
