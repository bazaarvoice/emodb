package com.bazaarvoice.emodb.sortedq.core;

public class ReadOnlyQueueException extends RuntimeException {
    public ReadOnlyQueueException() {
    }

    public ReadOnlyQueueException(String message) {
        super(message);
    }
}
