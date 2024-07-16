package com.bazaarvoice.emodb.web.resources.blob.messageQueue;

public class SQSMessageException extends RuntimeException {
    public SQSMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}