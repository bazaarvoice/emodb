package com.bazaarvoice.emodb.web.partition;

/**
 * Encapsulating exception for when a forwarding call to another partition fails due to issues such as connection
 * timeouts.
 */
public class PartitionForwardingException extends RuntimeException {

    public PartitionForwardingException(String message, Throwable cause) {
        super(message, cause);
    }

    public PartitionForwardingException(Throwable cause) {
        super(cause);
    }
}
