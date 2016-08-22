package com.bazaarvoice.emodb.common.json;

/**
 * A json streaming parser encountered an early end-of-stream, before all results were parsed.
 */
public class JsonStreamingEOFException extends RuntimeException {

    public JsonStreamingEOFException() {
    }

    public JsonStreamingEOFException(Throwable cause) {
        super(cause);
    }
}
