package com.bazaarvoice.emodb.client;

/**
 * Generic exception class for unhandled client exceptions thrown while making resource requests using an
 * {@link EmoClient}.
 */
public class EmoClientException extends RuntimeException {

    private final EmoResponse _response;

    public EmoClientException(EmoResponse response) {
        super();
        _response = response;
    }

    public EmoClientException(String message, Throwable cause, EmoResponse response) {
        super(message, cause);
        _response = response;
    }

    public EmoResponse getResponse() {
        return _response;
    }

}
