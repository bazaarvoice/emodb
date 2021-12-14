package com.bazaarvoice.emodb.client2;

/**
 * Generic exception class for unhandled client exceptions thrown while making resource requests using an
 * {@link EmoClient2}.
 */
public class EmoClientException2 extends RuntimeException {

    private final EmoResponse2 _response;

    public EmoClientException2(EmoResponse2 response) {
        super();
        _response = response;
    }

    public EmoClientException2(String message, Throwable cause, EmoResponse2 response) {
        super(message, cause);
        _response = response;
    }

    public EmoResponse2 getResponse() {
        return _response;
    }
}
