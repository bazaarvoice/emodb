package com.bazaarvoice.emodb.client2;

import java.net.URI;

/**
 * Root interface for making API requests to an EmoDB service.  This and its dependent classes are modeled using a
 * fluent API similar to that used by Jersey Client, although without a dependency on Jersey or any other
 * HTTP client implementation.
 */
public interface EmoClient {

    /**
     * Starts a request to the resource at the provided URI.
     */
    EmoResource resource(URI uri);
}
