package com.bazaarvoice.emodb.auth.apikey;

/**
 * This class contains constants used by a client to pass API keys to the server.
 */
public class ApiKeyRequest {
    // Name of header to use if passing the key via headers
    public final static String AUTHENTICATION_HEADER = "X-BV-API-Key";
    // Name of query parameters to use if passing the key via query parameters
    public final static String AUTHENTICATION_PARAM = "APIKey";
}
