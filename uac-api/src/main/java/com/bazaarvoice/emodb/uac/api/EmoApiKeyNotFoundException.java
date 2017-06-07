package com.bazaarvoice.emodb.uac.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Exception thrown when attempting to modify an API key which does not exist.
 */
@JsonIgnoreProperties({"cause", "localizedMessage", "stackTrace"})
public class EmoApiKeyNotFoundException extends RuntimeException {
    public EmoApiKeyNotFoundException() {
        super("API Key not found");
    }
}
