package com.bazaarvoice.emodb.uac.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Exception thrown when creating an API key which already exists.  Since API keys are most commonly randomly generated
 * by Emo this exception is rarely thrown.
 */
@JsonIgnoreProperties({"cause", "localizedMessage", "stackTrace"})
public class EmoApiKeyExistsException extends RuntimeException {

    public EmoApiKeyExistsException() {
        this("API Key exists");
    }

    @JsonCreator
    private EmoApiKeyExistsException(@JsonProperty ("message") String message) {
        super(message);
    }
}
