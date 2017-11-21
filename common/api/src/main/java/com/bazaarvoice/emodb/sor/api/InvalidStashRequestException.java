package com.bazaarvoice.emodb.sor.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Thrown when the caller makes a Stash request which cannot be satisfied.  The message will contain more details
 * for why the request is invalid.
 */
@JsonIgnoreProperties({"cause", "localizedMessage", "stackTrace"})
public class InvalidStashRequestException extends IllegalArgumentException {

    public InvalidStashRequestException(String s) {
        super(s);
    }
}
