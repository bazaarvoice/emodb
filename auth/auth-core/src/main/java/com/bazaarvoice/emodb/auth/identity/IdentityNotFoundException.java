package com.bazaarvoice.emodb.auth.identity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Exception thrown when a call is made to modify an identity which does not exist.
 */
@JsonIgnoreProperties({"cause", "localizedMessage", "stackTrace"})
public class IdentityNotFoundException extends RuntimeException {

    public IdentityNotFoundException() {
        super("Identity not found");
    }
}
