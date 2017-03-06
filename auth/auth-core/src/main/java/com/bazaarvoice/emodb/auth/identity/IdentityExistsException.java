package com.bazaarvoice.emodb.auth.identity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Exception thrown when a call is made to create or migrate an entity whose authentication ID is already in use.
 */
@JsonIgnoreProperties({"cause", "localizedMessage", "stackTrace"})
public class IdentityExistsException extends RuntimeException {

    public IdentityExistsException() {
        super("Identity exists");
    }
}
