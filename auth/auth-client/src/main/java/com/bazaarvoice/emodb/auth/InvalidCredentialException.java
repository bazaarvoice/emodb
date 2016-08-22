package com.bazaarvoice.emodb.auth;

/**
 * Exception thrown when the provided credentials are invalid.  This intentionally is not a subclass of
 * {@link java.security.InvalidKeyException} because the primary use case -- API keys -- are rarely invalid,
 * so we streamline exception handling for clients by inheriting from RuntimeException.
 */
public class InvalidCredentialException extends RuntimeException {
    public InvalidCredentialException() {
    }

    public InvalidCredentialException(String message) {
        super(message);
    }
}
