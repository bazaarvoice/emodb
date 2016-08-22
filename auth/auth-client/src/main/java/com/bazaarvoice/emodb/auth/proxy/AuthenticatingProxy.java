package com.bazaarvoice.emodb.auth.proxy;

/**
 * Interface for creating a view of an interface that uses credentials with the credentials removed.  Credentials
 * are set once and then proxied to the original instance.
 * @param <U> Interface which does not expose credentials in individual method calls
 * @param <C> Type for credentials
 */
public interface AuthenticatingProxy<U, C> {

    U usingCredentials(C credentials);
}
