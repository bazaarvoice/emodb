package com.bazaarvoice.emodb.auth.shiro;

import org.apache.shiro.authc.AuthenticationToken;

/**
 * Special authentication token assigned to anonymous requests.
 */
public class AnonymousToken implements AuthenticationToken {

    final private static AnonymousToken _instance = new AnonymousToken();
    final private static String ANONYMOUS = "anonymous";

    public static AnonymousToken getInstance() {
        return _instance;
    }

    /**
     * Efficient check for whether a token is anonymous by performing instance comparison with the singleton.
     */
    public static boolean isAnonymous(AuthenticationToken token) {
        return token == _instance;
    }

    /**
     * Efficient check for whether a principal is anonymous by performing instance comparison with the singleton.
     */
    public static boolean isAnonymousPrincipal(Object principal) {
        return principal == ANONYMOUS;
    }

    private AnonymousToken() {
        // empty
    }

    @Override
    public Object getPrincipal() {
        return ANONYMOUS;
    }

    @Override
    public Object getCredentials() {
        return null;
    }
}
