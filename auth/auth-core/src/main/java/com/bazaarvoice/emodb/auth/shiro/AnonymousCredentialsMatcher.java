package com.bazaarvoice.emodb.auth.shiro;

import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.credential.CredentialsMatcher;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Credentials matcher that will always match when the authentication token is an {@link AnonymousToken}.  Otherwise
 * it defers to another credentials matcher.
 */
public class AnonymousCredentialsMatcher implements CredentialsMatcher {

    private final CredentialsMatcher _matcher;

    private AnonymousCredentialsMatcher(CredentialsMatcher matcher) {
        _matcher = checkNotNull(matcher, "matcher");
    }

    public static AnonymousCredentialsMatcher anonymousOrMatchUsing(CredentialsMatcher matcher) {
        return new AnonymousCredentialsMatcher(matcher);
    }

    @Override
    public boolean doCredentialsMatch(AuthenticationToken token, AuthenticationInfo info) {
        if (AnonymousToken.isAnonymous(token)) {
            return true;
        }

        return _matcher.doCredentialsMatch(token, info);
    }
}
