package com.bazaarvoice.emodb.auth;

import org.apache.shiro.mgt.SecurityManager;

/**
 * Extension of the {@link SecurityManager} interface which adds methods for verifying permissions by internal ID
 * for users not currently authenticated.
 */
public interface EmoSecurityManager extends SecurityManager, InternalAuthorizer {
}
