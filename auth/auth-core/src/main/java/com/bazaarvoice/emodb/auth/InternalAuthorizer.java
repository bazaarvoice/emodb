package com.bazaarvoice.emodb.auth;

import org.apache.shiro.authz.Permission;

import javax.annotation.Nullable;

/**
 * Interface for performing authorization internally within the system.  Unlike SecurityManager this interface is
 * intended to be used primarily in contexts where the user is not authenticated.  The interface is intentionally
 * limited to discourage bypassing the SecurityManager when dealing with authenticated users.
 *
 * Internal systems are encouraged to identify relationships such as resource ownership with internal IDs instead of
 * public credentials like API keys for the following reasons:
 *
 * <ul>
 *     <li>If the API key for a user is changed the internal ID remains constant.</li>
 *     <li>They can safely log and store the internal ID of a user without risk of leaking plaintext credentials.</li>
 * </ul>
 */
public interface InternalAuthorizer {

    boolean hasPermissionByInternalId(String internalId, String permission);

    boolean hasPermissionByInternalId(String internalId, Permission permission);

    boolean hasPermissionsByInternalId(String internalId, String... permissions);

    boolean hasPermissionsByInternalId(String internalId, Permission... permissions);
}
