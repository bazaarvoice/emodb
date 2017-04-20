package com.bazaarvoice.emodb.auth;

import org.apache.shiro.authz.Permission;

/**
 * Interface for performing authorization internally within the system.  Unlike SecurityManager this interface is
 * intended to be used primarily in contexts where the user is not authenticated.  The interface is intentionally
 * limited to discourage bypassing the SecurityManager when dealing with authenticated users.
 *
 * Internal systems are encouraged to identify relationships such as resource ownership with IDs instead of
 * secret credentials like API keys for the following reasons:
 *
 * <ul>
 *     <li>If the API key for a user is changed the ID remains constant.</li>
 *     <li>They can safely log and store the ID of a user without risk of leaking plaintext credentials.</li>
 * </ul>
 */
public interface InternalAuthorizer {

    boolean hasPermissionById(String id, String permission);

    boolean hasPermissionById(String id, Permission permission);

    boolean hasPermissionsById(String id, String... permissions);

    boolean hasPermissionsById(String id, Permission... permissions);
}
