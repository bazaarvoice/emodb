package com.bazaarvoice.emodb.auth.role;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Interface for managing roles and the permissions associated with those roles.
 */
public interface RoleManager {

    /**
     * Gets a role by ID.
     * @return The role, or null of no role with the ID exists
     */
    Role getRole(RoleIdentifier id);

    /**
     * Gets all roles associated with a group.
     * @return The roles for the group, or an empty list if no roles exist for the group
     */
    List<Role> getRolesByGroup(@Nullable String group);

    /**
     * Gets all roles defined.
     * @return A role iterator.  If no roles exist the iterator will be empty
     */
    Iterator<Role> getAll();

    /**
     * Gets all permissions associated with a role.
     * @return The permissions.  If the role does not exist returns an empty set
     */
    Set<String> getPermissionsForRole(RoleIdentifier id);

    /**
     * Creates a role.  The method optionally accepts a list of initial permissions to associate with the role.
     * @return The role
     * @throws RoleExistsException Another role with the same ID exists
     */
    Role createRole(RoleIdentifier id, @Nullable String description, @Nullable Set<String> permissions);

    /**
     * Updates a role.  This method can be used to update the role's description and/or permissions.
     * @throws RoleNotFoundException No role with the ID exists
     */
    void updateRole(RoleIdentifier id, RoleUpdateRequest request);

    /**
     * Deletes a role.  If the role does not exist no action is performed.
     */
    void deleteRole(RoleIdentifier id);
}
