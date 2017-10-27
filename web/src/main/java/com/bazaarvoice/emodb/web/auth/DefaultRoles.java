package com.bazaarvoice.emodb.web.auth;

import com.bazaarvoice.emodb.auth.role.RoleIdentifier;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.web.auth.resource.ConditionResource;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static com.bazaarvoice.emodb.web.auth.Permissions.NON_SYSTEM_NON_PII_TABLE;
import static com.bazaarvoice.emodb.web.auth.Permissions.NON_SYSTEM_RESOURCE;
import static com.bazaarvoice.emodb.web.auth.Permissions.NON_SYSTEM_TABLE;

/**
 * This class maintains the default roles and permissions associated with those roles.  Additional custom roles can be
 * and managed besides these but the roles in this class are guaranteed to be always available and immutable.
 *
 * For each resource the roles available follow this pattern:
 * <ol>
 * <li>Restrictive permissions, such as "read only".</li>
 * <li>Standard permissions.  This is what would typically be granted to a trusted user.</li>
 * <li>Admin permissions.  This is what would typically be granted to an administrator.</li>
 * </ol>
 *
 */
public enum DefaultRoles {

    // Can read data in existing non-system system of record tables
    sor_read (
            Permissions.readSorTable(NON_SYSTEM_TABLE)),

    // Can update data in existing non-system system of record tables
    sor_update (
            Permissions.updateSorTable(NON_SYSTEM_TABLE)),

    // sor_read + sor_update + Can create and modify non-system system of record tables (notably cannot drop tables)
    sor_standard (
            ImmutableSet.of(sor_read, sor_update),
            Permissions.createSorTable(NON_SYSTEM_TABLE),
            Permissions.setSorTableAttributes(NON_SYSTEM_TABLE)),

    // sor_standard with excluding pii tables/placements.
    // Pii tables contain personally identifiable information, so in a way they are special compared to the rest of the sor tables.
    // this role can be used where we would like to give standard permissions to all sor tables except the pii & system tables.
    sor_standard_without_pii (
            Permissions.readSorTable(NON_SYSTEM_NON_PII_TABLE),
            Permissions.updateSorTable(NON_SYSTEM_NON_PII_TABLE),
            Permissions.createSorTable(NON_SYSTEM_NON_PII_TABLE),
            Permissions.setSorTableAttributes(NON_SYSTEM_NON_PII_TABLE)),

    // Can perform all actions on all system of record tables
    sor_admin (
            Permissions.unlimitedSorTable(new ConditionResource(Conditions.alwaysTrue()))),

    // Can update data in existing non-system blob tables
    blob_read (
            Permissions.readBlobTable(NON_SYSTEM_TABLE)),

    // Can update data in existing non-system blob tables
    blob_update (
            Permissions.updateBlobTable(NON_SYSTEM_TABLE)),

    // blob_read + blob_update + Can create and modify non-system blob tables (notably cannot drop tables)
    blob_standard (
            ImmutableSet.of(blob_read, blob_update),
            Permissions.createBlobTable(NON_SYSTEM_TABLE),
            Permissions.setBlobTableAttributes(NON_SYSTEM_TABLE)),

    // Can perform all actions on all blob tables
    blob_admin (
            Permissions.unlimitedBlobTable(NON_SYSTEM_TABLE)),

    // sor_update + blob_update
    record_update (
            ImmutableSet.of(sor_update, blob_update)),

    // sor_standard + blob_standard
    record_standard (
            ImmutableSet.of(sor_standard, blob_standard)),

    // Can perform all actions on all records, sor_admin + blob_admin
    record_admin (
            ImmutableSet.of(sor_admin, blob_admin)),

    // Facades are unique in that there is no restrictive or standard access, as the only application that has
    // access to facades, EmoDB Shovel, must have full CRUD access to facades and facade data.
    facade_admin (
            Permissions.unlimitedFacade(Permissions.ALL)),

    // Can post to non-system queues
    queue_post (
            Permissions.getQueueStatus(NON_SYSTEM_RESOURCE),
            Permissions.postQueue(NON_SYSTEM_RESOURCE)),

    // Can poll from non-system queues
    queue_poll (
            Permissions.getQueueStatus(NON_SYSTEM_RESOURCE),
            Permissions.pollQueue(NON_SYSTEM_RESOURCE)),

    // queue_post + queue_poll
    queue_standard (
            ImmutableSet.of(queue_post, queue_poll)),

    // Can perform all actions on all queues
    queue_admin (
            Permissions.unlimitedQueue(Permissions.ALL)),

    // Can poll non-system databus subscriptions
    databus_poll (
            Permissions.getDatabusStatus(NON_SYSTEM_RESOURCE),
            Permissions.pollDatabus(NON_SYSTEM_RESOURCE)),

    // databus_poll + Can subscribe and unsubscribe non-system databus subscriptions
    databus_standard (
            ImmutableSet.of(databus_poll),
            Permissions.subscribeDatabus(NON_SYSTEM_RESOURCE),
            Permissions.unsubscribeDatabus(NON_SYSTEM_RESOURCE)),

    // Can perform all actions on all databus subscriptions
    databus_admin (
            Permissions.unlimitedDatabus(Permissions.ALL)),

    // Can perform all standard actions
    standard (
            ImmutableSet.of(sor_standard, blob_standard, queue_standard, databus_standard)),

    // Can perform all actions
    admin (
            Permissions.unlimited()),

    // Reserved role for replication databus traffic between data centers
    replication (
            ImmutableSet.of(sor_read),
            Permissions.replicateDatabus());
    
    private Set<String> _permissions;

    private DefaultRoles(String... permissions) {
        this(ImmutableSet.<DefaultRoles>of(), permissions);
    }

    private DefaultRoles(Set<DefaultRoles> parents, String... permissions) {
        ImmutableSet.Builder<String> allPermissions = ImmutableSet.builder();

        for (DefaultRoles parent : parents) {
            allPermissions.addAll(parent._permissions);
        }
        allPermissions.add(permissions);

        _permissions = allPermissions.build();
    }

    public Set<String> getPermissions() {
        return _permissions;
    }

    public static boolean isDefaultRole(RoleIdentifier id) {
        // Default roles exist with no group, so if the ID has a group it's not a default role.
        if (id == null || id.getGroup() != null) {
            return false;
        }
        String checkId = id.getId();
        for (DefaultRoles defaultRole : DefaultRoles.values()) {
            if (defaultRole.name().equals(checkId)) {
                return true;
            }
        }
        return false;
    }
}
