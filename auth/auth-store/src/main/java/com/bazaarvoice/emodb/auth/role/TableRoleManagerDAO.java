package com.bazaarvoice.emodb.auth.role;

import com.bazaarvoice.emodb.auth.permissions.PermissionIDs;
import com.bazaarvoice.emodb.auth.permissions.PermissionManager;
import com.bazaarvoice.emodb.auth.permissions.PermissionUpdateRequest;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.Coordinate;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.Update;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.delta.MapDeltaBuilder;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.bazaarvoice.emodb.common.api.Names.isLegalRoleGroupName;
import static com.bazaarvoice.emodb.common.api.Names.isLegalRoleName;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * RoleManager implementation which persists roles using tables in a {@link DataStore}.
 */
public class TableRoleManagerDAO implements RoleManager {

    private final Logger _log = LoggerFactory.getLogger(TableRoleManagerDAO.class);

    // Even roles with no group belong to a group, the catch-all "no group".  The name for this group is reserved
    // and cannot be explicitly used by the API.
    private static final String NO_GROUP_NAME = "_";

    private final static String NAME_ATTR = "name";
    private final static String DESCRIPTION_ATTR = "description";
    private final static String IDS_ATTR = "ids";

    private final DataStore _dataStore;
    // DataStore table which maps role IDs to roles
    private final String _roleTableName;
    // DataStore table which maps groups to role names.  Basically serves as an index to read all roles by group.
    private final String _groupTableName;
    private final String _placement;
    private final PermissionManager _permissionManager;
    private volatile boolean _tablesValidated;

    public TableRoleManagerDAO(DataStore dataStore, String roleTableName, String groupTableName, String placement,
                               PermissionManager permissionManager) {
        _dataStore = checkNotNull(dataStore, "dataStore");
        _roleTableName = checkNotNull(roleTableName, "roleTableName");
        _groupTableName = checkNotNull(groupTableName, "groupTableName");
        checkArgument(!roleTableName.equals(groupTableName), "Role and group tables must be unique");
        _placement = checkNotNull(placement, "placement");
        _permissionManager = checkNotNull(permissionManager, "permissionManager");
    }

    private String checkGroup(@Nullable String group) {
        // Role groups follow the same naming conventions as role names
        checkArgument(group == null || isLegalRoleGroupName(group), "Group cannot be named %s", group);
        // Since legal role group names cannot equal "_" if the previous check passed then there cannot be a conflict
        return group == null ? NO_GROUP_NAME : group;
    }

    @Override
    public Role getRole(RoleIdentifier id) {
        checkNotNull(id, "id");
        checkGroup(id.getGroup());
        validateTables();

        Map<String, Object> record = _dataStore.get(_roleTableName, id.toString(), ReadConsistency.STRONG);
        return convertRecordToRole(record);
    }

    private Role convertRecordToRole(Map<String, Object> record) {
        if (Intrinsic.isDeleted(record)) {
            return null;
        }
        RoleIdentifier id = RoleIdentifier.fromString(Intrinsic.getId(record));
        return new Role(id.getGroup(), id.getId(), (String) record.get(NAME_ATTR), (String) record.get(DESCRIPTION_ATTR));
    }

    @Override
    public List<Role> getRolesByGroup(@Nullable String group) {
        String groupKey = checkGroup(group);
        List<Role> roles = null;
        validateTables();

        Map<String, Object> record = _dataStore.get(_groupTableName, groupKey);
        if (!Intrinsic.isDeleted(record)) {
            //noinspection unchecked
            List<String> names = (List<String>) record.get(IDS_ATTR);
            if (names != null && !names.isEmpty()) {
                List<Coordinate> coordinates = names.stream()
                        .map(name -> Coordinate.of(_roleTableName, new RoleIdentifier(group, name).toString()))
                        .collect(Collectors.toList());

                Iterator<Map<String, Object>> records = _dataStore.multiGet(coordinates, ReadConsistency.STRONG);

                roles = StreamSupport.stream(Spliterators.spliteratorUnknownSize(records, 0), false)
                        .map(this::convertRecordToRole)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
            }
        }

        return roles != null ? roles : ImmutableList.of();
    }

    @Override
    public Iterator<Role> getAll() {
        validateTables();

        Iterator<Map<String, Object>> records = _dataStore.scan(_roleTableName, null, Integer.MAX_VALUE, ReadConsistency.STRONG);

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(records, 0), false)
                .map(this::convertRecordToRole)
                .iterator();
    }

    @Override
    public Set<String> getPermissionsForRole(RoleIdentifier id) {
        checkNotNull(id, "id");
        return ImmutableSortedSet.copyOf(
                _permissionManager.getPermissions(PermissionIDs.forRole(id))
                        .stream()
                        .map(Objects::toString)
                        .iterator());
    }

    @Override
    public Role createRole(RoleIdentifier id, RoleModification modification) {
        checkNotNull(id, "id");
        checkArgument(isLegalRoleName(id.getId()), "Role cannot have ID %s", id.getId());
        String groupKey = checkGroup(id.getGroup());

        // First ensure that there is no conflicting role with the same name and group.
        Role existingRole = getRole(id);
        if (existingRole != null) {
            throw new RoleExistsException(id.getGroup(), id.getId());
        }

        // Without transactions it is not possible to ensure the role, group, and permissions are written without
        // failing independently.  Write the group entry first followed by the role.  This way if there is a failure
        // there won't be a dangling role with no group and the logic for role groups can tolerate missing references,
        // although actual cleanup of the missing reference would need to be performed manually.  Persist
        // permissions lastly since a failure at that point is most easily recoverable though other API calls.

        UUID changeId = TimeUUIDs.newUUID();

        Delta groupTableDelta = Deltas.mapBuilder()
                .update(IDS_ATTR, Deltas.setBuilder()
                        .add(id.getId())
                        .build())
                .build();

        Delta roleTableDelta = Deltas.mapBuilder()
                .put(NAME_ATTR, modification.getName())
                .put(DESCRIPTION_ATTR, modification.getDescription())
                .build();

        Audit audit = new AuditBuilder().setLocalHost().setComment("Create role " + id).build();

        _dataStore.updateAll(ImmutableList.of(
                new Update(_groupTableName, groupKey, changeId, groupTableDelta, audit, WriteConsistency.GLOBAL),
                new Update(_roleTableName, id.toString(), changeId, roleTableDelta, audit, WriteConsistency.GLOBAL)));

        if (modification.getPermissionUpdate() != null) {
            try {
                _permissionManager.updatePermissions(PermissionIDs.forRole(id),
                        new PermissionUpdateRequest().permit(modification.getPermissionUpdate().getPermitted()));
            } catch (Exception e) {
                // Attempt to rollback the role's creation
                try {
                    changeId = TimeUUIDs.newUUID();
                    audit = new AuditBuilder().setLocalHost().setComment("Rollback role " + id).build();
                    groupTableDelta = Deltas.mapBuilder()
                            .update(IDS_ATTR, Deltas.setBuilder()
                                    .remove(id.getId())
                                    .deleteIfEmpty()
                                    .build())
                            .deleteIfEmpty()
                            .build();
                    roleTableDelta = Deltas.delete();

                    _dataStore.updateAll(ImmutableList.of(
                            new Update(_groupTableName, groupKey, changeId, groupTableDelta, audit, WriteConsistency.GLOBAL),
                            new Update(_roleTableName, id.toString(), changeId, roleTableDelta, audit, WriteConsistency.GLOBAL)));
                } catch (Exception rollbackException) {
                    // Log this error, but favor throwing the original exception
                    _log.warn("Failed to delete role after failed permission create: {}", id, rollbackException);
                }

                throw Throwables.propagate(e);
            }
        }

        return new Role(id.getGroup(), id.getId(), modification.getName(), modification.getDescription());
    }

    @Override
    public void updateRole(RoleIdentifier id, RoleModification modification) {
        // First, verify the role exists
        Role role = getRole(id);
        if (role == null) {
            throw new RoleNotFoundException(id.getGroup(), id.getId());
        }

        // As with creating a role, updating role metadata and permissions cannot be performed atomically.  Update
        // role metadata first since a failure at that point poses the least security risk.

        MapDeltaBuilder rollbackDelta = null;
        if (modification.isNamePresent() || modification.isDescriptionPresent()) {
            MapDeltaBuilder delta = Deltas.mapBuilder();
            rollbackDelta = Deltas.mapBuilder();

            if (modification.isNamePresent()) {
                delta.put(NAME_ATTR, modification.getName());
                rollbackDelta.put(NAME_ATTR, role.getName());
            }
            if (modification.isDescriptionPresent()) {
                delta.put(DESCRIPTION_ATTR, modification.getDescription());
                rollbackDelta.put(DESCRIPTION_ATTR, role.getDescription());
            }

            _dataStore.update(_roleTableName, id.toString(), TimeUUIDs.newUUID(),
                    delta.build(),
                    new AuditBuilder().setLocalHost().setComment("Update role " + id).build(),
                    WriteConsistency.GLOBAL);
        }

        if (modification.getPermissionUpdate() != null) {
            try {
                _permissionManager.updatePermissions(PermissionIDs.forRole(id), modification.getPermissionUpdate());
            } catch (Exception e) {
                if (rollbackDelta != null) {
                    // Attempt to rollback the role's state
                    try {
                        _dataStore.update(_roleTableName, id.toString(), TimeUUIDs.newUUID(), rollbackDelta.build(),
                                new AuditBuilder().setLocalHost().setComment("Rollback role " + id).build(),
                                WriteConsistency.GLOBAL);
                    } catch (Exception rollbackException) {
                        // Log this error, but favor throwing the original exception
                        _log.warn("Failed to rollback role after failed permission update: {}", id, rollbackException);
                    }
                }

                throw Throwables.propagate(e);
            }
        }
    }

    @Override
    public void deleteRole(RoleIdentifier id) {
        // First, verify the role exists
        Role role = getRole(id);
        if (role == null) {
            // Role doesn't exist.  Don't raise an exception, just return now since there is no work to be done.
            return;
        }

        // Start by revoking all permissions.  Even if the subsequent steps fail any users with this role won't have
        // any permissions from it once this step completes.
        _permissionManager.revokePermissions(PermissionIDs.forRole(id));

        // As the inverse for creating roles the role is deleted before the group.
        UUID changeId = TimeUUIDs.newUUID();
        String groupKey = checkGroup(role.getGroup());

        Delta groupTableDelta = Deltas.mapBuilder()
                .update(IDS_ATTR, Deltas.setBuilder()
                        .remove(role.getId())
                        .deleteIfEmpty()
                        .build())
                .deleteIfEmpty()
                .build();

        Delta roleTableDelta = Deltas.delete();

        Audit audit = new AuditBuilder().setLocalHost().setComment("Delete role " + id).build();

        _dataStore.updateAll(ImmutableList.of(
                new Update(_groupTableName, groupKey, changeId, groupTableDelta, audit, WriteConsistency.GLOBAL),
                new Update(_roleTableName, id.toString(), changeId, roleTableDelta, audit, WriteConsistency.GLOBAL)));
    }

    /**
     * Lazy initial verification to ensure the role and group tables exist.
     */
    private void validateTables() {
        if (_tablesValidated) {
            return;
        }

        synchronized(this) {
            if (!_dataStore.getTableExists(_roleTableName)) {
                _dataStore.createTable(
                        _roleTableName,
                        new TableOptionsBuilder().setPlacement(_placement).build(),
                        ImmutableMap.<String, Object>of(),
                        new AuditBuilder().setLocalHost().setComment("create role table").build());
            }

            if (!_dataStore.getTableExists(_groupTableName)) {
                _dataStore.createTable(
                        _groupTableName,
                        new TableOptionsBuilder().setPlacement(_placement).build(),
                        ImmutableMap.<String, Object>of(),
                        new AuditBuilder().setLocalHost().setComment("create role group table").build());
            }

            _tablesValidated = true;
        }
    }
}
