package com.bazaarvoice.emodb.auth.permissions;

import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.delta.MapDeltaBuilder;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.InvalidPermissionStringException;
import org.apache.shiro.authz.permission.PermissionResolver;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Permission manager that uses an EmoDB table to store permissions.
 *
 * With a level of irony not seen since Alanis Morissette the application must have permission to perform operations
 * on the table in order to use this manager.
 */
public class TablePermissionManager implements PermissionManager {

    private final PermissionResolver _permissionResolver;
    private final DataStore _dataStore;
    private final String _tableName;
    private final String _placement;
    private volatile boolean _tableValidated;

    public TablePermissionManager(PermissionResolver permissionResolver, DataStore dataStore,
                                  String tableName, String placement) {
        _permissionResolver = checkNotNull(permissionResolver, "permissionResolver");
        _dataStore = checkNotNull(dataStore, "dataStore");
        _tableName = checkNotNull(tableName, "tableName");
        _placement = checkNotNull(placement, "placement");
    }

    @Override
    public Set<Permission> getAllForRole(String role) {
        return getAll(getRoleKey(role));
    }

    private Set<Permission> getAll(String key) {
        validateTable();
        Map<String, Object> map = _dataStore.get(_tableName, key, ReadConsistency.STRONG);
        return extractPermissionsFromRecord(map);
    }

    private Set<Permission> extractPermissionsFromRecord(Map<String, Object> map) {
        Set<Permission> permissions = Sets.newHashSet();

        for (String mapKey : map.keySet()) {
            if (mapKey.startsWith("perm_")) {
                permissions.add(_permissionResolver.resolvePermission(mapKey.substring(5)));
            }
        }

        return permissions;
    }

    @Override
    public void updateForRole(String role, PermissionUpdateRequest request) {
        update(getRoleKey(role), request);
    }

    private void update(String key, PermissionUpdateRequest request) {
        checkNotNull(request, "request");
        validateTable();

        _dataStore.update(
                _tableName,
                key,
                TimeUUIDs.newUUID(),
                createDelta(request),
                new AuditBuilder().setLocalHost().setComment("update permissions").build(),
                WriteConsistency.GLOBAL);
    }

    private Delta createDelta(PermissionUpdateRequest request) {
        MapDeltaBuilder builder = Deltas.mapBuilder();

        for (String permissionString : request.getPermitted()) {
            builder.put("perm_" + validated(permissionString), 1);
        }
        for (String permissionString : request.getRevoked()) {
            builder.remove("perm_" + validated(permissionString));
        }

        return builder.build();
    }

    /**
     * Validates that a permission string can be resolved.
     */
    private String validated(String permissionString)
            throws InvalidPermissionStringException {
        _permissionResolver.resolvePermission(permissionString);
        return permissionString;
    }

    @Override
    public void revokeAllForRole(String role) {
        revokeAll(getRoleKey(role));
    }

    private void revokeAll(String key) {
        validateTable();

        _dataStore.update(
                _tableName,
                key,
                TimeUUIDs.newUUID(),
                Deltas.delete(),
                new AuditBuilder().setLocalHost().setComment("delete permissions").build(),
                WriteConsistency.GLOBAL);

    }

    @Override
    public Iterable<Map.Entry<String, Set<Permission>>> getAll() {
        validateTable();

        // Wrap the table scan in an iterable
        Iterable<Map<String, Object>> recordIterable = new Iterable<Map<String, Object>>() {
            @Override
            public Iterator<Map<String, Object>> iterator() {
                return _dataStore.scan(_tableName, null, Long.MAX_VALUE, ReadConsistency.STRONG);
            }
        };

        // Transform and filter records that are associated with roles
        return FluentIterable.from(recordIterable)
                .transform(new Function<Map<String, Object>, Map.Entry<String, Set<Permission>>>() {
                    @Nullable
                    @Override
                    public Map.Entry<String, Set<Permission>> apply(Map<String, Object> map) {
                        String role = getRoleFromKey(Intrinsic.getId(map));
                        if (role != null) {
                            return Maps.immutableEntry(role, extractPermissionsFromRecord(map));
                        }
                        return null;
                    }
                })
                .filter(Predicates.notNull());
    }

    @Override
    public PermissionResolver getPermissionResolver() {
        return _permissionResolver;
    }

    private void validateTable() {
        if (_tableValidated) {
            return;
        }

        synchronized(this) {
            if (!_dataStore.getTableExists(_tableName)) {
                _dataStore.createTable(
                        _tableName,
                        new TableOptionsBuilder().setPlacement(_placement).build(),
                        ImmutableMap.<String, Object>of(),
                        new AuditBuilder().setLocalHost().setComment("create permissions table").build());
            }

            _tableValidated = true;
        }
    }

    // The reason we have "role:name" and not just name as the key is for backwards compatibility.
    // We used to have "principal:name" as we used to allow permissions attached directly to API keys instead of roles.
    // We have since deprecated directly attaching permissions to API Key. However, we need to keep the old naming convention.
    private String getRoleKey(String role) {
        checkNotNull(role, "role");
        return "role:" + role;
    }

    /**
     * Returns the role from a raw key of the form "role:name" or null if the key does not start with "role:".
     */
    @Nullable
    private String getRoleFromKey(String key) {
        if (key != null && key.startsWith("role:")) {
            return key.substring(5);
        }
        return null;
    }

}
