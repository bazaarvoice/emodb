package com.bazaarvoice.emodb.web.auth;

import com.bazaarvoice.emodb.web.EmoConfiguration;
import com.bazaarvoice.emodb.web.auth.service.SecretsManager;
import com.bazaarvoice.emodb.web.auth.service.serviceimpl.SecretsManagerImpl;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import javax.validation.constraints.NotNull;
import java.util.Set;

/**
 * Configuration for the API keys of internal EmoDB users.
 */
public class AuthorizationConfiguration {

    private final static String DEFAULT_IDENTITY_TABLE = "__auth:keys";
    private final static String DEFAULT_ID_INDEX_TABLE = "__auth:internal_ids";
    private final static String DEFAULT_PERMISSION_TABLE = "__auth:permissions";
    private final static String DEFAULT_ROLE_TABLE = "__auth:roles";
    private final static String DEFAULT_ROLE_GROUP_TABLE = "__auth:role_groups";


    // Table for storing API keys
    @NotNull
    private String _identityTable = DEFAULT_IDENTITY_TABLE;
    // Table for storing index of IDs to hashed identity keys
    @NotNull
    private String _idIndexTable = DEFAULT_ID_INDEX_TABLE;
    // Table for storing permissions
    @NotNull
    private String _permissionsTable = DEFAULT_PERMISSION_TABLE;
    // Table for storing roles
    @NotNull
    private String _roleTable = DEFAULT_ROLE_TABLE;
    // Table for storing mappings of role groups to roles
    @NotNull
    private String _roleGroupTable = DEFAULT_ROLE_GROUP_TABLE;
    // EmoDB administrator
//    @NotNull
    private String _adminApiKey;
    // Replication key used for replicating across data centers
//    @NotNull
    private String _replicationApiKey;

    // Set of roles assigned for anonymous user.  If the set is empty anonymous access is disabled.
    private Set<String> _anonymousRoles = ImmutableSet.of();

    // Compaction control key
    @NotNull
    private String _compControlApiKey;

    public String getIdentityTable() {
        return _identityTable;
    }

    public AuthorizationConfiguration setIdentityTable(String identityTable) {
        _identityTable = identityTable;
        return this;
    }

    public String getIdIndexTable() {
        return _idIndexTable;
    }

    public AuthorizationConfiguration setIdIndexTable(String idIndexTable) {
        _idIndexTable = idIndexTable;
        return this;
    }

    public String getPermissionsTable() {
        return _permissionsTable;
    }

    public AuthorizationConfiguration setPermissionsTable(String permissionsTable) {
        _permissionsTable = permissionsTable;
        return this;
    }

    public String getRoleTable() {
        return _roleTable;
    }

    public AuthorizationConfiguration setRoleTable(String roleTable) {
        _roleTable = roleTable;
        return this;
    }

    public String getRoleGroupTable() {
        return _roleGroupTable;
    }

    public AuthorizationConfiguration setRoleGroupTable(String roleGroupTable) {
        _roleGroupTable = roleGroupTable;
        return this;
    }

    public String getAdminApiKey() {
        return _adminApiKey;
    }

    public AuthorizationConfiguration setAdminApiKey(String adminApiKey) {
        _adminApiKey = adminApiKey;
        return this;
    }

    public String getReplicationApiKey() {
        return _replicationApiKey;
    }

    public AuthorizationConfiguration setReplicationApiKey(String replicationApiKey) {
        _replicationApiKey = replicationApiKey;
        return this;
    }

    public Set<String> getAnonymousRoles() {
        return _anonymousRoles;
    }

    public AuthorizationConfiguration setAnonymousRoles(Set<String> anonymousRoles) {
        _anonymousRoles = anonymousRoles;
        return this;
    }

    public String getCompControlApiKey() {
        return _compControlApiKey;
    }

    public AuthorizationConfiguration setCompControlApiKey(String compControlApiKey) {
        _compControlApiKey = compControlApiKey;
        return this;
    }
}
