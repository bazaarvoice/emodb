package com.bazaarvoice.emodb.web.auth;

import javax.validation.constraints.NotNull;

/**
 * Configuration for the API keys of internal EmoDB users.
 */
public class AuthorizationConfiguration {

    private final static String DEFAULT_IDENTITY_TABLE = "__auth:keys";
    private final static String DEFAULT_INTERNAL_ID_INDEX_TABLE = "__auth:internal_ids";
    private final static String DEFAULT_PERMISSION_TABLE = "__auth:permissions";

    // Table for storing API keys
    @NotNull
    private String _identityTable = DEFAULT_IDENTITY_TABLE;
    // Table for storing index of internal IDs to hashed identity keys
    @NotNull
    private String _internalIdIndexTable = DEFAULT_INTERNAL_ID_INDEX_TABLE;
    // Table for storing permissions
    @NotNull
    private String _permissionsTable = DEFAULT_PERMISSION_TABLE;
    // Placement for preceding tables
    @NotNull
    private String _tablePlacement;
    // EmoDB administrator
    @NotNull
    private String _adminApiKey;
    // Replication key used for replicating across data centers
    @NotNull
    private String _replicationApiKey;
    // If true allow anonymous access, otherwise all restricted resources will require authentication
    private boolean _allowAnonymousAccess;
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

    public String getInternalIdIndexTable() {
        return _internalIdIndexTable;
    }

    public AuthorizationConfiguration setInternalIdIndexTable(String internalIdIndexTable) {
        _internalIdIndexTable = internalIdIndexTable;
        return this;
    }

    public String getPermissionsTable() {
        return _permissionsTable;
    }

    public AuthorizationConfiguration setPermissionsTable(String permissionsTable) {
        _permissionsTable = permissionsTable;
        return this;
    }

    public String getTablePlacement() {
        return _tablePlacement;
    }

    public AuthorizationConfiguration setTablePlacement(String tablePlacement) {
        _tablePlacement = tablePlacement;
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

    public boolean isAllowAnonymousAccess() {
        return _allowAnonymousAccess;
    }

    public AuthorizationConfiguration setAllowAnonymousAccess(boolean allowAnonymousAccess) {
        _allowAnonymousAccess = allowAnonymousAccess;
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
