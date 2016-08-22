package com.bazaarvoice.emodb.web.auth;

import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.google.inject.Inject;
import org.apache.shiro.authz.permission.PermissionResolver;

/**
 * Permission resolver used to generate {@link EmoPermission} instances.
 */
public class EmoPermissionResolver implements PermissionResolver {

    private final DataStore _dataStore;
    private final BlobStore _blobStore;

    @Inject
    public EmoPermissionResolver(DataStore dataStore, BlobStore blobStore) {
        _dataStore = dataStore;
        _blobStore = blobStore;
    }

    @Override
    public EmoPermission resolvePermission(String permissionString) {
        return new EmoPermission(_dataStore, _blobStore, permissionString);
    }
}
