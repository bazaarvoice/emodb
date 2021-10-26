package com.bazaarvoice.emodb.blob.client;

import com.bazaarvoice.emodb.blob.api.AuthBlobStore;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.ostrich.dropwizard.healthcheck.ContainsHealthyEndPointCheck;
import com.bazaarvoice.ostrich.pool.ServicePoolProxies;

/**
 * Dropwizard health check.
 */
public class BlobStoreHealthCheck  {
    public static ContainsHealthyEndPointCheck create(BlobStore blobStore) {
        return ContainsHealthyEndPointCheck.forPool(ServicePoolProxies.getPool(toServicePoolProxy(blobStore)));
    }

    public static ContainsHealthyEndPointCheck create(AuthBlobStore authBlobStore) {
        return ContainsHealthyEndPointCheck.forPool(ServicePoolProxies.getPool(authBlobStore));
    }

    private static Object toServicePoolProxy(BlobStore blobStore) {
        if (blobStore instanceof BlobStoreAuthenticatorProxy) {
            return ((BlobStoreAuthenticatorProxy) blobStore).getProxiedInstance();
        }
        return blobStore;
    }
}
