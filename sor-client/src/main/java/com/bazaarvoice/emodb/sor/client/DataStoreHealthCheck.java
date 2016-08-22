package com.bazaarvoice.emodb.sor.client;

import com.bazaarvoice.emodb.sor.api.AuthDataStore;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.ostrich.dropwizard.healthcheck.ContainsHealthyEndPointCheck;
import com.bazaarvoice.ostrich.pool.ServicePoolProxies;

/**
 * Dropwizard health check.
 */
public class DataStoreHealthCheck {
    public static ContainsHealthyEndPointCheck create(DataStore dataStore) {
        return ContainsHealthyEndPointCheck.forPool(ServicePoolProxies.getPool(toServicePoolProxy(dataStore)));
    }

    public static ContainsHealthyEndPointCheck create(AuthDataStore authDataStore) {
        return ContainsHealthyEndPointCheck.forPool(ServicePoolProxies.getPool(authDataStore));
    }

    private static Object toServicePoolProxy(DataStore dataStore) {
        if (dataStore instanceof DataStoreAuthenticatorProxy) {
            return ((DataStoreAuthenticatorProxy) dataStore).getProxiedInstance();
        }
        return dataStore;
    }
}
