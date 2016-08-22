package com.bazaarvoice.emodb.databus.client;

import com.bazaarvoice.emodb.databus.api.AuthDatabus;
import com.bazaarvoice.emodb.databus.api.Databus;
import com.bazaarvoice.ostrich.dropwizard.healthcheck.ContainsHealthyEndPointCheck;
import com.bazaarvoice.ostrich.pool.ServicePoolProxies;

/**
 * Dropwizard health check.
 */
public class DatabusHealthCheck {
    public static ContainsHealthyEndPointCheck create(Databus databus) {
        return ContainsHealthyEndPointCheck.forPool(ServicePoolProxies.getPool(toServicePoolProxy(databus)));
    }

    public static ContainsHealthyEndPointCheck create(AuthDatabus authDatabus) {
        return ContainsHealthyEndPointCheck.forPool(ServicePoolProxies.getPool(authDatabus));
    }

    private static Object toServicePoolProxy(Databus databus) {
        if (databus instanceof DatabusAuthenticatorProxy) {
            return ((DatabusAuthenticatorProxy) databus).getProxiedInstance();
        }
        return databus;
    }
}
