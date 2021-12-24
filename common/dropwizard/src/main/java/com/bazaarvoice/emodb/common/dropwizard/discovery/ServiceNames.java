package com.bazaarvoice.emodb.common.dropwizard.discovery;

import com.google.common.base.Strings;

import static java.util.Objects.requireNonNull;

/**
 * This class encapsulates how service names are built -- generally clients and services that wish to construct
 * service names (e.g., for service discovery) should use this class, so that they're sharing the same naming
 * conventions.
 */
public class ServiceNames {

    /** Prevent instantiation. */
    private ServiceNames() {
    }

    public static String forNamespaceAndBaseServiceName(String namespace, String baseServiceName) {
        requireNonNull(baseServiceName);
        return Strings.isNullOrEmpty(namespace) ? baseServiceName : namespace + "-" + baseServiceName;
    }

    public static boolean isValidServiceName(String serviceName, String baseServiceName) {
        return serviceName.equals(baseServiceName) || serviceName.endsWith("-" + baseServiceName);
    }
}
